use std::io::{self, BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use crate::errors::*;

const MAX_ARRAY_BATCH: usize = 20;
const BUFFER_FLOOR: usize = 16 * 1024;
const BUDGET_CHUNK: usize = 64 * 1024;
const PER_LINE_MSG: &str = "response line exceeds max_response_bytes";
const GLOBAL_MSG: &str = "response budget exhausted, retry later";
const SKIPPED_MSG: &str = "batch response budget exhausted; not executed";

const _: () = {
    assert!(SKIPPED_MSG.len() >= PER_LINE_MSG.len());
    assert!(SKIPPED_MSG.len() >= GLOBAL_MSG.len());
};

// JSON-RPC errors plus Electrum application error codes.
#[repr(i16)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum JsonRpcV2Error {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    BadRequest = 1,
    DaemonError = 2,
}

impl JsonRpcV2Error {
    pub(super) fn into_i16(self) -> i16 {
        self as i16
    }
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorBody<'a>,
    id: &'a Value,
    jsonrpc: &'static str,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: i16,
    message: &'a str,
}

impl<'a> ErrorEnvelope<'a> {
    fn new(message: &'a str, id: Option<&'a Value>, code: JsonRpcV2Error) -> Self {
        Self {
            error: ErrorBody {
                code: code.into_i16(),
                message,
            },
            id: id.unwrap_or(&Value::Null),
            jsonrpc: "2.0",
        }
    }

    fn encoded_len(&self) -> usize {
        let mut counter = ByteCounter(0);
        serde_json::to_writer(&mut counter, self).expect("counting JSON values cannot fail");
        counter.0
    }
}

pub(super) fn json_rpc_error(
    message: impl std::fmt::Display,
    id: Option<&Value>,
    code: JsonRpcV2Error,
) -> Value {
    serde_json::to_value(ErrorEnvelope::new(&message.to_string(), id, code))
        .expect("an error envelope contains only JSON values")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResponseLimit {
    PerLine,
    Global,
}

impl ResponseLimit {
    fn message(self) -> &'static str {
        match self {
            Self::PerLine => PER_LINE_MSG,
            Self::Global => GLOBAL_MSG,
        }
    }

    fn code(self) -> JsonRpcV2Error {
        match self {
            Self::PerLine => JsonRpcV2Error::BadRequest,
            Self::Global => JsonRpcV2Error::DaemonError,
        }
    }

    fn error(self) -> Error {
        match self {
            Self::PerLine => ErrorKind::PerLineResponseOverflow.into(),
            Self::Global => ErrorKind::ResponseBudgetExhausted.into(),
        }
    }
}

pub(super) struct ResponseBudget {
    limit: usize,
    reserved: AtomicUsize,
}

impl ResponseBudget {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            reserved: AtomicUsize::new(0),
        })
    }
}

struct Reservation {
    budget: Arc<ResponseBudget>,
    granted: usize,
}

impl Reservation {
    fn ensure(&mut self, total: usize) -> std::result::Result<(), ResponseLimit> {
        if self.budget.limit == usize::MAX || total <= self.granted {
            return Ok(());
        }
        let required = total - self.granted;
        let preferred = required
            .checked_next_multiple_of(BUDGET_CHUNK)
            .unwrap_or(required);
        let mut reserved = self.budget.reserved.load(Ordering::Relaxed);
        loop {
            let available = self.budget.limit - reserved;
            if required > available {
                return Err(ResponseLimit::Global);
            }
            let grant = preferred.min(available);
            match self.budget.reserved.compare_exchange_weak(
                reserved,
                reserved + grant,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.granted += grant;
                    return Ok(());
                }
                Err(latest) => reserved = latest,
            }
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget
            .reserved
            .fetch_sub(self.granted, Ordering::AcqRel);
    }
}

// Fields drop in declaration order: free the buffer before releasing its grant.
struct ResponseBuffer {
    bytes: Vec<u8>,
    reservation: Reservation,
    limit: usize,
}

impl ResponseBuffer {
    fn new(bytes: Vec<u8>, budget: Arc<ResponseBudget>, limit: usize) -> Self {
        Self {
            bytes,
            reservation: Reservation { budget, granted: 0 },
            limit,
        }
    }

    fn reserve(&mut self, line_bytes: usize) -> std::result::Result<(), ResponseLimit> {
        self.reservation
            .ensure(line_bytes.saturating_sub(BUFFER_FLOOR))
    }

    fn encode(
        &mut self,
        value: &impl Serialize,
        tail: usize,
    ) -> std::result::Result<(), EncodeError> {
        let limit = self
            .limit
            .checked_sub(tail)
            .ok_or(EncodeError::Limited(ResponseLimit::PerLine))?;
        let start = self.bytes.len();
        let mut writer = LimitedWriter {
            buffer: self,
            limit,
            tail,
            failure: None,
        };
        match serde_json::to_writer(&mut writer, value) {
            Ok(()) => Ok(()),
            Err(error) => {
                writer.buffer.bytes.truncate(start);
                Err(match writer.failure {
                    Some(limit) => EncodeError::Limited(limit),
                    None => EncodeError::Json(error),
                })
            }
        }
    }

    fn framing(&mut self, bytes: &[u8]) -> Result<()> {
        let limit = self.limit;
        let mut writer = LimitedWriter {
            buffer: self,
            limit,
            tail: 0,
            failure: None,
        };
        writer
            .write_all(bytes)
            .map_err(|_| writer.failure.expect("bounded write failed").error())
    }

    fn recycle(mut self) -> Vec<u8> {
        cleanup_buffer(&mut self.bytes);
        std::mem::take(&mut self.bytes)
    }
}

enum EncodeError {
    Limited(ResponseLimit),
    Json(serde_json::Error),
}

impl From<EncodeError> for Error {
    fn from(error: EncodeError) -> Self {
        match error {
            EncodeError::Limited(limit) => limit.error(),
            EncodeError::Json(error) => Error::with_chain(error, "failed to serialize response"),
        }
    }
}

struct LimitedWriter<'a> {
    buffer: &'a mut ResponseBuffer,
    limit: usize,
    tail: usize,
    failure: Option<ResponseLimit>,
}

impl Write for LimitedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let result = self
            .buffer
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|len| *len <= self.limit)
            .ok_or(ResponseLimit::PerLine)
            .and_then(|len| {
                self.buffer.reserve(len.saturating_add(self.tail))?;
                Ok(len)
            });
        let len = match result {
            Ok(len) => len,
            Err(limit) => {
                self.failure = Some(limit);
                return Err(io::Error::new(io::ErrorKind::Other, limit.message()));
            }
        };
        if len > self.buffer.bytes.capacity() {
            // Avoid Vec's implicit doubling: every byte above the exempt floor
            // must be charged, including capacity retained after a rollback.
            let capacity = if len <= BUFFER_FLOOR {
                BUFFER_FLOOR
            } else if self.buffer.reservation.budget.limit == usize::MAX {
                len.checked_next_multiple_of(BUDGET_CHUNK).unwrap_or(len)
            } else {
                self.buffer.reservation.granted.saturating_add(BUFFER_FLOOR)
            }
            .min(self.limit);
            self.buffer
                .bytes
                .reserve_exact(capacity - self.buffer.bytes.len());
        }
        self.buffer.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ByteCounter(usize);

impl Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum Delivery {
    Buffered,
    BudgetErrors { ids: Vec<Value>, batch: bool },
}

impl Delivery {
    fn budget_errors(requests: Vec<Value>, batch: bool) -> Self {
        let ids = requests
            .into_iter()
            .map(|mut request| {
                request
                    .as_object_mut()
                    .and_then(|object| object.remove("id"))
                    .unwrap_or(Value::Null)
            })
            .collect();
        Self::BudgetErrors { ids, batch }
    }

    fn send(self, stream: &mut impl Write, buffer: &ResponseBuffer) -> Result<()> {
        match self {
            Self::Buffered => stream
                .write_all(&buffer.bytes)
                .chain_err(|| format!("failed to write {} response bytes", buffer.bytes.len())),
            Self::BudgetErrors { ids, batch } => {
                let mut writer = BufWriter::with_capacity(BUFFER_FLOOR, stream);
                if batch {
                    writer
                        .write_all(b"[")
                        .chain_err(|| "failed to send batch framing")?;
                }
                for (i, id) in ids.iter().enumerate() {
                    if i > 0 {
                        writer
                            .write_all(b",")
                            .chain_err(|| "failed to send batch framing")?;
                    }
                    let error =
                        ErrorEnvelope::new(GLOBAL_MSG, Some(id), JsonRpcV2Error::DaemonError);
                    serde_json::to_writer(&mut writer, &error)
                        .chain_err(|| "failed to send budget error")?;
                }
                writer
                    .write_all(if batch { b"]\n" } else { b"\n" })
                    .chain_err(|| "failed to send response framing")?;
                writer.flush().chain_err(|| "failed to send budget errors")
            }
        }
    }
}

pub(super) struct ResponseWriter {
    buffer: Vec<u8>,
    budget: Arc<ResponseBudget>,
    limit: usize,
}

impl ResponseWriter {
    pub(super) fn new(limit: usize, budget: Arc<ResponseBudget>) -> Self {
        Self {
            buffer: Vec::new(),
            budget,
            limit,
        }
    }

    pub(super) fn send(
        &mut self,
        stream: &mut impl Write,
        request: serde_json::Result<Value>,
        mut execute: impl FnMut(&Value) -> Value,
        mut rejected: impl FnMut(ResponseLimit),
    ) -> Result<()> {
        let mut buffer = ResponseBuffer::new(
            std::mem::take(&mut self.buffer),
            Arc::clone(&self.budget),
            self.limit,
        );
        let assembled = match request {
            Ok(Value::Array(commands)) => {
                assemble_batch(commands, &mut buffer, &mut execute, &mut rejected)
            }
            Ok(command) => assemble_single(command, &mut buffer, &mut execute, &mut rejected),
            Err(_) => {
                let error = ErrorEnvelope::new("parse error", None, JsonRpcV2Error::ParseError);
                buffer
                    .encode(&error, 1)
                    .map_err(|error| {
                        if let EncodeError::Limited(limit) = &error {
                            rejected(*limit);
                        }
                        Error::from(error)
                    })
                    .and_then(|()| buffer.framing(b"\n"))
                    .map(|()| Delivery::Buffered)
            }
        };
        let result = assembled.and_then(|delivery| delivery.send(stream, &buffer));
        self.buffer = buffer.recycle();
        result
    }

    // Notifications are exempt from byte limits; the caller still bounds writes.
    pub(super) fn send_notification(
        &mut self,
        stream: &mut impl Write,
        value: &Value,
    ) -> Result<()> {
        let result = serde_json::to_writer(&mut self.buffer, value)
            .chain_err(|| "failed to serialize notification")
            .and_then(|()| {
                self.buffer.push(b'\n');
                stream
                    .write_all(&self.buffer)
                    .chain_err(|| "failed to send notification")
            });
        cleanup_buffer(&mut self.buffer);
        result
    }
}

fn try_reserve_single_error_response(
    buffer: &mut ResponseBuffer,
    id: Option<&Value>,
    rejected: &mut impl FnMut(ResponseLimit),
) -> Result<bool> {
    let error_size = ErrorEnvelope::new(PER_LINE_MSG, id, JsonRpcV2Error::BadRequest)
        .encoded_len()
        .checked_add(1)
        .ok_or_else(|| ResponseLimit::PerLine.error())?;
    if let Err(limit) = buffer.reserve(error_size) {
        rejected(limit);
        if error_size > buffer.limit {
            return Err(ResponseLimit::PerLine.error());
        }
        buffer.bytes = Vec::new();
        return Ok(false);
    }
    Ok(true)
}

fn assemble_single(
    command: Value,
    buffer: &mut ResponseBuffer,
    execute: &mut impl FnMut(&Value) -> Value,
    rejected: &mut impl FnMut(ResponseLimit),
) -> Result<Delivery> {
    if buffer.limit == 0 {
        return Err(ResponseLimit::PerLine.error());
    }
    let id = command.get("id");
    if !try_reserve_single_error_response(buffer, id, rejected)? {
        return Ok(Delivery::budget_errors(vec![command], false));
    }
    match buffer.encode(&execute(&command), 1) {
        Ok(()) => {}
        Err(EncodeError::Limited(limit)) => {
            rejected(limit);
            buffer.encode(&ErrorEnvelope::new(limit.message(), id, limit.code()), 1)?;
        }
        Err(error) => return Err(error.into()),
    }
    buffer.framing(b"\n")?;
    Ok(Delivery::Buffered)
}

fn try_reserve_batch_error_responses(
    commands: &[Value],
    buffer: &mut ResponseBuffer,
    rejected: &mut impl FnMut(ResponseLimit),
) -> Result<Option<[usize; MAX_ARRAY_BATCH]>> {
    let n = commands.len();
    let mut error_sizes = [0; MAX_ARRAY_BATCH];
    let mut later_errors = 0usize;
    for (command, size) in commands.iter().zip(error_sizes.iter_mut()) {
        *size = ErrorEnvelope::new(SKIPPED_MSG, command.get("id"), JsonRpcV2Error::BadRequest)
            .encoded_len();
        later_errors = later_errors
            .checked_add(*size)
            .ok_or("batch reserve overflow")?;
    }
    // '[' + (n - 1) commas + ']\n'.
    let all_errors_size = later_errors
        .checked_add(n + 2)
        .ok_or("batch framing overflow")?;
    if all_errors_size > buffer.limit {
        bail!(
            "cannot fit correlated error envelopes for batch of {} \
             under electrum_rpc_max_response_num_bytes={}",
            n,
            buffer.limit
        );
    }
    if let Err(limit) = buffer.reserve(all_errors_size) {
        rejected(limit);
        buffer.bytes = Vec::new();
        return Ok(None);
    }

    Ok(Some(error_sizes))
}

fn assemble_batch(
    commands: Vec<Value>,
    buffer: &mut ResponseBuffer,
    execute: &mut impl FnMut(&Value) -> Value,
    rejected: &mut impl FnMut(ResponseLimit),
) -> Result<Delivery> {
    let n = commands.len();
    if n > MAX_ARRAY_BATCH {
        bail!(
            "Too many elements in batch requests {} max:{}",
            n,
            MAX_ARRAY_BATCH
        );
    }
    if n == 0 {
        buffer.framing(b"[]\n")?;
        return Ok(Delivery::Buffered);
    }

    let Some(error_sizes) = try_reserve_batch_error_responses(&commands, buffer, rejected)? else {
        return Ok(Delivery::budget_errors(commands, true));
    };
    let mut later_errors = error_sizes.iter().sum::<usize>();

    buffer.framing(b"[")?;
    let mut overflow: Option<ResponseLimit> = None;
    for (i, command) in commands.into_iter().enumerate() {
        if i > 0 {
            buffer.framing(b",")?;
        }
        later_errors -= error_sizes[i];
        let tail = later_errors + (n - 1 - i) + b"]\n".len();
        let (message, code) = match overflow {
            Some(limit) => (SKIPPED_MSG, limit.code()),
            None => match buffer.encode(&execute(&command), tail) {
                Ok(()) => continue,
                Err(EncodeError::Limited(limit)) => {
                    rejected(limit);
                    overflow = Some(limit);
                    (limit.message(), limit.code())
                }
                Err(error) => return Err(error.into()),
            },
        };
        buffer.encode(&ErrorEnvelope::new(message, command.get("id"), code), tail)?;
    }
    buffer.framing(b"]\n")?;
    Ok(Delivery::Buffered)
}

fn cleanup_buffer(buffer: &mut Vec<u8>) {
    buffer.clear();
    if buffer.capacity() > BUFFER_FLOOR {
        *buffer = Vec::new();
    }
}

pub(super) fn warn_limits(max_request_bytes: usize, max_response_bytes: usize, txs_limit: usize) {
    if max_response_bytes == usize::MAX {
        return;
    }
    let worst_reserve = max_request_bytes
        .saturating_add(MAX_ARRAY_BATCH * 128)
        .saturating_add(MAX_ARRAY_BATCH + 2);
    if worst_reserve >= max_response_bytes {
        warn!(
            "electrum_rpc_max_response_num_bytes={} is not large enough to guarantee \
             correlated error replies for a full batch of requests up to \
             electrum_rpc_max_request_num_bytes={}; such batches will be rejected by \
             closing the connection",
            max_response_bytes, max_request_bytes
        );
    }
    const HISTORY_ENTRY_ESTIMATE_BYTES: usize = 128;
    if txs_limit.saturating_mul(HISTORY_ENTRY_ESTIMATE_BYTES) >= max_response_bytes {
        warn!(
            "electrum_txs_limit={} can generate histories exceeding \
             electrum_rpc_max_response_num_bytes={} (rough estimate: {} bytes per \
             entry, IDs/UTXOs/transactions/headers excluded); legitimate history \
             replies can be rejected under this configuration",
            txs_limit, max_response_bytes, HISTORY_ENTRY_ESTIMATE_BYTES
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    fn send_reply(
        limit: usize,
        budget: usize,
        reply: &Value,
    ) -> (Value, usize, Vec<ResponseLimit>) {
        let mut writer = ResponseWriter::new(limit, ResponseBudget::new(budget));
        let mut bytes = Vec::new();
        let mut rejected = Vec::new();
        writer
            .send(
                &mut bytes,
                Ok(json!({"id": reply["id"]})),
                |_| reply.clone(),
                |limit| rejected.push(limit),
            )
            .unwrap();
        assert!(bytes.ends_with(b"\n"));
        assert_eq!(writer.budget.reserved.load(Ordering::Relaxed), 0);
        assert!(writer.buffer.capacity() <= BUFFER_FLOOR);
        (
            serde_json::from_slice(&bytes).unwrap(),
            bytes.len(),
            rejected,
        )
    }

    #[test]
    fn single_limit_includes_escaped_id_and_newline() {
        let reply = json!({"id": "a\n\"\\é", "jsonrpc": "2.0", "result": "x".repeat(256)});
        let exact = serde_json::to_vec(&reply).unwrap().len() + 1;
        let (actual, size, rejected) = send_reply(exact, usize::MAX, &reply);
        assert_eq!(actual, reply);
        assert_eq!(size, exact);
        assert!(rejected.is_empty());

        let (actual, size, rejected) = send_reply(exact - 1, usize::MAX, &reply);
        assert_eq!(actual["id"], reply["id"]);
        assert_eq!(actual["error"]["code"], 1);
        assert!(size < exact);
        assert_eq!(rejected, [ResponseLimit::PerLine]);
    }

    #[test]
    fn partial_final_grants_allow_replies_below_both_limits() {
        for limit in [32_768, 100_000] {
            let reply = json!({"id": 1, "result": "x".repeat(limit - 100)});
            let (actual, size, rejected) = send_reply(limit, limit, &reply);
            assert_eq!(actual, reply);
            assert!(size <= limit);
            assert!(rejected.is_empty());
        }
    }

    #[test]
    fn framing_and_rollback_keep_buffer_capacity_charged() {
        for suffix in [b"\n".as_slice(), b"]\n".as_slice(), b",".as_slice()] {
            let budget = ResponseBudget::new(100_000);
            let mut buffer = ResponseBuffer::new(Vec::new(), Arc::clone(&budget), 100_000);
            buffer
                .encode(&json!("x".repeat(90_000)), suffix.len())
                .map_err(Error::from)
                .unwrap();
            assert!(buffer
                .encode(&json!("y".repeat(20_000)), suffix.len())
                .is_err());
            buffer.framing(suffix).unwrap();
            assert!(buffer.bytes.capacity() <= buffer.reservation.granted + BUFFER_FLOOR);
            assert!(buffer.bytes.ends_with(suffix));
            assert_eq!(buffer.bytes.len(), 90_002 + suffix.len());
            drop(buffer);
            assert_eq!(budget.reserved.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn batch_error_uses_reserved_tail_when_no_more_budget_is_available() {
        let budget = ResponseBudget::new(BUDGET_CHUNK);
        let mut buffer = ResponseBuffer::new(Vec::new(), Arc::clone(&budget), 100_000);
        let delivery = assemble_batch(
            vec![json!({"id":1}), json!({"id":"b".repeat(1000)})],
            &mut buffer,
            &mut |command: &Value| {
                if command["id"] == 1 {
                    json!({"id":1, "result":"a".repeat(80_000)})
                } else {
                    json!({"id":command["id"], "result":"c".repeat(10_000)})
                }
            },
            &mut |limit| assert_eq!(limit, ResponseLimit::Global),
        )
        .unwrap();
        assert_eq!(budget.reserved.load(Ordering::Relaxed), BUDGET_CHUNK);
        assert!(buffer.bytes.capacity() <= BUDGET_CHUNK + BUFFER_FLOOR);
        let mut bytes = Vec::new();
        delivery.send(&mut bytes, &buffer).unwrap();
        let response: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(response[0]["result"].as_str().unwrap().len(), 80_000);
        assert_eq!(response[1]["id"], "b".repeat(1000));
        assert_eq!(response[1]["error"]["code"], 2);
        drop(buffer);
        assert_eq!(budget.reserved.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn batch_reserves_serialized_errors_before_executing_later_commands() {
        let first = json!({"id": 1, "jsonrpc": "2.0", "result": "x".repeat(1000)});
        let second_id = "a\n\"\\é".repeat(10);
        let skipped = |id: Value| {
            json!({
                "jsonrpc": "2.0", "id": id,
                "error": {"code": 1, "message": "batch response budget exhausted; not executed"}
            })
        };
        let reserved_line = json!([first, skipped(json!(second_id)), skipped(json!(3))]);
        let exact = serde_json::to_vec(&reserved_line).unwrap().len() + 1;
        for cap in [exact, exact - 1] {
            let mut writer = ResponseWriter::new(cap, ResponseBudget::new(usize::MAX));
            let mut calls = Vec::new();
            let mut bytes = Vec::new();
            writer
                .send(
                    &mut bytes,
                    Ok(json!([{"id":1}, {"id":second_id}, {"id":3}])),
                    |cmd| {
                        calls.push(cmd["id"].clone());
                        if cmd["id"] == 1 {
                            first.clone()
                        } else {
                            json!({"id": cmd["id"], "result": "y".repeat(cap)})
                        }
                    },
                    |limit| assert_eq!(limit, ResponseLimit::PerLine),
                )
                .unwrap();
            let actual: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(bytes.len() <= cap);
            assert_eq!(actual[1]["id"], second_id);
            assert_eq!(actual[2], skipped(json!(3)));
            if cap == exact {
                assert_eq!(actual[0], first);
                assert_eq!(calls, [json!(1), json!(second_id)]);
                assert_eq!(actual[1]["error"]["code"], 1);
            } else {
                assert_eq!(actual[0]["error"]["code"], 1);
                assert_eq!(calls, [json!(1)]);
            }
        }
    }

    #[test]
    fn fallback_moves_only_ids_and_streams_without_a_grant() {
        for batch in [false, true] {
            let id = "x".repeat(32 * 1024);
            let command = json!({"id": Value::String(id), "params": ["p".repeat(100_000)]});
            let allocation = command["id"].as_str().unwrap().as_ptr();
            let budget = ResponseBudget::new(1);
            let mut buffer = ResponseBuffer::new(Vec::new(), Arc::clone(&budget), 100_000);
            let mut execute = |_: &Value| panic!("rejected command executed");
            let mut rejected = |limit| assert_eq!(limit, ResponseLimit::Global);
            let delivery = if batch {
                assemble_batch(vec![command], &mut buffer, &mut execute, &mut rejected)
            } else {
                assemble_single(command, &mut buffer, &mut execute, &mut rejected)
            }
            .unwrap();
            match &delivery {
                Delivery::BudgetErrors {
                    ids,
                    batch: is_batch,
                } => {
                    assert_eq!(*is_batch, batch);
                    assert_eq!(ids.len(), 1);
                    assert_eq!(ids[0].as_str().unwrap().as_ptr(), allocation);
                }
                Delivery::Buffered => panic!("expected streaming fallback"),
            }
            assert_eq!(buffer.bytes.capacity(), 0);
            assert_eq!(budget.reserved.load(Ordering::Relaxed), 0);
            let mut bytes = Vec::new();
            delivery.send(&mut bytes, &buffer).unwrap();
            let response: Value = serde_json::from_slice(&bytes).unwrap();
            let response = if batch { &response[0] } else { &response };
            assert_eq!(response["id"].as_str().unwrap().len(), 32 * 1024);
            assert_eq!(response["error"]["code"], 2);
        }
    }

    #[test]
    fn budget_is_shared_until_reservations_drop_and_servers_are_independent() {
        let budget = ResponseBudget::new(2 * BUDGET_CHUNK);
        let reserve = || Reservation {
            budget: Arc::clone(&budget),
            granted: 0,
        };
        let mut first = reserve();
        let mut second = reserve();
        first.ensure(BUDGET_CHUNK).unwrap();
        second.ensure(BUDGET_CHUNK).unwrap();
        let mut third = reserve();
        assert_eq!(third.ensure(1), Err(ResponseLimit::Global));
        let mut independent = Reservation {
            budget: ResponseBudget::new(BUDGET_CHUNK),
            granted: 0,
        };
        independent.ensure(BUDGET_CHUNK).unwrap();
        drop(first);
        third.ensure(BUDGET_CHUNK).unwrap();
        drop(second);
        drop(third);
        assert_eq!(budget.reserved.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn concurrent_reservations_never_exceed_the_budget() {
        const THREADS: usize = 8;
        const GRANTS: usize = 4;
        for _ in 0..16 {
            let budget = ResponseBudget::new(GRANTS * BUDGET_CHUNK);
            let barrier = Arc::new(Barrier::new(THREADS));
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    let budget = Arc::clone(&budget);
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let mut reservation = Reservation { budget, granted: 0 };
                        barrier.wait();
                        let admitted = reservation.ensure(BUDGET_CHUNK).is_ok();
                        // Keep grants alive until every thread has attempted admission.
                        barrier.wait();
                        admitted
                    })
                })
                .collect();
            let admitted = handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap()))
                .sum::<usize>();
            assert_eq!(admitted, GRANTS);
            assert_eq!(budget.reserved.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn transmission_keeps_the_grant_and_releases_it_on_error_or_panic() {
        struct FailingSink {
            budget: Arc<ResponseBudget>,
            panic: bool,
        }
        impl Write for FailingSink {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                assert!(self.budget.reserved.load(Ordering::Relaxed) > 0);
                if self.panic {
                    panic!("simulated writer panic");
                }
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        for panic in [false, true] {
            let budget = ResponseBudget::new(100_000);
            let mut writer = ResponseWriter::new(100_000, Arc::clone(&budget));
            let mut sink = FailingSink {
                budget: Arc::clone(&budget),
                panic,
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.send(
                    &mut sink,
                    Ok(json!({"id":1})),
                    |_| json!({"id":1, "result":"x".repeat(90_000)}),
                    |_| panic!("unexpected rejection"),
                )
            }));
            if panic {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(budget.reserved.load(Ordering::Relaxed), 0);
            assert!(writer.buffer.capacity() <= BUFFER_FLOOR);
        }
    }

    #[test]
    fn parse_errors_empty_batches_and_notifications_keep_their_wire_behavior() {
        let mut writer = ResponseWriter::new(128, ResponseBudget::new(1));
        let mut bytes = Vec::new();
        writer
            .send(
                &mut bytes,
                serde_json::from_str("{"),
                |_| panic!("parse error executed"),
                |_| panic!("unexpected rejection"),
            )
            .unwrap();
        let error: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            error,
            json!({"jsonrpc":"2.0", "id":null, "error":{"code":-32700,"message":"parse error"}})
        );
        bytes.clear();
        writer
            .send(
                &mut bytes,
                Ok(json!([])),
                |_| panic!("empty batch executed"),
                |_| panic!("unexpected rejection"),
            )
            .unwrap();
        assert_eq!(bytes, b"[]\n");
        bytes.clear();
        let notification = json!({"method":"test", "params":["x".repeat(100_000)]});
        writer
            .send_notification(&mut bytes, &notification)
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            notification
        );
        assert_eq!(writer.budget.reserved.load(Ordering::Relaxed), 0);
        assert!(writer.buffer.capacity() <= BUFFER_FLOOR);
    }
}
