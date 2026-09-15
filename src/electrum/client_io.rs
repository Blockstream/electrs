//! One deadline for all writes making up a client-facing response.

use std::io::{self, Write};
use std::net::{Shutdown, TcpStream};
use std::time::{Duration, Instant};

use crate::errors::*;
use crate::metrics::Counter;

pub(super) fn write_to_client(
    stream: &TcpStream,
    timeout: Option<Duration>,
    timeouts: &Counter,
    write: impl FnOnce(&mut ClientWriter<'_>) -> Result<()>,
) -> Result<()> {
    if timeout.is_none() {
        stream
            .set_write_timeout(None)
            .chain_err(|| "failed to disable client write timeout")?;
    }
    let mut writer = ClientWriter {
        stream,
        timeout,
        deadline: None,
        timed_out: false,
    };
    let result = write(&mut writer);
    if writer.timed_out {
        timeouts.inc();
        // Preserve the classification even when serde wrapped the I/O error
        // while streaming a budget rejection.
        Err(ErrorKind::ClientWriteTimeout.into())
    } else {
        result
    }
}

pub(super) struct ClientWriter<'a> {
    stream: &'a TcpStream,
    timeout: Option<Duration>,
    // Start at the first write, after command execution and reply assembly.
    deadline: Option<Instant>,
    timed_out: bool,
}

impl ClientWriter<'_> {
    fn expired(&mut self) -> io::Error {
        if !self.timed_out {
            self.timed_out = true;
            // Discard queued output on close instead of leaving a FIN behind
            // bytes the client will not read. Only timed-out writes abort this way.
            if let Err(error) = socket2::SockRef::from(self.stream).set_linger(Some(Duration::ZERO))
            {
                warn!(
                    "failed to enable abortive close after client write timeout: {}",
                    error
                );
            }
            // Wake the reader; teardown closes all socket handles to send the RST.
            let _ = self.stream.shutdown(Shutdown::Both);
        }
        io::Error::new(io::ErrorKind::TimedOut, "client response write timed out")
    }
}

impl Write for ClientWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.timed_out {
            return Err(self.expired());
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        let timeout = match self.timeout {
            Some(timeout) => timeout,
            None => return (&*self.stream).write(bytes),
        };
        let now = Instant::now();
        let deadline = match self.deadline {
            Some(deadline) => deadline,
            None => {
                let deadline = now.checked_add(timeout).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "client write timeout is too large",
                    )
                })?;
                self.deadline = Some(deadline);
                deadline
            }
        };
        if now >= deadline {
            return Err(self.expired());
        }

        // A fresh full timeout on each partial write would let a trickling
        // reader extend the response indefinitely. Only use the remaining time.
        self.stream.set_write_timeout(Some(deadline - now))?;
        let result = (&*self.stream).write(bytes);
        if Instant::now() >= deadline
            || matches!(&result, Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut))
        {
            return Err(self.expired());
        }
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.timed_out
            || self
                .deadline
                .map_or(false, |deadline| Instant::now() >= deadline)
        {
            return Err(self.expired());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::sync::{mpsc, Arc};
    use std::thread;

    // A watchdog bounds the test even if the write deadline regresses.
    struct SocketPair {
        server: Arc<TcpStream>,
        client: TcpStream,
        stop: mpsc::Sender<()>,
        watchdog: Option<thread::JoinHandle<()>>,
    }

    impl SocketPair {
        fn new() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (server, _) = listener.accept().unwrap();
            let server = Arc::new(server);
            socket2::SockRef::from(&*server)
                .set_send_buffer_size(4096)
                .unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let (stop, receiver) = mpsc::channel();
            let socket = Arc::clone(&server);
            let watchdog = thread::spawn(move || {
                if receiver.recv_timeout(Duration::from_secs(5)).is_err() {
                    let _ = socket.shutdown(Shutdown::Both);
                }
            });
            Self {
                server,
                client,
                stop,
                watchdog: Some(watchdog),
            }
        }
    }

    impl Drop for SocketPair {
        fn drop(&mut self) {
            let _ = self.server.shutdown(Shutdown::Both);
            let _ = self.client.shutdown(Shutdown::Both);
            let _ = self.stop.send(());
            self.watchdog.take().unwrap().join().unwrap();
        }
    }

    fn counter() -> Counter {
        Counter::new("client_write_timeouts", "Test timeouts").unwrap()
    }

    #[test]
    fn stalled_writer_times_out_without_the_client_disconnecting() {
        let sockets = SocketPair::new();
        socket2::SockRef::from(&sockets.client)
            .set_recv_buffer_size(4096)
            .unwrap();
        let timeouts = counter();
        let start = Instant::now();
        let result = write_to_client(
            &sockets.server,
            Some(Duration::from_millis(150)),
            &timeouts,
            |writer| {
                writer
                    .write_all(&vec![b'x'; 2 * 1024 * 1024])
                    .chain_err(|| "write failed")
            },
        );
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::ClientWriteTimeout
        ));
        assert!(start.elapsed() < Duration::from_secs(3));
        assert_eq!(timeouts.get(), 1);
        // sockets.client is still open and has never read anything.
    }

    #[test]
    fn reading_client_receives_payload_and_newline() {
        let mut sockets = SocketPair::new();
        let timeouts = counter();
        write_to_client(
            &sockets.server,
            Some(Duration::from_secs(1)),
            &timeouts,
            |writer| {
                writer.write_all(b"hello").chain_err(|| "payload failed")?;
                writer.write_all(b"\n").chain_err(|| "newline failed")
            },
        )
        .unwrap();
        let mut bytes = [0; 6];
        sockets.client.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"hello\n");
        assert_eq!(timeouts.get(), 0);
    }

    #[test]
    fn delayed_reader_can_finish_and_disabled_timeout_clears_the_socket_limit() {
        for timeout in [Some(Duration::from_secs(4)), None] {
            let sockets = SocketPair::new();
            sockets
                .server
                .set_write_timeout(Some(Duration::from_millis(10)))
                .unwrap();
            let mut client = sockets.client.try_clone().unwrap();
            let reader = thread::spawn(move || {
                thread::sleep(Duration::from_millis(150));
                let mut bytes = Vec::new();
                client.read_to_end(&mut bytes).unwrap();
                bytes
            });
            let timeouts = counter();
            let payload = vec![b'x'; 512 * 1024];
            let result = write_to_client(&sockets.server, timeout, &timeouts, |writer| {
                writer.write_all(&payload).chain_err(|| "write failed")
            });
            let _ = sockets.server.shutdown(Shutdown::Write);
            let received = reader.join().unwrap();
            result.unwrap();
            assert_eq!(received, payload);
            assert_eq!(timeouts.get(), 0);
        }
    }

    #[test]
    fn chunks_and_newline_share_one_deadline() {
        let sockets = SocketPair::new();
        let timeouts = counter();
        let result = write_to_client(
            &sockets.server,
            Some(Duration::from_secs(1)),
            &timeouts,
            |writer| {
                assert!(
                    writer.deadline.is_none(),
                    "assembly must not start the write clock"
                );
                writer
                    .write_all(b"payload")
                    .chain_err(|| "payload failed")?;
                assert!(writer.deadline.is_some());
                // Advance the deadline explicitly instead of racing a short sleep.
                writer.deadline = Some(Instant::now() - Duration::from_secs(1));
                let newline = writer.write_all(b"\n").chain_err(|| "newline failed");
                assert!(writer.write_all(b"retry").is_err());
                newline
            },
        );
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::ClientWriteTimeout
        ));
        assert_eq!(timeouts.get(), 1);
    }

    #[test]
    fn partial_progress_does_not_extend_the_deadline() {
        let sockets = SocketPair::new();
        socket2::SockRef::from(&sockets.client)
            .set_recv_buffer_size(4096)
            .unwrap();
        let mut client = sockets.client.try_clone().unwrap();
        let (stop, receiver) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut total = 0;
            let mut bytes = [0; 1024];
            loop {
                match client.read(&mut bytes) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => total += n,
                }
                if receiver.recv_timeout(Duration::from_millis(10)).is_ok() {
                    break;
                }
            }
            total
        });
        let timeouts = counter();
        let start = Instant::now();
        let result = write_to_client(
            &sockets.server,
            Some(Duration::from_millis(250)),
            &timeouts,
            |writer| {
                writer
                    .write_all(&vec![b'x'; 2 * 1024 * 1024])
                    .chain_err(|| "write failed")
            },
        );
        let _ = stop.send(());
        assert!(reader.join().unwrap() > 0);
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::ClientWriteTimeout
        ));
        assert!(start.elapsed() < Duration::from_secs(3));
        assert_eq!(timeouts.get(), 1);
    }

    #[test]
    fn streaming_budget_errors_and_notifications_use_the_same_write_deadline() {
        use crate::electrum::response::{ResponseBudget, ResponseWriter};

        for notification in [false, true] {
            let sockets = SocketPair::new();
            socket2::SockRef::from(&sockets.client)
                .set_recv_buffer_size(4096)
                .unwrap();
            let timeouts = counter();
            let mut responses = ResponseWriter::new(2 * 1024 * 1024, ResponseBudget::new(1));
            let result = write_to_client(
                &sockets.server,
                Some(Duration::from_millis(150)),
                &timeouts,
                |writer| {
                    if notification {
                        responses
                            .send_notification(writer, &json!({"params":["x".repeat(1024 * 1024)]}))
                    } else {
                        responses.send(
                            writer,
                            Ok(json!({"id":"x".repeat(1024 * 1024), "method":"server.ping"})),
                            |_| panic!("budget rejection must not execute the command"),
                            |_| {},
                        )
                    }
                },
            );
            assert!(matches!(
                result.unwrap_err().kind(),
                ErrorKind::ClientWriteTimeout
            ));
            assert_eq!(timeouts.get(), 1);
        }
    }
}
