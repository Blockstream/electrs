mod block;
mod script;
mod transaction;

pub mod bincode;
pub mod electrum_merkle;
pub mod fees;

pub use self::block::{
    BlockHeaderMeta, BlockId, BlockMeta, BlockStatus, HeaderEntry, HeaderList, DEFAULT_BLOCKHASH,
};
pub use self::fees::get_tx_fee;
pub use self::script::{get_innerscripts, ScriptToAddr, ScriptToAsm};
pub use self::transaction::{
    extract_tx_prevouts, has_prevout, is_coinbase, is_spendable, serialize_outpoint,
    TransactionStatus, TxInput,
};

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;
use std::thread::{self, ThreadId};

use crate::chain::BlockHeader;
use bitcoin::hashes::sha256d::Hash as Sha256dHash;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;

pub type Bytes = Vec<u8>;
pub type HeaderMap = HashMap<Sha256dHash, BlockHeader>;

// TODO: consolidate serialization/deserialize code for bincode/bitcoin.
const HASH_LEN: usize = 32;

pub type FullHash = [u8; HASH_LEN];

pub fn full_hash(hash: &[u8]) -> FullHash {
    *array_ref![hash, 0, HASH_LEN]
}

pub struct SyncChannel<T> {
    tx: Option<crossbeam_channel::Sender<T>>,
    rx: Option<crossbeam_channel::Receiver<T>>,
}

impl<T> SyncChannel<T> {
    pub fn new(size: usize) -> SyncChannel<T> {
        let (tx, rx) = crossbeam_channel::bounded(size);
        SyncChannel {
            tx: Some(tx),
            rx: Some(rx),
        }
    }

    pub fn sender(&self) -> crossbeam_channel::Sender<T> {
        self.tx.as_ref().expect("No Sender").clone()
    }

    pub fn receiver(&self) -> &crossbeam_channel::Receiver<T> {
        self.rx.as_ref().expect("No Receiver")
    }

    pub fn into_receiver(self) -> crossbeam_channel::Receiver<T> {
        self.rx.expect("No Receiver")
    }

    /// This drops the sender and receiver, causing all other methods to panic.
    ///
    /// Use only when you know that the channel will no longer be used.
    /// ie. shutdown.
    pub fn close(&mut self) -> Option<crossbeam_channel::Receiver<T>> {
        self.tx.take();
        self.rx.take()
    }
}

pub struct Channel<T> {
    tx: Sender<T>,
    rx: Receiver<T>,
}

impl<T> Channel<T> {
    pub fn unbounded() -> Self {
        let (tx, rx) = channel();
        Channel { tx, rx }
    }

    pub fn sender(&self) -> Sender<T> {
        self.tx.clone()
    }

    pub fn receiver(&self) -> &Receiver<T> {
        &self.rx
    }

    pub fn into_receiver(self) -> Receiver<T> {
        self.rx
    }
}

/// This static HashMap contains all the threads spawned with [`spawn_thread`] with their name
#[inline]
pub fn with_spawned_threads<F>(f: F)
where
    F: FnOnce(&mut HashMap<ThreadId, String>),
{
    lazy_static! {
        static ref SPAWNED_THREADS: Mutex<HashMap<ThreadId, String>> = Mutex::new(HashMap::new());
    }
    let mut lock = match SPAWNED_THREADS.lock() {
        Ok(threads) => threads,
        // There's no possible broken state
        Err(threads) => {
            warn!("SPAWNED_THREADS is in a poisoned state! Be wary of incorrect logs!");
            threads.into_inner()
        }
    };
    f(&mut lock)
}

pub fn spawn_thread<F, T>(prefix: &str, do_work: F) -> thread::JoinHandle<T>
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    static THREAD_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let counter = THREAD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    thread::Builder::new()
        .name(format!("{}-{}", prefix, counter))
        .spawn(move || {
            let thread = std::thread::current();
            let name = thread.name().unwrap();
            let id = thread.id();

            trace!("[THREAD] GETHASHMAP INSERT | {name} {id:?}");
            with_spawned_threads(|threads| {
                threads.insert(id, name.to_owned());
            });
            trace!("[THREAD] START WORK        | {name} {id:?}");

            let result = do_work();

            trace!("[THREAD] FINISHED WORK     | {name} {id:?}");
            trace!("[THREAD] GETHASHMAP REMOVE | {name} {id:?}");
            with_spawned_threads(|threads| {
                threads.remove(&id);
            });
            trace!("[THREAD] HASHMAP REMOVED   | {name} {id:?}");

            result
        })
        .unwrap()
}

// Similar to https://doc.rust-lang.org/std/primitive.bool.html#method.then (nightly only),
// but with a function that returns an `Option<T>` instead of `T`. Adding something like
// this to std is being discussed: https://github.com/rust-lang/rust/issues/64260

pub trait BoolThen {
    fn and_then<T>(self, f: impl FnOnce() -> Option<T>) -> Option<T>;
}

impl BoolThen for bool {
    fn and_then<T>(self, f: impl FnOnce() -> Option<T>) -> Option<T> {
        if self {
            f()
        } else {
            None
        }
    }
}

pub fn create_socket(addr: &SocketAddr) -> Socket {
    let domain = match &addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket =
        Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).expect("creating socket failed");

    #[cfg(unix)]
    socket
        .set_reuse_port(true)
        .expect("cannot enable SO_REUSEPORT");

    socket.bind(&addr.clone().into()).expect("cannot bind");

    socket
}
