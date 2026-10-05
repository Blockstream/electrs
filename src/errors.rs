error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        Connection(msg: String) {
            description("Connection error")
            display("Connection error: {}", msg)
        }

        RpcError(code: i64, error: String, method: String) {
            description("RPC error")
            display("{} RPC error {}: {}", method, code, error)
        }

        Interrupt(sig: i32) {
            description("Interruption by external signal")
            display("Iterrupted by signal {}", sig)
        }

        TooPopular {
            description("Too many history entries")
            display("Too many history entries")
        }

        TooManyUtxos {
            description("Too many unspent outputs")
            display("Too many unspent outputs")
        }

        TooManySubscriptions(limit: usize) {
            description("Too many subscriptions")
            display("Too many subscriptions on this connection (limit: {})", limit)
        }

        InvalidParams(msg: String) {
            description("Invalid RPC params")
            display("{}", msg)
        }

        // Raised when a request made on behalf of an API client cannot get one of the
        // bounded client RPC slots within its wait budget. The daemon itself may be
        // perfectly healthy - we are simply refusing to queue any deeper.
        DaemonBusy(msg: String) {
            description("Daemon RPC concurrency limit reached")
            display("Daemon is busy: {}", msg)
        }

        // Raised when a request made on behalf of an API client fails at the transport
        // level (connect failure, or a read that exceeded the client-facing timeout).
        // Unlike internal callers, client requests are not retried indefinitely.
        DaemonUnavailable(msg: String) {
            description("Daemon RPC unavailable")
            display("Daemon is unavailable: {}", msg)
        }

        #[cfg(feature = "electrum-discovery")]
        ElectrumClient(e: electrum_client::Error) {
            description("Electrum client error")
            display("Electrum client error: {:?}", e)
        }

    }
}

#[cfg(feature = "electrum-discovery")]
impl From<electrum_client::Error> for Error {
    fn from(e: electrum_client::Error) -> Self {
        Error::from(ErrorKind::ElectrumClient(e))
    }
}
