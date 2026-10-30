# Esplora - Electrs backend API

A block chain index engine and HTTP API written in Rust based on [romanz/electrs](https://github.com/romanz/electrs).

Used as the backend for the [Esplora block explorer](https://github.com/Blockstream/esplora) powering [blockstream.info](https://blockstream.info/).

API documentation [is available here](https://github.com/blockstream/esplora/blob/master/API.md).

Documentation for the database schema and indexing process [is available here](doc/schema.md).

### Installing & indexing

Install Rust, Bitcoin Core (no `txindex` needed) and the `clang` and `cmake` packages, increase maximum number open files by `ulimit -n 100000` and then:

```bash
$ git clone https://github.com/blockstream/electrs && cd electrs
$ git checkout new-index
$ cargo run --release --bin electrs -- --daemon-dir ~/.bitcoin

# Or for liquid:
$ cargo run --features liquid --release --bin electrs -- --network liquid --daemon-dir ~/.liquid
```

See [electrs's original documentation](https://github.com/romanz/electrs/blob/master/doc/usage.md) for more detailed instructions.
Note that our indexes are incompatible with electrs's and has to be created separately.

The indexes require 610GB of storage after running compaction (as of June 2020), but you'll need to have
free space of about double that available during the index compaction process.
Creating the indexes should take a few hours on a beefy machine with SSD.

To deploy with Docker, follow the [instructions here](https://github.com/Blockstream/esplora#how-to-build-the-docker-image).

### Light mode

For personal or low-volume use, you may set `--lightmode` to reduce disk storage requirements
by roughly 50% at the cost of slower and more expensive lookups.

With this option set, raw transactions and metadata associated with blocks will not be kept in rocksdb
(the `T`, `X` and `M` indexes),
but instead queried from bitcoind on demand.

### Notable changes from Electrs:

- HTTP REST API in addition to the Electrum JSON-RPC protocol, with extended transaction information
  (previous outputs, spending transactions, script asm and more).

- Extended indexes and database storage for improved performance under high load:

  - A full transaction store mapping txids to raw transactions is kept in the database under the prefix `T`.
  - An index of all spendable transaction outputs is kept under the prefix `O`.
  - An index of all addresses (encoded as string) is kept under the prefix `a` to enable by-prefix address search.
  - A map of blockhash to txids is kept in the database under the prefix `X`.
  - Block stats metadata (number of transactions, size and weight) is kept in the database under the prefix `M`.

  With these new indexes, bitcoind is no longer queried to serve user requests and is only polled
  periodically for new blocks and for syncing the mempool.

- Support for Liquid and other Elements-based networks, including CT, peg-in/out and multi-asset.
  (requires enabling the `liquid` feature flag using `--features liquid`)

### CLI options

In addition to electrs's original configuration options, a few new options are also available:

- `--http-addr <addr:port>` - HTTP server address/port to listen on (default: `127.0.0.1:3000`).
- `--lightmode` - enable light mode (see above)
- `--cors <origins>` - origins allowed to make cross-site request (optional, defaults to none).
- `--address-search` - enables the by-prefix address search index. Best-effort: entries are
  written when a block is indexed but are not removed if that block is later reorged out, so
  the index may return addresses with no transaction in the current best chain. Run a full
  reindex to clear stale entries if this matters for your deployment.
- `--index-unspendables` - enables indexing of provably unspendable outputs.
- `--enable-mining-rest` - enables cached mining-related HTTP endpoints.
- `--utxos-limit <num>` - maximum number of utxos to return per address.
- `--history-scan-limit <num>` - approximate number of history rows scanned per address lookup before failing with a "too popular" error; progress is checkpointed so repeated lookups make headway. Must be at least `1`. Default: `100000`.
- `--utxos-checkpoint-limit <num>` - maximum number of utxos held in a partially scanned utxo set and its saved checkpoint, and of unconfirmed history entries scanned per address. Must be at least `--utxos-limit`. Default: 10 times `--utxos-limit`.
- `--electrum-txs-limit <num>` - maximum number of txs to return per address in the electrum server (does not apply for the http api).
- `--electrum-rpc-write-timeout <seconds>` - maximum time to transmit a complete Electrum reply, including a whole batch and its newline, an error reply, or a subscription notification. The deadline starts at the first write and is not extended by partial progress. Expiry closes the connection and releases its response buffers. Idle subscriptions and command execution are unaffected. Default: `30`, matching the RPC proxy's `CLIENT_WRITE_TIMEOUT_SECS`; `0` disables the bound, which is only accepted with `--electrum-rpc-conn-max-age` set or the global response budget disabled. The `electrum_client_write_timeouts_total` counter records connections closed by this deadline.
- `--electrum-rpc-max-response-num-bytes <num>` - maximum size, in bytes, of a single Electrum solicited reply line (brackets, commas, error objects, and newline included). Overflowing replies return a correlated error (code 1) and the rest of the batch is not executed; the command that overflowed may already have completed. Subscription notifications are exempt. `0` disables the cap. Default: `8388608` (8 MiB) on Bitcoin, `33554432` (32 MiB) on Liquid (sized for a 2016-header window during a dynafed parameter vote).
- `--electrum-rpc-global-response-budget-bytes <num>` - aggregate cap, in bytes, on Electrum solicited-reply buffer memory retained across all connections at any instant, including batch replies held while later batch elements execute. Subscription notifications and queued request lines are not counted. Small replies below a 16 KiB per-connection floor are exempt; above the floor each reply charges the budget in 64 KiB chunks (or a smaller final chunk) and releases on drop. Requests that would exceed the budget are rejected with a server error (code 2) instead of allocating, and the connection stays usable. Must be `>= --electrum-rpc-max-response-num-bytes`, and requires a non-zero `--electrum-rpc-write-timeout` or `--electrum-rpc-conn-max-age`. `0` disables the budget. Default: `67108864` (64 MiB) on Bitcoin, `268435456` (256 MiB) on Liquid.
- `--electrum-banner <text>` - welcome banner text for electrum server.

Additional options with the `liquid` feature:
- `--parent-network <network>` - the parent network this chain is pegged to.

Additional options with the `electrum-discovery` feature:
- `--electrum-hosts <json>` - a json map of the public hosts where the electrum server is reachable, in the [`server.features` format](https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#server-features).
- `--electrum-announce` - announce the electrum server on the electrum p2p server discovery network.

See `$ cargo run --release --bin electrs -- --help` for the full list of options.

### Mining-related HTTP endpoints

`GET /block-template` is available only with `--enable-mining-rest`. It proxies
the daemon's `getblocktemplate` response unchanged on Bitcoin-compatible chains.
On Liquid, it instead decodes the complete proposal returned by
`getnewblockhex` and projects the recoverable header, transaction, fee,
coinbase, and witness-commitment data into the same response shape. Fields that
have no equivalent mining semantics for signed dynafed blocks use compatibility
defaults or are omitted. The Liquid response is intended for template inspection
and distribution, not block reconstruction or federation signing.

Successful responses are cached for 15 seconds and invalidated early when
electrs indexes a new tip. Cache misses are coalesced into one daemon request.
Responses use `Cache-Control: no-store`, so downstream caches do not extend the
internal lifetime.

Template RPCs use an isolated daemon connection with a 30-second I/O timeout.
Failures are retained internally for one second to prevent HTTP pollers from
immediately repeating the same failing RPC, while error responses remain
`Cache-Control: no-store`. Malformed Bitcoin templates that cannot be validated
against the indexed tip are rejected with `502 Bad Gateway`; they are not served
or cached.

All connections to the configured daemon RPC endpoint are expected to expose a
coherent chain view. Deployments using an L4 load balancer must keep its daemon
backends synchronized or provide backend affinity. A template that conflicts
with electrs' indexed tip is rejected rather than serving potentially stale
mining work.

## License

MIT
