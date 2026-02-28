# CLAUDE.md

## Project Overview

Electrs (Esplora variant) is a blockchain index engine and HTTP API for Bitcoin (and Liquid/Litecoin) written in Rust. It is the backend for the [Esplora block explorer](https://github.com/Blockstream/esplora) powering blockstream.info. Forked from [romanz/electrs](https://github.com/romanz/electrs), it adds an HTTP REST API, extended indexes, and Elements/Liquid support, as well as Litecoin support.

## Build & Run

```bash
# Build (requires clang, cmake, and Rust 1.75.0 via rust-toolchain.toml)
cargo build --release

# Run (requires a running bitcoind, no txindex needed)
cargo run --release --bin electrs -- -vvvv --daemon-dir ~/.bitcoin

# Build with Liquid support
cargo build --features liquid --release

# Build with Litecoin support
cargo build --features litecoin --release

# Run with Litecoin (requires a running litecoind)
cargo run --features litecoin --release --bin electrs -- -vvvv --daemon-dir ~/.litecoin

# Build with OpenTelemetry tracing
cargo build --features otlp-tracing --release
```

## Testing

```bash
# Run all tests (requires bitcoind binary available in PATH or via BITCOIND_EXE)
RUST_LOG=debug cargo test

# Run Liquid tests
RUST_LOG=debug cargo test --features liquid

# Run tests with Bitcoin Core 28+ compatibility
RUST_LOG=debug cargo test --features bitcoind_28_0

# Run specific ignored test
cargo test -- --include-ignored test_electrum_raw

# Run benchmarks (requires bench feature)
cargo bench --features bench
```

Integration tests in `tests/` spin up a real bitcoind (or elementsd) instance, create a temporary RocksDB, index blocks, and run queries against the REST and Electrum APIs.

## Formatting & Linting

```bash
# Format check (used by pre-commit hook)
cargo fmt --all -- --check

# Format
cargo fmt --all

# Clippy
cargo clippy --all-targets
```

The pre-commit hook (`.hooks/pre-commit`) runs `cargo +stable fmt --all -- --check`.

## Architecture

### Key Modules (`src/`)

- **`bin/electrs.rs`** — Main entry point. Starts the daemon connection, indexer, mempool tracker, REST server, and Electrum RPC server.
- **`new_index/`** — Core indexing engine:
  - `schema.rs` — Index schema definitions: `Store`, `Indexer`, `ChainQuery`. Three RocksDB databases: `txstore`, `history`, `cache`.
  - `db.rs` — RocksDB wrapper and low-level operations.
  - `mempool.rs` — Mempool tracking and indexing.
  - `query.rs` — High-level query interface combining chain and mempool data.
  - `fetch.rs` — Block fetching from blk*.dat files or via JSON-RPC.
  - `zmq.rs` — ZMQ notifications for new blocks.
- **`rest.rs`** — HTTP REST API server (tiny_http-based).
- **`electrum/`** — Electrum JSON-RPC protocol server.
  - `server.rs` — TCP server and connection handling.
  - `client.rs` — Per-client state and request processing.
- **`daemon.rs`** — Bitcoin Core JSON-RPC client.
- **`config.rs`** — CLI argument parsing and configuration.
- **`chain.rs`** — Blockchain type aliases and network definitions.
- **`elements/`** — Liquid/Elements-specific code (behind `liquid` feature flag).
- **`util/`** — Helpers for transactions, scripts, fees, merkle proofs, and block parsing.
  - `litecoin_addr.rs` — Litecoin address encoding/decoding (bech32 `ltc1`/`tltc1`/`rltc1` and base58check with Litecoin version bytes). Behind `litecoin` feature flag.
- **`electrs_macros/`** — Proc-macro crate for optional OTLP tracing instrumentation.

### Data Flow

1. **Indexing**: Blocks are fetched (from blk*.dat files for initial sync, or JSON-RPC for incremental updates) and indexed into three RocksDB databases (`txstore` → `history` → `cache`).
2. **Serving**: The REST API and Electrum RPC server query the indexed data via `ChainQuery` (on-chain) and `Mempool` (unconfirmed), unified through `Query`.
3. **Mempool**: Synced from bitcoind and re-synced on each main loop iteration (every 5 seconds or on ZMQ notification).

### Feature Flags

- `liquid` — Liquid/Elements network support (mutually exclusive with `litecoin`)
- `litecoin` — Litecoin network support (mutually exclusive with `liquid`). Networks: litecoin, litecointestnet, litecoinregtest. Note: MWEB (MimbleWimble Extension Blocks) is not yet supported.
- `electrum-discovery` — Electrum server peer discovery
- `otlp-tracing` — OpenTelemetry tracing export
- `bitcoind_28_0` — Bitcoin Core 28.0+ compatibility
- `bench` — Enable benchmarks

### Database Schema

See `doc/schema.md` for the full RocksDB key/value schema. Key prefixes: `B` (block headers), `T` (raw txs), `O` (outpoints), `H` (history), `C` (confirmations), `S` (spending), `X` (block txids), `M` (block metadata).

## Configuration

Configuration is done via CLI args. Key options:
- `--daemon-dir` — bitcoind data directory
- `--db-dir` — electrs database directory
- `--http-addr` — REST API listen address (default: 127.0.0.1:3000)
- `--electrum-rpc-addr` — Electrum RPC listen address (default: 127.0.0.1:50001)
- `--lightmode` — reduce disk usage by querying bitcoind for raw txs on demand
- `--network` — bitcoin network (mainnet/testnet/regtest/signet/liquid/litecoin/litecointestnet/litecoinregtest)
- `--cors` — CORS origins for HTTP API

## Workspace

This is a Cargo workspace with two members:
- `electrs` (root) — main crate
- `electrs_macros` — proc-macro crate for OTLP tracing
