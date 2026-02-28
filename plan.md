# Plan: Add Litecoin Support to Electrs

## Overview

Add Litecoin (mainnet, testnet, regtest) support to electrs as a compile-time feature flag `litecoin`, following the same pattern used by the `liquid` feature. Litecoin Core's RPC API is compatible with Bitcoin Core's, and Litecoin uses the same transaction/block serialization format, so the core indexing engine works without changes. The main work is in network constants, address handling, and configuration.

## Key Litecoin Differences from Bitcoin
- Different genesis block hash
- Different network magic bytes (`0xdbb6c0fb` mainnet, `0xfcc1b7dc` testnet)
- Different default RPC port (9332 mainnet, 19332 testnet)
- Different address version bytes (P2PKH `0x30`/`L`, P2SH `0x32`/`M` or `0x05`/`3`)
- Different default data directory (`~/.litecoin/`)
- 2.5 minute block time (does not affect indexing logic)
- Scrypt PoW (does not affect indexing — only miners care)
- MWEB (MimbleWimble Extension Blocks) — **out of scope for initial implementation**; MWEB transactions are extension blocks that standard RPC returns as normal, but full MWEB peg-in/peg-out tracking would be a follow-up

## Implementation Steps

### Step 1: Add `litecoin` feature flag to `Cargo.toml`

**File: `Cargo.toml`**

- Add `litecoin = []` to `[features]`
- No new dependencies needed — Litecoin uses the same serialization as Bitcoin, and `rust-bitcoin` types work for Litecoin block/tx data since the formats are identical

### Step 2: Extend the `Network` enum in `chain.rs`

**File: `src/chain.rs`**

- Add three new variants gated by `#[cfg(feature = "litecoin")]`:
  - `Litecoin` (mainnet)
  - `LitecoinTestnet`
  - `LitecoinRegtest`
- Implement `magic()` for Litecoin variants:
  - Litecoin mainnet: `0xdbb6c0fb`
  - Litecoin testnet: `0xfcc1b7dc`
  - Litecoin regtest: `0xdab5bffa` (same as Bitcoin regtest)
- Implement `genesis_hash()` for Litecoin:
  - Mainnet: `12a765e31ffd4059bada1e25190f6e98c0fe1666a68542019d52529ccc51ee1d`
  - Testnet4: `a0293e4eeb3da6e6f56f81ed595f57880d1a21569e13eefdd951284b5a626649`
  - Regtest: use the Bitcoin regtest genesis hash (same genesis block structure)
- Implement `is_regtest()` for `LitecoinRegtest`
- Add to `Network::names()`: `"litecoin"`, `"litecointestnet"`, `"litecoinregtest"`
- Add `From<&str>` mappings for the new network names
- For the `From<Network> for BNetwork` and `From<BNetwork> for Network` conversions: Litecoin variants should NOT convert to/from `BNetwork` since they aren't Bitcoin networks. These trait impls are gated behind `#[cfg(not(feature = "liquid"))]` — they need to also be gated behind `#[cfg(not(feature = "litecoin"))]`, OR the Litecoin feature should be mutually exclusive with default Bitcoin (like `liquid` is). **Decision: Make `litecoin` mutually exclusive with default Bitcoin mode, following the same `cfg` pattern as `liquid`.**

### Step 3: Update configuration defaults in `config.rs`

**File: `src/config.rs`**

- Add Litecoin variants to all `match network_type` blocks:
  - **Daemon RPC port**: 9332 (mainnet), 19332 (testnet), 19443 (regtest)
  - **Electrum RPC port**: 50001 (mainnet), 60001 (testnet), 60401 (regtest)
  - **HTTP port**: 3000 (mainnet), 3001 (testnet), 3002 (regtest)
  - **Monitoring port**: 4224 (mainnet), 14224 (testnet), 24224 (regtest)
- Update `get_network_subdir()`:
  - `Litecoin` → `None` (top-level `~/.litecoin/`)
  - `LitecoinTestnet` → `Some("testnet4")`
  - `LitecoinRegtest` → `Some("regtest")`
- Update default `daemon_dir` to `~/.litecoin` when litecoin feature is enabled
- Update help text references from "Bitcoind" / "bitcoind" to be more generic or add litecoin-specific text

### Step 4: Handle address encoding/validation

**File: `src/util/script.rs`**

- The `ScriptToAddr` implementation for Bitcoin uses `bitcoin::Address::from_script(self, bitcoin::Network::from(network))`. Since Litecoin addresses use different version bytes than Bitcoin, we cannot use `bitcoin::Network` directly.
- **Approach**: For Litecoin, implement custom address-to-string conversion that handles Litecoin's address prefixes:
  - Litecoin P2PKH: version byte `0x30` (addresses start with `L`)
  - Litecoin P2SH: version byte `0x32` (addresses start with `M`) and legacy `0x05` (start with `3`)
  - Litecoin Bech32: `ltc1` prefix (P2WPKH and P2WSH)
  - Testnet uses `tltc1` for bech32, `0x6f` for P2PKH, `0xc4` for P2SH

**File: `src/rest.rs`**

- Update `address_to_scripthash()` to handle Litecoin address parsing:
  - Parse Litecoin bech32 addresses (`ltc1...` / `tltc1...`)
  - Parse base58 addresses with Litecoin version bytes
  - Validate that the address matches the configured network
- This likely requires a small helper function or use of a litecoin address parsing crate

### Step 5: Update daemon.rs for Litecoin compatibility

**File: `src/daemon.rs`**

- The version check `network_info.version < 16_00_00` may need adjustment for Litecoin Core version numbers (Litecoin Core uses its own versioning scheme, e.g., 21.x maps differently)
- Update log messages: change "bitcoind" references to be network-aware or generic ("daemon")
- The RPC API is otherwise identical — `getblockchaininfo`, `getblock`, `getrawtransaction`, etc. all work the same

### Step 6: Update Electrum discovery default servers

**File: `src/electrum/discovery/default_servers.rs`**

- Add `#[cfg(feature = "litecoin")]` blocks with Litecoin Electrum server entries for `Network::Litecoin` and `Network::LitecoinTestnet`
- Known Litecoin Electrum servers can be sourced from Electrum-LTC server lists

### Step 7: Ensure feature flag mutual exclusivity

**File: `Cargo.toml`** and across `src/`

- The `litecoin` feature must be mutually exclusive with `liquid` (and ideally with default Bitcoin mode)
- Follow the same `#[cfg(feature = "litecoin")]` / `#[cfg(not(feature = "litecoin"))]` pattern used by `liquid`
- Update `#[cfg(not(feature = "liquid"))]` guards to also exclude litecoin: `#[cfg(all(not(feature = "liquid"), not(feature = "litecoin")))]` for Bitcoin-only code paths
- Add compile-time check (or document) that `litecoin` and `liquid` cannot be combined

### Step 8: Update tests

**Files: `tests/common.rs`, `tests/rest.rs`, `tests/electrum.rs`**

- Integration tests use `bitcoind` crate to spawn a Bitcoin Core node. For Litecoin, we'd need a `litecoind` test fixture. This is complex and can be deferred.
- Add unit tests for:
  - Litecoin genesis hash correctness
  - Litecoin magic byte correctness
  - Litecoin address parsing and validation (bech32 `ltc1` and base58)
  - Network name parsing round-trip

### Step 9: Update documentation

**Files: `README.md`, `CLAUDE.md`**

- Document the `litecoin` feature flag
- Add build/run instructions for Litecoin mode:
  ```bash
  cargo run --features litecoin --release --bin electrs -- -vvvv --daemon-dir ~/.litecoin
  ```
- Document MWEB limitation (not yet supported)

## Files Changed (Summary)

| File | Changes |
|------|---------|
| `Cargo.toml` | Add `litecoin` feature flag |
| `src/chain.rs` | Network enum variants, magic bytes, genesis hashes, conversions |
| `src/config.rs` | Default ports, data dirs, network subdirs, help text |
| `src/util/script.rs` | Litecoin address encoding (bech32 `ltc1`, base58 version bytes) |
| `src/rest.rs` | Litecoin address parsing in `address_to_scripthash()` |
| `src/daemon.rs` | Version check adjustment, generic log messages |
| `src/electrum/discovery/default_servers.rs` | Litecoin Electrum server list |
| `tests/` | Unit tests for Litecoin constants and address handling |
| `README.md` | Documentation for Litecoin feature |
| `CLAUDE.md` | Update feature flags section |

## Out of Scope (Future Work)

- **MWEB support**: MimbleWimble Extension Blocks require tracking peg-in/peg-out transactions between the main chain and the extension block. This is analogous to Liquid's peg handling and would be a significant follow-up feature.
- **Litecoin integration tests**: Requires a `litecoind` test harness crate similar to the `bitcoind` crate used for Bitcoin tests.
- **Scrypt PoW verification**: Not needed — electrs trusts the daemon's chain validation and doesn't verify PoW.

## Risk Assessment

- **Low risk**: Network constants, ports, directory paths — straightforward additions following existing patterns.
- **Medium risk**: Address handling — Litecoin uses different bech32 HRP (`ltc1` vs `bc1`) and different base58 version bytes. The `rust-bitcoin` crate's `Address` type is Bitcoin-specific and won't natively handle Litecoin addresses. We may need to add a dependency like `litecoin-address` or implement custom parsing.
- **Low risk**: RPC compatibility — Litecoin Core's JSON-RPC API mirrors Bitcoin Core's closely. The same `getblockchaininfo`, `getblock`, `getrawmempool`, etc. calls work identically.
