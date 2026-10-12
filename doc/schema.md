# Index Schema

The index is stored as three RocksDB databases:

- `txstore`
- `history`
- `cache`

### Indexing process

The indexing is done in the two phase, where each can be done concurrently within itself.
The first phase populates the `txstore` database, the second phase populates the `history` database.

NOTE: in order to construct the history rows for spending inputs in phase #2, we rely on having the transactions being processed at phase #1, so they can be looked up efficiently (using parallel point lookups).

After the indexing is completed, both funding and spending are indexed as independent rows under `H{scripthash}`, so that they can be queried in-order in one go.

### `txstore`

Each block results in the following new rows:

 * `"B{blockhash}" → "{header}"`

 * `"X{blockhash}" → "{txids}"` (list of txids included in the block)

 * `"M{blockhash}" → "{metadata}"` (block weight, size and number of txs)

 * `"D{blockhash}" → ""` (signifies the block was added)

Each transaction results in the following new row:

 * `"T{txid}" → "{serialized-transaction}"`

Each output results in the following new rows:

 * `"O{txid}{vout}" → "{scriptpubkey}{value}"`
 * `"a{funding-address-str}" → ""` (for prefix address search, only saved when `--address-search` is enabled)

When the indexer is synced up to the tip of the chain, the hash of the tip is saved as following:

 * `"t" →  "{blockhash}"`

Before undoing blocks removed from the saved chain, `t` is moved back to the common ancestor. Repairing stale blocks left by older versions can also affect rows shared with already-indexed active blocks; `R` tracks their restoration.

Rows for reorged blocks are kept, so their transactions remain available and cleanup can rebuild those blocks without bitcoind.

### `history`

Each transaction results in the following new row:

 * `"C{txid}" → "{confirmed-height}"`

Each funding output (except for provably unspendable ones when `--index-unspendables` is not enabled) results in the following new row (`H` is for history, `F` is for funding):

 * `"H{funding-scripthash}{funding-height}F{funding-txid:vout}{value}" → ""`

Each spending input (except the coinbase) results in the following new rows (`S` is for spending):

 * `"H{funding-scripthash}{spending-height}S{spending-txid:vin}{funding-txid:vout}{value}" → ""`

 * `"S{funding-txid:vout}" → "{spending-txid:vin}{spending-height}"`

Each block results in the following new row:

 * `"D{blockhash}" → "{height}"` (signifies the block was indexed; `height` is a 4-byte little-endian u32. Older versions wrote an empty value.)

The height lets indexed blocks above a fetch gap, which are not in the saved header chain, be located and undone if the chain switches branch.

While stale-block cleanup is in progress, the following rows may be present:

 * `"u{blockhash}" → "{height}"` (4-byte little-endian u32: the block has partially undone history, with spending rows still to remove. Replaces `D` atomically with the first deletion batch, so a block that becomes active again is reindexed. Removed atomically when undo or reindexing completes.)
 * `"R" → "{height}"` (4-byte little-endian u32: the confirmation, spending-edge and issuance rows of indexed active blocks from the affected fork boundary up still need rewriting. Written before cleanup deletes anything and removed after all rewrite batches are durable.)

Older versions ignore `u` and/or `R` and cannot finish this recovery protocol. Downgrading during cleanup can leave missing or stale history until a version supporting both markers completes recovery. Before rolling back, confirm `history_db` has no `R` key and no key starting with `u`, and that the `stale_cleanup_failing` gauge is 0; once cleanup completes, both keys are gone and rolling back is safe.

While cleanup is failing (the `stale_cleanup_failing` gauge is 1) after a reorg or above a gap, indexing stays at the common ancestor, so the mempool is not synced either and new unconfirmed transactions are not indexed until cleanup succeeds. A failure that leaves the saved tip at the daemon tip (for example a pending `R` rewrite on a quiet chain) does not stall the mempool.

#### Elements only

Assets (re)issuances results in the following new rows (only for user-issued assets):

 * `"i{asset-id}" → "{issuing-txid:vin}{prev-txid:vout}{issuance}{reissuance_token}"` (`reissuance_token` is unused: it is derived from `prev-txid:vout` and `issuance` on read, since rows indexed by older versions may hold a token id derived from the wrong privacy bit. Being the last field, it can be dropped with the next `DB_VERSION` bump without a migration, since rows are read with trailing bytes allowed)
 * `"I{asset-id}{issuance-height}I{issuing-txid:vin}{is_reissuance}{amount}{tokens}" → ""`

Peg-ins/peg-outs results in the following new rows (only for the native asset, typically L-BTC):

 * `"I{asset-id}{pegin-height}F{pegin-txid:vin}{value}" → ""`
 * `"I{asset-id}{pegout-height}F{pegout-txid:vout}{value}" → ""`

Every burn (unspendable output) results in the following new row (both user-issued and native):

 * `"I{asset-id}{burn-height}F{burning-txid:vout}{value}" → ""`

### `cache`

Holds a cache for aggregated stats and unspent TXOs of scripthashes.

The cache is created on-demand, the first time the scripthash is requested by a user.

The cached data is kept next to the `blockhash` the cache is up-to-date for.
When requesting data, the cache is updated with the new history rows added since the `blockhash`.
If the `blockhash` was since orphaned, the cache is removed and re-computed.
Stale-history cleanup also invalidates affected script and asset caches, since older versions may have cached stale rows under an active block hash. Invalidation is durable before history deletion and excludes concurrent cache updates.

 * `"A{scripthash}" → "{stats}{blockhash}"` (where `stats` is composed of `tx_count`, `funded_txo_{count,sum}` and `spent_txo_{count,sum}`)

 * `"U{scripthash}" → "{utxo}{blockhash}"` (where `utxo` is a set of `(txid,vout)` outpoints)

#### Elements only:

Stats for issued assets:
 * `"z{asset-id}" → "{issued_stats}{blockhash}"` (where `issued_stats` is composed of `tx_count`, `issuance_count`, `issued_amount`, `burned_amount`, `has_blinded_issuances`, `reissuance_tokens`, `burned_reissuance_tokens`)

Stats for the native asset:
 * `"z{issued-asset}" → "{native_stats}{blockhash}"` (where `native_stats` is composed of `tx_count`, `peg_in_count`, `peg_in_amount`, `peg_out_count`, `peg_out_amount`, `burn_count` and `burn_amount`)
