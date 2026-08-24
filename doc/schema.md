# Index Schema

The index is stored as a RocksDB database with four column families:

- `default` - global metadata
- `txstore` - block, transaction and output rows
- `history` - scripthash history and spend rows
- `cache` - aggregated script and asset caches

### Indexing process

Electrs indexes blocks using the bitcoind binary REST `block` endpoint,
storing blocks, transactions, outputs and scripthash funding/spending history.

There are two indexing modes:

- Default (non-Elements) mode: when supported by Bitcoin Core (v30+), fetches
  previous outputs from its binary REST `spenttxouts` endpoint. Because indexing
  does not depend on local RocksDB TXO lookups, blocks can be fully processed in
  parallel and in any order.

- Legacy mode: used automatically for Elements or when Bitcoin Core does not
  support `spenttxouts`. Indexing is done in two phases, where each can be done
  concurrently within itself: first populate `txstore` with
  block/transaction/output data, then populate `history` by looking up spent
  TXOs in `txstore`.

After the indexing is completed, both funding and spending are indexed as independent
rows under `H{scripthash}`, so that they can be queried in-order in one go.

### `default`

Global metadata rows:

 * `"t" → "{blockhash}"` (the synced tip)

 * `"V" → "{db-version}"` (compatibility marker)

### `txstore`

Each block results in the following new rows:

 * `"B{blockhash}" → "{height, header}"`

 * `"X{blockhash}" → "{txids}"` (list of txids included in the block)

 * `"M{blockhash}" → "{metadata}"` (block weight, size and number of txs)

 * `"D{blockhash}" → ""` (signifies the block was added)

Each transaction results in the following new row:

 * `"T{txid}" → "{serialized-transaction}"`

Each output results in the following new rows:

 * `"O{txid}{vout}" → "{scriptpubkey}{value}"`
 * `"a{funding-address-str}" → ""` (for prefix address search, only saved when `--address-search` is enabled)

### `history`

Each transaction results in the following new row:

 * `"C{txid}" → "{confirmed-height}"`

Each funding output (except for provably unspendable ones when `--index-unspendables` is not enabled) results in the following new row (`H` is for history, `F` is for funding):

 * `"H{funding-scripthash}{funding-height}F{funding-txid:vout}{value}" → ""`

Each spending input (except the coinbase) results in the following new rows (`S` is for spending):

 * `"H{funding-scripthash}{spending-height}S{spending-txid:vin}{funding-txid:vout}{value}" → ""`

 * `"S{funding-txid:vout}" → "{spending-txid:vin}{spending-height}"`

Each block results in the following new row:

 * `"D{blockhash}" → ""` (signifies the block was indexed)

#### Elements only

Assets (re)issuances results in the following new rows (only for user-issued assets):

 * `"i{asset-id}" → "{issuing-txid:vin}{prev-txid:vout}{issuance}{reissuance_token}"`
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

 * `"A{scripthash}" → "{stats}{blockhash}"` (where `stats` is composed of `tx_count`, `funded_txo_{count,sum}` and `spent_txo_{count,sum}`)

 * `"U{scripthash}" → "{utxo}{blockhash}"` (where `utxo` is a set of `(txid,vout)` outpoints)

#### Elements only:

Stats for issued assets:
 * `"z{asset-id}" → "{issued_stats}{blockhash}"` (where `issued_stats` is composed of `tx_count`, `issuance_count`, `issued_amount`, `burned_amount`, `has_blinded_issuances`, `reissuance_tokens`, `burned_reissuance_tokens`)

Stats for the native asset:
 * `"z{issued-asset}" → "{native_stats}{blockhash}"` (where `native_stats` is composed of `tx_count`, `peg_in_count`, `peg_in_amount`, `peg_out_count`, `peg_out_amount`, `burn_count` and `burn_amount`)
