# Indexer Consistency And Crash Recovery

## Purpose

This document describes indexer consistency, live public API visibility, crash
recovery, and publication behavior. It records the failure modes and visibility
scenarios considered for the indexer and the reason the current algorithm
should expose and recover a consistent state.

Use this when reviewing future changes to `Store::open()`, `Indexer::update()`,
`HeaderList` visibility, completion markers, stale cleanup, or persisted tip
publication.

## Core Invariants

The indexer is consistent and crash-recoverable if these invariants hold. Treat
this section as the review checklist when changing indexer publication or
recovery code.

### Visibility And Staging

- The indexer has two publication cutoffs: persisted `t` for startup visibility
  and in-memory `indexed_headers` (`HeaderList`) for live public API visibility.
  Both treat rows beyond the visible tip as staging data.
- Public best-chain APIs derive block visibility, confirmations, and
  height-based history visibility from the current `indexed_headers` chain.
- During forward indexing, new block rows are written before `indexed_headers`
  is appended, so partial new-block best-chain state is hidden from public APIs
  until the update completes.
- During reorg cleanup, `indexed_headers` is first rolled back to the common
  ancestor. This immediately hides stale headers and height-based stale history
  from public APIs; stale deletion and replacement indexing then run in the
  staging area.
- On startup, `Store::open()` reconstructs visibility from persisted `t` and
  defensively trims it to the contiguous prefix where both completion markers
  exist. Under current publication rules this trim should not be needed in
  normal operation; it is a fail-closed guard if `t` and completion markers
  disagree.

### Completion State

- Txstore and history each have a per-block `D` marker, written atomically with
  that side's rows, recording that the corresponding side of the block is
  complete.
- A block is fully indexed only when both txstore and history are complete.
- A block's txstore-side rows required for recovery are written before, or in
  the same atomic write as, that block's history-side rows.

### Startup Recovery

- The first daemon-aware `Indexer::update()` keeps `new_headers` and the current
  `indexed_headers` chain. History-complete blocks outside that set are
  stale and are undone.
- Startup recovery runs before live reorg rollback, so live-reorg stale headers
  are still in `indexed_headers` and are left for the normal reorg path.

### Stale Cleanup And Reorg Publication

- Live reorg cleanup and startup stale cleanup both reconstruct stale blocks
  and spent prevouts from local RocksDB data, then delete stale history rows
  through `process_stale()` and `undo_index()`.
- On reorg, the live `HeaderList` tip and persisted `t` are rolled back to the
  common ancestor before stale history rows are undone and replacement rows are
  indexed in the staging area.
- Stale history deletion is written and flushed before replacement history rows.
  Write order protects same-height keys; the flush is the crash barrier before
  replacement indexing.
- Stale cleanup deletes history rows only. Txstore rows for stale blocks are
  cheap to keep around, stay outside the history-index lookup hot path, can be
  reused if the block becomes best chain again, and may be useful for future
  archival access. These rows are not public best-chain state unless
  `indexed_headers` contains the block.
- The live `HeaderList` tip and persisted `t` advance only after stale cleanup,
  block processing, and required flushes have completed.

### Storage Durability

- Txstore and history live in column families in one RocksDB database with
  atomic flush enabled.
- RocksDB may flush memtables before `Store::flush_block_writes()` runs. This is
  safe: atomic flushes publish a coherent sequence-number cutoff across column
  families and cannot split a write batch.
- Therefore, a durable history `D` implies the required txstore-side recovery
  rows are durable from the same or an earlier flush. Early durable rows remain
  unpublished until `t` and `indexed_headers` advance; restart uses completion
  markers to skip, finish, or undo them.
- When bulk writes are disabled, `Store::flush_block_writes()` still atomically
  flushes txstore and history before the final `t` advance is published.
- Persisted tip `t` is written synchronously.

## Failure And Visibility Matrix

### Startup And Initial Sync

1. **No `t` exists and initial sync crashed**

   `Store::open()` starts with an empty visible chain. The first `update()`
   downloads the current best chain from genesis. Blocks that are already
   complete are skipped by `headers_to_process()`, incomplete blocks are
   finished, and history-complete blocks outside the new best chain are undone.

   Result: correct.

2. **`t` points behind durable completed best-chain work**

   Completed blocks beyond `t` are initially hidden because `Store::open()` only
   rebuilds from the stored `t` chain. The first `update()` receives those blocks
   as `new_headers`, skips any sides that are already complete, finishes missing
   work, and republishes `t`.

   Result: correct.

3. **The stored `t` chain has an incomplete suffix**

   Current publication rules write `t` only after block processing and required
   flushes complete, so normal crash recovery should not produce this state. If
   the durable markers and stored `t` nevertheless disagree, `Store::open()`
   rebuilds the stored `t` chain, then walks forward from genesis and trims at
   the first block missing either completion marker. The incomplete suffix is
   hidden until the first `update()` finishes it or undoes it if it became
   stale.

   Result: correct.

4. **Crash during `Store::open()`**

   `Store::open()` does not mutate the database. A crash while opening leaves
   the database unchanged, so the next startup repeats the same derivation.

   Result: correct.

### Forward Processing And Publication

5. **Crash after txstore completion but before history completion**

   Txstore `D` exists and history `D` does not. `Store::open()` hides the block
   unless all prior blocks are complete and this block is later completed. The
   next `update()` sees `need_history = true` and writes the missing history
   side if the block remains best chain.

   Result: correct.

6. **History completion exists without txstore recovery data**

   Txstore-side recovery data is written before, or in the same atomic write as,
   history `D`, and atomic flush makes a durable history `D` imply the required
   txstore rows are durable. This state is not expected from normal crash
   recovery. If it exists anyway, `Store::open()` hides the block because both
   markers are required. The next `update()` writes the missing txstore side if
   the block remains best chain.

   Result: correct.

7. **Crash after both sides complete, before final `t` publication**

   Both completion markers exist, but public visibility may still be behind.
   The first `update()` gets the completed blocks as `new_headers`, skips their
   completed sides, and publishes `t` after confirming the update is complete.

   Result: correct.

8. **Public API query while new blocks are staged**

   New block rows may already exist in RocksDB, but `indexed_headers` has not
   advanced yet. Best-chain APIs derive block visibility, confirmations, and
   height-based history from `indexed_headers`, so staged rows are not exposed
   as confirmed best-chain state.

   Result: correct.

9. **Crash after final `t` publication, before in-memory append**

   The final daemon-tip `t` advance is written with `put_sync()` only after
   block processing and required flushes. On restart, `Store::open()`
   reconstructs visibility from that persisted tip and the completion markers.

   Result: correct.

10. **Spenttxouts workers complete blocks out of order**

   Later blocks may have both `D` markers before earlier blocks do.
   Persisted `t` is not published to the target tip until all blocks in the
   update range and required flushes complete. On restart, completed blocks
   beyond `t` remain staged; the next `update()` fills gaps and skips
   already-complete blocks.

   Result: correct.

11. **Legacy batch processing crashes mid-batch**

    Legacy forward mode uses the same txstore/history completion markers per
    side. Restart uses the shared marker logic: txstore-complete and
    history-incomplete blocks are indexed, fully complete blocks are skipped,
    and incomplete blocks remain hidden until finished.

    Result: correct.

### Best-Chain Blocks That Become Stale While Electrs Is Down

12. **Crash during forward processing, then the partially processed suffix
    becomes stale**

    Persisted `t` keeps the partially processed suffix hidden on startup.
    The first daemon-aware `update()` keeps `new_headers` and the current
    `indexed_headers` chain. Because startup recovery runs before live reorg
    rollback, any live-reorg stale suffix is still in `indexed_headers` and is
    left for the normal reorg path. Any other history-complete block is
    stale and is reconstructed from local txstore rows for undo.

    Result: correct.

13. **Crash during initial sync with no published `t`, then indexed blocks
    become stale**

    With no `t`, the visible chain starts empty. The first `update()` downloads
    the current best chain from genesis. History-complete blocks not in that
    chain are found by startup stale recovery and undone.

    Result: correct.

14. **Startup recovery finds no new headers but stale history exists**

    This can happen if the daemon tip appears unchanged relative to the retained
    prefix but hidden history-complete stale blocks exist. The pending startup
    sweep still runs once and undoes history-complete blocks outside
    `indexed_headers`.

    Result: correct.

### Live Reorg And Stale Cleanup

15. **Crash before persisting rollback to the common ancestor**

    The in-memory pop is lost with the process. On restart, `Store::open()` uses
    the old persisted `t` plus completion markers, and the next `update()`
    redetects the reorg against bitcoind.

    Result: correct.

16. **Crash after persisting rollback, before stale rows are deleted**

    Persisted `t` points to the common ancestor. Stale rows are hidden from
    public visibility. The next startup's first `update()` sees stale
    history-complete blocks outside the target chain and undoes them.

    Result: correct.

17. **Public API query during stale cleanup**

    `indexed_headers` has already been rolled back to the common ancestor. Stale
    headers and height-based stale history are hidden from public APIs even if
    the stale history rows have not yet been deleted.

    Result: correct.

18. **Crash during stale history deletion**

    Deleting stale history rows is idempotent. If the delete batch was lost, the
    stale history `D` remains and startup recovery undoes it again. If the delete
    batch survived, the stale `D` is gone and there is nothing left to undo.

    Result: correct.

19. **Crash after stale deletion, before the history flush**

    RocksDB will expose either the old state or the deleted state after restart.
    The startup sweep handles both: stale `D` present means undo again; stale `D`
    absent means no action.

    Result: correct.

20. **Crash after stale deletion flush, before replacement blocks are indexed**

    Stale rows are durably gone and persisted `t` is at the common ancestor. The
    next `update()` indexes the replacement `new_headers`.

    Result: correct.

21. **Crash after replacement rows are written, before final `t` publication**

    Replacement rows may be durable, but public visibility remains at the common
    ancestor. On restart, replacement blocks are in `new_headers`; completed
    sides are skipped, incomplete sides are finished, and `t` is published.

    Result: correct.

22. **Stale and replacement blocks share a height**

    Stale history deletion is written before replacement history rows and
    flushed before replacement indexing. Write order protects same-height keys;
    the flush keeps that order durable across a crash while WAL-disabled writes
    may be in use.

    Result: correct.

23. **Manual chain shortening with no new headers**

    `HeaderList::preprocess()` can return `reorged_since` even when
    `new_headers` is empty. The normal stale path pops the suffix, persists the
    common ancestor tip, undoes stale rows, and republishes.

    Result: correct.

### Daemon And Retry Failures

24. **Daemon reorgs again while `update()` is running**

    Electrs finishes or fails the update against the tip observed at the start.
    If it finishes, the next `update()` detects the newer reorg. If fetches fail,
    `update()` returns an error before final publication, leaving recovery to
    retry from durable markers.

    Result: eventually correct; transient lag or retry is expected.

25. **Daemon/RPC/REST fetch fails during forward processing**

    `process_blocks()` returns an error before final `t` publication. Any
    completed sides are represented by `D` markers and will be skipped next time;
    incomplete sides are retried.

    Result: correct after retry.

26. **Current daemon cannot serve stale block during stale undo**

    Stale cleanup reconstructs stale blocks and spent prevouts from local
    RocksDB data. It does not need the current daemon to serve stale block
    contents, so a multiple-backend setup can still undo stale history even if
    the daemon that originally served the stale block is no longer reachable.

    Result: correct.

27. **In-process retry after startup sweep succeeds but later processing fails**

    The startup flag may already be cleared in memory, but stale deletion has
    already been completed and flushed before that happens. Later retries only
    need normal forward/reorg processing.

    Result: correct.
