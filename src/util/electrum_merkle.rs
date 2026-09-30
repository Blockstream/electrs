use crate::chain::{BlockHash, Txid};
use crate::errors::*;
use crate::new_index::ChainQuery;
use bitcoin::hashes::{sha256d::Hash as Sha256dHash, Hash};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use electrs_macros::trace;

const MAX_CACHE_ENTRIES: usize = 256;

struct CachedLevels {
    cp_hash: BlockHash,
    levels: Vec<Vec<Sha256dHash>>,
    size_bytes: usize,
}

impl CachedLevels {
    fn new(cp_hash: BlockHash, levels: Vec<Vec<Sha256dHash>>) -> Self {
        let size_bytes = levels.iter().map(|level| level.len()).sum::<usize>()
            * std::mem::size_of::<Sha256dHash>();
        Self {
            cp_hash,
            levels,
            size_bytes,
        }
    }
}

struct RebuildPermit<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> RebuildPermit<'a> {
    fn acquire(counter: &'a AtomicUsize, limit: usize) -> Result<Self> {
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            if current >= limit {
                bail!("too many concurrent checkpoint merkle proof rebuilds, try again later");
            }
            match counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(Self { counter }),
                Err(observed) => current = observed,
            }
        }
    }
}

impl<'a> Drop for RebuildPermit<'a> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

struct MemoryPermit<'a> {
    counter: &'a AtomicUsize,
    weight_bytes: usize,
}

impl<'a> MemoryPermit<'a> {
    fn acquire(counter: &'a AtomicUsize, limit_bytes: usize, weight_bytes: usize) -> Result<Self> {
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            if current > 0 && current.saturating_add(weight_bytes) > limit_bytes {
                bail!("checkpoint merkle proof rebuild would exceed the configured memory budget, try again later");
            }
            match counter.compare_exchange_weak(
                current,
                current + weight_bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(Self {
                        counter,
                        weight_bytes,
                    })
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl<'a> Drop for MemoryPermit<'a> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.weight_bytes, Ordering::AcqRel);
    }
}

fn estimated_peak_build_bytes(cp_height: usize) -> usize {
    (cp_height + 1)
        .saturating_mul(std::mem::size_of::<Sha256dHash>())
        .saturating_mul(3)
}

struct MemoryCharge {
    counter: Arc<AtomicUsize>,
    bytes: usize,
}

impl MemoryCharge {
    fn new(counter: Arc<AtomicUsize>, bytes: usize) -> Self {
        counter.fetch_add(bytes, Ordering::AcqRel);
        Self { counter, bytes }
    }
}

impl Drop for MemoryCharge {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct RetainedTree {
    tree: Arc<CachedLevels>,
    _charge: MemoryCharge,
}

struct BuildSlot {
    waiters: AtomicUsize,
    last_built: Mutex<Option<RetainedTree>>,
}

struct BuildSlotGuard<'a> {
    cache: &'a CheckpointMerkleCache,
    cp_height: usize,
    slot: Arc<BuildSlot>,
}

impl<'a> Drop for BuildSlotGuard<'a> {
    fn drop(&mut self) {
        let mut locks = self.cache.build_locks.lock().unwrap();
        if self.slot.waiters.fetch_sub(1, Ordering::AcqRel) == 1 {
            if let Some(existing) = locks.get(&self.cp_height) {
                if Arc::ptr_eq(existing, &self.slot) {
                    locks.remove(&self.cp_height);
                }
            }
        }
    }
}

pub struct CheckpointMerkleCache {
    entries: Mutex<Vec<(usize, Arc<CachedLevels>)>>,
    build_locks: Mutex<HashMap<usize, Arc<BuildSlot>>>,
    inflight_rebuilds: AtomicUsize,
    inflight_build_bytes: Arc<AtomicUsize>,
    capacity_bytes: usize,
    cache_capacity_bytes: usize,
}

impl CheckpointMerkleCache {
    /// Cached trees may use half of `capacity_bytes`, so a full cache always leaves at least
    /// the other half for rebuild working memory.
    pub fn new(capacity_bytes: usize) -> Self {
        Self::with_cache_capacity(capacity_bytes, capacity_bytes / 2)
    }

    fn with_cache_capacity(capacity_bytes: usize, cache_capacity_bytes: usize) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            build_locks: Mutex::new(HashMap::new()),
            inflight_rebuilds: AtomicUsize::new(0),
            inflight_build_bytes: Arc::new(AtomicUsize::new(0)),
            capacity_bytes,
            cache_capacity_bytes: cache_capacity_bytes.min(capacity_bytes),
        }
    }

    fn get_valid(&self, cp_height: usize, cp_hash: BlockHash) -> Option<Arc<CachedLevels>> {
        let entries = self.entries.lock().unwrap();
        entries.iter().find_map(|(h, cached)| {
            (*h == cp_height && cached.cp_hash == cp_hash).then(|| cached.clone())
        })
    }

    fn insert(&self, cp_height: usize, best_height: usize, cached: Arc<CachedLevels>) {
        if cached.size_bytes > self.cache_capacity_bytes {
            return;
        }

        let mut entries = self.entries.lock().unwrap();
        entries.retain(|(h, _)| *h != cp_height && *h <= best_height);
        if cp_height > best_height {
            return;
        }

        let mut total_bytes: usize = entries.iter().map(|(_, c)| c.size_bytes).sum();
        let mut remaining_entries = entries.len();
        let mut eviction_positions: Vec<usize> = entries
            .iter()
            .enumerate()
            .filter_map(|(pos, (height, _))| (*height < cp_height).then_some(pos))
            .collect();
        eviction_positions.sort_unstable_by_key(|pos| entries[*pos].0);

        let mut evictions_needed = 0;
        while total_bytes + cached.size_bytes > self.cache_capacity_bytes
            || remaining_entries >= MAX_CACHE_ENTRIES
        {
            let Some(pos) = eviction_positions.get(evictions_needed).copied() else {
                return;
            };
            total_bytes -= entries[pos].1.size_bytes;
            remaining_entries -= 1;
            evictions_needed += 1;
        }

        let mut selected_positions = eviction_positions[..evictions_needed].to_vec();
        selected_positions.sort_unstable_by(|a, b| b.cmp(a));
        for pos in selected_positions {
            entries.remove(pos);
        }

        entries.push((cp_height, cached));
    }

    fn build_lock_for(&self, cp_height: usize) -> BuildSlotGuard<'_> {
        let mut locks = self.build_locks.lock().unwrap();
        let slot = locks
            .entry(cp_height)
            .or_insert_with(|| {
                Arc::new(BuildSlot {
                    waiters: AtomicUsize::new(0),
                    last_built: Mutex::new(None),
                })
            })
            .clone();
        slot.waiters.fetch_add(1, Ordering::AcqRel);
        BuildSlotGuard {
            cache: self,
            cp_height,
            slot,
        }
    }

    fn get_or_build(
        &self,
        cp_height: usize,
        current_best_height: impl FnOnce() -> usize,
        concurrency_limit: usize,
        snapshot_cp_hash_and_sibling: impl Fn() -> Result<(BlockHash, Sha256dHash)>,
        build: impl FnOnce() -> Result<(BlockHash, Vec<Vec<Sha256dHash>>, Sha256dHash)>,
    ) -> Result<(Arc<CachedLevels>, Sha256dHash)> {
        let (cp_hash, sibling) = snapshot_cp_hash_and_sibling()?;
        if let Some(cached) = self.get_valid(cp_height, cp_hash) {
            return Ok((cached, sibling));
        }

        let build_lock = self.build_lock_for(cp_height);
        self.build_under_lock(
            cp_height,
            current_best_height,
            cp_hash,
            sibling,
            concurrency_limit,
            &build_lock.slot,
            build,
        )
    }

    fn build_under_lock(
        &self,
        cp_height: usize,
        current_best_height: impl FnOnce() -> usize,
        cp_hash: BlockHash,
        sibling: Sha256dHash,
        concurrency_limit: usize,
        build_lock: &BuildSlot,
        build: impl FnOnce() -> Result<(BlockHash, Vec<Vec<Sha256dHash>>, Sha256dHash)>,
    ) -> Result<(Arc<CachedLevels>, Sha256dHash)> {
        let mut last_built = build_lock
            .last_built
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(cached) = self.get_valid(cp_height, cp_hash) {
            return Ok((cached, sibling));
        }
        if let Some(retained) = last_built.as_ref().filter(|r| r.tree.cp_hash == cp_hash) {
            return Ok((retained.tree.clone(), sibling));
        }

        let _permit = RebuildPermit::acquire(&self.inflight_rebuilds, concurrency_limit)?;
        let entries = self.entries.lock().unwrap();
        let resident_bytes: usize = entries.iter().map(|(_, cached)| cached.size_bytes).sum();
        let _memory_permit = MemoryPermit::acquire(
            &self.inflight_build_bytes,
            self.capacity_bytes.saturating_sub(resident_bytes),
            estimated_peak_build_bytes(cp_height),
        )?;
        drop(entries);
        let (cp_hash, levels, sibling) = build()?;
        let cached = Arc::new(CachedLevels::new(cp_hash, levels));
        self.insert(cp_height, current_best_height(), cached.clone());
        *last_built = Some(RetainedTree {
            _charge: MemoryCharge::new(self.inflight_build_bytes.clone(), cached.size_bytes),
            tree: cached.clone(),
        });
        Ok((cached, sibling))
    }
}

fn merklize(left: Sha256dHash, right: Sha256dHash) -> Sha256dHash {
    let mut data = [0u8; 64];
    data[..32].copy_from_slice(&left[..]);
    data[32..].copy_from_slice(&right[..]);
    Sha256dHash::hash(&data)
}

fn build_levels_above_leaves(leaves: &[Sha256dHash]) -> Result<Vec<Vec<Sha256dHash>>> {
    ensure!(!leaves.is_empty(), "missing block headers");
    let mut current: Vec<Sha256dHash> = leaves.to_vec();

    let mut levels = Vec::new();
    while current.len() > 1 {
        if current.len() % 2 != 0 {
            let last = *current.last().unwrap();
            current.reserve_exact(1);
            current.push(last);
        }
        current = current
            .chunks(2)
            .map(|pair| merklize(pair[0], pair[1]))
            .collect();
        levels.push(current.clone());
    }
    Ok(levels)
}

fn extract_branch_and_root(
    levels_above_leaves: &[Vec<Sha256dHash>],
    sibling_leaf: Sha256dHash,
    mut index: usize,
) -> Result<(Vec<Sha256dHash>, Sha256dHash)> {
    let mut merkle = vec![sibling_leaf];
    index /= 2;

    for level in &levels_above_leaves[..levels_above_leaves.len().saturating_sub(1)] {
        let len = level.len();
        let sibling_index = if index % 2 == 0 { index + 1 } else { index - 1 };
        let sibling = if sibling_index < len {
            level[sibling_index]
        } else {
            level[len - 1]
        };
        merkle.push(sibling);
        index /= 2;
    }

    let root = *levels_above_leaves
        .last()
        .chain_err(|| "empty checkpoint merkle tree")?
        .last()
        .unwrap();
    Ok((merkle, root))
}

#[trace]
pub fn get_tx_merkle_proof(
    chain: &ChainQuery,
    tx_hash: &Txid,
    block_hash: &BlockHash,
) -> Result<(Vec<Sha256dHash>, usize)> {
    let txids = chain
        .get_block_txids(&block_hash)
        .chain_err(|| format!("missing block txids for #{}", block_hash))?;
    let pos = txids
        .iter()
        .position(|txid| txid == tx_hash)
        .chain_err(|| format!("missing txid {}", tx_hash))?;
    let txids = txids.into_iter().map(Sha256dHash::from).collect();

    let (branch, _root) = create_merkle_branch_and_root(txids, pos);
    Ok((branch, pos))
}

#[trace]
pub fn get_header_merkle_proof(
    chain: &ChainQuery,
    height: usize,
    cp_height: usize,
    checkpoint_proof_concurrency_limit: usize,
) -> Result<(Vec<Sha256dHash>, Sha256dHash, BlockHash)> {
    if cp_height < height {
        bail!("cp_height #{} < height #{}", cp_height, height);
    }

    let best_height = chain.best_height();
    if best_height < cp_height {
        bail!(
            "cp_height #{} above best block height #{}",
            cp_height,
            best_height
        );
    }

    if cp_height == 0 {
        let headers = chain.store().headers();
        let cp_hash = *headers
            .header_by_height(0)
            .chain_err(|| "missing block header at height 0")?
            .hash();
        let root = Sha256dHash::from(cp_hash);
        return Ok((vec![], root, cp_hash));
    }

    let sibling_index = if height % 2 == 0 {
        height + 1
    } else {
        height - 1
    };
    let sibling_height = sibling_index.min(cp_height);

    let snapshot_cp_hash_and_sibling = || -> Result<(BlockHash, Sha256dHash)> {
        let headers = chain.store().headers();
        let cp_hash = *headers
            .header_by_height(cp_height)
            .chain_err(|| format!("missing block header at height {}", cp_height))?
            .hash();
        let sibling = Sha256dHash::from(
            *headers
                .header_by_height(sibling_height)
                .chain_err(|| format!("missing block header at height {}", sibling_height))?
                .hash(),
        );
        Ok((cp_hash, sibling))
    };

    let build = || -> Result<(BlockHash, Vec<Vec<Sha256dHash>>, Sha256dHash)> {
        let headers = chain.store().headers();
        let cp_hash = *headers
            .header_by_height(cp_height)
            .chain_err(|| format!("missing block header at height {}", cp_height))?
            .hash();
        let leaves: Vec<Sha256dHash> = (0..=cp_height)
            .map(|h| {
                headers
                    .header_by_height(h)
                    .map(|entry| Sha256dHash::from(*entry.hash()))
            })
            .collect::<Option<Vec<_>>>()
            .chain_err(|| "missing block headers")?;
        drop(headers);

        let sibling = leaves[sibling_height];
        let levels = build_levels_above_leaves(&leaves)?;
        Ok((cp_hash, levels, sibling))
    };

    let (cached, sibling) = chain.checkpoint_merkle_cache().get_or_build(
        cp_height,
        || chain.best_height(),
        checkpoint_proof_concurrency_limit,
        snapshot_cp_hash_and_sibling,
        build,
    )?;

    let (branch, root) = extract_branch_and_root(&cached.levels, sibling, height)?;
    Ok((branch, root, cached.cp_hash))
}

#[trace]
pub fn get_id_from_pos(
    chain: &ChainQuery,
    height: usize,
    tx_pos: usize,
    want_merkle: bool,
) -> Result<(Txid, Vec<Sha256dHash>)> {
    let header_hash = chain
        .hash_by_height(height)
        .chain_err(|| format!("missing block #{}", height))?;

    let txids = chain
        .get_block_txids(&header_hash)
        .chain_err(|| format!("missing block txids #{}", height))?;

    let txid = *txids
        .get(tx_pos)
        .chain_err(|| format!("No tx in position #{} in block #{}", tx_pos, height))?;

    let txids = txids.into_iter().map(Sha256dHash::from).collect();

    let branch = if want_merkle {
        create_merkle_branch_and_root(txids, tx_pos).0
    } else {
        vec![]
    };
    Ok((txid, branch))
}

fn create_merkle_branch_and_root(
    mut hashes: Vec<Sha256dHash>,
    mut index: usize,
) -> (Vec<Sha256dHash>, Sha256dHash) {
    let mut merkle = vec![];
    while hashes.len() > 1 {
        if hashes.len() % 2 != 0 {
            let last = *hashes.last().unwrap();
            hashes.push(last);
        }
        index = if index % 2 == 0 { index + 1 } else { index - 1 };
        merkle.push(hashes[index]);
        index /= 2;
        hashes = hashes
            .chunks(2)
            .map(|pair| merklize(pair[0], pair[1]))
            .collect()
    }
    (merkle, hashes[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    const GENEROUS_TEST_CACHE_BYTES: usize = 1024 * 1024;

    fn full_share_cache(capacity_bytes: usize) -> CheckpointMerkleCache {
        CheckpointMerkleCache::with_cache_capacity(capacity_bytes, capacity_bytes)
    }

    fn leaves(n: usize) -> Vec<Sha256dHash> {
        (0..n).map(|i| Sha256dHash::hash(&[i as u8])).collect()
    }

    fn sibling_leaf(leaves: &[Sha256dHash], cp_height: usize, height: usize) -> Sha256dHash {
        let sibling_index = if height % 2 == 0 {
            height + 1
        } else {
            height - 1
        };
        leaves[sibling_index.min(cp_height)]
    }

    #[test]
    fn incremental_levels_match_naive_algorithm_for_every_height() {
        for n in 1..=17usize {
            let leaves = leaves(n);
            let cp_height = n - 1;

            for height in 0..=cp_height {
                let (expected_branch, expected_root) =
                    create_merkle_branch_and_root(leaves.clone(), height);

                let (branch, root) = if cp_height == 0 {
                    (vec![], leaves[0])
                } else {
                    let levels = build_levels_above_leaves(&leaves).unwrap();
                    let sibling = sibling_leaf(&leaves, cp_height, height);
                    extract_branch_and_root(&levels, sibling, height).unwrap()
                };

                assert_eq!(
                    branch, expected_branch,
                    "branch mismatch for n={} height={}",
                    n, height
                );
                assert_eq!(
                    root, expected_root,
                    "root mismatch for n={} height={}",
                    n, height
                );
            }
        }
    }

    #[test]
    fn cache_hit_serves_different_heights_without_rebuilding() {
        let leaves = leaves(9);
        let cp_height = 8;
        let cp_hash = BlockHash::hash(&[7u8]);

        let cache = full_share_cache(GENEROUS_TEST_CACHE_BYTES);
        let build_count = AtomicUsize::new(0);

        for height in [0usize, 3, 8] {
            let sibling = sibling_leaf(&leaves, cp_height, height);
            let (cached, _) = cache
                .get_or_build(
                    cp_height,
                    || usize::MAX,
                    1,
                    || Ok((cp_hash, sibling)),
                    || {
                        build_count.fetch_add(1, Ordering::SeqCst);
                        Ok((cp_hash, build_levels_above_leaves(&leaves)?, sibling))
                    },
                )
                .unwrap();
            let (branch, root) = extract_branch_and_root(&cached.levels, sibling, height).unwrap();
            let (expected_branch, expected_root) =
                create_merkle_branch_and_root(leaves.clone(), height);
            assert_eq!(branch, expected_branch);
            assert_eq!(root, expected_root);
        }

        assert_eq!(
            build_count.load(Ordering::SeqCst),
            1,
            "second and third lookups should hit the cache, not rebuild"
        );
    }

    #[test]
    fn stale_entry_is_rebuilt_after_checkpoint_hash_changes() {
        let leaves = leaves(5);
        let cp_height = 4;
        let sibling = sibling_leaf(&leaves, cp_height, 0);
        let old_hash = BlockHash::hash(&[1u8]);
        let new_hash = BlockHash::hash(&[2u8]);

        let cache = full_share_cache(GENEROUS_TEST_CACHE_BYTES);
        let build_count = AtomicUsize::new(0);

        cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                4,
                || Ok((old_hash, sibling)),
                || {
                    build_count.fetch_add(1, Ordering::SeqCst);
                    Ok((old_hash, build_levels_above_leaves(&leaves)?, sibling))
                },
            )
            .unwrap();
        cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                4,
                || Ok((old_hash, sibling)),
                || {
                    build_count.fetch_add(1, Ordering::SeqCst);
                    Ok((old_hash, build_levels_above_leaves(&leaves)?, sibling))
                },
            )
            .unwrap();
        assert_eq!(build_count.load(Ordering::SeqCst), 1);

        cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                4,
                || Ok((new_hash, sibling)),
                || {
                    build_count.fetch_add(1, Ordering::SeqCst);
                    Ok((new_hash, build_levels_above_leaves(&leaves)?, sibling))
                },
            )
            .unwrap();
        assert_eq!(
            build_count.load(Ordering::SeqCst),
            2,
            "a changed checkpoint hash must trigger a rebuild"
        );
    }

    #[test]
    fn stale_entry_returns_correct_branch_and_root_after_checkpoint_hash_changes() {
        let old_leaves = leaves(5);
        let new_leaves: Vec<Sha256dHash> =
            (100..105).map(|i| Sha256dHash::hash(&[i as u8])).collect();
        let cp_height = 4;
        let height = 1;
        let old_sibling = sibling_leaf(&old_leaves, cp_height, height);
        let new_sibling = sibling_leaf(&new_leaves, cp_height, height);
        let old_hash = BlockHash::hash(&[1u8]);
        let new_hash = BlockHash::hash(&[2u8]);

        let cache = full_share_cache(GENEROUS_TEST_CACHE_BYTES);

        let (old_cached, old_ret_sibling) = cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                4,
                || Ok((old_hash, old_sibling)),
                || {
                    Ok((
                        old_hash,
                        build_levels_above_leaves(&old_leaves)?,
                        old_sibling,
                    ))
                },
            )
            .unwrap();
        let (old_branch, old_root) =
            extract_branch_and_root(&old_cached.levels, old_ret_sibling, height).unwrap();
        let (expected_old_branch, expected_old_root) =
            create_merkle_branch_and_root(old_leaves.clone(), height);
        assert_eq!(old_branch, expected_old_branch);
        assert_eq!(old_root, expected_old_root);

        let (new_cached, new_ret_sibling) = cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                4,
                || Ok((new_hash, new_sibling)),
                || {
                    Ok((
                        new_hash,
                        build_levels_above_leaves(&new_leaves)?,
                        new_sibling,
                    ))
                },
            )
            .unwrap();
        let (new_branch, new_root) =
            extract_branch_and_root(&new_cached.levels, new_ret_sibling, height).unwrap();
        let (expected_new_branch, expected_new_root) =
            create_merkle_branch_and_root(new_leaves.clone(), height);
        assert_eq!(new_branch, expected_new_branch);
        assert_eq!(new_root, expected_new_root);
        assert_ne!(
            new_root, old_root,
            "a rebuild after a checkpoint hash change must reflect the new chain state, not the stale cached one"
        );
    }

    #[test]
    fn insert_evicts_lowest_height_for_a_higher_value() {
        let entry_leaves = leaves(2);
        let entry_bytes = build_levels_above_leaves(&entry_leaves).unwrap()[0].len()
            * std::mem::size_of::<Sha256dHash>();
        let cache = full_share_cache(5 * entry_bytes);
        let leaf = entry_leaves[0];

        for cp_height in [10usize, 20, 30, 40, 50] {
            let hash = BlockHash::hash(&[cp_height as u8]);
            cache
                .get_or_build(
                    cp_height,
                    || usize::MAX,
                    4,
                    || Ok((hash, leaf)),
                    || Ok((hash, build_levels_above_leaves(&entry_leaves)?, leaf)),
                )
                .unwrap();
        }
        assert_eq!(cache.entries.lock().unwrap().len(), 5);

        let hash60 = BlockHash::hash(&[60u8]);
        cache
            .get_or_build(
                60,
                || usize::MAX,
                4,
                || Ok((hash60, leaf)),
                || Ok((hash60, build_levels_above_leaves(&entry_leaves)?, leaf)),
            )
            .unwrap();

        let heights: Vec<usize> = cache
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(h, _)| *h)
            .collect();
        assert_eq!(heights.len(), 5);
        assert!(
            !heights.contains(&10),
            "lowest cp_height should be evicted, got {:?}",
            heights
        );
        assert!(heights.contains(&60));
    }

    #[test]
    fn insert_skips_values_below_the_cached_minimum() {
        let entry_leaves = leaves(2);
        let entry_bytes = build_levels_above_leaves(&entry_leaves).unwrap()[0].len()
            * std::mem::size_of::<Sha256dHash>();
        let cache = full_share_cache(5 * entry_bytes);
        let leaf = entry_leaves[0];

        for cp_height in [10usize, 20, 30, 40, 50] {
            let hash = BlockHash::hash(&[cp_height as u8]);
            cache
                .get_or_build(
                    cp_height,
                    || usize::MAX,
                    4,
                    || Ok((hash, leaf)),
                    || Ok((hash, build_levels_above_leaves(&entry_leaves)?, leaf)),
                )
                .unwrap();
        }
        assert_eq!(cache.entries.lock().unwrap().len(), 5);

        let hash5 = BlockHash::hash(&[5u8]);
        cache
            .get_or_build(
                5,
                || usize::MAX,
                4,
                || Ok((hash5, leaf)),
                || Ok((hash5, build_levels_above_leaves(&entry_leaves)?, leaf)),
            )
            .unwrap();

        let heights: Vec<usize> = cache
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(h, _)| *h)
            .collect();
        assert_eq!(heights.len(), 5);
        assert!(
            heights.contains(&10),
            "a lower cp_height must not displace an existing higher entry, got {:?}",
            heights
        );
        assert!(!heights.contains(&5));
    }

    #[test]
    fn failed_insert_does_not_partially_evict_lower_entries() {
        let hash_bytes = std::mem::size_of::<Sha256dHash>();
        let cache = full_share_cache(5 * hash_bytes);
        let make_cached = |height: usize, hashes: usize| {
            Arc::new(CachedLevels::new(
                BlockHash::hash(&[height as u8]),
                vec![vec![Sha256dHash::hash(&[height as u8]); hashes]],
            ))
        };

        cache.insert(10, usize::MAX, make_cached(10, 1));
        cache.insert(30, usize::MAX, make_cached(30, 4));
        cache.insert(20, usize::MAX, make_cached(20, 2));

        let heights: Vec<usize> = cache
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(height, _)| *height)
            .collect();
        assert_eq!(heights, vec![10, 30]);
    }

    #[test]
    fn insert_caps_entry_count_even_under_a_generous_byte_budget() {
        let entry_leaves = leaves(2);
        let entry_bytes = build_levels_above_leaves(&entry_leaves).unwrap()[0].len()
            * std::mem::size_of::<Sha256dHash>();
        let cache = full_share_cache((MAX_CACHE_ENTRIES + 100) * entry_bytes);
        let leaf = entry_leaves[0];
        let hash = BlockHash::hash(&[7u8]);

        for cp_height in 0..MAX_CACHE_ENTRIES + 50 {
            cache
                .get_or_build(
                    cp_height,
                    || usize::MAX,
                    4,
                    || Ok((hash, leaf)),
                    || Ok((hash, build_levels_above_leaves(&entry_leaves)?, leaf)),
                )
                .unwrap();
        }

        let len = cache.entries.lock().unwrap().len();
        assert!(
            len <= MAX_CACHE_ENTRIES,
            "entry count exceeded the cap: {}",
            len
        );
    }

    #[test]
    fn insert_prunes_entries_orphaned_by_a_shorter_reorg() {
        let entry_leaves = leaves(2);
        let entry_bytes = build_levels_above_leaves(&entry_leaves).unwrap()[0].len()
            * std::mem::size_of::<Sha256dHash>();
        let leaf = entry_leaves[0];
        let cache = full_share_cache(entry_bytes);

        let old_height = 100;
        let old_hash = BlockHash::hash(&[1u8]);
        cache
            .get_or_build(
                old_height,
                || old_height,
                4,
                || Ok((old_hash, leaf)),
                || Ok((old_hash, build_levels_above_leaves(&entry_leaves)?, leaf)),
            )
            .unwrap();
        assert_eq!(cache.entries.lock().unwrap().len(), 1);

        let new_height = old_height - 1;
        let new_hash = BlockHash::hash(&[2u8]);
        let build_count = AtomicUsize::new(0);

        cache
            .get_or_build(
                new_height,
                || new_height,
                4,
                || Ok((new_hash, leaf)),
                || {
                    build_count.fetch_add(1, Ordering::SeqCst);
                    Ok((new_hash, build_levels_above_leaves(&entry_leaves)?, leaf))
                },
            )
            .unwrap();
        assert_eq!(build_count.load(Ordering::SeqCst), 1);

        cache
            .get_or_build(
                new_height,
                || new_height,
                4,
                || Ok((new_hash, leaf)),
                || {
                    build_count.fetch_add(1, Ordering::SeqCst);
                    panic!("must serve from cache instead of rebuilding")
                },
            )
            .unwrap();
        assert_eq!(
            build_count.load(Ordering::SeqCst),
            1,
            "the post-reorg checkpoint must be cached once built, not rebuilt on every request \
             just because the old, now-unreachable height still occupied the budget"
        );

        let heights: Vec<usize> = cache
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(h, _)| *h)
            .collect();
        assert_eq!(heights, vec![new_height]);
    }

    #[test]
    fn insertion_reads_best_height_after_a_slow_rebuild() {
        let entry_leaves = leaves(2);
        let levels = build_levels_above_leaves(&entry_leaves).unwrap();
        let entry_bytes = levels[0].len() * std::mem::size_of::<Sha256dHash>();
        let cache = full_share_cache(2 * entry_bytes);
        let leaf = entry_leaves[0];

        let new_tip_height = 101;
        cache.insert(
            new_tip_height,
            new_tip_height,
            Arc::new(CachedLevels::new(
                BlockHash::hash(&[new_tip_height as u8]),
                levels.clone(),
            )),
        );

        let best_height = AtomicUsize::new(new_tip_height - 1);
        let cp_height = new_tip_height - 1;
        let cp_hash = BlockHash::hash(&[cp_height as u8]);
        cache
            .get_or_build(
                cp_height,
                || best_height.load(Ordering::SeqCst),
                1,
                || Ok((cp_hash, leaf)),
                || {
                    best_height.store(new_tip_height, Ordering::SeqCst);
                    Ok((cp_hash, levels, leaf))
                },
            )
            .unwrap();

        let heights: Vec<usize> = cache
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(height, _)| *height)
            .collect();
        assert!(
            heights.contains(&new_tip_height),
            "a tip cached during the rebuild must not be pruned using the rebuild's starting height"
        );
        assert!(heights.contains(&cp_height));
    }

    #[test]
    fn concurrent_rebuilds_of_same_cp_height_run_build_once() {
        let leaves = Arc::new(leaves(9));
        let cp_height = 8;
        let cp_hash = BlockHash::hash(&[9u8]);
        let sibling = sibling_leaf(&leaves, cp_height, 0);

        let cache = Arc::new(full_share_cache(GENEROUS_TEST_CACHE_BYTES));
        let build_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let build_count = build_count.clone();
                let barrier = barrier.clone();
                let leaves = leaves.clone();
                thread::spawn(move || {
                    barrier.wait();
                    cache
                        .get_or_build(
                            cp_height,
                            || usize::MAX,
                            8,
                            || Ok((cp_hash, sibling)),
                            || {
                                build_count.fetch_add(1, Ordering::SeqCst);
                                thread::sleep(std::time::Duration::from_millis(20));
                                Ok((cp_hash, build_levels_above_leaves(&leaves)?, sibling))
                            },
                        )
                        .unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            build_count.load(Ordering::SeqCst),
            1,
            "8 concurrent requests for the same cp_height must build exactly once"
        );
        assert!(
            cache.build_locks.lock().unwrap().is_empty(),
            "no build slot may remain after all requests finished"
        );
    }

    #[test]
    fn concurrent_requests_for_an_uncacheable_height_still_build_once() {
        let leaves = Arc::new(leaves(9));
        let cp_height = 8;
        let cp_hash = BlockHash::hash(&[9u8]);
        let sibling = sibling_leaf(&leaves, cp_height, 0);

        let cache = Arc::new(full_share_cache(0));
        let build_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let _keep_build_slot_alive = cache.build_lock_for(cp_height);

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let build_count = build_count.clone();
                let barrier = barrier.clone();
                let leaves = leaves.clone();
                thread::spawn(move || {
                    barrier.wait();
                    cache
                        .get_or_build(
                            cp_height,
                            || usize::MAX,
                            8,
                            || Ok((cp_hash, sibling)),
                            || {
                                build_count.fetch_add(1, Ordering::SeqCst);
                                thread::sleep(std::time::Duration::from_millis(20));
                                Ok((cp_hash, build_levels_above_leaves(&leaves)?, sibling))
                            },
                        )
                        .unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            build_count.load(Ordering::SeqCst),
            1,
            "8 concurrent requests for the same uncacheable cp_height must still build exactly once"
        );
        drop(_keep_build_slot_alive);
        assert!(
            cache.build_locks.lock().unwrap().is_empty(),
            "no build slot may remain once the last request releases it"
        );
    }

    #[test]
    fn concurrent_releases_must_not_leak_the_build_slot_and_its_tree() {
        let leaves = leaves(9);
        let cp_height = 8;
        let cp_hash = BlockHash::hash(&[9u8]);
        let sibling = sibling_leaf(&leaves, cp_height, 0);

        let cache = full_share_cache(0);

        let slot_a = cache.build_lock_for(cp_height);
        let slot_b = cache.build_lock_for(cp_height);

        cache
            .build_under_lock(
                cp_height,
                || usize::MAX,
                cp_hash,
                sibling,
                1,
                &slot_a.slot,
                || Ok((cp_hash, build_levels_above_leaves(&leaves)?, sibling)),
            )
            .unwrap();

        drop(slot_a);
        assert_eq!(
            cache.build_locks.lock().unwrap().len(),
            1,
            "slot must stay while another request still holds it"
        );
        drop(slot_b);

        let locks = cache.build_locks.lock().unwrap();
        assert!(
            locks.is_empty(),
            "build slot leaked after all requests finished, retaining the tree \
             outside the cache's memory accounting"
        );
    }

    #[test]
    fn tree_retained_only_by_a_build_slot_counts_against_the_memory_budget() {
        let entry_leaves = leaves(2);
        let retained_bytes = build_levels_above_leaves(&entry_leaves).unwrap()[0].len()
            * std::mem::size_of::<Sha256dHash>();
        let leaf = entry_leaves[0];

        let blocked_height = 1_000usize;
        let cache = full_share_cache(estimated_peak_build_bytes(blocked_height));

        let uncached_height = 100;
        let uncached_hash = BlockHash::hash(&[1u8]);
        let slot_guard = cache.build_lock_for(uncached_height);
        cache
            .build_under_lock(
                uncached_height,
                || uncached_height - 1,
                uncached_hash,
                leaf,
                4,
                &slot_guard.slot,
                || {
                    Ok((
                        uncached_hash,
                        build_levels_above_leaves(&entry_leaves)?,
                        leaf,
                    ))
                },
            )
            .unwrap();
        assert!(cache.entries.lock().unwrap().is_empty());
        assert_eq!(
            cache.inflight_build_bytes.load(Ordering::SeqCst),
            retained_bytes,
            "a tree retained only by last_built must be charged to the budget"
        );

        let blocked_hash = BlockHash::hash(&[2u8]);
        let result = cache.get_or_build(
            blocked_height,
            || usize::MAX,
            4,
            || Ok((blocked_hash, leaf)),
            || panic!("must not build while retained slot bytes exhaust the budget"),
        );
        assert!(
            result.is_err(),
            "a rebuild must be rejected while slot-retained bytes exhaust the budget"
        );

        drop(slot_guard);
        assert_eq!(
            cache.inflight_build_bytes.load(Ordering::SeqCst),
            0,
            "releasing the slot must release its memory charge"
        );
        cache
            .get_or_build(
                blocked_height,
                || usize::MAX,
                4,
                || Ok((blocked_hash, leaf)),
                || Ok((blocked_hash, build_levels_above_leaves(&entry_leaves)?, leaf)),
            )
            .unwrap();
    }

    #[test]
    fn a_panicking_build_does_not_poison_the_slot_for_waiters() {
        let leaves = Arc::new(leaves(5));
        let cp_height = 4;
        let cp_hash = BlockHash::hash(&[1u8]);
        let sibling = sibling_leaf(&leaves, cp_height, 0);

        let cache = Arc::new(full_share_cache(GENEROUS_TEST_CACHE_BYTES));
        let _keep_slot_alive = cache.build_lock_for(cp_height);

        let panicking = {
            let cache = cache.clone();
            thread::spawn(move || {
                let _ = cache.get_or_build(
                    cp_height,
                    || usize::MAX,
                    4,
                    || Ok((cp_hash, sibling)),
                    || panic!("simulated build failure"),
                );
            })
        };
        assert!(panicking.join().is_err());
        assert_eq!(
            cache.build_locks.lock().unwrap().len(),
            1,
            "the slot must survive for the still-registered request"
        );

        cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                4,
                || Ok((cp_hash, sibling)),
                || Ok((cp_hash, build_levels_above_leaves(&leaves)?, sibling)),
            )
            .expect("a waiter must recover from a poisoned slot instead of panicking");
    }

    #[test]
    fn rebuild_permit_caps_concurrency_and_releases_on_drop() {
        const LIMIT: usize = 2;
        let counter = AtomicUsize::new(0);
        let mut permits = Vec::new();
        for _ in 0..LIMIT {
            permits.push(RebuildPermit::acquire(&counter, LIMIT).unwrap());
        }

        assert!(RebuildPermit::acquire(&counter, LIMIT).is_err());

        permits.pop();
        let extra = RebuildPermit::acquire(&counter, LIMIT).unwrap();
        assert!(RebuildPermit::acquire(&counter, LIMIT).is_err());

        drop(extra);
        drop(permits);
    }

    #[test]
    fn cache_hits_bypass_the_rebuild_permit_entirely() {
        let leaves = leaves(3);
        let cp_height = 2;
        let sibling = sibling_leaf(&leaves, cp_height, 0);
        let cp_hash = BlockHash::hash(&[3u8]);

        let cache = full_share_cache(GENEROUS_TEST_CACHE_BYTES);
        cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                1,
                || Ok((cp_hash, sibling)),
                || Ok((cp_hash, build_levels_above_leaves(&leaves)?, sibling)),
            )
            .unwrap();

        cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                0,
                || Ok((cp_hash, sibling)),
                || panic!("must not rebuild on a cache hit"),
            )
            .unwrap();
    }

    #[test]
    fn cache_hit_succeeds_while_all_rebuild_permits_are_held() {
        let leaves = leaves(3);
        let cp_height = 2;
        let sibling = sibling_leaf(&leaves, cp_height, 0);
        let cp_hash = BlockHash::hash(&[3u8]);

        let cache = Arc::new(full_share_cache(GENEROUS_TEST_CACHE_BYTES));
        cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                1,
                || Ok((cp_hash, sibling)),
                || Ok((cp_hash, build_levels_above_leaves(&leaves)?, sibling)),
            )
            .unwrap();

        const HOLDERS: usize = 2;
        let inside_build = Arc::new(Barrier::new(HOLDERS + 1));
        let release_build = Arc::new(Barrier::new(HOLDERS + 1));

        let handles: Vec<_> = (0..HOLDERS)
            .map(|i| {
                let cache = cache.clone();
                let inside_build = inside_build.clone();
                let release_build = release_build.clone();
                thread::spawn(move || {
                    let miss_height = 1_000 + i;
                    let hash = BlockHash::hash(&[100 + i as u8]);
                    let leaf = Sha256dHash::hash(&[0u8]);
                    cache
                        .get_or_build(
                            miss_height,
                            || usize::MAX,
                            HOLDERS,
                            || Ok((hash, leaf)),
                            || {
                                inside_build.wait();
                                release_build.wait();
                                Ok((hash, build_levels_above_leaves(&[leaf])?, leaf))
                            },
                        )
                        .unwrap();
                })
            })
            .collect();

        inside_build.wait();

        cache
            .get_or_build(
                cp_height,
                || usize::MAX,
                0,
                || Ok((cp_hash, sibling)),
                || panic!("must not rebuild on a cache hit"),
            )
            .unwrap();

        release_build.wait();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn memory_permit_caps_concurrent_rebuild_bytes_independently_of_concurrency_limit() {
        const HOLDERS: usize = 2;
        let holder_heights = [1_000usize, 2_000usize];
        let capacity_bytes: usize = holder_heights
            .iter()
            .map(|h| estimated_peak_build_bytes(*h))
            .sum();

        let cache = Arc::new(full_share_cache(capacity_bytes));
        let inside_build = Arc::new(Barrier::new(HOLDERS + 1));
        let release_build = Arc::new(Barrier::new(HOLDERS + 1));

        let handles: Vec<_> = holder_heights
            .iter()
            .map(|&miss_height| {
                let cache = cache.clone();
                let inside_build = inside_build.clone();
                let release_build = release_build.clone();
                thread::spawn(move || {
                    let hash = BlockHash::hash(&[miss_height as u8]);
                    let leaf = Sha256dHash::hash(&[0u8]);
                    cache
                        .get_or_build(
                            miss_height,
                            || usize::MAX,
                            100,
                            || Ok((hash, leaf)),
                            || {
                                inside_build.wait();
                                release_build.wait();
                                Ok((hash, build_levels_above_leaves(&[leaf])?, leaf))
                            },
                        )
                        .unwrap();
                })
            })
            .collect();

        inside_build.wait();

        let result = cache.get_or_build(
            3_000,
            || usize::MAX,
            100,
            || Ok((BlockHash::hash(&[9u8]), Sha256dHash::hash(&[0u8]))),
            || panic!("must not rebuild once the memory budget is fully held"),
        );
        assert!(
            result.is_err(),
            "a third concurrent rebuild must be rejected once the memory budget is exhausted, even with a generous concurrency_limit"
        );

        release_build.wait();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn memory_permit_budget_accounts_for_already_resident_cache_bytes() {
        let resident_leaves = leaves(2);
        let resident_bytes = build_levels_above_leaves(&resident_leaves).unwrap()[0].len()
            * std::mem::size_of::<Sha256dHash>();
        let resident_height = 500usize;
        let resident_hash = BlockHash::hash(&[7u8]);
        let resident_sibling = resident_leaves[0];

        let holder_height = 1_000usize;
        let extra_height = 2_000usize;
        let capacity_bytes = resident_bytes
            + estimated_peak_build_bytes(holder_height)
            + estimated_peak_build_bytes(extra_height)
            - 1;

        let cache = Arc::new(full_share_cache(capacity_bytes));
        cache
            .get_or_build(
                resident_height,
                || usize::MAX,
                1,
                || Ok((resident_hash, resident_sibling)),
                || {
                    Ok((
                        resident_hash,
                        build_levels_above_leaves(&resident_leaves)?,
                        resident_sibling,
                    ))
                },
            )
            .unwrap();

        let inside_build = Arc::new(Barrier::new(2));
        let release_build = Arc::new(Barrier::new(2));

        let holder = {
            let cache = cache.clone();
            let inside_build = inside_build.clone();
            let release_build = release_build.clone();
            thread::spawn(move || {
                let hash = BlockHash::hash(&[9u8]);
                let leaf = Sha256dHash::hash(&[0u8]);
                cache
                    .get_or_build(
                        holder_height,
                        || usize::MAX,
                        100,
                        || Ok((hash, leaf)),
                        || {
                            inside_build.wait();
                            release_build.wait();
                            Ok((hash, build_levels_above_leaves(&[leaf])?, leaf))
                        },
                    )
                    .unwrap();
            })
        };

        inside_build.wait();

        let result = cache.get_or_build(
            extra_height,
            || usize::MAX,
            100,
            || Ok((BlockHash::hash(&[11u8]), Sha256dHash::hash(&[0u8]))),
            || panic!("must not rebuild once resident cache bytes and the held build already exhaust the budget"),
        );
        assert!(
            result.is_err(),
            "already-resident cache bytes must count against the budget available to concurrent rebuilds"
        );

        release_build.wait();
        holder.join().unwrap();
    }

    #[test]
    fn a_full_cache_still_admits_concurrent_rebuilds() {
        let build_height = 1_000usize;
        let capacity_bytes = 4 * estimated_peak_build_bytes(build_height);
        let cache = Arc::new(CheckpointMerkleCache::new(capacity_bytes));

        let filler_leaves = leaves(1 << 12);
        let filler_bytes = CachedLevels::new(
            BlockHash::hash(&[0u8]),
            build_levels_above_leaves(&filler_leaves).unwrap(),
        )
        .size_bytes;
        let mut height = 1usize << 20;
        let mut resident = 0usize;
        while resident + filler_bytes <= capacity_bytes / 2 {
            let hash = BlockHash::hash(&height.to_le_bytes());
            cache
                .get_or_build(
                    height,
                    || usize::MAX,
                    1,
                    || Ok((hash, filler_leaves[0])),
                    || Ok((hash, build_levels_above_leaves(&filler_leaves)?, filler_leaves[0])),
                )
                .unwrap();
            resident += filler_bytes;
            height += 1;
        }
        assert!(resident > 0);

        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handles: Vec<_> = vec![build_height, build_height + 1]
            .into_iter()
            .map(|miss_height: usize| {
                let cache = cache.clone();
                let entered = entered.clone();
                let release = release.clone();
                thread::spawn(move || {
                    let hash = BlockHash::hash(&[miss_height as u8]);
                    let leaf = Sha256dHash::hash(&[0u8]);
                    cache.get_or_build(
                        miss_height,
                        || usize::MAX,
                        100,
                        || Ok((hash, leaf)),
                        || {
                            entered.fetch_add(1, Ordering::SeqCst);
                            while !release.load(Ordering::SeqCst) {
                                thread::sleep(std::time::Duration::from_millis(1));
                            }
                            Ok((hash, build_levels_above_leaves(&[leaf])?, leaf))
                        },
                    )
                })
            })
            .collect();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while entered.load(Ordering::SeqCst) < 2
            && !handles.iter().any(|handle| handle.is_finished())
            && std::time::Instant::now() < deadline
        {
            thread::sleep(std::time::Duration::from_millis(1));
        }
        release.store(true, Ordering::SeqCst);
        for handle in handles {
            assert!(
                handle.join().unwrap().is_ok(),
                "cached trees must not leave room for only a single rebuild"
            );
        }
    }
}
