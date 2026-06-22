use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use super::block_hash::BlockHash;
use super::fibonacci::shard_for_with_count;
use super::host_bitmap::HostBitmap;
use tracing::debug;

#[derive(Debug)]
pub struct ShardedBlockIndexer {
    shards: Vec<RwLock<HashMap<BlockHash, HostBitmap>>>,
    alive: RwLock<HostBitmap>,
    pod_count: AtomicUsize,
}

impl ShardedBlockIndexer {
    pub fn new(pod_count: usize) -> Self {
        Self::with_shards(pod_count, super::fibonacci::DEFAULT_SHARD_COUNT)
    }

    pub fn with_shards(pod_count: usize, shard_count: usize) -> Self {
        Self::with_shards_and_alive(
            pod_count,
            shard_count,
            HostBitmap::full_for_count(pod_count),
        )
    }

    pub fn with_shards_empty_alive(pod_count: usize, shard_count: usize) -> Self {
        Self::with_shards_and_alive(pod_count, shard_count, HostBitmap::empty())
    }

    pub fn with_shards_and_alive(pod_count: usize, shard_count: usize, alive: HostBitmap) -> Self {
        let shard_count = shard_count.max(1);
        let shards = (0..shard_count)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self {
            shards,
            alive: RwLock::new(alive),
            pod_count: AtomicUsize::new(pod_count),
        }
    }

    pub fn pod_count(&self) -> usize {
        self.pod_count.load(Ordering::Relaxed)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn register(&self, pod_id: usize, cumulative_hash: BlockHash) -> bool {
        self.observe_pod(pod_id);

        let shard = self.shard_for(cumulative_hash);

        let mut guard = self.shards[shard].write().expect("index shard poisoned");
        let owners = guard
            .entry(cumulative_hash)
            .or_insert_with(HostBitmap::empty);
        if owners.contains(pod_id) {
            debug!(
                pod_id,
                cumulative_hash, shard, "cache block register skipped; already owned by pod"
            );
            return false;
        }

        owners.set(pod_id);
        debug!(pod_id, cumulative_hash, shard, "cache block registered");
        true
    }

    pub fn register_chain(&self, pod_id: usize, cumulative_hashes: &[BlockHash]) -> usize {
        let mut registered = 0;
        for hash in cumulative_hashes {
            if self.register(pod_id, *hash) {
                registered += 1;
            }
        }
        registered
    }

    pub fn evict(&self, pod_id: usize, cumulative_hash: BlockHash) {
        let shard = self.shard_for(cumulative_hash);
        let mut guard = self.shards[shard].write().expect("index shard poisoned");
        if let Some(owners) = guard.get_mut(&cumulative_hash) {
            owners.clear(pod_id);
            if owners.is_empty() {
                guard.remove(&cumulative_hash);
            }
        }
        debug!(pod_id, cumulative_hash, shard, "cache block evicted");
    }

    pub fn evict_chain(&self, pod_id: usize, cumulative_hashes: &[BlockHash]) {
        for hash in cumulative_hashes {
            self.evict(pod_id, *hash);
        }
    }

    pub fn mark_alive(&self, pod_id: usize) {
        self.observe_pod(pod_id);
        self.alive
            .write()
            .expect("alive bitmap poisoned")
            .set(pod_id);
    }

    pub fn shutdown(&self, pod_id: usize) {
        self.alive
            .write()
            .expect("alive bitmap poisoned")
            .clear(pod_id);
        debug!(pod_id, "cache pod marked shutdown");
    }

    pub fn owners(&self, cumulative_hash: BlockHash) -> HostBitmap {
        let shard = self.shard_for(cumulative_hash);
        self.shards[shard]
            .read()
            .expect("index shard poisoned")
            .get(&cumulative_hash)
            .copied()
            .unwrap_or_else(HostBitmap::empty)
    }

    pub fn owners_alive(&self, cumulative_hash: BlockHash) -> HostBitmap {
        self.owners(cumulative_hash).and(&self.alive())
    }

    pub fn alive(&self) -> HostBitmap {
        *self.alive.read().expect("alive bitmap poisoned")
    }

    pub fn cleanup_dead_pod(&self, pod_id: usize) {
        for shard in &self.shards {
            let mut guard = shard.write().expect("index shard poisoned");
            guard.retain(|_, owners| {
                owners.clear(pod_id);
                !owners.is_empty()
            });
        }
    }

    pub fn cleanup_not_alive(&self) {
        let alive = self.alive();
        for shard in &self.shards {
            let mut guard = shard.write().expect("index shard poisoned");
            guard.retain(|_, owners| {
                *owners = owners.and(&alive);
                !owners.is_empty()
            });
        }
    }

    pub fn shard_entry_counts(&self) -> Vec<usize> {
        self.shards
            .iter()
            .map(|shard| shard.read().expect("index shard poisoned").len())
            .collect()
    }

    pub fn block_owners(&self) -> Vec<(BlockHash, HostBitmap)> {
        let mut block_owners = Vec::new();
        for shard in &self.shards {
            let guard = shard.read().expect("index shard poisoned");
            block_owners.extend(guard.iter().map(|(block_hash, owners)| (*block_hash, *owners)));
        }
        block_owners.sort_by_key(|(block_hash, _)| *block_hash);
        block_owners
    }

    pub fn total_entries(&self) -> usize {
        self.shard_entry_counts().iter().sum()
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().expect("index shard poisoned").clear();
        }
    }

    fn shard_for(&self, cumulative_hash: BlockHash) -> usize {
        shard_for_with_count(cumulative_hash, self.shards.len())
    }

    fn observe_pod(&self, pod_id: usize) {
        let needed = pod_id + 1;
        let mut current = self.pod_count.load(Ordering::Relaxed);
        while needed > current {
            match self.pod_count.compare_exchange_weak(
                current,
                needed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }
}


