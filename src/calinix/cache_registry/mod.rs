pub mod alive;
pub mod block_hash;
pub mod cleanup;
pub mod cumulative_hash;
pub mod events;
pub mod fibonacci;
pub mod host_bitmap;
pub mod prefix_query;
pub mod sharded_index;
pub mod snapshot;
pub mod types;

pub use alive::AliveSet;
pub use block_hash::{
    fnv1a64, hash_block, prompt_to_block_hashes, prompt_to_block_hashes_with_size,
    prompt_to_token_blocks_with_size, tokenize, BlockHash, DEFAULT_BLOCK_SIZE,
};
pub use cleanup::{cleanup_dead_pod, cleanup_not_alive, CleanupReport};
pub use cumulative_hash::{
    combine_cumulative, cumulative_hashes_from_blocks, make_synthetic_chain,
    prompt_to_cumulative_hashes, prompt_to_cumulative_hashes_with_block_size,
};
pub use events::{apply_event, CacheEvent};
pub use fibonacci::{
    shard_for, shard_for_fibonacci, shard_for_fibonacci_with_count, shard_for_low_bits,
    shard_for_low_bits_with_count, shard_for_with_count, DEFAULT_SHARD_COUNT, FIBONACCI,
};
pub use host_bitmap::{HostBitmap, PodId};
pub use prefix_query::{
    best_prefix_match_for_candidates, longest_prefix_lengths_debug,
    longest_prefix_lengths_for_candidates, longest_prefix_lengths_into, PrefixMatch,
    PrefixMatchDebug, SearchFrame,
};
pub use sharded_index::ShardedBlockIndexer;
pub use snapshot::CacheRegistrySnapshot;
pub use types::{CacheRegistryStats, PrefixDepth, PrefixDepthByPod, ShardId};

#[derive(Debug)]
pub struct CacheRegistry {
    block_index: ShardedBlockIndexer,
}

impl CacheRegistry {
    pub fn new(pod_count: usize) -> Self {
        Self {
            block_index: ShardedBlockIndexer::new(pod_count),
        }
    }

    pub fn with_shards(pod_count: usize, shard_count: usize) -> Self {
        Self {
            block_index: ShardedBlockIndexer::with_shards(pod_count, shard_count),
        }
    }

    pub fn new_empty_alive(pod_count: usize) -> Self {
        Self {
            block_index: ShardedBlockIndexer::with_shards_empty_alive(
                pod_count,
                DEFAULT_SHARD_COUNT,
            ),
        }
    }

    pub fn with_shards_empty_alive(pod_count: usize, shard_count: usize) -> Self {
        Self {
            block_index: ShardedBlockIndexer::with_shards_empty_alive(pod_count, shard_count),
        }
    }

    pub fn index(&self) -> &ShardedBlockIndexer {
        &self.block_index
    }

    pub fn register_prefix(&self, pod_id: usize, cumulative_hash: BlockHash) -> bool {
        self.block_index.register(pod_id, cumulative_hash)
    }

    pub fn register_chain(&self, pod_id: usize, cumulative_hashes: &[BlockHash]) -> usize {
        self.block_index.register_chain(pod_id, cumulative_hashes)
    }

    pub fn evict_prefix(&self, pod_id: usize, cumulative_hash: BlockHash) {
        self.block_index.evict(pod_id, cumulative_hash);
    }

    pub fn evict_chain(&self, pod_id: usize, cumulative_hashes: &[BlockHash]) {
        self.block_index.evict_chain(pod_id, cumulative_hashes);
    }

    pub fn shutdown_pod(&self, pod_id: usize) {
        self.block_index.shutdown(pod_id);
    }

    pub fn mark_pod_alive(&self, pod_id: usize) {
        self.block_index.mark_alive(pod_id);
    }

    pub fn owners(&self, cumulative_hash: BlockHash) -> HostBitmap {
        self.block_index.owners(cumulative_hash)
    }

    pub fn owners_alive(&self, cumulative_hash: BlockHash) -> HostBitmap {
        self.block_index.owners_alive(cumulative_hash)
    }

    pub fn block_owners(&self) -> Vec<(BlockHash, HostBitmap)> {
        self.block_index.block_owners()
    }

    pub fn alive(&self) -> HostBitmap {
        self.block_index.alive()
    }

    pub fn longest_prefix_lengths(
        &self,
        cumulative_hashes: &[BlockHash],
        candidate_pods: HostBitmap,
    ) -> Vec<usize> {
        longest_prefix_lengths_for_candidates(&self.block_index, cumulative_hashes, candidate_pods)
    }

    pub fn best_prefix_match(
        &self,
        cumulative_hashes: &[BlockHash],
        candidate_pods: HostBitmap,
    ) -> Option<PrefixMatch> {
        best_prefix_match_for_candidates(&self.block_index, cumulative_hashes, candidate_pods)
    }

    pub fn cleanup_dead_pod(&self, pod_id: usize) -> CleanupReport {
        cleanup::cleanup_dead_pod(&self.block_index, pod_id)
    }

    pub fn snapshot(&self) -> CacheRegistrySnapshot {
        CacheRegistrySnapshot::from_indexer(&self.block_index)
    }

    pub fn stats(&self) -> CacheRegistryStats {
        let snapshot = self.snapshot();
        CacheRegistryStats {
            pod_count: snapshot.pod_count,
            alive_pods: snapshot.alive.count_ones(),
            total_entries: snapshot.total_entries,
            non_empty_shards: snapshot
                .shard_entry_counts
                .iter()
                .filter(|entry_count| **entry_count > 0)
                .count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{prompt_to_cumulative_hashes, CacheRegistry, HostBitmap};

    #[test]
    fn cache_registry_scores_prefix_depths() {
        let registry = CacheRegistry::new(3);
        let hashes =
            prompt_to_cumulative_hashes("one two three four five six seven eight nine ten");

        registry.register_chain(0, &hashes);
        registry.register_prefix(1, hashes[0]);

        let depths = registry.longest_prefix_lengths(&hashes, HostBitmap::full_for_count(3));
        assert_eq!(depths[0], hashes.len());
        assert_eq!(depths[1], 1);
        assert_eq!(depths[2], 0);
    }
}
