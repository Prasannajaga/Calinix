use super::host_bitmap::HostBitmap;
use super::sharded_index::ShardedBlockIndexer;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheRegistrySnapshot {
    pub pod_count: usize,
    pub alive: HostBitmap,
    pub shard_entry_counts: Vec<usize>,
    pub total_entries: usize,
}

impl CacheRegistrySnapshot {
    pub fn from_indexer(indexer: &ShardedBlockIndexer) -> Self {
        let shard_entry_counts = indexer.shard_entry_counts();
        let total_entries = shard_entry_counts.iter().sum();
        Self {
            pod_count: indexer.pod_count(),
            alive: indexer.alive(),
            shard_entry_counts,
            total_entries,
        }
    }
}
