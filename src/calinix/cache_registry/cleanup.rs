use super::sharded_index::ShardedBlockIndexer;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupReport {
    pub entries_before: usize,
    pub entries_after: usize,
}

impl CleanupReport {
    pub fn removed_entries(&self) -> usize {
        self.entries_before.saturating_sub(self.entries_after)
    }
}

pub fn cleanup_dead_pod(indexer: &ShardedBlockIndexer, pod_id: usize) -> CleanupReport {
    let entries_before = indexer.total_entries();
    indexer.cleanup_dead_pod(pod_id);
    let entries_after = indexer.total_entries();
    CleanupReport {
        entries_before,
        entries_after,
    }
}

pub fn cleanup_not_alive(indexer: &ShardedBlockIndexer) -> CleanupReport {
    let entries_before = indexer.total_entries();
    indexer.cleanup_not_alive();
    let entries_after = indexer.total_entries();
    CleanupReport {
        entries_before,
        entries_after,
    }
}
