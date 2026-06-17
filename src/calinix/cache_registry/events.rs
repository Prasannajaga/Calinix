use super::block_hash::BlockHash;
use super::sharded_index::ShardedBlockIndexer;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheEvent {
    PrefixCached {
        pod_id: usize,
        cumulative_hash: BlockHash,
    },
    PrefixEvicted {
        pod_id: usize,
        cumulative_hash: BlockHash,
    },
    PodStarted {
        pod_id: usize,
    },
    PodShutdown {
        pod_id: usize,
    },
    CleanupPod {
        pod_id: usize,
    },
}

pub fn apply_event(indexer: &ShardedBlockIndexer, event: CacheEvent) {
    match event {
        CacheEvent::PrefixCached {
            pod_id,
            cumulative_hash,
        } => {
            indexer.register(pod_id, cumulative_hash);
        }
        CacheEvent::PrefixEvicted {
            pod_id,
            cumulative_hash,
        } => indexer.evict(pod_id, cumulative_hash),
        CacheEvent::PodStarted { pod_id } => indexer.mark_alive(pod_id),
        CacheEvent::PodShutdown { pod_id } => indexer.shutdown(pod_id),
        CacheEvent::CleanupPod { pod_id } => indexer.cleanup_dead_pod(pod_id),
    }
}
