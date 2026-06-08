use std::collections::HashMap;
use std::sync::RwLock;

use crate::bitmap::{HostBitmap, MAX_PODS};
use crate::types::{BlockHash, CacheEvent, PodId};

const SHARDS: usize = 256;
const SHARD_BITS: u32 = 8;
const FIBONACCI: u64 = 11400714819323198485;

pub fn shard_for(hash: u64) -> usize {
    ((hash.wrapping_mul(FIBONACCI)) >> (64 - SHARD_BITS)) as usize
}

pub struct ShardedBlockIndexer {
    shards: Vec<RwLock<HashMap<BlockHash, HostBitmap>>>,
    alive: RwLock<HostBitmap>,
}

impl ShardedBlockIndexer {
    pub fn new(alive_count: usize) -> Self {
        let shards = (0..SHARDS)
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>();
        Self {
            shards,
            alive: RwLock::new(HostBitmap::full_for_count(alive_count)),
        }
    }

    pub fn apply_event(&self, event: CacheEvent) {
        match event {
            CacheEvent::Registered { pod_id, block_hash } => {
                let shard = shard_for(block_hash);
                let mut guard = self.shards[shard].write().expect("index shard poisoned");
                let bitmap = guard.entry(block_hash).or_insert_with(HostBitmap::empty);
                bitmap.set(pod_id);
            }
            CacheEvent::Evicted { pod_id, block_hash } => {
                let shard = shard_for(block_hash);
                let mut guard = self.shards[shard].write().expect("index shard poisoned");
                if let Some(bitmap) = guard.get_mut(&block_hash) {
                    bitmap.clear(pod_id);
                    if bitmap.is_empty() {
                        guard.remove(&block_hash);
                    }
                }
            }
            CacheEvent::Shutdown { pod_id } => {
                self.alive
                    .write()
                    .expect("alive bitmap poisoned")
                    .clear(pod_id);
            }
        }
    }

    pub fn owners(&self, block_hash: BlockHash) -> HostBitmap {
        let shard = shard_for(block_hash);
        self.shards[shard]
            .read()
            .expect("index shard poisoned")
            .get(&block_hash)
            .copied()
            .unwrap_or_else(HostBitmap::empty)
    }

    pub fn alive(&self) -> HostBitmap {
        *self.alive.read().expect("alive bitmap poisoned")
    }

    pub fn owners_alive(&self, block_hash: BlockHash) -> HostBitmap {
        self.owners(block_hash).and(self.alive())
    }

    pub fn cleanup_dead_pod(&self, pod_id: PodId) {
        for shard in &self.shards {
            let mut guard = shard.write().expect("index shard poisoned");
            guard.retain(|_, bitmap| {
                bitmap.clear(pod_id);
                !bitmap.is_empty()
            });
        }
    }
}

struct SearchFrame {
    lo: usize,
    hi: usize,
    hosts: HostBitmap,
}

pub fn longest_prefix_lengths_for_candidates(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[BlockHash],
    candidate_hosts: HostBitmap,
) -> Vec<usize> {
    let mut lengths = vec![0; MAX_PODS];
    let initial_hosts = candidate_hosts.and(indexer.alive());
    let mut stack = vec![SearchFrame {
        lo: 0,
        hi: cumulative_hashes.len(),
        hosts: initial_hosts,
    }];

    while let Some(frame) = stack.pop() {
        if frame.hosts.is_empty() {
            continue;
        }
        if frame.lo == frame.hi {
            for pod_id in frame.hosts.iter_set_bits() {
                lengths[pod_id] = frame.lo;
            }
            continue;
        }

        let mid = (frame.lo + frame.hi + 1) / 2;
        let owners_at_mid = indexer.owners_alive(cumulative_hashes[mid - 1]);
        let yes = frame.hosts.and(owners_at_mid);
        let no = frame.hosts.minus(yes);

        if !yes.is_empty() {
            stack.push(SearchFrame {
                lo: mid,
                hi: frame.hi,
                hosts: yes,
            });
        }
        if !no.is_empty() {
            stack.push(SearchFrame {
                lo: frame.lo,
                hi: mid - 1,
                hosts: no,
            });
        }
    }

    lengths
}
