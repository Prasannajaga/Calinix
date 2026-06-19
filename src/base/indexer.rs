#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::RwLock;

use crate::bitmap::{HostBitmap, MAX_PODS};
use crate::hash::BlockHash;

pub const SHARDS: usize = 256;
pub const SHARD_BITS: u32 = 8;
pub const FIBONACCI: u64 = 0x9E3779B97F4A7C15;

pub const SHARD_COUNT: usize = SHARDS;

pub fn shard_for_fibonacci(hash: BlockHash) -> usize {
    ((hash.wrapping_mul(FIBONACCI)) >> (64 - SHARD_BITS)) as usize
}

pub fn shard_for_low_bits(hash: BlockHash) -> usize {
    (hash as usize) & (SHARDS - 1)
}

pub fn shard_for(hash: BlockHash) -> usize {
    shard_for_fibonacci(hash)
}

#[derive(Debug)]
pub struct ShardedBlockIndexer {
    shards: Vec<RwLock<HashMap<BlockHash, HostBitmap>>>,
    alive: RwLock<HostBitmap>,
}

impl ShardedBlockIndexer {
    pub fn new(pod_count: usize) -> Self {
        let shards = (0..SHARDS).map(|_| RwLock::new(HashMap::new())).collect();
        Self {
            shards,
            alive: RwLock::new(HostBitmap::full_for_count(pod_count)),
        }
    }

    pub fn register(&self, pod_id: usize, cumulative_hash: BlockHash) {
        let shard = shard_for_fibonacci(cumulative_hash);
        let mut guard = self.shards[shard].write().expect("index shard poisoned");
        guard
            .entry(cumulative_hash)
            .or_insert_with(HostBitmap::empty)
            .set(pod_id);
    }

    pub fn evict(&self, pod_id: usize, cumulative_hash: BlockHash) {
        let shard = shard_for_fibonacci(cumulative_hash);
        let mut guard = self.shards[shard].write().expect("index shard poisoned");
        if let Some(owners) = guard.get_mut(&cumulative_hash) {
            owners.clear(pod_id);
            if owners.is_empty() {
                guard.remove(&cumulative_hash);
            }
        }
    }

    pub fn shutdown(&self, pod_id: usize) {
        self.alive
            .write()
            .expect("alive bitmap poisoned")
            .clear(pod_id);
    }

    pub fn owners(&self, cumulative_hash: BlockHash) -> HostBitmap {
        let shard = shard_for_fibonacci(cumulative_hash);
        self.shards[shard]
            .read()
            .expect("index shard poisoned")
            .get(&cumulative_hash)
            .copied()
            .unwrap_or_else(HostBitmap::empty)
    }

    pub fn owners_alive(&self, cumulative_hash: BlockHash) -> HostBitmap {
        self.owners(cumulative_hash).and(self.alive())
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

    pub fn shard_entry_counts(&self) -> Vec<usize> {
        self.shards
            .iter()
            .map(|shard| shard.read().expect("index shard poisoned").len())
            .collect()
    }
}

pub struct SearchFrame {
    min_prefix_depth: usize,
    max_prefix_depth: usize,
    candidate_pods: HostBitmap,
}

pub fn longest_prefix_lengths_for_candidates(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[BlockHash],
    candidate_pods: HostBitmap,
) -> Vec<usize> {
    let mut lengths = vec![0; MAX_PODS];
    let mut stack = Vec::with_capacity(cumulative_hashes.len().saturating_add(1));
    longest_prefix_lengths_into(
        indexer,
        cumulative_hashes,
        candidate_pods,
        &mut lengths,
        &mut stack,
    );
    lengths
}

pub fn longest_prefix_lengths_into(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[BlockHash],
    candidate_pods: HostBitmap,
    lengths: &mut [usize],
    stack: &mut Vec<SearchFrame>,
) {
    lengths.fill(0);
    stack.clear();
    stack.push(SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: cumulative_hashes.len(),
        candidate_pods: candidate_pods.and(indexer.alive()),
    });

    while let Some(frame) = stack.pop() {
        if frame.candidate_pods.is_empty() {
            continue;
        }
        if frame.min_prefix_depth == frame.max_prefix_depth {
            for pod_id in frame.candidate_pods.iter_set_bits() {
                lengths[pod_id] = frame.min_prefix_depth;
            }
            continue;
        }

        let probe_prefix_depth = (frame.min_prefix_depth + frame.max_prefix_depth + 1) / 2;
        let pods_with_probe_prefix =
            indexer.owners_alive(cumulative_hashes[probe_prefix_depth - 1]);
        let pods_at_or_above_probe = frame.candidate_pods.and(pods_with_probe_prefix);
        let pods_below_probe = frame.candidate_pods.minus(pods_at_or_above_probe);

        stack.push(SearchFrame {
            min_prefix_depth: probe_prefix_depth,
            max_prefix_depth: frame.max_prefix_depth,
            candidate_pods: pods_at_or_above_probe,
        });
        stack.push(SearchFrame {
            min_prefix_depth: frame.min_prefix_depth,
            max_prefix_depth: probe_prefix_depth - 1,
            candidate_pods: pods_below_probe,
        });
    }
}

#[derive(Clone, Debug)]
pub struct PrefixMatchDebug {
    pub lengths: Vec<usize>,
    pub frames_processed: usize,
    pub shard_lookups: usize,
    pub bitmap_intersections: usize,
}

pub fn longest_prefix_lengths_debug(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[BlockHash],
    candidate_pods: HostBitmap,
) -> PrefixMatchDebug {
    let mut lengths = vec![0; MAX_PODS];
    let mut frames_processed = 0;
    let mut shard_lookups = 0;
    let mut bitmap_intersections = 1;
    let mut stack = vec![SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: cumulative_hashes.len(),
        candidate_pods: candidate_pods.and(indexer.alive()),
    }];

    while let Some(frame) = stack.pop() {
        frames_processed += 1;
        if frame.candidate_pods.is_empty() {
            continue;
        }
        if frame.min_prefix_depth == frame.max_prefix_depth {
            for pod_id in frame.candidate_pods.iter_set_bits() {
                lengths[pod_id] = frame.min_prefix_depth;
            }
            continue;
        }

        let probe_prefix_depth = (frame.min_prefix_depth + frame.max_prefix_depth + 1) / 2;
        shard_lookups += 1;
        let pods_with_probe_prefix =
            indexer.owners_alive(cumulative_hashes[probe_prefix_depth - 1]);
        let pods_at_or_above_probe = frame.candidate_pods.and(pods_with_probe_prefix);
        let pods_below_probe = frame.candidate_pods.minus(pods_at_or_above_probe);
        bitmap_intersections += 2;

        stack.push(SearchFrame {
            min_prefix_depth: probe_prefix_depth,
            max_prefix_depth: frame.max_prefix_depth,
            candidate_pods: pods_at_or_above_probe,
        });
        stack.push(SearchFrame {
            min_prefix_depth: frame.min_prefix_depth,
            max_prefix_depth: probe_prefix_depth - 1,
            candidate_pods: pods_below_probe,
        });
    }

    PrefixMatchDebug {
        lengths,
        frames_processed,
        shard_lookups,
        bitmap_intersections,
    }
}
