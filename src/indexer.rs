#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::RwLock;

use crate::bitmap::{HostBitmap, MAX_PODS};

const SHARDS: usize = 256;
const SHARD_BITS: u32 = 8;
const FIBONACCI: u64 = 11400714819323198485;

pub fn shard_for(hash: u64) -> usize {
    ((hash.wrapping_mul(FIBONACCI)) >> (64 - SHARD_BITS)) as usize
}

pub struct ShardedBlockIndexer {
    shards: Vec<RwLock<HashMap<u64, HostBitmap>>>,
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

    pub fn register(&self, pod_id: usize, cumulative_hash: u64) {
        let shard = shard_for(cumulative_hash);
        let mut guard = self.shards[shard].write().expect("index shard poisoned");
        guard
            .entry(cumulative_hash)
            .or_insert_with(HostBitmap::empty)
            .set(pod_id);
    }

    pub fn shutdown(&self, pod_id: usize) {
        self.alive
            .write()
            .expect("alive bitmap poisoned")
            .clear(pod_id);
    }

    pub fn owners_alive(&self, cumulative_hash: u64) -> HostBitmap {
        let shard = shard_for(cumulative_hash);
        let owners = self.shards[shard]
            .read()
            .expect("index shard poisoned")
            .get(&cumulative_hash)
            .copied()
            .unwrap_or_else(HostBitmap::empty);
        owners.and(self.alive())
    }

    pub fn alive(&self) -> HostBitmap {
        *self.alive.read().expect("alive bitmap poisoned")
    }
}

struct SearchFrame {
    lo: usize,
    hi: usize,
    hosts: HostBitmap,
}

pub fn longest_prefix_lengths_for_candidates(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[u64],
    candidate_hosts: HostBitmap,
) -> Vec<usize> {
    let mut lengths = vec![0; MAX_PODS];
    let mut stack = vec![SearchFrame {
        lo: 0,
        hi: cumulative_hashes.len(),
        hosts: candidate_hosts.and(indexer.alive()),
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
        let owners = indexer.owners_alive(cumulative_hashes[mid - 1]);
        let yes = frame.hosts.and(owners);
        let no = frame.hosts.minus(yes);

        stack.push(SearchFrame {
            lo: mid,
            hi: frame.hi,
            hosts: yes,
        });
        stack.push(SearchFrame {
            lo: frame.lo,
            hi: mid - 1,
            hosts: no,
        });
    }

    lengths
}
