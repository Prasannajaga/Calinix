use super::block_hash::BlockHash;
use super::host_bitmap::HostBitmap;
use super::sharded_index::ShardedBlockIndexer;

#[derive(Clone, Debug)]
pub struct SearchFrame {
    min_prefix_depth: usize,
    max_prefix_depth: usize,
    candidate_pods: HostBitmap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixMatchDebug {
    pub lengths: Vec<usize>,
    pub frames_processed: usize,
    pub shard_lookups: usize,
    pub bitmap_intersections: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrefixMatch {
    pub pod_id: usize,
    pub prefix_depth: usize,
}

pub fn longest_prefix_lengths_for_candidates(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[BlockHash],
    candidate_pods: HostBitmap,
) -> Vec<usize> {
    let mut lengths = vec![0; result_len(indexer, &candidate_pods)];
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
    let alive = indexer.alive();
    stack.push(SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: cumulative_hashes.len(),
        candidate_pods: candidate_pods.and(&alive),
    });

    while let Some(frame) = stack.pop() {
        if frame.candidate_pods.is_empty() {
            continue;
        }
        if frame.min_prefix_depth == frame.max_prefix_depth {
            for pod_id in frame.candidate_pods.iter_set_bits() {
                if pod_id < lengths.len() {
                    lengths[pod_id] = frame.min_prefix_depth;
                }
            }
            continue;
        }

        let probe_prefix_depth = (frame.min_prefix_depth + frame.max_prefix_depth).div_ceil(2);
        let pods_with_probe_prefix = indexer
            .owners(cumulative_hashes[probe_prefix_depth - 1])
            .and(&alive);
        let pods_at_or_above_probe = frame.candidate_pods.and(&pods_with_probe_prefix);
        let pods_below_probe = frame.candidate_pods.minus(&pods_at_or_above_probe);

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

pub fn best_prefix_match_for_candidates(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[BlockHash],
    candidate_pods: HostBitmap,
) -> Option<PrefixMatch> {
    let alive = indexer.alive();
    let alive_candidates = candidate_pods.and(&alive);
    let fallback_pod = alive_candidates
        .first_set_bit()
        .or_else(|| candidate_pods.first_set_bit())?;

    if cumulative_hashes.is_empty() || alive_candidates.is_empty() {
        return Some(PrefixMatch {
            pod_id: fallback_pod,
            prefix_depth: 0,
        });
    }

    let full_prefix_depth = cumulative_hashes.len();
    let mut best = PrefixMatch {
        pod_id: fallback_pod,
        prefix_depth: 0,
    };
    let mut stack = Vec::with_capacity(cumulative_hashes.len().saturating_add(1));
    stack.push(SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: full_prefix_depth,
        candidate_pods: alive_candidates,
    });

    while let Some(frame) = stack.pop() {
        if frame.candidate_pods.is_empty() || frame.max_prefix_depth < best.prefix_depth {
            continue;
        }
        if frame.min_prefix_depth == frame.max_prefix_depth {
            if let Some(pod_id) = frame.candidate_pods.first_set_bit() {
                if frame.min_prefix_depth > best.prefix_depth
                    || (frame.min_prefix_depth == best.prefix_depth && pod_id < best.pod_id)
                {
                    best = PrefixMatch {
                        pod_id,
                        prefix_depth: frame.min_prefix_depth,
                    };
                }
            }
            continue;
        }

        let probe_prefix_depth = (frame.min_prefix_depth + frame.max_prefix_depth).div_ceil(2);
        let pods_with_probe_prefix = indexer
            .owners(cumulative_hashes[probe_prefix_depth - 1])
            .and(&alive);
        let pods_at_or_above_probe = frame.candidate_pods.and(&pods_with_probe_prefix);

        if probe_prefix_depth == full_prefix_depth {
            if let Some(pod_id) = pods_at_or_above_probe.first_set_bit() {
                return Some(PrefixMatch {
                    pod_id,
                    prefix_depth: full_prefix_depth,
                });
            }
        }

        let pods_below_probe = frame.candidate_pods.minus(&pods_at_or_above_probe);

        stack.push(SearchFrame {
            min_prefix_depth: frame.min_prefix_depth,
            max_prefix_depth: probe_prefix_depth - 1,
            candidate_pods: pods_below_probe,
        });
        stack.push(SearchFrame {
            min_prefix_depth: probe_prefix_depth,
            max_prefix_depth: frame.max_prefix_depth,
            candidate_pods: pods_at_or_above_probe,
        });
    }

    Some(best)
}

pub fn longest_prefix_lengths_debug(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[BlockHash],
    candidate_pods: HostBitmap,
) -> PrefixMatchDebug {
    let mut lengths = vec![0; result_len(indexer, &candidate_pods)];
    let mut frames_processed = 0;
    let mut shard_lookups = 0;
    let mut bitmap_intersections = 1;
    let alive = indexer.alive();
    let mut stack = vec![SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: cumulative_hashes.len(),
        candidate_pods: candidate_pods.and(&alive),
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

        let probe_prefix_depth = (frame.min_prefix_depth + frame.max_prefix_depth).div_ceil(2);
        shard_lookups += 1;
        let pods_with_probe_prefix = indexer
            .owners(cumulative_hashes[probe_prefix_depth - 1])
            .and(&alive);
        let pods_at_or_above_probe = frame.candidate_pods.and(&pods_with_probe_prefix);
        let pods_below_probe = frame.candidate_pods.minus(&pods_at_or_above_probe);
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

fn result_len(indexer: &ShardedBlockIndexer, candidate_pods: &HostBitmap) -> usize {
    indexer
        .pod_count()
        .max(candidate_pods.highest_set_bit_plus_one())
}

#[cfg(test)]
mod tests {
    use super::{best_prefix_match_for_candidates, longest_prefix_lengths_for_candidates};
    use crate::cache_registry::cumulative_hash::{
        make_synthetic_chain, prompt_to_cumulative_hashes,
    };
    use crate::cache_registry::host_bitmap::HostBitmap;
    use crate::cache_registry::sharded_index::ShardedBlockIndexer;

    #[test]
    fn binary_search_finds_longest_cumulative_match() {
        let prompt = "one two three four five six seven eight nine ten eleven twelve";
        let hashes = prompt_to_cumulative_hashes(prompt);
        let indexer = ShardedBlockIndexer::new(3);

        for hash in hashes.iter().take(3) {
            indexer.register(0, *hash);
        }
        indexer.register(1, hashes[2]);
        indexer.register(2, hashes[0]);

        let candidates = HostBitmap::full_for_count(3);
        let lengths = longest_prefix_lengths_for_candidates(&indexer, &hashes, candidates);

        assert_eq!(lengths[0], 3);
        assert_eq!(lengths[1], 0);
        assert_eq!(lengths[2], 1);
    }

    #[test]
    fn prefix_query_masks_shutdown_pods_with_alive_snapshot() {
        let hashes = prompt_to_cumulative_hashes("one two three four five six");
        let indexer = ShardedBlockIndexer::new(2);
        indexer.register_chain(0, &hashes);
        indexer.register_chain(1, &hashes);
        indexer.shutdown(0);

        let lengths =
            longest_prefix_lengths_for_candidates(&indexer, &hashes, HostBitmap::full_for_count(2));

        assert_eq!(lengths[0], 0);
        assert_eq!(lengths[1], hashes.len());
    }

    #[test]
    fn best_prefix_match_picks_deepest_cache_owner() {
        let hashes = make_synthetic_chain(1, 8);
        let indexer = ShardedBlockIndexer::new(3);
        indexer.register_chain(0, &hashes[..2]);
        indexer.register_chain(1, &hashes[..4]);
        indexer.register_chain(2, &hashes);

        let best =
            best_prefix_match_for_candidates(&indexer, &hashes, HostBitmap::full_for_count(3))
                .expect("candidate exists");

        assert_eq!(best.pod_id, 2);
        assert_eq!(best.prefix_depth, hashes.len());
    }

    #[test]
    fn best_prefix_match_falls_back_to_zero_depth_when_no_alive_pods() {
        let hashes = prompt_to_cumulative_hashes("one two three four");
        let indexer = ShardedBlockIndexer::with_shards_empty_alive(2, 4);

        let best =
            best_prefix_match_for_candidates(&indexer, &hashes, HostBitmap::full_for_count(2))
                .expect("candidate exists");

        assert_eq!(best.pod_id, 0);
        assert_eq!(best.prefix_depth, 0);
    }
}
