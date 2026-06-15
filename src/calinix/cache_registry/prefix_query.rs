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
    stack.push(SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: cumulative_hashes.len(),
        candidate_pods: candidate_pods.and(&indexer.alive()),
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

        let probe_prefix_depth = (frame.min_prefix_depth + frame.max_prefix_depth + 1) / 2;
        let pods_with_probe_prefix =
            indexer.owners_alive(cumulative_hashes[probe_prefix_depth - 1]);
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

pub fn longest_prefix_lengths_debug(
    indexer: &ShardedBlockIndexer,
    cumulative_hashes: &[BlockHash],
    candidate_pods: HostBitmap,
) -> PrefixMatchDebug {
    let mut lengths = vec![0; result_len(indexer, &candidate_pods)];
    let mut frames_processed = 0;
    let mut shard_lookups = 0;
    let mut bitmap_intersections = 1;
    let mut stack = vec![SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: cumulative_hashes.len(),
        candidate_pods: candidate_pods.and(&indexer.alive()),
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
    use super::longest_prefix_lengths_for_candidates;
    use crate::cache_registry::cumulative_hash::prompt_to_cumulative_hashes;
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
}
