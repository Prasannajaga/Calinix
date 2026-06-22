use crate::cache_registry::cumulative_hash::{
    cumulative_hashes_from_blocks, make_synthetic_chain, prompt_to_cumulative_hashes,
    prompt_to_cumulative_hashes_streaming,
};
use crate::cache_registry::block_hash::{
    hash_block, prompt_to_block_hashes, prompt_to_block_hashes_with_size, tokenize,
    DEFAULT_BLOCK_SIZE,
};
use crate::cache_registry::fibonacci::{shard_for_with_count, DEFAULT_SHARD_COUNT};
use crate::cache_registry::prefix_query::{
    best_prefix_match_for_candidates, longest_prefix_lengths_for_candidates,
};
use crate::cache_registry::sharded_index::ShardedBlockIndexer;
use crate::cache_registry::{
    prompt_to_cumulative_hashes_with_block_size, prompt_to_token_blocks_with_size,
    shard_for_fibonacci_with_count, CacheRegistry, HostBitmap,
};
use crate::upstream::RuntimeRegistry;

use std::sync::Arc;
use std::thread;

use super::parse_config;

// --- From original tests/mod.rs ---

#[test]
fn sharded_index_is_initialized_for_assigned_pods() {
    let config = parse_config();
    let registry = RuntimeRegistry::from_config(&config).expect("registry builds from config");
    let index = registry.cache_registry.index();

    println!(
        "index: {:#?}, total_pods: {}",
        index.total_entries(),
        registry.total_pods()
    );
    println!("index shard count: {}", index.shard_count());
    println!(
        "registry cache registry alive pods: {:#?}",
        registry.cache_registry.alive()
    );
    println!(
        "registry pod table by external id: {:#?}",
        registry.pod_table
    );

    assert_eq!(index.pod_count(), registry.total_pods());
    assert_eq!(index.shard_count(), 64);
    assert_eq!(registry.cache_registry.alive().count(), 0);
    assert_eq!(registry.pod_table.by_external_id.get("single-1"), Some(&0));
    assert_eq!(registry.pod_table.by_external_id.get("decode-2"), Some(&5));
}

#[test]
fn example_prompts_hash_to_shards_and_bitmap_lookup() {
    const BLOCK_SIZE: usize = 2;
    const POD_SIZE: usize = 10;
    let prompts = [
        "the cat sat on the table",
        "the cat slept near the window",
        "cache aware routing picks warm pods",
        "prefill workers reuse prompt blocks",
        "decode workers stream tokens fast",
        "fibonacci hashing spreads block keys",
        "sharded indexes reduce lock contention",
        "bitmap lookup returns owner pods",
        "hot prefixes should stay local",
        "gpu cache hits save latency",
    ];

    let indexer = ShardedBlockIndexer::with_shards(POD_SIZE, DEFAULT_SHARD_COUNT);
    let mut prompt_block_hashes = Vec::with_capacity(prompts.len());
    let mut prompt_prefixes = Vec::with_capacity(prompts.len());

    for (pod_id, prompt) in prompts.iter().enumerate() {
        let block_hashes = prompt_to_cumulative_hashes_with_block_size(prompt, BLOCK_SIZE);
        let block_texts = prompt_to_token_blocks_with_size(prompt, BLOCK_SIZE);
        let prefix_texts = block_texts
            .iter()
            .enumerate()
            .map(|(block_id, _)| block_texts[..=block_id].join(" "))
            .collect::<Vec<_>>();
        let shards = block_hashes
            .iter()
            .map(|hash| shard_for_fibonacci_with_count(*hash, DEFAULT_SHARD_COUNT))
            .collect::<Vec<_>>();

        println!(
            "prompt_example={pod_id} prompt=\"{prompt}\" cumulative_hashes={block_hashes:?} shards={shards:?}"
        );

        for block_hash in &block_hashes {
            indexer.register(pod_id, *block_hash);
        }

        prompt_block_hashes.push(block_hashes);
        prompt_prefixes.push(prefix_texts);
    }

    for (pod_id, block_hashes) in prompt_block_hashes.iter().enumerate() {
        println!(
            "prompt_example={pod_id} original_prompt=\"{}\"",
            prompts[pod_id]
        );

        for (block_id, block_hash) in block_hashes.iter().enumerate() {
            let prefix_prompt = &prompt_prefixes[pod_id][block_id];
            let shard = shard_for_fibonacci_with_count(*block_hash, DEFAULT_SHARD_COUNT);

            println!(
                "  block_id={block_id} cumulative_hash={block_hash} shard={shard} prompt_prefix=\"{prefix_prompt}\""
            );
        }
    }

    let shared_block_hash = prompt_block_hashes[0][0];
    let shared_owners = indexer.owners(shared_block_hash);

    assert_eq!(prompt_block_hashes[1][0], shared_block_hash);
    assert_eq!(shared_owners.iter_set_bits(), vec![0, 1]);
    assert!(shared_owners.contains(0));
    assert!(shared_owners.contains(1));
}

#[test]
fn fibonacci_hashing_distributes_blocks_across_shards_for_many_pods() {
    const PODS: usize = 10;
    const BLOCKS_PER_POD: usize = 64;

    let indexer = ShardedBlockIndexer::with_shards(PODS, DEFAULT_SHARD_COUNT);
    let mut expected_counts = vec![0usize; DEFAULT_SHARD_COUNT];

    for pod_id in 0..PODS {
        for block_id in 0..BLOCKS_PER_POD {
            let block_hash = (pod_id * BLOCKS_PER_POD + block_id) as u64;
            let shard = shard_for_fibonacci_with_count(block_hash, DEFAULT_SHARD_COUNT);

            expected_counts[shard] += 1;
            indexer.register(pod_id, block_hash);
        }
    }

    let shard_counts = indexer.shard_entry_counts();
    let total_blocks = PODS * BLOCKS_PER_POD;
    let expected_per_shard = total_blocks / DEFAULT_SHARD_COUNT;
    let min_entries = *shard_counts.iter().min().expect("shards exist");
    let max_entries = *shard_counts.iter().max().expect("shards exist");
    let allowed_skew = 2;

    assert_eq!(indexer.total_entries(), total_blocks);
    assert_eq!(shard_counts, expected_counts);
    assert_eq!(
        shard_counts.iter().filter(|entries| **entries > 0).count(),
        DEFAULT_SHARD_COUNT
    );
    assert!(min_entries >= expected_per_shard - 1);
    assert!(max_entries <= expected_per_shard + allowed_skew);
}

// --- From cache_registry/mod.rs ---

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

// --- From cache_registry/cumulative_hash.rs ---

#[test]
fn cumulative_hashes_change_with_prefix_order() {
    let first = prompt_to_cumulative_hashes("one two three four five six");
    let second = prompt_to_cumulative_hashes("five six one two three four");

    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    assert_ne!(first, second);
}

#[test]
fn streaming_matches_original_behavior() {
    let prompt = "one two three four five six seven eight nine ten eleven twelve";
    let block_size = 3;

    // original
    let blocks = prompt_to_block_hashes_with_size(prompt, block_size);
    let orig = cumulative_hashes_from_blocks(&blocks);

    // streaming
    let stream = prompt_to_cumulative_hashes_streaming(prompt, block_size);

    assert_eq!(orig, stream);
}

// --- From cache_registry/host_bitmap.rs ---

#[test]
fn bitmap_intersections_work_across_words() {
    let mut a = HostBitmap::empty();
    a.set(0);
    a.set(65);
    a.set(255);

    let mut b = HostBitmap::empty();
    b.set(65);
    b.set(127);

    assert!(a.contains(0));
    assert!(a.contains(65));
    assert_eq!(a.count_ones(), 3);
    assert_eq!(a.and(&b).iter_set_bits(), vec![65]);
    assert_eq!(a.or(&b).iter_set_bits(), vec![0, 65, 127, 255]);
    assert_eq!(a.minus(&b).iter_set_bits(), vec![0, 255]);

    a.clear(65);
    assert_eq!(a.iter_set_bits(), vec![0, 255]);
}

#[test]
fn full_for_count_sets_correct_bits() {
    let bm = HostBitmap::full_for_count(3);
    assert_eq!(bm.iter_set_bits(), vec![0, 1, 2]);
    assert_eq!(bm.count_ones(), 3);

    let bm = HostBitmap::full_for_count(64);
    assert_eq!(bm.count_ones(), 64);
    assert!(bm.contains(63));
    assert!(!bm.contains(64));

    let bm = HostBitmap::full_for_count(256);
    assert_eq!(bm.count_ones(), 256);
    assert!(bm.contains(255));
}

#[test]
fn out_of_bounds_pod_is_silently_ignored() {
    let mut bm = HostBitmap::empty();
    bm.set(300);
    assert!(bm.is_empty());
    assert!(!bm.contains(300));
}

#[test]
fn bitmap_is_copy() {
    let a = HostBitmap::full_for_count(8);
    let b = a;
    assert_eq!(a, b);
}

// --- From cache_registry/block_hash.rs ---

#[test]
fn prompt_is_split_into_configured_token_blocks() {
    let hashes = prompt_to_block_hashes("one two three four five", None);

    assert_eq!(DEFAULT_BLOCK_SIZE, 4);
    assert_eq!(hashes.len(), 2);
    assert_ne!(hashes[0], hashes[1]);
}

#[test]
fn testing() {
    let prompt = "Explain the kuberenetes";
    let block_size = 3;

    let tokenize = tokenize(prompt);
    let block_split = tokenize.chunks(block_size);
    let block_hashes: Vec<_> = block_split.map(hash_block).collect();

    println!("tokenize={:?}", tokenize);
    println!("block_hashes={:?}", block_hashes);
}

// --- From cache_registry/prefix_query.rs ---

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

// --- From cache_registry/fibonacci.rs ---

#[test]
fn shard_is_inside_runtime_range() {
    assert!(shard_for_with_count(42, DEFAULT_SHARD_COUNT) < DEFAULT_SHARD_COUNT);
    assert!(shard_for_with_count(u64::MAX, 17) < 17);
}

// --- From cache_registry/sharded_index.rs ---

#[test]
fn shutdown_masks_pods_without_cleanup() {
    let indexer = ShardedBlockIndexer::new(2);
    indexer.register(0, 42);
    indexer.register(1, 42);

    assert_eq!(indexer.owners_alive(42).iter_set_bits(), vec![0, 1]);
    indexer.shutdown(0);
    assert_eq!(indexer.owners_alive(42).iter_set_bits(), vec![1]);
}

#[test]
fn block_owners_snapshot_is_sorted() {
    let indexer = ShardedBlockIndexer::new(2);
    indexer.register(1, 99);
    indexer.register(0, 42);
    indexer.register(1, 42);

    let block_owners = indexer.block_owners();

    assert_eq!(block_owners.len(), 2);
    assert_eq!(block_owners[0].0, 42);
    assert_eq!(block_owners[0].1.iter_set_bits(), vec![0, 1]);
    assert_eq!(block_owners[1].0, 99);
    assert_eq!(block_owners[1].1.iter_set_bits(), vec![1]);
}

#[test]
fn duplicate_register_is_noop_for_same_pod_and_hash() {
    let indexer = ShardedBlockIndexer::new(2);

    assert!(indexer.register(0, 42));
    assert!(!indexer.register(0, 42));

    let owners = indexer.owners(42);
    assert_eq!(owners.iter_set_bits(), vec![0]);
    assert_eq!(indexer.total_entries(), 1);
}

#[test]
fn parallel_register_evict_and_shutdown_remain_consistent() {
    const PODS: usize = 128;
    const SHARDS: usize = 32;
    const HASHES_PER_POD: usize = 8;

    let indexer = Arc::new(ShardedBlockIndexer::with_shards(PODS, SHARDS));
    let mut handles = Vec::new();

    for pod_id in 0..PODS {
        let indexer = Arc::clone(&indexer);
        handles.push(thread::spawn(move || {
            for offset in 0..HASHES_PER_POD {
                indexer.register(pod_id, (offset as u64) + 10_000);
            }
            if pod_id % 2 == 0 {
                indexer.shutdown(pod_id);
            }
            for offset in 0..HASHES_PER_POD {
                if pod_id % 3 == 0 {
                    indexer.evict(pod_id, (offset as u64) + 10_000);
                }
            }
        }));
    }

    for handle in handles {
        handle.join().expect("worker did not panic");
    }

    for offset in 0..HASHES_PER_POD {
        let owners = indexer.owners((offset as u64) + 10_000);
        for pod_id in (0..PODS).filter(|pod_id| pod_id % 3 != 0) {
            assert!(
                owners.contains(pod_id),
                "pod {pod_id} should still own hash"
            );
        }

        let owners_alive = indexer.owners_alive((offset as u64) + 10_000);
        for pod_id in 0..PODS {
            assert_eq!(
                owners_alive.contains(pod_id),
                pod_id % 2 != 0 && pod_id % 3 != 0,
                "unexpected live ownership for pod {pod_id}"
            );
        }
    }
}
