#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::bitmap::{HostBitmap, MAX_PODS};
use crate::hash::{make_synthetic_chain, BlockHash};
use crate::indexer::{shard_for_fibonacci, shard_for_low_bits, ShardedBlockIndexer, SHARDS};
use crate::metrics::summarize_ns;

pub struct SyntheticPart2State {
    pub indexer: ShardedBlockIndexer,
    pub query_chain: Vec<BlockHash>,
    pub expected_depths: Vec<usize>,
    pub candidate_pods: HostBitmap,
}

#[derive(Clone, Debug)]
pub struct PrefixQueryResult {
    pub depths: Vec<usize>,
    pub shard_lookups: usize,
    pub bitmap_intersections: usize,
    pub search_frames: usize,
}

#[derive(Clone, Debug)]
pub struct NaiveQueryResult {
    pub depths: Vec<usize>,
    pub shard_lookups: usize,
    pub pod_bit_tests: usize,
}

#[derive(Clone, Copy, Debug)]
struct SearchFrame {
    min_prefix_depth: usize,
    max_prefix_depth: usize,
    candidate_pods: HostBitmap,
}

#[derive(Clone, Copy, Debug, Default)]
struct QueryCounters {
    shard_lookups: usize,
    bitmap_intersections: usize,
    search_frames: usize,
}

#[derive(Clone, Debug)]
struct ShardStats {
    min: usize,
    max: usize,
    avg: f64,
    stddev: f64,
    empty: usize,
    skew_ratio: f64,
    hottest_shard: usize,
}

pub fn build_synthetic_state(pods: usize, blocks: usize, dropoffs: usize) -> SyntheticPart2State {
    let pods = pods.max(1).min(MAX_PODS);
    let query_chain = make_synthetic_chain(0, blocks);
    let indexer = ShardedBlockIndexer::new(pods);
    let mut expected_depths = vec![0usize; MAX_PODS];
    let mut candidate_pods = HostBitmap::empty();

    let distinct_depths = if blocks == 0 {
        1
    } else {
        dropoffs.max(1).min(blocks)
    };

    let step = if blocks == 0 {
        0
    } else {
        (blocks / distinct_depths).max(1)
    };

    for pod in 0..pods {
        let slot = pod % distinct_depths;
        let depth = if blocks == 0 {
            0
        } else {
            blocks.saturating_sub(slot * step).max(1)
        };
        expected_depths[pod] = depth;
        candidate_pods.set(pod);
        for hash in query_chain.iter().take(depth) {
            indexer.register(pod, *hash);
        }
    }

    println!(
        "Built synthetic state: pods={}, blocks={}, dropoffs={}, distinct_depths={}",
        pods, blocks, dropoffs, distinct_depths
    );

    println!("sharedIndexer: {:?}", indexer.shard_entry_counts());

    SyntheticPart2State {
        indexer,
        query_chain,
        expected_depths,
        candidate_pods,
    }
}

pub fn query_prefix_depths_binary(
    indexer: &ShardedBlockIndexer,
    query_chain: &[BlockHash],
    candidate_pods: HostBitmap,
) -> PrefixQueryResult {
    let mut depths = vec![0usize; MAX_PODS];
    let mut stack = Vec::with_capacity(query_chain.len().saturating_add(MAX_PODS));
    let counters = query_prefix_depths_binary_into(
        indexer,
        query_chain,
        candidate_pods,
        &mut depths,
        &mut stack,
    );

    PrefixQueryResult {
        depths,
        shard_lookups: counters.shard_lookups,
        bitmap_intersections: counters.bitmap_intersections,
        search_frames: counters.search_frames,
    }
}

fn query_prefix_depths_binary_into(
    indexer: &ShardedBlockIndexer,
    query_chain: &[BlockHash],
    candidate_pods: HostBitmap,
    depths: &mut [usize],
    stack: &mut Vec<SearchFrame>,
) -> QueryCounters {
    depths.fill(0);
    stack.clear();

    let mut counters = QueryCounters {
        bitmap_intersections: 1,
        ..QueryCounters::default()
    };

    stack.push(SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: query_chain.len(),
        candidate_pods: candidate_pods.and(indexer.alive()),
    });

    while let Some(frame) = stack.pop() {
        counters.search_frames += 1;
        if frame.candidate_pods.is_empty() {
            continue;
        }

        if frame.min_prefix_depth == frame.max_prefix_depth {
            frame
                .candidate_pods
                .for_each_set_bit(|pod| depths[pod] = frame.min_prefix_depth);
            continue;
        }

        let probe_prefix_depth = (frame.min_prefix_depth + frame.max_prefix_depth + 1) / 2;
        let pods_with_probe_prefix = indexer.owners_alive(query_chain[probe_prefix_depth - 1]);
        counters.shard_lookups += 1;

        let pods_at_or_above_probe = frame.candidate_pods.and(pods_with_probe_prefix);
        let pods_below_probe = frame.candidate_pods.minus(pods_at_or_above_probe);
        counters.bitmap_intersections += 2;

        if !pods_at_or_above_probe.is_empty() {
            stack.push(SearchFrame {
                min_prefix_depth: probe_prefix_depth,
                max_prefix_depth: frame.max_prefix_depth,
                candidate_pods: pods_at_or_above_probe,
            });
        }
        if !pods_below_probe.is_empty() {
            stack.push(SearchFrame {
                min_prefix_depth: frame.min_prefix_depth,
                max_prefix_depth: probe_prefix_depth - 1,
                candidate_pods: pods_below_probe,
            });
        }
    }

    counters
}

pub fn query_prefix_depths_naive(
    indexer: &ShardedBlockIndexer,
    query_chain: &[BlockHash],
    candidate_pods: HostBitmap,
) -> NaiveQueryResult {
    let mut depths = vec![0usize; MAX_PODS];
    let counters =
        query_prefix_depths_naive_into(indexer, query_chain, candidate_pods, &mut depths);

    NaiveQueryResult {
        depths,
        shard_lookups: counters.shard_lookups,
        pod_bit_tests: counters.pod_bit_tests,
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct NaiveCounters {
    shard_lookups: usize,
    pod_bit_tests: usize,
}

fn query_prefix_depths_naive_into(
    indexer: &ShardedBlockIndexer,
    query_chain: &[BlockHash],
    candidate_pods: HostBitmap,
    depths: &mut [usize],
) -> NaiveCounters {
    let alive_candidate_pods = candidate_pods.and(indexer.alive());
    let mut still_matching_pods = alive_candidate_pods;
    let mut counters = NaiveCounters::default();

    depths.fill(0);

    for (block_index, hash) in query_chain.iter().enumerate() {
        let pods_owning_block = indexer.owners_alive(*hash);
        counters.shard_lookups += 1;

        alive_candidate_pods.for_each_set_bit(|pod| {
            if still_matching_pods.contains(pod) {
                counters.pod_bit_tests += 1;
                if pods_owning_block.contains(pod) {
                    depths[pod] = block_index + 1;
                } else {
                    still_matching_pods.clear(pod);
                }
            }
        });
    }

    counters
}

pub fn bench_part2_query(iterations: usize, pods: usize, blocks: usize, dropoffs: usize) {
    let iterations = iterations.max(1);
    let pods = pods.max(1).min(MAX_PODS);
    let state = build_synthetic_state(pods, blocks, dropoffs);
    let mut samples = Vec::with_capacity(iterations);
    let mut depths = vec![0usize; MAX_PODS];
    let mut stack = Vec::with_capacity(blocks.saturating_add(MAX_PODS));
    let mut total_shard_lookups = 0usize;
    let mut total_bitmap_intersections = 0usize;
    let mut total_search_frames = 0usize;
    let mut checksum = 0usize;

    for _ in 0..iterations {
        let start = Instant::now();
        let counters = query_prefix_depths_binary_into(
            &state.indexer,
            &state.query_chain,
            state.candidate_pods,
            &mut depths,
            &mut stack,
        );
        samples.push(start.elapsed().as_nanos());

        total_shard_lookups += counters.shard_lookups;
        total_bitmap_intersections += counters.bitmap_intersections;
        total_search_frames += counters.search_frames;
        checksum = checksum.wrapping_add(depths.iter().take(pods).sum::<usize>());
    }

    let stats = summarize_ns(&mut samples);

    println!("PART2 QUERY PATH: BINARY PREFIX MATCH");
    println!("--------------------------------------------------");
    println!("pods={pods}");
    println!("query_blocks={blocks}");
    println!("distinct_dropoff_depths={dropoffs}");
    println!("iterations={iterations}");
    print_synthetic_state(&state, pods);
    println!();
    println!("latency_p50_us={:.3}", ns_to_us(stats.p50_ns));
    println!("latency_p50_ms={:.6}", ns_to_ms(stats.p50_ns));
    println!("latency_p95_us={:.3}", ns_to_us(stats.p95_ns));
    println!("latency_p95_ms={:.6}", ns_to_ms(stats.p95_ns));
    println!("latency_p99_us={:.3}", ns_to_us(stats.p99_ns));
    println!("latency_p99_ms={:.6}", ns_to_ms(stats.p99_ns));
    println!("latency_max_us={:.3}", ns_to_us(stats.max_ns));
    println!("latency_max_ms={:.6}", ns_to_ms(stats.max_ns));
    println!();
    println!(
        "avg_shard_lookups_per_query={:.3}",
        total_shard_lookups as f64 / iterations as f64
    );
    println!(
        "avg_bitmap_intersections_per_query={:.3}",
        total_bitmap_intersections as f64 / iterations as f64
    );
    println!(
        "avg_search_frames_per_query={:.3}",
        total_search_frames as f64 / iterations as f64
    );
    println!("checksum={checksum}");
}

pub fn bench_part2_compare(iterations: usize, pods: usize, blocks: usize, dropoffs: usize) {
    let iterations = iterations.max(1);
    let pods = pods.max(1).min(MAX_PODS);
    let state = build_synthetic_state(pods, blocks, dropoffs);
    let mut binary_samples = Vec::with_capacity(iterations);
    let mut naive_samples = Vec::with_capacity(iterations);
    let mut binary_depths = vec![0usize; MAX_PODS];
    let mut naive_depths = vec![0usize; MAX_PODS];
    let mut binary_stack = Vec::with_capacity(blocks.saturating_add(MAX_PODS));
    let mut binary_lookup_total = 0usize;
    let mut naive_lookup_total = 0usize;
    let mut naive_pod_bit_tests_total = 0usize;
    let mut checksum = 0usize;

    for _ in 0..iterations {
        let start = Instant::now();
        let binary_counters = query_prefix_depths_binary_into(
            &state.indexer,
            &state.query_chain,
            state.candidate_pods,
            &mut binary_depths,
            &mut binary_stack,
        );
        binary_samples.push(start.elapsed().as_nanos());

        let start = Instant::now();
        let naive = query_prefix_depths_naive_into(
            &state.indexer,
            &state.query_chain,
            state.candidate_pods,
            &mut naive_depths,
        );
        naive_samples.push(start.elapsed().as_nanos());

        assert_eq!(binary_depths, naive_depths);
        binary_lookup_total += binary_counters.shard_lookups;
        naive_lookup_total += naive.shard_lookups;
        naive_pod_bit_tests_total += naive.pod_bit_tests;
        checksum = checksum.wrapping_add(binary_depths.iter().take(pods).sum::<usize>());
    }

    let binary_stats = summarize_ns(&mut binary_samples);
    let naive_stats = summarize_ns(&mut naive_samples);
    let binary_avg_lookups = binary_lookup_total as f64 / iterations as f64;
    let naive_avg_lookups = naive_lookup_total as f64 / iterations as f64;

    println!("PART2 QUERY COMPARISON: BINARY VS NAIVE");
    println!("--------------------------------------------------");
    println!("pods={pods}");
    println!("query_blocks={blocks}");
    println!("distinct_dropoff_depths={dropoffs}");
    println!("iterations={iterations}");
    print_synthetic_state(&state, pods);
    println!();
    println!("binary_p50_us={:.3}", ns_to_us(binary_stats.p50_ns));
    println!("binary_p50_ms={:.6}", ns_to_ms(binary_stats.p50_ns));
    println!("binary_p95_us={:.3}", ns_to_us(binary_stats.p95_ns));
    println!("binary_p95_ms={:.6}", ns_to_ms(binary_stats.p95_ns));
    println!("binary_p99_us={:.3}", ns_to_us(binary_stats.p99_ns));
    println!("binary_p99_ms={:.6}", ns_to_ms(binary_stats.p99_ns));
    println!("binary_avg_shard_lookups={binary_avg_lookups:.3}");
    println!();
    println!("naive_p50_us={:.3}", ns_to_us(naive_stats.p50_ns));
    println!("naive_p50_ms={:.6}", ns_to_ms(naive_stats.p50_ns));
    println!("naive_p95_us={:.3}", ns_to_us(naive_stats.p95_ns));
    println!("naive_p95_ms={:.6}", ns_to_ms(naive_stats.p95_ns));
    println!("naive_p99_us={:.3}", ns_to_us(naive_stats.p99_ns));
    println!("naive_p99_ms={:.6}", ns_to_ms(naive_stats.p99_ns));
    println!("naive_avg_shard_lookups={naive_avg_lookups:.3}");
    println!(
        "naive_avg_pod_bit_tests={:.3}",
        naive_pod_bit_tests_total as f64 / iterations as f64
    );
    println!();
    println!(
        "speedup_p50={:.3}",
        ratio(naive_stats.p50_ns, binary_stats.p50_ns)
    );
    println!(
        "speedup_p99={:.3}",
        ratio(naive_stats.p99_ns, binary_stats.p99_ns)
    );
    println!(
        "lookup_reduction={:.3}",
        ratio_f64(naive_avg_lookups, binary_avg_lookups)
    );
    println!("checksum={checksum}");
}

pub fn bench_part2_shards(chains: usize, blocks_per_chain: usize) {
    let total_hashes = chains.saturating_mul(blocks_per_chain);
    let mut low_bits = vec![0usize; SHARDS];
    let mut fibonacci = vec![0usize; SHARDS];

    for chain_id in 0..chains {
        for hash in make_synthetic_chain(chain_id as u64, blocks_per_chain) {
            low_bits[shard_for_low_bits(hash)] += 1;
            fibonacci[shard_for_fibonacci(hash)] += 1;
        }
    }

    let low_stats = shard_stats(&low_bits);
    let fib_stats = shard_stats(&fibonacci);

    println!("PART2 SHARD DISTRIBUTION: FIBONACCI VS LOW_BITS");
    println!("--------------------------------------------------");
    println!("chains={chains}");
    println!("blocks_per_chain={blocks_per_chain}");
    println!("total_hashes={total_hashes}");
    println!("shards={SHARDS}");
    println!();
    print_shard_stats("LOW_BITS", &low_stats);
    println!();
    print_shard_stats("FIBONACCI", &fib_stats);
}

pub fn bench_part2_concurrency(
    readers: usize,
    writers: usize,
    duration_secs: u64,
    pods: usize,
    blocks: usize,
) {
    let readers = readers.max(1);
    let pods = pods.max(1).min(MAX_PODS);
    let blocks = blocks.max(1);
    let duration_secs = duration_secs.max(1);
    let SyntheticPart2State {
        indexer,
        query_chain,
        expected_depths,
        candidate_pods,
    } = build_synthetic_state(pods, blocks, 4);
    let indexer = Arc::new(indexer);
    let query_chain = Arc::new(query_chain);
    let stop = Arc::new(AtomicBool::new(false));
    let reader_ops = Arc::new(AtomicU64::new(0));
    let writer_ops = Arc::new(AtomicU64::new(0));
    let dead_pod_returned = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let mut reader_handles = Vec::with_capacity(readers);
    let mut writer_handles = Vec::with_capacity(writers);

    for _ in 0..readers {
        let indexer = Arc::clone(&indexer);
        let query_chain = Arc::clone(&query_chain);
        let stop = Arc::clone(&stop);
        let reader_ops = Arc::clone(&reader_ops);
        let dead_pod_returned = Arc::clone(&dead_pod_returned);
        let checksum = Arc::clone(&checksum);
        reader_handles.push(thread::spawn(move || {
            let mut samples = Vec::with_capacity(100_000);
            let mut depths = vec![0usize; MAX_PODS];
            let mut stack = Vec::with_capacity(query_chain.len().saturating_add(MAX_PODS));
            let mut local_checksum = 0u64;
            let mut local_dead = 0u64;

            while !stop.load(Ordering::Relaxed) {
                let alive_before = indexer.alive();
                let start = Instant::now();
                let counters = query_prefix_depths_binary_into(
                    &indexer,
                    &query_chain,
                    candidate_pods,
                    &mut depths,
                    &mut stack,
                );
                samples.push(start.elapsed().as_nanos());

                candidate_pods.for_each_set_bit(|pod| {
                    if !alive_before.contains(pod) && depths[pod] != 0 {
                        local_dead += 1;
                    }
                });
                local_checksum = local_checksum.wrapping_add(
                    depths.iter().take(pods).sum::<usize>() as u64 ^ counters.shard_lookups as u64,
                );
                reader_ops.fetch_add(1, Ordering::Relaxed);
            }

            dead_pod_returned.fetch_add(local_dead, Ordering::Relaxed);
            checksum.fetch_xor(local_checksum, Ordering::Relaxed);
            samples
        }));
    }

    for writer_id in 0..writers {
        let indexer = Arc::clone(&indexer);
        let query_chain = Arc::clone(&query_chain);
        let stop = Arc::clone(&stop);
        let writer_ops = Arc::clone(&writer_ops);
        writer_handles.push(thread::spawn(move || {
            let mut samples = Vec::with_capacity(100_000);
            let mut i = writer_id;

            while !stop.load(Ordering::Relaxed) {
                let hash = query_chain[i % query_chain.len()];
                let pod = i % pods;

                let start = Instant::now();
                match i % 3 {
                    0 => indexer.register(pod, hash),
                    1 => indexer.evict(pod, hash),
                    _ => indexer.register(pod, hash),
                }
                if i % 257 == 0 {
                    indexer.shutdown(pod);
                }
                samples.push(start.elapsed().as_nanos());

                writer_ops.fetch_add(1, Ordering::Relaxed);
                i = i.wrapping_add(writers.max(1));
            }

            samples
        }));
    }

    thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);

    let mut reader_samples = Vec::new();
    for handle in reader_handles {
        let mut samples = handle.join().expect("reader benchmark thread panicked");
        reader_samples.append(&mut samples);
    }

    let mut writer_samples = Vec::new();
    for handle in writer_handles {
        let mut samples = handle.join().expect("writer benchmark thread panicked");
        writer_samples.append(&mut samples);
    }

    let reader_stats = summarize_ns(&mut reader_samples);
    let writer_stats = summarize_ns(&mut writer_samples);
    let reader_ops = reader_ops.load(Ordering::Relaxed);
    let writer_ops = writer_ops.load(Ordering::Relaxed);
    let dead_pod_returned_count = dead_pod_returned.load(Ordering::Relaxed);

    println!("PART2 CONCURRENCY: QUERIES + EVENTS");
    println!("--------------------------------------------------");
    println!("readers={readers}");
    println!("writers={writers}");
    println!("duration_secs={duration_secs}");
    println!("pods={pods}");
    println!("query_blocks={blocks}");
    print_synthetic_state_parts(
        indexer.as_ref(),
        query_chain.as_ref(),
        &expected_depths,
        candidate_pods,
        pods,
    );
    println!();
    println!("reader_ops={reader_ops}");
    println!("writer_ops={writer_ops}");
    println!(
        "reader_ops_per_sec={:.3}",
        reader_ops as f64 / duration_secs as f64
    );
    println!(
        "writer_ops_per_sec={:.3}",
        writer_ops as f64 / duration_secs as f64
    );
    println!();
    println!("query_p50_us={:.3}", ns_to_us(reader_stats.p50_ns));
    println!("query_p50_ms={:.6}", ns_to_ms(reader_stats.p50_ns));
    println!("query_p95_us={:.3}", ns_to_us(reader_stats.p95_ns));
    println!("query_p95_ms={:.6}", ns_to_ms(reader_stats.p95_ns));
    println!("query_p99_us={:.3}", ns_to_us(reader_stats.p99_ns));
    println!("query_p99_ms={:.6}", ns_to_ms(reader_stats.p99_ns));
    println!();
    println!("event_p50_us={:.3}", ns_to_us(writer_stats.p50_ns));
    println!("event_p50_ms={:.6}", ns_to_ms(writer_stats.p50_ns));
    println!("event_p95_us={:.3}", ns_to_us(writer_stats.p95_ns));
    println!("event_p95_ms={:.6}", ns_to_ms(writer_stats.p95_ns));
    println!("event_p99_us={:.3}", ns_to_us(writer_stats.p99_ns));
    println!("event_p99_ms={:.6}", ns_to_ms(writer_stats.p99_ns));
    println!();
    println!("dead_pod_returned_count={dead_pod_returned_count}");
    if dead_pod_returned_count != 0 {
        println!("ERROR: dead pod returned by prefix query");
    }
    println!("checksum={}", checksum.load(Ordering::Relaxed));
}

fn shard_stats(counts: &[usize]) -> ShardStats {
    let total = counts.iter().sum::<usize>();
    let min = counts.iter().copied().min().unwrap_or(0);
    let max = counts.iter().copied().max().unwrap_or(0);
    let avg = if counts.is_empty() {
        0.0
    } else {
        total as f64 / counts.len() as f64
    };
    let variance = if counts.is_empty() {
        0.0
    } else {
        counts
            .iter()
            .map(|count| {
                let diff = *count as f64 - avg;
                diff * diff
            })
            .sum::<f64>()
            / counts.len() as f64
    };
    let hottest_shard = counts
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| **count)
        .map(|(shard, _)| shard)
        .unwrap_or(0);

    ShardStats {
        min,
        max,
        avg,
        stddev: variance.sqrt(),
        empty: counts.iter().filter(|count| **count == 0).count(),
        skew_ratio: if avg == 0.0 { 0.0 } else { max as f64 / avg },
        hottest_shard,
    }
}

fn print_shard_stats(name: &str, stats: &ShardStats) {
    println!("{name}");
    println!("  min_entries={}", stats.min);
    println!("  max_entries={}", stats.max);
    println!("  avg_entries={:.3}", stats.avg);
    println!("  stddev={:.3}", stats.stddev);
    println!("  empty_shards={}", stats.empty);
    println!("  skew_ratio={:.6}", stats.skew_ratio);
    println!("  hottest_shard={}", stats.hottest_shard);
}

fn print_synthetic_state(state: &SyntheticPart2State, pods: usize) {
    print_synthetic_state_parts(
        &state.indexer,
        &state.query_chain,
        &state.expected_depths,
        state.candidate_pods,
        pods,
    );
}

fn print_synthetic_state_parts(
    indexer: &ShardedBlockIndexer,
    query_chain: &[BlockHash],
    expected_depths: &[usize],
    candidate_pods: HostBitmap,
    pods: usize,
) {
    println!("indexer={indexer:#?}");
    println!("query_chain={query_chain:?}");
    println!("expected_depths={:?}", &expected_depths[..pods]);
    println!("candidate_pods={:?}", candidate_pods.iter_set_bits());
}

fn ns_to_us(ns: u128) -> f64 {
    ns as f64 / 1_000.0
}

fn ns_to_ms(ns: u128) -> f64 {
    ns as f64 / 1_000_000.0
}

fn ratio(numerator: u128, denominator: u128) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn ratio_f64(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}
