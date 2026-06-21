use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use calinix::cache_registry::{
    prompt_to_cumulative_hashes_with_block_size, CacheRegistry, HostBitmap,
};
use calinix::config::{
    CacheRegistryConfig, CalinixConfig, DispatchUpstreamsConfig, GatewayConfig, HealthConfig,
    PodConfig, PodGroupConfig, SingleUpstreamsConfig, Strategy, UpstreamMode, UpstreamsConfig,
};
use calinix::protocol::routing_headers::CalinixMode;
use calinix::routing::pipeline::RoutingPipeline;
use calinix::routing::plan::RoutingPlan;
use calinix::routing::score::{ScoreStage, ScoreWeights};
use calinix::session::StickyStore;
use calinix::upstream::{LoadState, RuntimeRegistry};
use http::HeaderMap;
use serde_json::json;

const RESULTS_ROOT: &str = "benchmark/results";
const DEFAULT_RUN_NAME: &str = "policy-bench";
const DEFAULT_OUTPUT_FILE: &str = "policy_bench.csv";
const DEFAULT_BLOCK_SIZE: usize = 32;

#[derive(Clone, Debug)]
struct BenchConfig {
    name: String,
    pods: usize,
    shards: usize,
    requests: usize,
    concurrency: usize,
    concurrency_sweep: Vec<usize>,
    block_size: usize,
    prompt_blocks: usize,
    shared_prefix_blocks: usize,
    skew_percent: usize,
    write_ratio_percent: usize,
    lag_sweep_ms: Vec<u64>,
    qps: usize,
    cache_weight: f64,
    load_weight: f64,
    output: PathBuf,
}

#[derive(Clone, Debug)]
struct MetricRow {
    scenario: String,
    concurrency: usize,
    lag_ms: u64,
    operations: usize,
    elapsed_ms: u128,
    rps: f64,
    latency_count: usize,
    avg_us: f64,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
    write_latency_count: usize,
    write_avg_us: f64,
    write_p99_us: u128,
    jain_fairness: Option<f64>,
    misroute_rate: Option<f64>,
    cache_gap_avg_blocks: Option<f64>,
    notes: String,
}

#[derive(Clone, Debug)]
struct Workload {
    hot_prompt: String,
    cold_prompts: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
enum CacheEventKind {
    Register,
    Evict,
}

#[derive(Clone, Debug)]
struct PendingCacheEvent {
    due_ms: u64,
    pod_id: usize,
    hashes: Vec<u64>,
    kind: CacheEventKind,
}

fn main() {
    let config = match parse_args(env::args().skip(1).collect()) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            print_usage();
            std::process::exit(1);
        }
    };

    if let Err(err) = run(config) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run(config: BenchConfig) -> Result<(), String> {
    let workload = Workload::new(&config);
    let mut rows = Vec::new();

    for concurrency in &config.concurrency_sweep {
        let mut run_config = config.clone();
        run_config.concurrency = *concurrency;
        eprintln!("[policy-bench] running decision_latency concurrency={concurrency}");
        rows.push(run_decision_latency(&run_config, &workload)?);
        eprintln!("[policy-bench] running index_contention concurrency={concurrency}");
        rows.push(run_index_contention(&run_config, &workload)?);
        eprintln!("[policy-bench] running hotspot_fairness concurrency={concurrency}");
        rows.push(run_fairness(&run_config, &workload)?);
    }

    let mut staleness_config = config.clone();
    staleness_config.concurrency = config
        .concurrency_sweep
        .last()
        .copied()
        .unwrap_or(config.concurrency);
    for lag_ms in &config.lag_sweep_ms {
        eprintln!("[policy-bench] running staleness_sensitivity lag_ms={lag_ms}");
        rows.push(run_staleness(&staleness_config, &workload, *lag_ms)?);
    }

    write_csv(&config.output, &rows)?;
    print_summary(&config, &rows);
    Ok(())
}

fn run_decision_latency(config: &BenchConfig, workload: &Workload) -> Result<MetricRow, String> {
    let registry = Arc::new(setup_registry(config)?);
    seed_warm_cache(&registry.cache_registry, config, workload);
    let loads = Arc::new(LoadState::new(config.pods));
    let sticky = Arc::new(StickyStore::new());
    let workload = Arc::new(workload.clone());
    let next_request = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(Mutex::new(Vec::with_capacity(config.requests)));
    let selected = Arc::new(Mutex::new(vec![0usize; config.pods]));
    let errors = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(config.concurrency.max(1));

    for _ in 0..config.concurrency.max(1) {
        let registry = Arc::clone(&registry);
        let loads = Arc::clone(&loads);
        let sticky = Arc::clone(&sticky);
        let workload = Arc::clone(&workload);
        let next_request = Arc::clone(&next_request);
        let samples = Arc::clone(&samples);
        let selected = Arc::clone(&selected);
        let errors = Arc::clone(&errors);
        let pipeline = routing_pipeline(config);
        let skew_percent = config.skew_percent;
        let request_limit = config.requests;
        handles.push(thread::spawn(move || {
            let headers = HeaderMap::new();
            let mut local_samples = Vec::new();
            let mut local_selected = vec![0usize; registry.total_pods()];
            loop {
                let request_id = next_request.fetch_add(1, Ordering::Relaxed);
                if request_id >= request_limit {
                    break;
                }
                let prompt = workload.prompt_for(request_id, skew_percent);
                let body = chat_body(prompt, request_id);
                let route_started = Instant::now();
                match pipeline.route_openai_request(
                    &registry,
                    &loads,
                    &sticky,
                    "/v1/chat/completions",
                    "POST",
                    &headers,
                    body.as_bytes(),
                ) {
                    Ok(routed) => {
                        local_samples.push(route_started.elapsed().as_micros());
                        let pod_id = selected_pod(&routed.plan);
                        local_selected[pod_id] += 1;
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            samples
                .lock()
                .expect("decision samples poisoned")
                .extend(local_samples);
            let mut selected = selected.lock().expect("decision counts poisoned");
            for (pod_id, count) in local_selected.into_iter().enumerate() {
                selected[pod_id] += count;
            }
        }));
    }

    for handle in handles {
        handle
            .join()
            .map_err(|_| "decision latency worker panicked".to_string())?;
    }

    let samples = samples.lock().expect("decision samples poisoned").clone();
    let selected = selected.lock().expect("decision counts poisoned").clone();
    let errors = errors.load(Ordering::Relaxed);

    Ok(row(
        "decision_latency",
        config.concurrency,
        0,
        config.requests,
        started.elapsed(),
        &samples,
        &[],
        Some(jain_fairness(&selected)),
        None,
        None,
        format!("route_errors={errors}; includes parse/tokenize/hash/index/score"),
    ))
}

fn run_index_contention(config: &BenchConfig, workload: &Workload) -> Result<MetricRow, String> {
    let registry = Arc::new(CacheRegistry::with_shards(config.pods, config.shards));
    seed_warm_cache(&registry, config, workload);

    let candidate_pods = HostBitmap::full_for_count(config.pods);
    let hot_hashes = Arc::new(prompt_to_cumulative_hashes_with_block_size(
        &workload.hot_prompt,
        config.block_size,
    ));
    let cold_hashes = Arc::new(
        workload
            .cold_prompts
            .iter()
            .map(|prompt| prompt_to_cumulative_hashes_with_block_size(prompt, config.block_size))
            .collect::<Vec<_>>(),
    );
    let read_ops = config.requests;
    let write_ops = config
        .requests
        .saturating_mul(config.write_ratio_percent)
        .div_ceil(100);
    let next_read = Arc::new(AtomicUsize::new(0));
    let next_write = Arc::new(AtomicUsize::new(0));
    let read_samples = Arc::new(Mutex::new(Vec::with_capacity(read_ops)));
    let write_samples = Arc::new(Mutex::new(Vec::with_capacity(write_ops)));
    let writer_count = config
        .concurrency
        .saturating_mul(config.write_ratio_percent)
        .div_ceil(100)
        .max(1);
    let reader_count = config.concurrency.max(1);
    let started = Instant::now();
    let mut handles = Vec::with_capacity(reader_count + writer_count);

    for _ in 0..reader_count {
        let registry = Arc::clone(&registry);
        let hot_hashes = Arc::clone(&hot_hashes);
        let cold_hashes = Arc::clone(&cold_hashes);
        let candidate_pods = candidate_pods.clone();
        let next_read = Arc::clone(&next_read);
        let read_samples = Arc::clone(&read_samples);
        handles.push(thread::spawn(move || {
            let mut local = Vec::new();
            loop {
                let op = next_read.fetch_add(1, Ordering::Relaxed);
                if op >= read_ops {
                    break;
                }
                let hashes = if op % 10 == 0 {
                    &cold_hashes[op % cold_hashes.len()][..]
                } else {
                    &hot_hashes[..]
                };
                let query_started = Instant::now();
                let _ = registry.longest_prefix_lengths(hashes, candidate_pods.clone());
                local.push(query_started.elapsed().as_micros());
            }
            read_samples
                .lock()
                .expect("read samples poisoned")
                .extend(local);
        }));
    }

    for writer_id in 0..writer_count {
        let registry = Arc::clone(&registry);
        let cold_hashes = Arc::clone(&cold_hashes);
        let next_write = Arc::clone(&next_write);
        let write_samples = Arc::clone(&write_samples);
        handles.push(thread::spawn(move || {
            let mut local = Vec::new();
            loop {
                let op = next_write.fetch_add(1, Ordering::Relaxed);
                if op >= write_ops {
                    break;
                }
                let hashes = &cold_hashes[(op + writer_id) % cold_hashes.len()];
                let pod_id = (op + writer_id) % registry.index().pod_count();
                let write_started = Instant::now();
                if op % 2 == 0 {
                    registry.register_chain(pod_id, hashes);
                } else {
                    registry.evict_chain(pod_id, hashes);
                }
                local.push(write_started.elapsed().as_micros());
            }
            write_samples
                .lock()
                .expect("write samples poisoned")
                .extend(local);
        }));
    }

    for handle in handles {
        handle
            .join()
            .map_err(|_| "index contention worker panicked".to_string())?;
    }

    let read_samples = read_samples.lock().expect("read samples poisoned").clone();
    let write_samples = write_samples
        .lock()
        .expect("write samples poisoned")
        .clone();

    Ok(row(
        "index_contention",
        config.concurrency,
        0,
        read_samples.len(),
        started.elapsed(),
        &read_samples,
        &write_samples,
        None,
        None,
        None,
        format!("write_ops={write_ops}; writer_threads={writer_count}"),
    ))
}

fn run_fairness(config: &BenchConfig, workload: &Workload) -> Result<MetricRow, String> {
    let registry = setup_registry(config)?;
    seed_hotspot_cache(&registry.cache_registry, config, workload);
    let loads = LoadState::new(config.pods);
    let sticky = StickyStore::new();
    let pipeline = routing_pipeline(config);
    let headers = HeaderMap::new();
    let started = Instant::now();
    let mut samples = Vec::with_capacity(config.requests);
    let mut selected = vec![0usize; config.pods];
    let mut errors = 0usize;

    for request_id in 0..config.requests {
        let prompt = workload.prompt_for(request_id, config.skew_percent);
        let body = chat_body(prompt, request_id);
        let route_started = Instant::now();
        match pipeline.route_openai_request(
            &registry,
            &loads,
            &sticky,
            "/v1/chat/completions",
            "POST",
            &headers,
            body.as_bytes(),
        ) {
            Ok(routed) => {
                samples.push(route_started.elapsed().as_micros());
                let pod_id = selected_pod(&routed.plan);
                selected[pod_id] += 1;
                loads.set_inflight_for_test(pod_id as u16, selected[pod_id]);
            }
            Err(_) => errors += 1,
        }
    }

    Ok(row(
        "hotspot_fairness",
        config.concurrency,
        0,
        config.requests,
        started.elapsed(),
        &samples,
        &[],
        Some(jain_fairness(&selected)),
        None,
        None,
        format!(
            "route_errors={errors}; load_distribution={}",
            format_distribution(&selected)
        ),
    ))
}

fn run_staleness(
    config: &BenchConfig,
    workload: &Workload,
    lag_ms: u64,
) -> Result<MetricRow, String> {
    let lb_registry = setup_registry(config)?;
    let actual_registry = setup_registry(config)?;
    seed_hotspot_cache(&lb_registry.cache_registry, config, workload);
    seed_hotspot_cache(&actual_registry.cache_registry, config, workload);

    let loads = LoadState::new(config.pods);
    let sticky = StickyStore::new();
    let pipeline = routing_pipeline(config);
    let headers = HeaderMap::new();
    let candidate_pods = HostBitmap::full_for_count(config.pods);
    let interarrival_ms = if config.qps == 0 {
        1
    } else {
        (1000 / config.qps as u64).max(1)
    };
    let mut pending = VecDeque::new();
    let mut samples = Vec::with_capacity(config.requests);
    let mut misroutes = 0usize;
    let mut gap_blocks = 0usize;
    let mut gap_samples = 0usize;
    let mut selected = vec![0usize; config.pods];
    let started = Instant::now();

    for request_id in 0..config.requests {
        let now_ms = request_id as u64 * interarrival_ms;
        apply_due_events(&lb_registry.cache_registry, &mut pending, now_ms);
        mutate_actual_and_enqueue(
            &actual_registry.cache_registry,
            &mut pending,
            workload,
            config,
            request_id,
            now_ms.saturating_add(lag_ms),
        );

        let prompt = workload.prompt_for(request_id, config.skew_percent);
        let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, config.block_size);
        let body = chat_body(prompt, request_id);
        let route_started = Instant::now();
        let Ok(routed) = pipeline.route_openai_request(
            &lb_registry,
            &loads,
            &sticky,
            "/v1/chat/completions",
            "POST",
            &headers,
            body.as_bytes(),
        ) else {
            continue;
        };
        samples.push(route_started.elapsed().as_micros());

        let chosen_pod = selected_pod(&routed.plan);
        selected[chosen_pod] += 1;
        let predicted_depth = routed.plan.cache_prefix_depth();
        let actual_depth = depth_for_pod(&actual_registry.cache_registry, &hashes, chosen_pod);
        let best_actual_depth = actual_registry
            .cache_registry
            .best_prefix_match(&hashes, candidate_pods.clone())
            .map(|prefix| prefix.prefix_depth)
            .unwrap_or(0);

        if actual_depth < predicted_depth {
            misroutes += 1;
        }
        gap_blocks += best_actual_depth.saturating_sub(actual_depth);
        gap_samples += 1;
    }

    Ok(row(
        "staleness_sensitivity",
        config.concurrency,
        lag_ms,
        samples.len(),
        started.elapsed(),
        &samples,
        &[],
        Some(jain_fairness(&selected)),
        ratio(misroutes, gap_samples),
        mean_gap(gap_blocks, gap_samples),
        format!(
            "qps={}; pending_events_after_run={}",
            config.qps,
            pending.len()
        ),
    ))
}

fn setup_registry(config: &BenchConfig) -> Result<RuntimeRegistry, String> {
    let config = synthetic_config(config);
    let registry = RuntimeRegistry::from_config(&config)?;
    for pod_id in 0..registry.total_pods() {
        registry.cache_registry.mark_pod_alive(pod_id);
    }
    Ok(registry)
}

fn synthetic_config(config: &BenchConfig) -> CalinixConfig {
    let pods = (0..config.pods)
        .map(|pod_id| PodConfig {
            id: format!("pod-{pod_id}"),
            url: format!("http://127.0.0.1:{}", 30_000 + pod_id),
            healthy: true,
            draining: false,
            max_conns: config.requests.max(100),
            capabilities: None,
        })
        .collect::<Vec<_>>();

    CalinixConfig {
        version: 1,
        gateway: GatewayConfig {
            port: 18080,
            strategy: Strategy::CacheAware,
        },
        health: HealthConfig {
            endpoint: "/health".to_string(),
            interval_ms: 1000,
            timeout_ms: 250,
            healthy_threshold: 1,
            unhealthy_threshold: 1,
        },
        cache_registry: CacheRegistryConfig {
            enabled: true,
            max_pods: config.pods,
            shards_count: config.shards,
            stale_ttl_ms: 30_000,
        },
        upstreams: UpstreamsConfig {
            single: SingleUpstreamsConfig {
                mode: UpstreamMode::Single,
                pods,
            },
            dispatch: DispatchUpstreamsConfig {
                mode: UpstreamMode::Dispatch,
                prefill: PodGroupConfig { pods: Vec::new() },
                decode: PodGroupConfig { pods: Vec::new() },
            },
        },
    }
}

fn routing_pipeline(config: &BenchConfig) -> RoutingPipeline {
    let mut pipeline = RoutingPipeline {
        default_mode: CalinixMode::Single,
        block_size: config.block_size,
        ..RoutingPipeline::default()
    };
    pipeline.score_stage = ScoreStage {
        single_weights: ScoreWeights {
            cache: config.cache_weight,
            load: config.load_weight,
            sticky: 0.0,
            locality: 0.0,
        },
        prefill_weights: ScoreWeights {
            cache: config.cache_weight,
            load: config.load_weight,
            sticky: 0.0,
            locality: 0.0,
        },
        decode_weights: ScoreWeights {
            cache: config.cache_weight,
            load: config.load_weight,
            sticky: 0.0,
            locality: 0.0,
        },
    };
    pipeline
}

fn seed_warm_cache(registry: &CacheRegistry, config: &BenchConfig, workload: &Workload) {
    seed_hotspot_cache(registry, config, workload);
    for (i, prompt) in workload.cold_prompts.iter().enumerate() {
        let pod_id = i % config.pods;
        let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, config.block_size);
        registry.register_chain(pod_id, &hashes);
    }
}

fn seed_hotspot_cache(registry: &CacheRegistry, config: &BenchConfig, workload: &Workload) {
    let hashes =
        prompt_to_cumulative_hashes_with_block_size(&workload.hot_prompt, config.block_size);
    let hot_depth = config.shared_prefix_blocks.min(hashes.len());
    registry.register_chain(0, &hashes[..hot_depth]);
    for pod_id in 1..config.pods {
        let depth = hot_depth.saturating_sub(pod_id).max(hot_depth / 4);
        registry.register_chain(pod_id, &hashes[..depth.min(hashes.len())]);
    }
}

fn apply_due_events(
    registry: &CacheRegistry,
    pending: &mut VecDeque<PendingCacheEvent>,
    now_ms: u64,
) {
    while pending.front().is_some_and(|event| event.due_ms <= now_ms) {
        let event = pending.pop_front().expect("pending event exists");
        match event.kind {
            CacheEventKind::Register => {
                registry.register_chain(event.pod_id, &event.hashes);
            }
            CacheEventKind::Evict => {
                registry.evict_chain(event.pod_id, &event.hashes);
            }
        }
    }
}

fn mutate_actual_and_enqueue(
    registry: &CacheRegistry,
    pending: &mut VecDeque<PendingCacheEvent>,
    workload: &Workload,
    config: &BenchConfig,
    request_id: usize,
    due_ms: u64,
) {
    let pod_id = request_id % config.pods;
    let prompt = if request_id % 3 == 0 {
        &workload.hot_prompt
    } else {
        &workload.cold_prompts[request_id % workload.cold_prompts.len()]
    };
    let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, config.block_size);
    let depth = config.shared_prefix_blocks.min(hashes.len());
    let event_hashes = hashes[..depth].to_vec();
    let kind = if request_id % 4 == 0 {
        CacheEventKind::Evict
    } else {
        CacheEventKind::Register
    };

    match kind {
        CacheEventKind::Register => {
            registry.register_chain(pod_id, &event_hashes);
        }
        CacheEventKind::Evict => {
            registry.evict_chain(pod_id, &event_hashes);
        }
    }

    pending.push_back(PendingCacheEvent {
        due_ms,
        pod_id,
        hashes: event_hashes,
        kind,
    });
}

impl Workload {
    fn new(config: &BenchConfig) -> Self {
        let shared_prefix = words(
            "shared-rag-context",
            config.shared_prefix_blocks * config.block_size,
        );
        let hot_tail = words(
            "hot-question",
            config
                .prompt_blocks
                .saturating_sub(config.shared_prefix_blocks)
                * config.block_size,
        );
        let hot_prompt = format!("{shared_prefix} {hot_tail}");
        let cold_prompts = (0..config.pods.max(16))
            .map(|i| {
                let tail = words(
                    &format!("cold-question-{i}"),
                    config
                        .prompt_blocks
                        .saturating_sub(config.shared_prefix_blocks)
                        .max(1)
                        * config.block_size,
                );
                format!("{shared_prefix} {tail}")
            })
            .collect();
        Self {
            hot_prompt,
            cold_prompts,
        }
    }

    fn prompt_for(&self, request_id: usize, skew_percent: usize) -> &str {
        if (request_id * 97) % 100 < skew_percent {
            &self.hot_prompt
        } else {
            &self.cold_prompts[request_id % self.cold_prompts.len()]
        }
    }
}

fn words(prefix: &str, count: usize) -> String {
    (0..count.max(1))
        .map(|i| format!("{prefix}-{i}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn chat_body(prompt: &str, request_id: usize) -> String {
    json!({
        "model": "policy-bench-model",
        "messages": [{"role": "user", "content": prompt}],
        "user": format!("policy-session-{}", request_id % 128),
        "max_tokens": 16,
        "temperature": 0.0,
        "stream": false
    })
    .to_string()
}

fn selected_pod(plan: &RoutingPlan) -> usize {
    plan.primary_pod_id() as usize
}

fn depth_for_pod(registry: &CacheRegistry, hashes: &[u64], pod_id: usize) -> usize {
    let mut candidate = HostBitmap::empty();
    candidate.set(pod_id);
    registry
        .longest_prefix_lengths(hashes, candidate)
        .get(pod_id)
        .copied()
        .unwrap_or(0)
}

fn row(
    scenario: &str,
    concurrency: usize,
    lag_ms: u64,
    operations: usize,
    elapsed: Duration,
    samples: &[u128],
    write_samples: &[u128],
    fairness: Option<f64>,
    misroute_rate: Option<f64>,
    cache_gap_avg_blocks: Option<f64>,
    notes: String,
) -> MetricRow {
    let read_stats = latency_stats(samples);
    let write_stats = latency_stats(write_samples);
    MetricRow {
        scenario: scenario.to_string(),
        concurrency,
        lag_ms,
        operations,
        elapsed_ms: elapsed.as_millis(),
        rps: operations as f64 / elapsed.as_secs_f64().max(0.001),
        latency_count: samples.len(),
        avg_us: read_stats.avg,
        p50_us: read_stats.p50,
        p95_us: read_stats.p95,
        p99_us: read_stats.p99,
        max_us: read_stats.max,
        write_latency_count: write_samples.len(),
        write_avg_us: write_stats.avg,
        write_p99_us: write_stats.p99,
        jain_fairness: fairness,
        misroute_rate,
        cache_gap_avg_blocks,
        notes,
    }
}

#[derive(Clone, Copy, Debug)]
struct LatencyStats {
    avg: f64,
    p50: u128,
    p95: u128,
    p99: u128,
    max: u128,
}

fn latency_stats(samples: &[u128]) -> LatencyStats {
    if samples.is_empty() {
        return LatencyStats {
            avg: 0.0,
            p50: 0,
            p95: 0,
            p99: 0,
            max: 0,
        };
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum = sorted.iter().sum::<u128>();
    LatencyStats {
        avg: sum as f64 / sorted.len() as f64,
        p50: percentile(&sorted, 0.50),
        p95: percentile(&sorted, 0.95),
        p99: percentile(&sorted, 0.99),
        max: *sorted.last().unwrap_or(&0),
    }
}

fn percentile(samples: &[u128], p: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let index = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[index.min(samples.len() - 1)]
}

fn jain_fairness(counts: &[usize]) -> f64 {
    let sum = counts.iter().sum::<usize>() as f64;
    if sum == 0.0 {
        return 0.0;
    }
    let sum_squares = counts
        .iter()
        .map(|count| {
            let count = *count as f64;
            count * count
        })
        .sum::<f64>();
    (sum * sum) / (counts.len() as f64 * sum_squares.max(1.0))
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn mean_gap(total: usize, samples: usize) -> Option<f64> {
    if samples == 0 {
        None
    } else {
        Some(total as f64 / samples as f64)
    }
}

fn format_distribution(counts: &[usize]) -> String {
    let mut compact = BTreeMap::new();
    for (pod_id, count) in counts.iter().enumerate() {
        if *count > 0 {
            compact.insert(pod_id, *count);
        }
    }
    compact
        .iter()
        .map(|(pod_id, count)| format!("{pod_id}:{count}"))
        .collect::<Vec<_>>()
        .join("|")
}

fn write_csv(path: &Path, rows: &[MetricRow]) -> Result<(), String> {
    create_parent_dir(path)?;
    let file = File::create(path)
        .map_err(|err| format!("failed to create CSV {}: {err}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "scenario,concurrency,lag_ms,operations,elapsed_ms,rps,latency_count,avg_us,p50_us,p95_us,p99_us,max_us,write_latency_count,write_avg_us,write_p99_us,jain_fairness,misroute_rate,cache_gap_avg_blocks,notes"
    )
    .map_err(|err| format!("failed writing CSV header: {err}"))?;

    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{},{:.3},{},{:.3},{},{},{},{},{},{:.3},{},{},{},{},{}",
            csv(&row.scenario),
            row.concurrency,
            row.lag_ms,
            row.operations,
            row.elapsed_ms,
            row.rps,
            row.latency_count,
            row.avg_us,
            row.p50_us,
            row.p95_us,
            row.p99_us,
            row.max_us,
            row.write_latency_count,
            row.write_avg_us,
            row.write_p99_us,
            format_option_f64(row.jain_fairness),
            format_option_f64(row.misroute_rate),
            format_option_f64(row.cache_gap_avg_blocks),
            csv(&row.notes)
        )
        .map_err(|err| format!("failed writing CSV row: {err}"))?;
    }
    writer
        .flush()
        .map_err(|err| format!("failed flushing CSV: {err}"))?;
    Ok(())
}

fn create_parent_dir(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create output dir {}: {err}", parent.display()))?;
    }
    Ok(())
}

fn print_summary(config: &BenchConfig, rows: &[MetricRow]) {
    println!("CALINIX POLICY BENCH");
    println!("--------------------------------------------------");
    println!("name={}", config.name);
    println!("pods={}", config.pods);
    println!("shards={}", config.shards);
    println!("requests={}", config.requests);
    println!("block_size={}", config.block_size);
    println!("csv={}", config.output.display());
    println!();
    for row in rows {
        println!(
            "scenario={} concurrency={} lag_ms={} p99_us={} fairness={} misroute_rate={} rps={:.3}",
            row.scenario,
            row.concurrency,
            row.lag_ms,
            row.p99_us,
            format_option_f64(row.jain_fairness),
            format_option_f64(row.misroute_rate),
            row.rps
        );
    }
}

fn parse_args(args: Vec<String>) -> Result<BenchConfig, String> {
    let mut name = DEFAULT_RUN_NAME.to_string();
    let mut output_file: Option<String> = None;
    let mut config = BenchConfig {
        name: DEFAULT_RUN_NAME.to_string(),
        pods: 16,
        shards: 64,
        requests: 10_000,
        concurrency: 64,
        concurrency_sweep: Vec::new(),
        block_size: DEFAULT_BLOCK_SIZE,
        prompt_blocks: 256,
        shared_prefix_blocks: 128,
        skew_percent: 90,
        write_ratio_percent: 25,
        lag_sweep_ms: vec![0, 100, 500, 1000, 5000],
        qps: 1000,
        cache_weight: 0.60,
        load_weight: 0.30,
        output: PathBuf::new(),
    };

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--name" => {
                name = sanitize_run_name(&take_value(&args, &mut i, "--name")?)?;
            }
            "--pods" => {
                config.pods = take_value(&args, &mut i, "--pods")?
                    .parse()
                    .map_err(|_| "--pods must be an integer".to_string())?;
            }
            "--shards" => {
                config.shards = take_value(&args, &mut i, "--shards")?
                    .parse()
                    .map_err(|_| "--shards must be an integer".to_string())?;
            }
            "--requests" => {
                config.requests = take_value(&args, &mut i, "--requests")?
                    .parse()
                    .map_err(|_| "--requests must be an integer".to_string())?;
            }
            "--concurrency" => {
                config.concurrency = take_value(&args, &mut i, "--concurrency")?
                    .parse()
                    .map_err(|_| "--concurrency must be an integer".to_string())?;
            }
            "--concurrency-sweep" => {
                config.concurrency_sweep =
                    parse_usize_list(&take_value(&args, &mut i, "--concurrency-sweep")?)?;
            }
            "--block-size" => {
                config.block_size = take_value(&args, &mut i, "--block-size")?
                    .parse()
                    .map_err(|_| "--block-size must be an integer".to_string())?;
            }
            "--prompt-blocks" => {
                config.prompt_blocks = take_value(&args, &mut i, "--prompt-blocks")?
                    .parse()
                    .map_err(|_| "--prompt-blocks must be an integer".to_string())?;
            }
            "--shared-prefix-blocks" => {
                config.shared_prefix_blocks = take_value(&args, &mut i, "--shared-prefix-blocks")?
                    .parse()
                    .map_err(|_| "--shared-prefix-blocks must be an integer".to_string())?;
            }
            "--skew-percent" => {
                config.skew_percent = take_value(&args, &mut i, "--skew-percent")?
                    .parse()
                    .map_err(|_| "--skew-percent must be an integer".to_string())?;
            }
            "--write-ratio-percent" => {
                config.write_ratio_percent = take_value(&args, &mut i, "--write-ratio-percent")?
                    .parse()
                    .map_err(|_| "--write-ratio-percent must be an integer".to_string())?;
            }
            "--lag-sweep-ms" => {
                config.lag_sweep_ms =
                    parse_u64_list(&take_value(&args, &mut i, "--lag-sweep-ms")?)?;
            }
            "--qps" => {
                config.qps = take_value(&args, &mut i, "--qps")?
                    .parse()
                    .map_err(|_| "--qps must be an integer".to_string())?;
            }
            "--cache-weight" => {
                config.cache_weight = take_value(&args, &mut i, "--cache-weight")?
                    .parse()
                    .map_err(|_| "--cache-weight must be a float".to_string())?;
            }
            "--load-weight" => {
                config.load_weight = take_value(&args, &mut i, "--load-weight")?
                    .parse()
                    .map_err(|_| "--load-weight must be a float".to_string())?;
            }
            "--output" => {
                output_file = Some(take_value(&args, &mut i, "--output")?);
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        i += 1;
    }

    if config.pods == 0 {
        return Err("--pods must be greater than zero".to_string());
    }
    if config.requests == 0 {
        return Err("--requests must be greater than zero".to_string());
    }
    if config.block_size == 0 {
        return Err("--block-size must be greater than zero".to_string());
    }
    if config.prompt_blocks == 0 {
        return Err("--prompt-blocks must be greater than zero".to_string());
    }
    if config.shared_prefix_blocks > config.prompt_blocks {
        return Err("--shared-prefix-blocks cannot exceed --prompt-blocks".to_string());
    }
    if config.skew_percent > 100 {
        return Err("--skew-percent cannot exceed 100".to_string());
    }
    if config.concurrency_sweep.is_empty() {
        config.concurrency_sweep = vec![config.concurrency];
    }

    config.name = name;
    config.output = resolve_output_path(&config.name, output_file.as_deref());
    Ok(config)
}

fn take_value(args: &[String], i: &mut usize, flag: &str) -> Result<String, String> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_usize_list(value: &str) -> Result<Vec<usize>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse()
                .map_err(|_| format!("invalid integer in list: {item}"))
        })
        .collect()
}

fn parse_u64_list(value: &str) -> Result<Vec<u64>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse()
                .map_err(|_| format!("invalid integer in list: {item}"))
        })
        .collect()
}

fn resolve_output_path(name: &str, output: Option<&str>) -> PathBuf {
    let output = output.unwrap_or(DEFAULT_OUTPUT_FILE);
    let path = PathBuf::from(output);
    if path.is_absolute()
        || path
            .parent()
            .is_some_and(|parent| !parent.as_os_str().is_empty())
    {
        path
    } else {
        Path::new(RESULTS_ROOT).join(name).join(path)
    }
}

fn sanitize_run_name(name: &str) -> Result<String, String> {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if sanitized.is_empty() {
        Err("--name must contain at least one alphanumeric character".to_string())
    } else {
        Ok(sanitized)
    }
}

fn format_option_f64(value: Option<f64>) -> String {
    value.map(|value| format!("{value:.6}")).unwrap_or_default()
}

fn csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn print_usage() {
    eprintln!(
        r#"usage: cargo run --bin calinix-policy-bench -- [options]

Measures local cache-aware routing policy behavior without backend or network noise.

Options:
  --name <name>                    Result directory name (default: policy-bench)
  --pods <n>                       Synthetic single-mode pod count (default: 16)
  --shards <n>                     Cache index shard count (default: 64)
  --requests <n>                   Operations per scenario (default: 10000)
  --concurrency <n>                Worker count for contention scenario (default: 64)
  --concurrency-sweep <list>       Comma-separated concurrency levels
  --block-size <n>                 Cache block size in tokens (default: 32)
  --prompt-blocks <n>              Synthetic prompt length in blocks (default: 256)
  --shared-prefix-blocks <n>       Shared RAG-like prefix length in blocks (default: 128)
  --skew-percent <0-100>           Percent hot shared-prefix traffic (default: 90)
  --write-ratio-percent <n>        Index writes as percent of reads (default: 25)
  --lag-sweep-ms <list>            Event-lag sweep for staleness (default: 0,100,500,1000,5000)
  --qps <n>                        Simulated staleness arrival rate (default: 1000)
  --cache-weight <f>               Cache score weight (default: 0.60)
  --load-weight <f>                Load score weight (default: 0.30)
  --output <path>                  CSV path or file name (default: policy_bench.csv)
"#
    );
}
