use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use calinix::cache_registry::tokenize;
use calinix::protocol::openai::extract_openai_routing_view;
use http::HeaderMap as HttpHeaderMap;
use reqwest::header::HeaderMap;
use serde_json::{json, Value};
use tokio::task::JoinHandle;

const RESULTS_ROOT: &str = "benchmark/results";
const DEFAULT_RUN_NAME: &str = "url-bench";
const DEFAULT_OUTPUT_FILE: &str = "url_bench.csv";
const DEFAULT_SWEEP_OUTPUT_FILE: &str = "url_bench_sweep.csv";
const DEFAULT_MODEL: &str = "llama-3.1-8b";
const DEFAULT_PROMPT: &str = "Explain cache aware routing in simple words";
const DEFAULT_PAYLOAD_FILE: &str = "benchmark/data/example_payloads.json";
const DEFAULT_BLOCK_SIZE: usize = 32;
const BEST_CACHE_PREFIX_DEPTH_HEADER: &str = "x-calinix-best-cache-prefix-depth";
const ACTUAL_CACHE_PREFIX_DEPTH_HEADER: &str = "x-calinix-actual-cache-prefix-depth";

#[derive(Clone, Debug)]
struct BenchConfig {
    name: String,
    url: String,
    concurrency: usize,
    concurrency_sweep: Option<Vec<usize>>,
    threads: usize,
    payload: Option<PayloadArg>,
    prompt: String,
    model: String,
    mode: Option<BenchMode>,
    endpoint_path: String,
    block_size: usize,
    timeout_ms: u64,
    requests: Option<usize>,
    stream: bool,
    output: PathBuf,
}

#[derive(Clone, Debug)]
enum PayloadArg {
    File(PathBuf),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchMode {
    Single,
    Disaggregated,
}

impl BenchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Disaggregated => "disaggregated",
        }
    }
}

#[derive(Clone, Debug)]
struct RequestRecord {
    request_id: usize,
    session_id: String,
    mode: String,
    status: Option<u16>,
    success: bool,
    timeout: bool,
    latency_ms: u128,
    response_bytes: usize,
    prompt_tokens: usize,
    prompt_blocks: usize,
    block_size: usize,
    cache_hit: Option<String>,
    cache_prefix_depth: Option<String>,
    best_cache_prefix_depth: Option<String>,
    actual_cache_prefix_depth: Option<String>,
    target_pod_id: Option<String>,
    prefill_pod_id: Option<String>,
    decode_pod_id: Option<String>,
    error: Option<String>,
}

#[derive(Debug)]
struct LatencyStats {
    count: usize,
    avg_ms: f64,
    p50_ms: u128,
    p95_ms: u128,
    p99_ms: u128,
    max_ms: u128,
}

#[derive(Debug)]
struct RunSummary {
    total_requests: usize,
    success_count: usize,
    error_count: usize,
    timeout_count: usize,
    rps: f64,
    latency: LatencyStats,
    status_2xx: usize,
    status_4xx: usize,
    status_5xx: usize,
    cache_hit_count: usize,
    cache_miss_count: usize,
    cache_effectiveness: CacheEffectivenessSummary,
    target_pods: BTreeMap<String, usize>,
    prefill_pods: BTreeMap<String, usize>,
    decode_pods: BTreeMap<String, usize>,
}

#[derive(Debug)]
struct CacheEffectivenessSummary {
    samples: usize,
    total_prompt_tokens: usize,
    matched_prefix_tokens: usize,
    avoided_prefill_tokens: usize,
    token_weighted_prefix_hit_rate: Option<f64>,
    request_avg_prefix_hit_rate: Option<f64>,
    matched_prefix_depth_avg: Option<f64>,
    matched_prefix_depth_p95: usize,
    best_gap_samples: usize,
    best_possible_prefix_tokens: usize,
    cache_gap_tokens: usize,
    avg_cache_gap_tokens: Option<f64>,
    avg_cache_gap_rate: Option<f64>,
    misroute_samples: usize,
    misroute_rate: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PromptStats {
    tokens: usize,
    blocks: usize,
}

#[derive(Debug)]
struct SweepRow {
    concurrency: usize,
    summary: RunSummary,
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

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.threads)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("failed to create Tokio runtime: {err}");
            std::process::exit(1);
        }
    };

    if let Err(err) = runtime.block_on(run(config)) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run(config: BenchConfig) -> Result<(), String> {
    let payloads = Arc::new(load_payloads(&config)?);
    if payloads.is_empty() {
        return Err("benchmark setup invalid: no payloads available".to_string());
    }

    if let Some(sweep) = &config.concurrency_sweep {
        let mut rows = Vec::new();
        for concurrency in sweep {
            let run_config = BenchConfig {
                concurrency: *concurrency,
                concurrency_sweep: None,
                ..config.clone()
            };
            let (records, elapsed) = run_once(&run_config, Arc::clone(&payloads)).await?;
            let summary = summarize(&records, elapsed);
            print_summary(&run_config, &summary);
            rows.push(SweepRow {
                concurrency: *concurrency,
                summary,
            });
        }
        write_sweep_csv(&config.output, &rows)?;
        println!("csv={}", config.output.display());
    } else {
        let (records, elapsed) = run_once(&config, payloads).await?;
        write_records_csv(&config.output, &records)?;
        let summary = summarize(&records, elapsed);
        print_summary(&config, &summary);
    }

    Ok(())
}

async fn run_once(
    config: &BenchConfig,
    payloads: Arc<Vec<String>>,
) -> Result<(Vec<RequestRecord>, Duration), String> {
    if config.concurrency == 0 {
        return Err("benchmark setup invalid: concurrency must be greater than zero".to_string());
    }

    let run_duration = Duration::from_millis(config.timeout_ms);
    let mut client_builder = reqwest::Client::builder().pool_max_idle_per_host(config.concurrency);
    if config.requests.is_none() {
        client_builder = client_builder.timeout(run_duration);
    }
    let client = client_builder
        .build()
        .map_err(|err| format!("failed to create HTTP client: {err}"))?;

    let client = Arc::new(client);
    let next_request = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let deadline = config.requests.is_none().then_some(started + run_duration);
    let request_limit = config.requests;
    let worker_count = config.concurrency;
    let mut handles: Vec<JoinHandle<Vec<RequestRecord>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let client = Arc::clone(&client);
        let payloads = Arc::clone(&payloads);
        let next_request = Arc::clone(&next_request);
        let config = config.clone();

        handles.push(tokio::spawn(async move {
            let mut records = Vec::new();
            loop {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    break;
                }

                let request_id = next_request.fetch_add(1, Ordering::Relaxed);
                if request_limit.is_some_and(|limit| request_id >= limit) {
                    break;
                }

                let payload = &payloads[request_id % payloads.len()];
                records.push(send_request(&client, &config, request_id, payload).await);
            }
            records
        }));
    }

    let mut records = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(mut worker_records) => records.append(&mut worker_records),
            Err(err) => {
                records.push(RequestRecord {
                    request_id: records.len(),
                    session_id: String::new(),
                    mode: String::new(),
                    status: None,
                    success: false,
                    timeout: false,
                    latency_ms: 0,
                    response_bytes: 0,
                    prompt_tokens: 0,
                    prompt_blocks: 0,
                    block_size: config.block_size,
                    cache_hit: None,
                    cache_prefix_depth: None,
                    best_cache_prefix_depth: None,
                    actual_cache_prefix_depth: None,
                    target_pod_id: None,
                    prefill_pod_id: None,
                    decode_pod_id: None,
                    error: Some(format!("worker task failed: {err}")),
                });
            }
        }
    }

    records.sort_by_key(|record| record.request_id);
    Ok((records, started.elapsed()))
}

async fn send_request(
    client: &reqwest::Client,
    config: &BenchConfig,
    request_id: usize,
    payload: &str,
) -> RequestRecord {
    let session_id = format!("bench-session-{}", request_id % config.concurrency);
    let prompt = prompt_stats(payload, &config.endpoint_path, config.block_size);
    let request_body = payload_with_user(payload, &session_id);
    let started = Instant::now();

    let mut request = client
        .post(&config.url)
        .header("content-type", "application/json")
        .body(request_body);
    if let Some(mode) = config.mode {
        request = request.header("x-calinix-mode", mode.as_str());
    }
    let result = request.send().await;

    match result {
        Ok(response) => {
            let status = response.status();
            let headers = response.headers().clone();
            match response.bytes().await {
                Ok(bytes) => RequestRecord {
                    request_id,
                    session_id,
                    mode: header_value(&headers, "x-calinix-mode").unwrap_or_default(),
                    status: Some(status.as_u16()),
                    success: status.is_success(),
                    timeout: false,
                    latency_ms: started.elapsed().as_millis(),
                    response_bytes: bytes.len(),
                    prompt_tokens: prompt.tokens,
                    prompt_blocks: prompt.blocks,
                    block_size: config.block_size,
                    cache_hit: header_value(&headers, "x-calinix-cache-hit"),
                    cache_prefix_depth: header_value(&headers, "x-calinix-cache-prefix-depth"),
                    best_cache_prefix_depth: header_value(&headers, BEST_CACHE_PREFIX_DEPTH_HEADER),
                    actual_cache_prefix_depth: header_value(
                        &headers,
                        ACTUAL_CACHE_PREFIX_DEPTH_HEADER,
                    ),
                    target_pod_id: header_value(&headers, "x-calinix-target-pod-id"),
                    prefill_pod_id: header_value(&headers, "x-calinix-prefill-pod-id"),
                    decode_pod_id: header_value(&headers, "x-calinix-decode-pod-id"),
                    error: None,
                },
                Err(err) => RequestRecord {
                    request_id,
                    session_id,
                    mode: header_value(&headers, "x-calinix-mode").unwrap_or_default(),
                    status: Some(status.as_u16()),
                    success: false,
                    timeout: err.is_timeout(),
                    latency_ms: started.elapsed().as_millis(),
                    response_bytes: 0,
                    prompt_tokens: prompt.tokens,
                    prompt_blocks: prompt.blocks,
                    block_size: config.block_size,
                    cache_hit: header_value(&headers, "x-calinix-cache-hit"),
                    cache_prefix_depth: header_value(&headers, "x-calinix-cache-prefix-depth"),
                    best_cache_prefix_depth: header_value(&headers, BEST_CACHE_PREFIX_DEPTH_HEADER),
                    actual_cache_prefix_depth: header_value(
                        &headers,
                        ACTUAL_CACHE_PREFIX_DEPTH_HEADER,
                    ),
                    target_pod_id: header_value(&headers, "x-calinix-target-pod-id"),
                    prefill_pod_id: header_value(&headers, "x-calinix-prefill-pod-id"),
                    decode_pod_id: header_value(&headers, "x-calinix-decode-pod-id"),
                    error: Some(err.to_string()),
                },
            }
        }
        Err(err) => RequestRecord {
            request_id,
            session_id,
            mode: String::new(),
            status: None,
            success: false,
            timeout: err.is_timeout(),
            latency_ms: started.elapsed().as_millis(),
            response_bytes: 0,
            prompt_tokens: prompt.tokens,
            prompt_blocks: prompt.blocks,
            block_size: config.block_size,
            cache_hit: None,
            cache_prefix_depth: None,
            best_cache_prefix_depth: None,
            actual_cache_prefix_depth: None,
            target_pod_id: None,
            prefill_pod_id: None,
            decode_pod_id: None,
            error: Some(err.to_string()),
        },
    }
}

fn summarize(records: &[RequestRecord], elapsed: Duration) -> RunSummary {
    let success_latencies = records
        .iter()
        .filter(|record| record.success)
        .map(|record| record.latency_ms)
        .collect::<Vec<_>>();
    let success_count = records.iter().filter(|record| record.success).count();
    let timeout_count = records.iter().filter(|record| record.timeout).count();
    let error_count = records
        .iter()
        .filter(|record| !record.success && !record.timeout)
        .count();

    RunSummary {
        total_requests: records.len(),
        success_count,
        error_count,
        timeout_count,
        rps: records.len() as f64 / elapsed.as_secs_f64().max(0.001),
        latency: latency_stats(success_latencies),
        status_2xx: records
            .iter()
            .filter(|record| {
                record
                    .status
                    .is_some_and(|status| (200..300).contains(&status))
            })
            .count(),
        status_4xx: records
            .iter()
            .filter(|record| {
                record
                    .status
                    .is_some_and(|status| (400..500).contains(&status))
            })
            .count(),
        status_5xx: records
            .iter()
            .filter(|record| {
                record
                    .status
                    .is_some_and(|status| (500..600).contains(&status))
            })
            .count(),
        cache_hit_count: records
            .iter()
            .filter(|record| record.cache_hit.as_deref() == Some("true"))
            .count(),
        cache_miss_count: records
            .iter()
            .filter(|record| record.cache_hit.as_deref() == Some("false"))
            .count(),
        cache_effectiveness: summarize_cache_effectiveness(records),
        target_pods: pod_counts(
            records
                .iter()
                .filter_map(|record| record.target_pod_id.as_ref()),
        ),
        prefill_pods: pod_counts(
            records
                .iter()
                .filter_map(|record| record.prefill_pod_id.as_ref()),
        ),
        decode_pods: pod_counts(
            records
                .iter()
                .filter_map(|record| record.decode_pod_id.as_ref()),
        ),
    }
}

fn latency_stats(mut samples: Vec<u128>) -> LatencyStats {
    if samples.is_empty() {
        return LatencyStats {
            count: 0,
            avg_ms: 0.0,
            p50_ms: 0,
            p95_ms: 0,
            p99_ms: 0,
            max_ms: 0,
        };
    }

    samples.sort_unstable();
    let count = samples.len();
    let sum: u128 = samples.iter().sum();
    LatencyStats {
        count,
        avg_ms: sum as f64 / count as f64,
        p50_ms: percentile(&samples, 0.50),
        p95_ms: percentile(&samples, 0.95),
        p99_ms: percentile(&samples, 0.99),
        max_ms: *samples.last().unwrap_or(&0),
    }
}

fn percentile(samples: &[u128], percentile: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let index = ((samples.len() as f64 - 1.0) * percentile).round() as usize;
    samples[index.min(samples.len() - 1)]
}

fn summarize_cache_effectiveness(records: &[RequestRecord]) -> CacheEffectivenessSummary {
    let samples = records
        .iter()
        .filter(|record| record.prompt_tokens > 0)
        .collect::<Vec<_>>();
    let total_prompt_tokens = samples
        .iter()
        .map(|record| record.prompt_tokens)
        .sum::<usize>();

    let mut matched_depths = Vec::new();
    let mut prefix_hit_rates = Vec::new();
    let mut matched_prefix_tokens = 0usize;
    let mut best_possible_prefix_tokens = 0usize;
    let mut cache_gap_tokens = 0usize;
    let mut cache_gap_rates = Vec::new();
    let mut best_gap_samples = 0usize;
    let mut misroute_samples = 0usize;
    let mut misroutes = 0usize;

    for record in &samples {
        let chosen_depth = parsed_usize(record.cache_prefix_depth.as_deref()).unwrap_or(0);
        let chosen_tokens =
            prefix_depth_tokens(chosen_depth, record.prompt_tokens, record.block_size);
        matched_depths.push(chosen_depth as u128);
        matched_prefix_tokens += chosen_tokens;
        prefix_hit_rates.push(chosen_tokens as f64 / record.prompt_tokens as f64);

        if let Some(best_depth) = parsed_usize(record.best_cache_prefix_depth.as_deref()) {
            let best_tokens =
                prefix_depth_tokens(best_depth, record.prompt_tokens, record.block_size);
            let gap_tokens = best_tokens.saturating_sub(chosen_tokens);
            best_gap_samples += 1;
            best_possible_prefix_tokens += best_tokens;
            cache_gap_tokens += gap_tokens;
            cache_gap_rates.push(gap_tokens as f64 / record.prompt_tokens as f64);
        }

        if let Some(actual_depth) = parsed_usize(record.actual_cache_prefix_depth.as_deref()) {
            misroute_samples += 1;
            if actual_depth < chosen_depth {
                misroutes += 1;
            }
        }
    }

    CacheEffectivenessSummary {
        samples: samples.len(),
        total_prompt_tokens,
        matched_prefix_tokens,
        avoided_prefill_tokens: matched_prefix_tokens,
        token_weighted_prefix_hit_rate: ratio(matched_prefix_tokens, total_prompt_tokens),
        request_avg_prefix_hit_rate: mean(&prefix_hit_rates),
        matched_prefix_depth_avg: mean_u128(&matched_depths),
        matched_prefix_depth_p95: percentile(&matched_depths, 0.95) as usize,
        best_gap_samples,
        best_possible_prefix_tokens,
        cache_gap_tokens,
        avg_cache_gap_tokens: if best_gap_samples == 0 {
            None
        } else {
            Some(cache_gap_tokens as f64 / best_gap_samples as f64)
        },
        avg_cache_gap_rate: mean(&cache_gap_rates),
        misroute_samples,
        misroute_rate: ratio(misroutes, misroute_samples),
    }
}

fn prompt_stats(payload: &str, endpoint_path: &str, block_size: usize) -> PromptStats {
    let headers = HttpHeaderMap::new();
    let Ok(view) = extract_openai_routing_view(endpoint_path, &headers, payload.as_bytes()) else {
        return PromptStats {
            tokens: 0,
            blocks: 0,
        };
    };

    let tokens = tokenize(&view.prompt_text).len();
    let blocks = tokens.div_ceil(block_size.max(1));
    PromptStats { tokens, blocks }
}

fn prefix_depth_tokens(prefix_depth: usize, prompt_tokens: usize, block_size: usize) -> usize {
    prefix_depth
        .saturating_mul(block_size.max(1))
        .min(prompt_tokens)
}

fn parsed_usize(value: Option<&str>) -> Option<usize> {
    value?.trim().parse().ok()
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn mean_u128(values: &[u128]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<u128>() as f64 / values.len() as f64)
    }
}

fn pod_counts<'a>(values: impl Iterator<Item = &'a String>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for value in values {
        if value.is_empty() {
            continue;
        }
        *counts.entry(value.clone()).or_insert(0) += 1;
    }
    counts
}

fn write_records_csv(path: &Path, records: &[RequestRecord]) -> Result<(), String> {
    create_parent_dir(path)?;
    let file = File::create(path)
        .map_err(|err| format!("failed to create CSV {}: {err}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "request_id,session_id,mode,status,success,timeout,latency_ms,response_bytes,prompt_tokens,prompt_blocks,block_size,cache_hit,cache_prefix_depth,matched_prefix_tokens,prefix_hit_rate,best_cache_prefix_depth,best_cache_gap_tokens,best_cache_gap_rate,actual_cache_prefix_depth,misroute,target_pod_id,prefill_pod_id,decode_pod_id,error"
    )
    .map_err(|err| format!("failed writing CSV header: {err}"))?;

    for record in records {
        let cache_prefix_depth = parsed_usize(record.cache_prefix_depth.as_deref()).unwrap_or(0);
        let matched_prefix_tokens =
            prefix_depth_tokens(cache_prefix_depth, record.prompt_tokens, record.block_size);
        let prefix_hit_rate = ratio(matched_prefix_tokens, record.prompt_tokens);
        let best_cache_prefix_depth = parsed_usize(record.best_cache_prefix_depth.as_deref());
        let best_cache_gap_tokens = best_cache_prefix_depth.map(|best_depth| {
            prefix_depth_tokens(best_depth, record.prompt_tokens, record.block_size)
                .saturating_sub(matched_prefix_tokens)
        });
        let best_cache_gap_rate =
            best_cache_gap_tokens.and_then(|gap| ratio(gap, record.prompt_tokens));
        let actual_cache_prefix_depth = parsed_usize(record.actual_cache_prefix_depth.as_deref());
        let misroute =
            actual_cache_prefix_depth.map(|actual_depth| actual_depth < cache_prefix_depth);

        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.request_id,
            csv(&record.session_id),
            csv(&record.mode),
            record
                .status
                .map(|status| status.to_string())
                .unwrap_or_default(),
            record.success,
            record.timeout,
            record.latency_ms,
            record.response_bytes,
            record.prompt_tokens,
            record.prompt_blocks,
            record.block_size,
            csv(record.cache_hit.as_deref().unwrap_or_default()),
            csv(record.cache_prefix_depth.as_deref().unwrap_or_default()),
            matched_prefix_tokens,
            format_option_f64(prefix_hit_rate),
            csv(record
                .best_cache_prefix_depth
                .as_deref()
                .unwrap_or_default()),
            format_option_usize(best_cache_gap_tokens),
            format_option_f64(best_cache_gap_rate),
            csv(record
                .actual_cache_prefix_depth
                .as_deref()
                .unwrap_or_default()),
            format_option_bool(misroute),
            csv(record.target_pod_id.as_deref().unwrap_or_default()),
            csv(record.prefill_pod_id.as_deref().unwrap_or_default()),
            csv(record.decode_pod_id.as_deref().unwrap_or_default()),
            csv(record.error.as_deref().unwrap_or_default())
        )
        .map_err(|err| format!("failed writing CSV record: {err}"))?;
    }

    writer
        .flush()
        .map_err(|err| format!("failed flushing CSV: {err}"))?;
    Ok(())
}

fn write_sweep_csv(path: &Path, rows: &[SweepRow]) -> Result<(), String> {
    create_parent_dir(path)?;
    let file = File::create(path)
        .map_err(|err| format!("failed to create CSV {}: {err}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "concurrency,total_requests,success_count,error_count,timeout_count,rps,avg_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,max_latency_ms,status_2xx,status_4xx,status_5xx,cache_hit_count,cache_miss_count,cache_samples,total_prompt_tokens,matched_prefix_tokens,token_weighted_prefix_hit_rate,request_avg_prefix_hit_rate,matched_prefix_depth_avg,matched_prefix_depth_p95,best_gap_samples,best_possible_prefix_tokens,cache_gap_tokens,avg_cache_gap_tokens,avg_cache_gap_rate,misroute_samples,misroute_rate"
    )
    .map_err(|err| format!("failed writing sweep CSV header: {err}"))?;

    for row in rows {
        let summary = &row.summary;
        let cache = &summary.cache_effectiveness;
        writeln!(
            writer,
            "{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            row.concurrency,
            summary.total_requests,
            summary.success_count,
            summary.error_count,
            summary.timeout_count,
            summary.rps,
            summary.latency.avg_ms,
            summary.latency.p50_ms,
            summary.latency.p95_ms,
            summary.latency.p99_ms,
            summary.latency.max_ms,
            summary.status_2xx,
            summary.status_4xx,
            summary.status_5xx,
            summary.cache_hit_count,
            summary.cache_miss_count,
            cache.samples,
            cache.total_prompt_tokens,
            cache.matched_prefix_tokens,
            format_option_f64(cache.token_weighted_prefix_hit_rate),
            format_option_f64(cache.request_avg_prefix_hit_rate),
            format_option_f64(cache.matched_prefix_depth_avg),
            cache.matched_prefix_depth_p95,
            cache.best_gap_samples,
            cache.best_possible_prefix_tokens,
            cache.cache_gap_tokens,
            format_option_f64(cache.avg_cache_gap_tokens),
            format_option_f64(cache.avg_cache_gap_rate),
            cache.misroute_samples,
            format_option_f64(cache.misroute_rate)
        )
        .map_err(|err| format!("failed writing sweep CSV record: {err}"))?;
    }

    writer
        .flush()
        .map_err(|err| format!("failed flushing sweep CSV: {err}"))?;
    Ok(())
}

fn print_summary(config: &BenchConfig, summary: &RunSummary) {
    println!("CALINIX URL BENCH");
    println!("--------------------------------------------------");
    println!("name={}", config.name);
    println!("url={}", config.url);
    println!("requests={}", summary.total_requests);
    println!(
        "request_limit={}",
        config
            .requests
            .map(|requests| requests.to_string())
            .unwrap_or_else(|| "duration".to_string())
    );
    println!("concurrency={}", config.concurrency);
    println!();
    println!("success_count={}", summary.success_count);
    println!("error_count={}", summary.error_count);
    println!("timeout_count={}", summary.timeout_count);
    println!();
    println!("rps={:.3}", summary.rps);
    println!("latency_success_samples={}", summary.latency.count);
    println!("avg_latency_ms={:.3}", summary.latency.avg_ms);
    println!("p50_latency_ms={}", summary.latency.p50_ms);
    println!("p95_latency_ms={}", summary.latency.p95_ms);
    println!("p99_latency_ms={}", summary.latency.p99_ms);
    println!("max_latency_ms={}", summary.latency.max_ms);
    println!();
    println!("status_2xx={}", summary.status_2xx);
    println!("status_4xx={}", summary.status_4xx);
    println!("status_5xx={}", summary.status_5xx);
    println!();
    println!("cache_hit_count={}", summary.cache_hit_count);
    println!("cache_miss_count={}", summary.cache_miss_count);
    println!("cache_samples={}", summary.cache_effectiveness.samples);
    println!(
        "token_weighted_prefix_hit_rate={}",
        format_option_f64(summary.cache_effectiveness.token_weighted_prefix_hit_rate)
    );
    println!(
        "request_avg_prefix_hit_rate={}",
        format_option_f64(summary.cache_effectiveness.request_avg_prefix_hit_rate)
    );
    println!(
        "matched_prefix_depth_avg={}",
        format_option_f64(summary.cache_effectiveness.matched_prefix_depth_avg)
    );
    println!(
        "matched_prefix_depth_p95={}",
        summary.cache_effectiveness.matched_prefix_depth_p95
    );
    println!(
        "avoided_prefill_tokens={}",
        summary.cache_effectiveness.avoided_prefill_tokens
    );
    println!(
        "best_gap_samples={}",
        summary.cache_effectiveness.best_gap_samples
    );
    println!(
        "best_possible_prefix_tokens={}",
        summary.cache_effectiveness.best_possible_prefix_tokens
    );
    println!(
        "cache_gap_tokens={}",
        summary.cache_effectiveness.cache_gap_tokens
    );
    println!(
        "avg_cache_gap_rate={}",
        format_option_f64(summary.cache_effectiveness.avg_cache_gap_rate)
    );
    println!(
        "misroute_samples={}",
        summary.cache_effectiveness.misroute_samples
    );
    println!(
        "misroute_rate={}",
        format_option_f64(summary.cache_effectiveness.misroute_rate)
    );
    println!();
    print_pod_counts("selected_target_pods", &summary.target_pods);
    println!();
    print_pod_counts("selected_prefill_pods", &summary.prefill_pods);
    println!();
    print_pod_counts("selected_decode_pods", &summary.decode_pods);
    println!();
    println!("csv={}", config.output.display());
}

fn print_pod_counts(title: &str, counts: &BTreeMap<String, usize>) {
    println!("{title}:");
    for (pod_id, count) in counts {
        println!("  pod_id={pod_id} count={count}");
    }
}

fn load_payloads(config: &BenchConfig) -> Result<Vec<String>, String> {
    match &config.payload {
        Some(PayloadArg::File(path)) => load_payload_file(path, config),
        None => Ok(vec![build_chat_payload(
            &config.model,
            &config.prompt,
            config.stream,
        )]),
    }
}

fn load_payload_file(path: &Path, config: &BenchConfig) -> Result<Vec<String>, String> {
    let content = fs::read_to_string(path)
        .map_err(|err| format!("failed to read payload file {}: {err}", path.display()))?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return match value {
            Value::Array(items) => Ok(items
                .into_iter()
                .map(|item| normalize_payload_value(item, config))
                .collect()),
            other => Ok(vec![normalize_payload_value(other, config)]),
        };
    }

    let payloads = trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_str::<Value>(line)
                .map(|value| normalize_payload_value(value, config))
                .unwrap_or_else(|_| build_chat_payload(&config.model, line, config.stream))
        })
        .collect();
    Ok(payloads)
}

fn normalize_payload_value(value: Value, config: &BenchConfig) -> String {
    match value {
        Value::String(prompt) => build_chat_payload(&config.model, &prompt, config.stream),
        other => serde_json::to_string(&ensure_openai_payload(other, config))
            .unwrap_or_else(|_| build_chat_payload(&config.model, &config.prompt, config.stream)),
    }
}

fn payload_with_user(payload: &str, session_id: &str) -> String {
    let Ok(mut value) = serde_json::from_str::<Value>(payload) else {
        return payload.to_string();
    };

    let Some(object) = value.as_object_mut() else {
        return payload.to_string();
    };

    object.insert("user".to_string(), Value::String(session_id.to_string()));
    serde_json::to_string(&value).unwrap_or_else(|_| payload.to_string())
}

fn ensure_openai_payload(mut value: Value, config: &BenchConfig) -> Value {
    let Some(object) = value.as_object_mut() else {
        return json_payload(&config.model, &config.prompt, config.stream);
    };

    object
        .entry("model")
        .or_insert_with(|| Value::String(config.model.clone()));
    object.entry("temperature").or_insert_with(|| json!(0.7));
    object.entry("max_tokens").or_insert_with(|| json!(128));
    object.insert("stream".to_string(), json!(config.stream));
    value
}

fn build_chat_payload(model: &str, prompt: &str, stream: bool) -> String {
    serde_json::to_string(&json_payload(model, prompt, stream))
        .expect("static JSON payload serializes")
}

fn json_payload(model: &str, prompt: &str, stream: bool) -> Value {
    json!({
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.7,
        "max_tokens": 128,
        "stream": stream
    })
}

fn parse_args(args: Vec<String>) -> Result<BenchConfig, String> {
    let mut name = DEFAULT_RUN_NAME.to_string();
    let mut output_file: Option<String> = None;
    let mut config = BenchConfig {
        name: DEFAULT_RUN_NAME.to_string(),
        url: String::new(),
        concurrency: 100,
        concurrency_sweep: None,
        threads: 4,
        payload: None,
        prompt: DEFAULT_PROMPT.to_string(),
        model: DEFAULT_MODEL.to_string(),
        mode: None,
        endpoint_path: String::new(),
        block_size: DEFAULT_BLOCK_SIZE,
        timeout_ms: 30_000,
        requests: None,
        stream: false,
        output: PathBuf::new(),
    };

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--name" => {
                name = sanitize_run_name(&take_value(&args, &mut i, "--name")?)?;
            }
            "--url" => {
                config.url = take_value(&args, &mut i, "--url")?;
            }
            "--concurrency" => {
                config.concurrency = take_value(&args, &mut i, "--concurrency")?
                    .parse()
                    .map_err(|_| "--concurrency must be a positive integer".to_string())?;
            }
            "--concurrency-sweep" => {
                let value = take_value(&args, &mut i, "--concurrency-sweep")?;
                config.concurrency_sweep = Some(parse_sweep(&value)?);
            }
            "--threads" => {
                config.threads = take_value(&args, &mut i, "--threads")?
                    .parse()
                    .map_err(|_| "--threads must be a positive integer".to_string())?;
            }
            "--payload" => {
                let value = take_value(&args, &mut i, "--payload")?;
                let path = if value == "file" {
                    if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                        i += 1;
                        PathBuf::from(&args[i])
                    } else {
                        PathBuf::from(DEFAULT_PAYLOAD_FILE)
                    }
                } else {
                    PathBuf::from(value)
                };
                config.payload = Some(PayloadArg::File(path));
            }
            "--prompt" => {
                config.prompt = take_value(&args, &mut i, "--prompt")?;
                config.payload = None;
            }
            "--model" => {
                config.model = take_value(&args, &mut i, "--model")?;
            }
            "--mode" => {
                config.mode = Some(parse_mode(&take_value(&args, &mut i, "--mode")?)?);
            }
            "--block-size" => {
                config.block_size = take_value(&args, &mut i, "--block-size")?
                    .parse()
                    .map_err(|_| "--block-size must be a positive integer".to_string())?;
            }
            "--timeout-ms" => {
                config.timeout_ms = take_value(&args, &mut i, "--timeout-ms")?
                    .parse()
                    .map_err(|_| "--timeout-ms must be a positive integer".to_string())?;
            }
            "--requests" => {
                config.requests = Some(
                    take_value(&args, &mut i, "--requests")?
                        .parse()
                        .map_err(|_| "--requests must be a positive integer".to_string())?,
                );
            }
            "--stream" => {
                config.stream = if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                    let val = take_value(&args, &mut i, "--stream")?;
                    val.parse()
                        .map_err(|_| "--stream must be true or false".to_string())?
                } else {
                    true
                };
            }
            "--output" => {
                output_file = Some(parse_output_file(&take_value(&args, &mut i, "--output")?)?);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        i += 1;
    }

    if config.url.is_empty() {
        return Err("benchmark setup invalid: --url is required".to_string());
    }
    config.endpoint_path = parse_endpoint_path(&config.url)?;
    if config.concurrency == 0 {
        return Err("benchmark setup invalid: --concurrency must be greater than zero".to_string());
    }
    if config.threads == 0 {
        return Err("benchmark setup invalid: --threads must be greater than zero".to_string());
    }
    if config.timeout_ms == 0 {
        return Err("benchmark setup invalid: --timeout-ms must be greater than zero".to_string());
    }
    if config.requests == Some(0) {
        return Err("benchmark setup invalid: --requests must be greater than zero".to_string());
    }
    if config.block_size == 0 {
        return Err("benchmark setup invalid: --block-size must be greater than zero".to_string());
    }
    if let Some(sweep) = &config.concurrency_sweep {
        if sweep.is_empty() || sweep.contains(&0) {
            return Err(
                "benchmark setup invalid: --concurrency-sweep must contain positive integers"
                    .to_string(),
            );
        }
    }

    let output_file = output_file.unwrap_or_else(|| {
        if config.concurrency_sweep.is_some() {
            DEFAULT_SWEEP_OUTPUT_FILE.to_string()
        } else {
            DEFAULT_OUTPUT_FILE.to_string()
        }
    });
    config.name = name;
    config.output = PathBuf::from(RESULTS_ROOT)
        .join(&config.name)
        .join(output_file);

    Ok(config)
}

fn sanitize_run_name(name: &str) -> Result<String, String> {
    let sanitized = name
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();

    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        return Err("--name must contain at least one safe filename character".to_string());
    }

    Ok(sanitized)
}

fn parse_output_file(value: &str) -> Result<String, String> {
    let path = Path::new(value);
    if path.is_absolute() || path.components().count() != 1 {
        return Err(
            "--output must be a file name only; results are written under benchmark/results/<name>"
                .to_string(),
        );
    }

    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return Err("--output must be a valid file name".to_string());
    };
    if file_name.is_empty() || file_name == "." || file_name == ".." {
        return Err("--output must be a valid file name".to_string());
    }

    Ok(file_name.to_string())
}

fn take_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    if *index + 1 >= args.len() {
        return Err(format!("{flag} requires a value"));
    }
    *index += 1;
    Ok(args[*index].clone())
}

fn parse_sweep(value: &str) -> Result<Vec<usize>, String> {
    value
        .split(',')
        .map(|entry| {
            entry
                .trim()
                .parse()
                .map_err(|_| format!("invalid concurrency value in sweep: {entry}"))
        })
        .collect()
}

fn parse_mode(value: &str) -> Result<BenchMode, String> {
    match value {
        "single" => Ok(BenchMode::Single),
        "disaggregated" | "dispatch" => Ok(BenchMode::Disaggregated),
        _ => Err("--mode must be single or disaggregated".to_string()),
    }
}

fn parse_endpoint_path(url: &str) -> Result<String, String> {
    let parsed = reqwest::Url::parse(url).map_err(|err| format!("invalid --url: {err}"))?;
    Ok(parsed.path().to_string())
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn create_parent_dir(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create output directory {}: {err}",
                parent.display()
            )
        })?;
    }
    Ok(())
}

fn format_option_usize(value: Option<usize>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn format_option_bool(value: Option<bool>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn format_option_f64(value: Option<f64>) -> String {
    value.map(|value| format!("{value:.6}")).unwrap_or_default()
}

fn csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --bin calinix-url-bench -- --name <run-name> --url <load-balancer-url> --concurrency <n> --threads <n> [--payload <file>|--prompt <text>] [--mode single|disaggregated] [--block-size <n>] [--timeout-ms <ms>] [--requests <n>] [--stream [true|false]] [--output <csv-file-name>]"
    );
}

#[cfg(test)]
mod tests {
    use super::{
        parse_args, parse_endpoint_path, parse_mode, payload_with_user, prompt_stats,
        summarize_cache_effectiveness, BenchMode, RequestRecord,
    };

    #[test]
    fn prompt_stats_uses_openai_payload_and_block_size() {
        let payload =
            r#"{"model":"m","messages":[{"role":"user","content":"one two three four five"}]}"#;

        let stats = prompt_stats(payload, "/v1/chat/completions", 2);

        assert_eq!(stats.tokens, 5);
        assert_eq!(stats.blocks, 3);
    }

    #[test]
    fn cache_effectiveness_summary_calculates_section_three_metrics() {
        let records = vec![
            record(8, 2, Some("2"), Some("3"), Some("2")),
            record(10, 2, Some("1"), Some("3"), Some("0")),
            record(0, 2, Some("4"), None, None),
        ];

        let summary = summarize_cache_effectiveness(&records);

        assert_eq!(summary.samples, 2);
        assert_eq!(summary.total_prompt_tokens, 18);
        assert_eq!(summary.matched_prefix_tokens, 6);
        assert_eq!(summary.avoided_prefill_tokens, 6);
        assert_eq!(summary.best_gap_samples, 2);
        assert_eq!(summary.cache_gap_tokens, 6);
        assert_eq!(summary.misroute_samples, 2);
        assert_eq!(summary.misroute_rate, Some(0.5));
        assert_eq!(summary.token_weighted_prefix_hit_rate, Some(6.0 / 18.0));
    }

    #[test]
    fn endpoint_path_is_parsed_from_url() {
        assert_eq!(
            parse_endpoint_path("http://127.0.0.1:18080/v1/chat/completions?x=1").unwrap(),
            "/v1/chat/completions"
        );
    }

    #[test]
    fn payload_user_field_carries_benchmark_session() {
        let payload = r#"{"model":"m","messages":[{"role":"user","content":"hello"}]}"#;

        let with_user = payload_with_user(payload, "bench-session-7");
        let value: serde_json::Value = serde_json::from_str(&with_user).unwrap();

        assert_eq!(
            value.get("user").and_then(|value| value.as_str()),
            Some("bench-session-7")
        );
    }

    #[test]
    fn parses_optional_benchmark_mode() {
        assert_eq!(parse_mode("single").unwrap(), BenchMode::Single);
        assert_eq!(
            parse_mode("disaggregated").unwrap(),
            BenchMode::Disaggregated
        );
        assert_eq!(parse_mode("dispatch").unwrap(), BenchMode::Disaggregated);
        assert!(parse_mode("other").is_err());
    }

    #[test]
    fn parses_request_limit() {
        let config = parse_args(vec![
            "--url".to_string(),
            "http://127.0.0.1:18080/v1/chat/completions".to_string(),
            "--requests".to_string(),
            "10000".to_string(),
        ])
        .unwrap();

        assert_eq!(config.requests, Some(10_000));
    }

    fn record(
        prompt_tokens: usize,
        block_size: usize,
        chosen_depth: Option<&str>,
        best_depth: Option<&str>,
        actual_depth: Option<&str>,
    ) -> RequestRecord {
        RequestRecord {
            request_id: 0,
            session_id: "s".to_string(),
            mode: "single".to_string(),
            status: Some(200),
            success: true,
            timeout: false,
            latency_ms: 1,
            response_bytes: 1,
            prompt_tokens,
            prompt_blocks: 0,
            block_size,
            cache_hit: None,
            cache_prefix_depth: chosen_depth.map(ToOwned::to_owned),
            best_cache_prefix_depth: best_depth.map(ToOwned::to_owned),
            actual_cache_prefix_depth: actual_depth.map(ToOwned::to_owned),
            target_pod_id: None,
            prefill_pod_id: None,
            decode_pod_id: None,
            error: None,
        }
    }
}
