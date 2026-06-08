mod bitmap;
mod hash;
mod indexer;
#[cfg(test)]
mod tests;
mod types;

use std::env;
use std::time::Instant;

use bitmap::HostBitmap;
use hash::prompt_to_cumulative_hashes;
use indexer::{longest_prefix_lengths_for_candidates, ShardedBlockIndexer};
use types::{
    Config, ExecutionPlan, FilteredCandidates, Mode, PickedPod, Pod, PodRole, PreparedRequest,
    RouteResult, ScoredCandidate,
};

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match parse_args(&args) {
        Ok(config) => run_bench(config),
        Err(err) => {
            eprintln!("{err}");
            eprintln!();
            print_usage();
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!("usage:");
    eprintln!("  cargo run -- --single \"the cat sat on the table\" --hits 1000");
    eprintln!("  cargo run -- --disaggregated \"the cat sat on the table\" --hits 1000");
    eprintln!();
    eprintln!("if running the compiled binary directly:");
    eprintln!("  target/debug/cache-aware-routing --single \"prompt\" --hits 1000");
}

fn parse_args(args: &[String]) -> Result<Config, String> {
    let mode = if args.iter().any(|arg| arg == "--single") {
        Mode::Single
    } else if args.iter().any(|arg| arg == "--disaggregated") {
        Mode::Disaggregated
    } else {
        return Err("choose --single or --disaggregated".to_string());
    };

    let mode_index = args
        .iter()
        .position(|arg| arg == "--single" || arg == "--disaggregated")
        .unwrap();
    let prompt = args
        .get(mode_index + 1)
        .ok_or_else(|| "missing user prompt".to_string())?
        .clone();
    if prompt.starts_with("--") {
        return Err("missing user prompt".to_string());
    }

    let hits = arg_value(args, "--hits")
        .unwrap_or_else(|| "1000".to_string())
        .parse::<usize>()
        .map_err(|err| format!("invalid --hits: {err}"))?;

    Ok(Config { mode, prompt, hits })
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

fn run_bench(config: Config) {
    let pods = static_pods();
    let indexer = warm_indexer(&config.prompt, &pods);
    let mut latencies = Vec::with_capacity(config.hits);
    let mut selected = [0_usize; 8];
    let mut last = RouteResult::default();

    for _ in 0..config.hits {
        let start = Instant::now();

        // The benchmark loop intentionally keeps these phases separate.
        let prepared = prepare(&config.prompt);
        let filtered = filter(&pods, &indexer, config.mode);
        let scored = score(&indexer, &prepared, &pods, &filtered, config.mode);
        let picked = pick(&pods, &scored, config.mode).expect("static pods provide a route");
        last = execute(&picked);

        latencies.push(start.elapsed().as_secs_f64() * 1_000_000.0);
        if let Some(pod_id) = last.response_pod {
            selected[pod_id] += 1;
        }
    }

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let avg = if latencies.is_empty() {
        0.0
    } else {
        latencies.iter().sum::<f64>() / latencies.len() as f64
    };
    let blocks = prompt_to_cumulative_hashes(&config.prompt).len();

    println!("mode: {:?}", config.mode);
    println!("prompt blocks: {blocks}");
    println!("cache-hit iterations: {}", config.hits);
    println!("last result: {:?}", last);
    println!("last mock response: {}", last.text);
    println!("avg route time: {avg:.3} us");
    println!("p50 route time: {:.3} us", percentile(&latencies, 50));
    println!("p95 route time: {:.3} us", percentile(&latencies, 95));
    println!("selected response pod counts: {:?}", selected);
}

fn prepare(prompt: &str) -> PreparedRequest {
    PreparedRequest {
        prompt: prompt.to_string(),
        cumulative_hashes: prompt_to_cumulative_hashes(prompt),
    }
}

fn filter(pods: &[Pod], indexer: &ShardedBlockIndexer, mode: Mode) -> Vec<FilteredCandidates> {
    match mode {
        Mode::Single => vec![FilteredCandidates {
            role: PodRole::Both,
            hosts: role_candidates(pods, PodRole::Both).and(indexer.alive()),
        }],
        Mode::Disaggregated => vec![
            FilteredCandidates {
                role: PodRole::Prefill,
                hosts: role_candidates(pods, PodRole::Prefill).and(indexer.alive()),
            },
            FilteredCandidates {
                role: PodRole::Decode,
                hosts: role_candidates(pods, PodRole::Decode).and(indexer.alive()),
            },
        ],
    }
}

fn score(
    indexer: &ShardedBlockIndexer,
    prepared: &PreparedRequest,
    pods: &[Pod],
    filtered: &[FilteredCandidates],
    mode: Mode,
) -> Vec<Vec<ScoredCandidate>> {
    filtered
        .iter()
        .map(|candidates| {
            let prefix_lengths = longest_prefix_lengths_for_candidates(
                indexer,
                &prepared.cumulative_hashes,
                candidates.hosts,
            );

            candidates
                .hosts
                .iter_set_bits()
                .into_iter()
                .map(|pod_id| {
                    let prefix_len = prefix_lengths[pod_id];
                    let locality = locality_bonus(pods, pod_id, candidates.role, mode);
                    ScoredCandidate {
                        pod_id,
                        prefix_len,
                        score: prefix_len * 100 + locality,
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn pick(pods: &[Pod], scored: &[Vec<ScoredCandidate>], mode: Mode) -> Option<ExecutionPlan> {
    match mode {
        Mode::Single => {
            let single = pick_best(&scored[0])?;
            Some(ExecutionPlan::Single {
                pod_id: single.pod_id,
            })
        }
        Mode::Disaggregated => {
            let prefill = pick_best(&scored[0])?;
            let decode = pick_decode(pods, &scored[1], prefill.pod_id)?;
            Some(ExecutionPlan::Disaggregated {
                prefill_pod: prefill.pod_id,
                decode_pod: decode.pod_id,
            })
        }
    }
}

fn execute(plan: &ExecutionPlan) -> RouteResult {
    match plan {
        ExecutionPlan::Single { pod_id } => single_execution(*pod_id),
        ExecutionPlan::Disaggregated {
            prefill_pod,
            decode_pod,
        } => {
            let transfer_id = prefill_execution(*prefill_pod);
            decode_execution(*decode_pod, &transfer_id)
        }
    }
}

fn single_execution(pod_id: usize) -> RouteResult {
    RouteResult {
        prefill_pod: None,
        decode_pod: None,
        response_pod: Some(pod_id),
        text: format!("mock single response from pod {pod_id}"),
    }
}

fn prefill_execution(pod_id: usize) -> String {
    format!("cache-transfer-from-pod-{pod_id}")
}

fn decode_execution(pod_id: usize, transfer_id: &str) -> RouteResult {
    RouteResult {
        prefill_pod: transfer_id
            .rsplit_once('-')
            .and_then(|(_, id)| id.parse::<usize>().ok()),
        decode_pod: Some(pod_id),
        response_pod: Some(pod_id),
        text: format!("mock decode response from pod {pod_id} using {transfer_id}"),
    }
}

fn warm_indexer(prompt: &str, pods: &[Pod]) -> ShardedBlockIndexer {
    let indexer = ShardedBlockIndexer::new(pods.len());
    let hashes = prompt_to_cumulative_hashes(prompt);

    // Pod 0 owns the full prefix chain; pod 1 owns only the first prefix.
    for hash in &hashes {
        indexer.register(0, *hash);
    }
    if let Some(first) = hashes.first() {
        indexer.register(1, *first);
    }

    indexer
}

fn role_candidates(pods: &[Pod], wanted: PodRole) -> HostBitmap {
    let mut bitmap = HostBitmap::empty();
    for pod in pods {
        let matches = match wanted {
            PodRole::Both => pod.role == PodRole::Both,
            PodRole::Prefill => pod.role == PodRole::Prefill || pod.role == PodRole::Both,
            PodRole::Decode => pod.role == PodRole::Decode || pod.role == PodRole::Both,
        };
        if matches {
            bitmap.set(pod.id);
        }
    }
    bitmap
}

fn pick_best(scores: &[ScoredCandidate]) -> Option<PickedPod> {
    scores
        .iter()
        .max_by_key(|candidate| (candidate.score, std::cmp::Reverse(candidate.pod_id)))
        .map(|candidate| PickedPod {
            pod_id: candidate.pod_id,
        })
}

fn pick_decode(pods: &[Pod], scores: &[ScoredCandidate], prefill_pod: usize) -> Option<PickedPod> {
    let prefill_node = pods.iter().find(|pod| pod.id == prefill_pod)?.node;
    scores
        .iter()
        .max_by_key(|candidate| {
            let same_node = pods
                .iter()
                .find(|pod| pod.id == candidate.pod_id)
                .map(|pod| pod.node == prefill_node)
                .unwrap_or(false);
            (
                same_node,
                candidate.score,
                std::cmp::Reverse(candidate.pod_id),
            )
        })
        .map(|candidate| PickedPod {
            pod_id: candidate.pod_id,
        })
}

fn locality_bonus(pods: &[Pod], pod_id: usize, role: PodRole, mode: Mode) -> usize {
    if mode == Mode::Disaggregated && role == PodRole::Decode {
        let has_node_peer = pods
            .iter()
            .any(|pod| pod.role != PodRole::Decode && pod.id != pod_id);
        if has_node_peer {
            return 1;
        }
    }
    0
}

fn static_pods() -> Vec<Pod> {
    vec![
        Pod::new(0, PodRole::Both, "node-a"),
        Pod::new(1, PodRole::Prefill, "node-a"),
        Pod::new(2, PodRole::Prefill, "node-b"),
        Pod::new(3, PodRole::Decode, "node-b"),
        Pod::new(4, PodRole::Prefill, "node-c"),
        Pod::new(5, PodRole::Decode, "node-c"),
        Pod::new(6, PodRole::Decode, "node-d"),
        Pod::new(7, PodRole::Decode, "node-d"),
    ]
}

fn percentile(values: &[f64], percentile: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values[((values.len() - 1) * percentile) / 100]
}
