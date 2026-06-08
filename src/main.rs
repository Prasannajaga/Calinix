mod bitmap;
mod execute;
mod filter;
mod hash;
mod indexer;
mod mock_pod;
mod pick;
mod prepare;
mod protocol;
mod score;
#[cfg(test)]
mod tests;
mod types;
mod workflow;

use std::collections::HashMap;
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use execute::{execute_plan, ExecutionContext};
use filter::filter_candidates;
use hash::prompt_to_cumulative_hashes;
use indexer::ShardedBlockIndexer;
use mock_pod::{run_mock_pod_server, spawn_mock_pods};
use pick::pick_one;
use prepare::prepare;
use protocol::{get_field, quote_value, send_line};
use score::score_candidates;
use types::{CacheEvent, Pod, PodId, PodRole, RoutingMode, SessionId, StepRole};
use workflow::{build_disaggregated_plan, build_single_plan};

const REQUEST_ADDR: &str = "127.0.0.1:7000";
const ADMIN_ADDR: &str = "127.0.0.1:7001";

type Inflight = [AtomicUsize; 256];

#[derive(Clone)]
struct RouterState {
    indexer: Arc<ShardedBlockIndexer>,
    pods: Arc<Vec<Pod>>,
    inflight: Arc<Inflight>,
    session_map: Arc<Mutex<HashMap<SessionId, PodId>>>,
    request_counter: Arc<AtomicU64>,
}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        print_usage();
        return;
    }

    let result = match args[0].as_str() {
        "router" => run_router(args.get(1..).unwrap_or(&[])),
        "mock-pod" => run_mock_pod_cli(args.get(1..).unwrap_or(&[])),
        "event" => run_event_cli(args.get(1..).unwrap_or(&[])),
        "request" => run_request_cli(args.get(1..).unwrap_or(&[])),
        "bench" => run_bench_cli(args.get(1..).unwrap_or(&[])),
        _ => {
            print_usage();
            Ok(())
        }
    };

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn print_usage() {
    eprintln!("usage:");
    eprintln!("  cargo run -- router --spawn-mocks");
    eprintln!("  cargo run -- mock-pod --pod-id 0 --role both --port 9100");
    eprintln!("  cargo run -- event register --pod 0 --prompt \"the cat sat\"");
    eprintln!("  cargo run -- event evict --pod 0 --prompt \"the cat sat\"");
    eprintln!("  cargo run -- event shutdown --pod 0");
    eprintln!("  cargo run -- request --session user_123 --prompt \"the cat sat\" --mode single");
    eprintln!("  cargo run -- bench --requests 1000 --sessions 50");
}

fn run_router(args: &[String]) -> Result<(), String> {
    let state = RouterState {
        indexer: Arc::new(ShardedBlockIndexer::new(8)),
        pods: Arc::new(static_pods()),
        inflight: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
        session_map: Arc::new(Mutex::new(HashMap::new())),
        request_counter: Arc::new(AtomicU64::new(1)),
    };

    if args.iter().any(|arg| arg == "--spawn-mocks") {
        spawn_mock_pods(&state.pods);
    }

    let admin_state = state.clone();
    thread::spawn(move || {
        if let Err(err) = run_admin_server(admin_state) {
            eprintln!("admin server failed: {err}");
        }
    });

    run_request_server(state)
}

fn run_mock_pod_cli(args: &[String]) -> Result<(), String> {
    let pod_id = arg_value(args, "--pod-id")
        .ok_or_else(|| "--pod-id is required".to_string())?
        .parse::<usize>()
        .map_err(|err| format!("invalid --pod-id: {err}"))?;
    let role = parse_role(&arg_value(args, "--role").unwrap_or_else(|| "both".to_string()))?;
    let port = arg_value(args, "--port")
        .ok_or_else(|| "--port is required".to_string())?
        .parse::<u16>()
        .map_err(|err| format!("invalid --port: {err}"))?;
    run_mock_pod_server(pod_id, role, port).map_err(|err| err.to_string())
}

fn run_event_cli(args: &[String]) -> Result<(), String> {
    
    let action = args
        .first()
        .ok_or_else(|| "event action is required".to_string())?;

    let pod = arg_value(args, "--pod")
        .ok_or_else(|| "--pod is required".to_string())?
        .parse::<usize>()
        .map_err(|err| format!("invalid --pod: {err}"))?;

    let line = match action.as_str() {
        "register" | "register-prompt" => {
            if let Some(block) = arg_value(args, "--block") {
                format!("REGISTER pod={pod} block={block}")
            } else {
                let prompt = arg_value(args, "--prompt")
                    .ok_or_else(|| "--prompt is required".to_string())?;
                format!(
                    "REGISTER_PROMPT pod={pod} prompt=\"{}\"",
                    quote_value(&prompt)
                )
            }
        }
        "evict" | "evict-prompt" => {
            if let Some(block) = arg_value(args, "--block") {
                format!("EVICT pod={pod} block={block}")
            } else {
                let prompt = arg_value(args, "--prompt")
                    .ok_or_else(|| "--prompt is required".to_string())?;
                format!("EVICT_PROMPT pod={pod} prompt=\"{}\"", quote_value(&prompt))
            }
        }
        "shutdown" => format!("SHUTDOWN pod={pod}"),
        _ => return Err(format!("unknown event action: {action}")),
    };

    for response in send_line(ADMIN_ADDR, &line)? {
        println!("{response}");
    }
    Ok(())
}

fn run_request_cli(args: &[String]) -> Result<(), String> {
    let session =
        arg_value(args, "--session").ok_or_else(|| "--session is required".to_string())?;
    let prompt = arg_value(args, "--prompt").ok_or_else(|| "--prompt is required".to_string())?;
    let mode = arg_value(args, "--mode").unwrap_or_else(|| "single".to_string());
    let line = format!(
        "REQUEST session={} mode={} prompt=\"{}\"",
        session,
        mode,
        quote_value(&prompt)
    );
    for response in send_line(REQUEST_ADDR, &line)? {
        println!("{response}");
    }
    Ok(())
}

fn run_bench_cli(args: &[String]) -> Result<(), String> {
    let requests = arg_value(args, "--requests")
        .unwrap_or_else(|| "1000".to_string())
        .parse::<usize>()
        .map_err(|err| format!("invalid --requests: {err}"))?;
    let sessions = arg_value(args, "--sessions")
        .unwrap_or_else(|| "50".to_string())
        .parse::<usize>()
        .map_err(|err| format!("invalid --sessions: {err}"))?;

    let mut latencies = Vec::with_capacity(requests);
    let mut success = 0;
    let mut errors = 0;
    let mut pod_counts: HashMap<String, usize> = HashMap::new();

    for i in 0..requests {
        let prompt = format!("bench prompt {} common prefix words", i % 10);
        let session = format!("session_{}", i % sessions.max(1));
        let line = format!(
            "REQUEST session={} mode=disaggregated prompt=\"{}\"",
            session,
            quote_value(&prompt)
        );
        let start = Instant::now();
        match send_line(REQUEST_ADDR, &line) {
            Ok(lines) => {
                latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                let joined = lines.join(" ");
                if joined.contains("ROUTER_OK") {
                    success += 1;
                    if let Some(pod) = get_field(&joined, "decode_pod") {
                        *pod_counts.entry(pod).or_insert(0) += 1;
                    }
                } else {
                    errors += 1;
                }
            }
            Err(_) => errors += 1,
        }
    }

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let avg = if latencies.is_empty() {
        0.0
    } else {
        latencies.iter().sum::<f64>() / latencies.len() as f64
    };
    println!("total requests: {requests}");
    println!("success count: {success}");
    println!("error count: {errors}");
    println!("average latency ms: {avg:.2}");
    println!("p50 latency ms: {:.2}", percentile(&latencies, 50));
    println!("p95 latency ms: {:.2}", percentile(&latencies, 95));
    println!("selected pod counts: {:?}", pod_counts);
    Ok(())
}

fn run_admin_server(state: RouterState) -> Result<(), String> {
    let listener = TcpListener::bind(ADMIN_ADDR).map_err(|err| err.to_string())?;
    println!("admin/event server listening on {ADMIN_ADDR}");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state = state.clone();
                thread::spawn(move || handle_admin_connection(stream, state));
            }
            Err(err) => eprintln!("admin accept error: {err}"),
        }
    }
    Ok(())
}

fn run_request_server(state: RouterState) -> Result<(), String> {
    let listener = TcpListener::bind(REQUEST_ADDR).map_err(|err| err.to_string())?;
    println!("request server listening on {REQUEST_ADDR}");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state = state.clone();
                thread::spawn(move || handle_request_connection(stream, state));
            }
            Err(err) => eprintln!("request accept error: {err}"),
        }
    }
    Ok(())
}

fn handle_admin_connection(mut stream: TcpStream, state: RouterState) {
    let line = match read_one_line(&stream) {
        Ok(line) => line,
        Err(err) => {
            let _ = writeln!(stream, "ERROR message=\"{err}\"");
            return;
        }
    };
    let response =
        handle_admin_line(&line, &state).unwrap_or_else(|err| format!("ERROR message=\"{err}\""));
    let _ = writeln!(stream, "{response}");
}

fn handle_admin_line(line: &str, state: &RouterState) -> Result<String, String> {
    if line.starts_with("REGISTER ") {
        let pod_id = parse_field::<usize>(line, "pod")?;
        let block_hash = parse_field::<u64>(line, "block")?;
        state
            .indexer
            .apply_event(CacheEvent::Registered { pod_id, block_hash });
        Ok("OK registered".to_string())
    } else if line.starts_with("EVICT ") {
        let pod_id = parse_field::<usize>(line, "pod")?;
        let block_hash = parse_field::<u64>(line, "block")?;
        state
            .indexer
            .apply_event(CacheEvent::Evicted { pod_id, block_hash });
        Ok("OK evicted".to_string())
    } else if line.starts_with("SHUTDOWN ") {
        let pod_id = parse_field::<usize>(line, "pod")?;
        state.indexer.apply_event(CacheEvent::Shutdown { pod_id });
        Ok("OK shutdown".to_string())
    } else if line.starts_with("REGISTER_PROMPT ") {
        let pod_id = parse_field::<usize>(line, "pod")?;
        let prompt = get_field(line, "prompt").ok_or_else(|| "missing prompt".to_string())?;
        for block_hash in prompt_to_cumulative_hashes(&prompt) {
            state
                .indexer
                .apply_event(CacheEvent::Registered { pod_id, block_hash });
        }
        Ok("OK registered_prompt".to_string())
    } else if line.starts_with("EVICT_PROMPT ") {
        let pod_id = parse_field::<usize>(line, "pod")?;
        let prompt = get_field(line, "prompt").ok_or_else(|| "missing prompt".to_string())?;
        for block_hash in prompt_to_cumulative_hashes(&prompt) {
            state
                .indexer
                .apply_event(CacheEvent::Evicted { pod_id, block_hash });
        }
        Ok("OK evicted_prompt".to_string())
    } else if line.starts_with("CLEANUP_DEAD ") {
        let pod_id = parse_field::<usize>(line, "pod")?;
        state.indexer.cleanup_dead_pod(pod_id);
        Ok("OK cleanup_dead".to_string())
    } else if line.starts_with("DUMP") {
        Ok(format!(
            "OK alive={:?}",
            state.indexer.alive().iter_set_bits()
        ))
    } else {
        Err("unknown admin command".to_string())
    }
}

fn handle_request_connection(mut stream: TcpStream, state: RouterState) {
    let line = match read_one_line(&stream) {
        Ok(line) => line,
        Err(err) => {
            let _ = writeln!(stream, "ROUTER_ERROR message=\"{err}\"");
            return;
        }
    };
    let response = handle_request_line(&line, &state)
        .unwrap_or_else(|err| format!("ROUTER_ERROR message=\"{err}\""));
    let _ = writeln!(stream, "{response}");
}

fn handle_request_line(line: &str, state: &RouterState) -> Result<String, String> {
    if !line.starts_with("REQUEST ") {
        return Err("unknown request command".to_string());
    }
    let session = get_field(line, "session").ok_or_else(|| "missing session".to_string())?;
    let prompt = get_field(line, "prompt").ok_or_else(|| "missing prompt".to_string())?;
    let mode = parse_mode(&get_field(line, "mode").unwrap_or_else(|| "single".to_string()))?;
    route_request(state, session, prompt, mode)
}

fn route_request(
    state: &RouterState,
    session_id: String,
    prompt: String,
    mode: RoutingMode,
) -> Result<String, String> {
    let ctx = prepare(session_id.clone(), prompt.clone(), mode.clone());
    let request_id = state.request_counter.fetch_add(1, Ordering::SeqCst);

    match mode {
        RoutingMode::Single => {
            let candidates = filter_candidates(
                &state.pods,
                StepRole::Single,
                state.indexer.alive(),
                &state.inflight,
            );
            let scores = score_candidates(
                &state.indexer,
                &ctx,
                &state.pods,
                candidates,
                &state.inflight,
                &state.session_map,
                StepRole::Single,
                None,
            );
            let pod_id = pick_one(&session_id, &scores, &state.session_map, candidates)
                .ok_or_else(|| "no single candidates".to_string())?;
            let plan = build_single_plan(pod_id);
            let response = execute_plan(
                plan,
                ExecutionContext {
                    request_id,
                    session_id,
                    prompt,
                    cache_transfer_id: None,
                    last_prefill_pod: None,
                },
                state.pods.clone(),
                state.inflight.clone(),
            )?;
            Ok(format!("ROUTER_OK mode=single pod={pod_id} {response}"))
        }
        RoutingMode::Disaggregated => {
            let prefill_candidates = filter_candidates(
                &state.pods,
                StepRole::Prefill,
                state.indexer.alive(),
                &state.inflight,
            );
            let prefill_scores = score_candidates(
                &state.indexer,
                &ctx,
                &state.pods,
                prefill_candidates,
                &state.inflight,
                &state.session_map,
                StepRole::Prefill,
                None,
            );
            let prefill_pod = pick_one(
                &session_id,
                &prefill_scores,
                &state.session_map,
                prefill_candidates,
            )
            .ok_or_else(|| "no prefill candidates".to_string())?;

            let decode_candidates = filter_candidates(
                &state.pods,
                StepRole::Decode,
                state.indexer.alive(),
                &state.inflight,
            );
            let decode_scores = score_candidates(
                &state.indexer,
                &ctx,
                &state.pods,
                decode_candidates,
                &state.inflight,
                &state.session_map,
                StepRole::Decode,
                Some(prefill_pod),
            );
            let decode_pod = pick_one(
                &session_id,
                &decode_scores,
                &state.session_map,
                decode_candidates,
            )
            .ok_or_else(|| "no decode candidates".to_string())?;

            let plan = build_disaggregated_plan(prefill_pod, decode_pod);
            let response = execute_plan(
                plan,
                ExecutionContext {
                    request_id,
                    session_id,
                    prompt,
                    cache_transfer_id: None,
                    last_prefill_pod: None,
                },
                state.pods.clone(),
                state.inflight.clone(),
            )?;
            Ok(format!(
                "ROUTER_OK mode=disaggregated prefill_pod={prefill_pod} decode_pod={decode_pod} {response}"
            ))
        }
    }
}

fn static_pods() -> Vec<Pod> {
    vec![
        pod(0, PodRole::Both, "node-a", 9100),
        pod(1, PodRole::Prefill, "node-a", 9101),
        pod(2, PodRole::Prefill, "node-b", 9102),
        pod(3, PodRole::Decode, "node-b", 9103),
        pod(4, PodRole::Prefill, "node-c", 9104),
        pod(5, PodRole::Decode, "node-c", 9105),
        pod(6, PodRole::Decode, "node-d", 9106),
        pod(7, PodRole::Decode, "node-d", 9107),
    ]
}

fn pod(id: usize, role: PodRole, node: &str, port: u16) -> Pod {
    Pod {
        id,
        role,
        node: node.to_string(),
        addr: format!("127.0.0.1:{port}"),
        healthy: true,
        max_concurrency: 64,
    }
}

fn read_one_line(stream: &TcpStream) -> Result<String, String> {
    let mut reader = BufReader::new(stream.try_clone().map_err(|err| err.to_string())?);
    let mut line = String::new();
    reader.read_line(&mut line).map_err(|err| err.to_string())?;
    Ok(line.trim_end().to_string())
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

fn parse_role(role: &str) -> Result<PodRole, String> {
    match role {
        "prefill" => Ok(PodRole::Prefill),
        "decode" => Ok(PodRole::Decode),
        "both" => Ok(PodRole::Both),
        _ => Err(format!("unknown role: {role}")),
    }
}

fn parse_mode(mode: &str) -> Result<RoutingMode, String> {
    match mode {
        "single" => Ok(RoutingMode::Single),
        "disaggregated" => Ok(RoutingMode::Disaggregated),
        _ => Err(format!("unknown mode: {mode}")),
    }
}

fn parse_field<T: std::str::FromStr>(line: &str, key: &str) -> Result<T, String> {
    get_field(line, key)
        .ok_or_else(|| format!("missing {key}"))?
        .parse::<T>()
        .map_err(|_| format!("invalid {key}"))
}

fn percentile(values: &[f64], percentile: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let rank = ((values.len() - 1) * percentile) / 100;
    values[rank]
}
