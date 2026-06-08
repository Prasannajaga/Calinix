use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::thread;
use std::time::Duration;

use crate::bitmap::HostBitmap;
use crate::execute::{execute_plan, ExecutionContext};
use crate::filter::filter_candidates;
use crate::hash::prompt_to_cumulative_hashes;
use crate::indexer::{longest_prefix_lengths_for_candidates, ShardedBlockIndexer};
use crate::mock_pod::run_mock_pod_server;
use crate::pick::pick_one;
use crate::prepare::prepare;
use crate::score::score_candidates;
use crate::types::{
    CacheEvent, CandidateScore, FailurePolicy, Pod, PodRole, RoutingMode, RoutingPlan, RoutingStep,
    StepRole,
};
use crate::workflow::{build_disaggregated_plan, build_single_plan};
use crate::{route_request, RouterState};

fn test_inflight() -> [AtomicUsize; 256] {
    std::array::from_fn(|_| AtomicUsize::new(0))
}

fn test_pod(id: usize, role: PodRole, node: &str, port: u16) -> Pod {
    Pod {
        id,
        role,
        node: node.to_string(),
        addr: format!("127.0.0.1:{port}"),
        healthy: true,
        max_concurrency: 2,
    }
}

fn candidate_score(pod_id: usize, final_score: f64) -> CandidateScore {
    CandidateScore {
        pod_id,
        cache_prefix_len: 0,
        cache_score: 0.0,
        load_score: 0.0,
        locality_score: 0.0,
        sticky_score: 0.0,
        final_score,
    }
}

#[test]
fn bitmap_operations() {
    let mut a = HostBitmap::empty();
    assert!(a.is_empty());
    a.set(1);
    a.set(65);
    a.set(255);
    assert!(a.contains(1));
    assert!(a.contains(65));
    assert!(a.contains(255));
    assert_eq!(a.count_ones(), 3);

    let mut b = HostBitmap::empty();
    b.set(65);
    b.set(66);
    assert_eq!(a.and(b).iter_set_bits(), vec![65]);
    assert_eq!(a.or(b).iter_set_bits(), vec![1, 65, 66, 255]);
    assert_eq!(a.minus(b).iter_set_bits(), vec![1, 255]);

    a.clear(65);
    assert!(!a.contains(65));
    assert_eq!(HostBitmap::full_for_count(3).iter_set_bits(), vec![0, 1, 2]);
}

#[test]
fn indexer_events_are_idempotent_and_cleanup_dead_pods() {
    let indexer = ShardedBlockIndexer::new(4);
    indexer.apply_event(CacheEvent::Registered {
        pod_id: 1,
        block_hash: 100,
    });
    indexer.apply_event(CacheEvent::Registered {
        pod_id: 1,
        block_hash: 100,
    });
    assert_eq!(indexer.owners(100).count_ones(), 1);
    assert!(indexer.owners(100).contains(1));

    indexer.apply_event(CacheEvent::Evicted {
        pod_id: 2,
        block_hash: 100,
    });
    assert!(indexer.owners(100).contains(1));
    indexer.apply_event(CacheEvent::Evicted {
        pod_id: 1,
        block_hash: 100,
    });
    indexer.apply_event(CacheEvent::Evicted {
        pod_id: 1,
        block_hash: 100,
    });
    assert!(indexer.owners(100).is_empty());

    indexer.apply_event(CacheEvent::Registered {
        pod_id: 2,
        block_hash: 200,
    });
    indexer.apply_event(CacheEvent::Shutdown { pod_id: 2 });
    indexer.apply_event(CacheEvent::Shutdown { pod_id: 2 });
    assert!(indexer.owners(200).contains(2));
    assert!(!indexer.owners_alive(200).contains(2));
    indexer.cleanup_dead_pod(2);
    assert!(indexer.owners(200).is_empty());
}

#[test]
fn prefix_matching_uses_cumulative_chain() {
    let prompt =
        "one two three four five six seven eight nine ten eleven twelve alpha beta gamma delta";
    let hashes = prompt_to_cumulative_hashes(prompt);
    assert_eq!(hashes.len(), 4);

    let indexer = ShardedBlockIndexer::new(4);
    for hash in hashes.iter().take(3) {
        indexer.apply_event(CacheEvent::Registered {
            pod_id: 0,
            block_hash: *hash,
        });
    }
    indexer.apply_event(CacheEvent::Registered {
        pod_id: 1,
        block_hash: hashes[2],
    });
    indexer.apply_event(CacheEvent::Registered {
        pod_id: 1,
        block_hash: hashes[3],
    });
    indexer.apply_event(CacheEvent::Registered {
        pod_id: 2,
        block_hash: hashes[0],
    });

    let mut candidates = HostBitmap::empty();
    candidates.set(0);
    candidates.set(1);
    candidates.set(2);
    let lengths = longest_prefix_lengths_for_candidates(&indexer, &hashes, candidates);
    assert_eq!(lengths[0], 3);
    assert_eq!(lengths[1], 0);
    assert_eq!(lengths[2], 1);
}

#[test]
fn filter_removes_dead_wrong_role_and_over_concurrency_pods() {
    let pods = vec![
        test_pod(0, PodRole::Prefill, "node-a", 0),
        test_pod(1, PodRole::Decode, "node-b", 0),
        test_pod(2, PodRole::Prefill, "node-c", 0),
    ];
    let mut alive = HostBitmap::full_for_count(3);
    alive.clear(2);
    let inflight = test_inflight();
    inflight[0].store(2, Ordering::SeqCst);

    let candidates = filter_candidates(&pods, StepRole::Prefill, alive, &inflight);
    assert!(candidates.is_empty());

    let mut alive = HostBitmap::full_for_count(3);
    alive.clear(0);
    let candidates = filter_candidates(&pods, StepRole::Decode, alive, &inflight);
    assert!(candidates.contains(1));
    assert!(!candidates.contains(0));
    assert!(!candidates.contains(2));
}

#[test]
fn score_calculates_cache_load_sticky_and_locality() {
    let pods = vec![
        test_pod(0, PodRole::Prefill, "node-a", 0),
        test_pod(1, PodRole::Prefill, "node-b", 0),
        test_pod(2, PodRole::Decode, "node-a", 0),
        test_pod(3, PodRole::Decode, "node-z", 0),
    ];
    let ctx = prepare(
        "session-a".to_string(),
        "one two three four five six seven eight".to_string(),
        RoutingMode::Disaggregated,
    );
    let indexer = ShardedBlockIndexer::new(4);
    for hash in &ctx.cumulative_hashes {
        indexer.apply_event(CacheEvent::Registered {
            pod_id: 0,
            block_hash: *hash,
        });
    }
    let inflight = test_inflight();
    inflight[1].store(1, Ordering::SeqCst);
    let sessions = Mutex::new(HashMap::from([("session-a".to_string(), 1_usize)]));

    let mut candidates = HostBitmap::empty();
    candidates.set(0);
    candidates.set(1);
    let scores = score_candidates(
        &indexer,
        &ctx,
        &pods,
        candidates,
        &inflight,
        &sessions,
        StepRole::Prefill,
        None,
    );
    let pod0 = scores.iter().find(|score| score.pod_id == 0).unwrap();
    let pod1 = scores.iter().find(|score| score.pod_id == 1).unwrap();
    assert!(pod0.cache_score > pod1.cache_score);
    assert!(pod0.load_score > pod1.load_score);
    assert_eq!(pod1.sticky_score, 100.0);

    let mut decode_candidates = HostBitmap::empty();
    decode_candidates.set(2);
    decode_candidates.set(3);
    let decode_scores = score_candidates(
        &indexer,
        &ctx,
        &pods,
        decode_candidates,
        &inflight,
        &sessions,
        StepRole::Decode,
        Some(0),
    );
    let same_node = decode_scores
        .iter()
        .find(|score| score.pod_id == 2)
        .unwrap();
    let other_node = decode_scores
        .iter()
        .find(|score| score.pod_id == 3)
        .unwrap();
    assert!(same_node.locality_score > other_node.locality_score);
}

#[test]
fn pick_prefers_valid_sticky_then_max_score_and_ties_lower_id() {
    let session_map = Mutex::new(HashMap::from([("s".to_string(), 2_usize)]));
    let mut valid = HostBitmap::empty();
    valid.set(1);
    valid.set(2);
    let scores = vec![candidate_score(1, 90.0), candidate_score(2, 25.0)];
    assert_eq!(pick_one("s", &scores, &session_map, valid), Some(2));

    let session_map = Mutex::new(HashMap::new());
    let scores = vec![candidate_score(3, 50.0), candidate_score(1, 50.0)];
    let mut valid = HostBitmap::empty();
    valid.set(1);
    valid.set(3);
    assert_eq!(pick_one("fresh", &scores, &session_map, valid), Some(1));
}

#[test]
fn workflow_builds_expected_steps() {
    let single = build_single_plan(3);
    assert_eq!(single.steps.len(), 1);
    assert_eq!(single.steps[0].role, StepRole::Single);
    assert_eq!(single.steps[0].pod_id, 3);

    let disaggregated = build_disaggregated_plan(1, 2);
    assert_eq!(disaggregated.steps.len(), 2);
    assert_eq!(disaggregated.steps[0].role, StepRole::Prefill);
    assert_eq!(disaggregated.steps[1].role, StepRole::Decode);
}

static MOCKS: Once = Once::new();

fn start_test_mocks() {
    MOCKS.call_once(|| {
        thread::spawn(|| {
            let _ = run_mock_pod_server(0, PodRole::Both, 19100);
        });
        thread::spawn(|| {
            let _ = run_mock_pod_server(1, PodRole::Prefill, 19101);
        });
        thread::spawn(|| {
            let _ = run_mock_pod_server(2, PodRole::Decode, 19102);
        });
        thread::sleep(Duration::from_millis(80));
    });
}

#[test]
fn execute_single_and_disaggregated_and_cleans_inflight() {
    start_test_mocks();
    let pods = Arc::new(vec![
        test_pod(0, PodRole::Both, "node-a", 19100),
        test_pod(1, PodRole::Prefill, "node-a", 19101),
        test_pod(2, PodRole::Decode, "node-a", 19102),
    ]);
    let inflight = Arc::new(test_inflight());

    let response = execute_plan(
        build_single_plan(0),
        ExecutionContext {
            request_id: 1,
            session_id: "s".to_string(),
            prompt: "hello world".to_string(),
            cache_transfer_id: None,
            last_prefill_pod: None,
        },
        pods.clone(),
        inflight.clone(),
    )
    .unwrap();
    assert!(response.contains("mock response from pod 0"));
    assert_eq!(inflight[0].load(Ordering::SeqCst), 0);

    let response = execute_plan(
        build_disaggregated_plan(1, 2),
        ExecutionContext {
            request_id: 2,
            session_id: "s".to_string(),
            prompt: "hello world".to_string(),
            cache_transfer_id: None,
            last_prefill_pod: None,
        },
        pods.clone(),
        inflight.clone(),
    )
    .unwrap();
    assert!(response.contains("TOKEN pod=2"));
    assert!(response.contains("DONE pod=2"));
    assert_eq!(inflight[1].load(Ordering::SeqCst), 0);
    assert_eq!(inflight[2].load(Ordering::SeqCst), 0);

    let failed = execute_plan(
        RoutingPlan {
            steps: vec![RoutingStep {
                role: StepRole::Single,
                pod_id: 0,
                failure_policy: FailurePolicy::FailFast,
                cache_hint: None,
            }],
        },
        ExecutionContext {
            request_id: 3,
            session_id: "s".to_string(),
            prompt: "hello world".to_string(),
            cache_transfer_id: None,
            last_prefill_pod: None,
        },
        Arc::new(vec![test_pod(0, PodRole::Both, "node-a", 19999)]),
        inflight.clone(),
    );
    assert!(failed.is_err());
    assert_eq!(inflight[0].load(Ordering::SeqCst), 0);
}

#[test]
fn end_to_end_pipeline_cache_sticky_and_shutdown() {
    start_test_mocks();
    let pods = Arc::new(vec![
        test_pod(0, PodRole::Both, "node-a", 19100),
        test_pod(1, PodRole::Both, "node-b", 19101),
        test_pod(2, PodRole::Decode, "node-c", 19102),
    ]);
    let state = RouterState {
        indexer: Arc::new(ShardedBlockIndexer::new(3)),
        pods,
        inflight: Arc::new(test_inflight()),
        session_map: Arc::new(Mutex::new(HashMap::new())),
        request_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
    };
    let prompt = "the cat sat on the table near the warm window";
    for block_hash in prompt_to_cumulative_hashes(prompt) {
        state.indexer.apply_event(CacheEvent::Registered {
            pod_id: 0,
            block_hash,
        });
    }

    let first = route_request(
        &state,
        "user_123".to_string(),
        prompt.to_string(),
        RoutingMode::Single,
    )
    .unwrap();
    assert!(first.contains("pod=0"));

    let second = route_request(
        &state,
        "user_123".to_string(),
        "a different prompt with no registered cache".to_string(),
        RoutingMode::Single,
    )
    .unwrap();
    assert!(second.contains("pod=0"));

    state
        .indexer
        .apply_event(CacheEvent::Shutdown { pod_id: 0 });
    let third = route_request(
        &state,
        "user_123".to_string(),
        prompt.to_string(),
        RoutingMode::Single,
    )
    .unwrap();
    assert!(!third.contains("pod=0"));
    assert!(third.contains("pod=1"));
}
