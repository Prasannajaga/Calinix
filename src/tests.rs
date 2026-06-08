use crate::bitmap::HostBitmap;
use crate::hash::prompt_to_cumulative_hashes;
use crate::indexer::{longest_prefix_lengths_for_candidates, ShardedBlockIndexer};
use crate::types::{ExecutionPlan, Mode, PodRole};
use crate::{execute, filter, parse_args, pick, prepare, role_candidates, score, static_pods};

#[test]
fn bitmap_is_fixed_and_fast_to_intersect() {
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
    assert_eq!(a.and(b).iter_set_bits(), vec![65]);
    assert_eq!(a.or(b).iter_set_bits(), vec![0, 65, 127, 255]);
    assert_eq!(a.minus(b).iter_set_bits(), vec![0, 255]);

    a.clear(65);
    assert_eq!(a.iter_set_bits(), vec![0, 255]);
}

#[test]
fn indexer_masks_shutdown_pods_without_cleanup() {
    let indexer = ShardedBlockIndexer::new(2);
    indexer.register(0, 42);
    indexer.register(1, 42);

    assert_eq!(indexer.owners_alive(42).iter_set_bits(), vec![0, 1]);
    indexer.shutdown(0);
    assert_eq!(indexer.owners_alive(42).iter_set_bits(), vec![1]);
}

#[test]
fn prefix_binary_search_finds_longest_cumulative_match() {
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
fn minimal_routes_pick_cache_hot_prefill_and_local_decode() {
    let pods = static_pods();
    let prompt = "the cat sat on the table near the window";
    let hashes = prompt_to_cumulative_hashes(prompt);
    let indexer = ShardedBlockIndexer::new(pods.len());

    for hash in &hashes {
        indexer.register(0, *hash);
    }

    let prepared = prepare(prompt);
    let filtered = filter(&pods, &indexer, Mode::Single);
    let scored = score(&indexer, &prepared, &pods, &filtered, Mode::Single);
    let picked = pick(&pods, &scored, Mode::Single).unwrap();
    let single = execute(&picked);
    assert_eq!(single.response_pod, Some(0));

    let prepared = prepare(prompt);
    let filtered = filter(&pods, &indexer, Mode::Disaggregated);
    let scored = score(&indexer, &prepared, &pods, &filtered, Mode::Disaggregated);
    let picked = pick(&pods, &scored, Mode::Disaggregated).unwrap();
    let disaggregated = execute(&picked);
    assert_eq!(disaggregated.prefill_pod, Some(0));
    assert_eq!(disaggregated.decode_pod, Some(0));
}

#[test]
fn role_candidates_and_decode_locality_are_deterministic() {
    let pods = static_pods();
    assert_eq!(
        role_candidates(&pods, PodRole::Both).iter_set_bits(),
        vec![0]
    );
    assert_eq!(
        role_candidates(&pods, PodRole::Prefill).iter_set_bits(),
        vec![0, 1, 2, 4]
    );
    assert_eq!(
        role_candidates(&pods, PodRole::Decode).iter_set_bits(),
        vec![0, 3, 5, 6, 7]
    );

    let prompt = "the cat sat on the table near the window";
    let indexer = ShardedBlockIndexer::new(pods.len());
    let prepared = prepare(prompt);
    let filtered = filter(&pods, &indexer, Mode::Disaggregated);
    let scored = score(&indexer, &prepared, &pods, &filtered, Mode::Disaggregated);
    let picked = pick(&pods, &scored, Mode::Disaggregated).unwrap();

    assert!(matches!(
        picked,
        ExecutionPlan::Disaggregated {
            prefill_pod: 0,
            decode_pod: 0
        }
    ));
}

#[test]
fn cli_matches_requested_shape_after_cargo_separator() {
    let args = vec![
        "--single".to_string(),
        "hello world".to_string(),
        "--hits".to_string(),
        "1000".to_string(),
    ];
    let config = parse_args(&args).unwrap();
    assert_eq!(config.mode, Mode::Single);
    assert_eq!(config.prompt, "hello world");
    assert_eq!(config.hits, 1000);
}

#[test]
fn visible_pipeline_runs_prepare_filter_score_pick_execute() {
    let pods = static_pods();
    let prompt = "the cat sat on the table near the window";
    let indexer = ShardedBlockIndexer::new(pods.len());
    for hash in prompt_to_cumulative_hashes(prompt) {
        indexer.register(0, hash);
    }

    let prepared = prepare(prompt);
    let filtered = filter(&pods, &indexer, Mode::Single);
    let scored = score(&indexer, &prepared, &pods, &filtered, Mode::Single);
    let picked = pick(&pods, &scored, Mode::Single).unwrap();
    let result = execute(&picked);

    assert!(matches!(picked, ExecutionPlan::Single { pod_id: 0 }));
    assert_eq!(result.response_pod, Some(0));
}
