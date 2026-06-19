use crate::cache_registry::{
    prompt_to_cumulative_hashes_with_block_size, prompt_to_token_blocks_with_size,
    shard_for_fibonacci_with_count, ShardedBlockIndexer, DEFAULT_SHARD_COUNT,
};
use crate::config::{validate_config, CalinixConfig};
use crate::upstream::RuntimeRegistry;

mod routing;

const CONFIG_YAML: &str = r#"
version: 1

gateway:
  port: 8080
  strategy: cacheAware

health:
  endpoint: /health
  intervalMs: 2000
  timeoutMs: 500
  healthyThreshold: 2
  unhealthyThreshold: 3

cacheRegistry:
  enabled: true
  maxPods: 256
  shardsCount: 64
  staleTtlMs: 30000

upstreams:
  single:
    mode: single
    pods:
      - id: single-1
        url: http://single-pod-1:8000
      - id: single-2
        url: http://single-pod-2:8000

  dispatch:
    mode: dispatch
    prefill:
      pods:
        - id: prefill-1
          url: http://prefill-pod-1:8001
        - id: prefill-2
          url: http://prefill-pod-2:8001

    decode:
      pods:
        - id: decode-1
          url: http://decode-pod-1:9001
        - id: decode-2
          url: http://decode-pod-2:9001
"#;

#[test]
fn init_startup_builds_runtime_registry_from_yaml() {
    let config = parse_config();
    let registry = RuntimeRegistry::from_config(&config).expect("registry builds from config");

    assert_eq!(registry.total_pods(), 6);
    assert_eq!(registry.single_pods.count(), 2);
    assert_eq!(registry.prefill_pods.count(), 2);
    assert_eq!(registry.decode_pods.count(), 2);
    assert_eq!(registry.cache_registry.alive().count(), 0);

    println!("registry: {:#?}", registry.cache_registry.alive());
    print!("registry prefillPods: {:#?}", registry.prefill_pods);
    print!("registry decodePOds: {:#?}", registry.decode_pods);
    print!("registry single pods: {:#?}", registry.single_pods);

    let pods = &registry.pod_table.pods;
    assert_eq!(pods[0].pod_id, 0);
    assert_eq!(pods[0].address, "http://single-pod-1:8000");
    assert_eq!(pods[2].pod_id, 2);
    assert_eq!(pods[2].address, "http://prefill-pod-1:8001");
    assert_eq!(pods[4].pod_id, 4);
    assert_eq!(pods[4].address, "http://decode-pod-1:9001");
}

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

fn parse_config() -> CalinixConfig {
    let config = serde_yaml::from_str::<CalinixConfig>(CONFIG_YAML).expect("yaml parses");
    validate_config(&config).expect("yaml validates");
    config
}
