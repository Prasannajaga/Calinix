

// YAML CONFIG types  
struct GatewayConfig {
    port: u16,
    strategy: Strategy,
}

enum Strategy {
    CacheAware,
}

struct HealthConfig {
    endpoint: String,
    interval_ms: u64,
    timeout_ms: u64,
    healthy_threshold: u8,
    unhealthy_threshold: u8,
}

struct PodConfig {
    id: String,
    url: String,
}

enum PodRole {
    Single,
    Prefill,
    Decode,
}

struct PodEndpoint {
    internal_id: u16,
    external_id: String,
    url: String,
    role: PodRole,
    healthy: bool,
    generation: u64,
}

struct CacheRegistry {
    alive: HostBitmap,
    // block_hash -> pod bitmap
    block_index: ShardedBlockIndexer,
}




// OPENAI COMPATIBLE 

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,

    #[serde(default)]
    pub stream: bool,

    #[serde(default)]
    pub temperature: Option<f32>,

    #[serde(default)]
    pub max_tokens: Option<u32>,

    #[serde(default)]
    pub top_p: Option<f32>,

    #[serde(default)]
    pub stop: Option<serde_json::Value>,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,

    // Keep flexible because content can be string or structured multimodal content.
    pub content: serde_json::Value,

    #[serde(default)]
    pub name: Option<String>,

    #[serde(default)]
    pub tool_calls: Option<serde_json::Value>,

    #[serde(default)]
    pub tool_call_id: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}