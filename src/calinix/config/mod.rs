pub mod loader;
pub mod schema;
pub mod validation;

pub use loader::load_config;
pub use schema::{
    CacheRegistryConfig, CalinixConfig, DispatchUpstreamsConfig, GatewayConfig, HealthConfig,
    PodConfig, PodGroupConfig, SingleUpstreamsConfig, Strategy, UpstreamMode, UpstreamsConfig,
};
pub use validation::validate_config;
