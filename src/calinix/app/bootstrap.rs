use std::env;
use std::net::SocketAddr;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use tokio::net::TcpListener;
use tracing::{info, Level};

use crate::app::state::{AppState, RegistrySummary};
use crate::config::{load_config, CalinixConfig};
use crate::upstream::RuntimeRegistry;

const DEFAULT_CONFIG_PATH: &str = "./config.yaml";

pub async fn run_from_cli() -> Result<(), String> {
    init_tracing();

    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_CONFIG_PATH.to_string());

    let config = load_config(&config_path)?;
    run(config).await
}

pub async fn run(config: CalinixConfig) -> Result<(), String> {
    let registry: RuntimeRegistry = RuntimeRegistry::from_config(&config)?;
    log_registry_startup(&registry);

    let port = config.gateway.port;
    let health_endpoint = config.health.endpoint.clone();
    let state = AppState::new(registry);
    let app = router(state, &health_endpoint);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| format!("failed to bind {addr}: {err}"))?;

    info!(%addr, "calinix gateway listening");
    axum::serve(listener, app)
        .await
        .map_err(|err| format!("gateway server failed: {err}"))
}

fn router(state: AppState, health_endpoint: &str) -> Router {
    let mut app = Router::new().route("/debug/registry", get(debug_registry));

    app = app.route(normalize_route_path(health_endpoint).as_str(), get(health));
    if health_endpoint != "/health" {
        app = app.route("/health", get(health));
    }

    app.with_state(state)
}

async fn health() -> impl IntoResponse {
    "ok"
}

async fn debug_registry(State(state): State<AppState>) -> Json<RegistrySummary> {
    Json(RegistrySummary::from(state.registry.as_ref()))
}

fn log_registry_startup(registry: &RuntimeRegistry) {
    for pod in &registry.pod_table.pods {
        info!(
            pod_id = pod.pod_id,
            external_id = %pod.external_id,
            url = %pod.url,
            role = ?pod.role,
            "loaded upstream pod"
        );
    }

    info!(
        total_pods = registry.total_pods(),
        single_pods = registry.single_pods.count(),
        prefill_pods = registry.prefill_pods.count(),
        decode_pods = registry.decode_pods.count(),
        alive_pods = registry.cache_registry.alive().count(),
        "runtime registry initialized"
    );
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();
}

fn normalize_route_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}
