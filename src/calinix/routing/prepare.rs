use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use http::HeaderMap;

use crate::cache_registry::{prompt_to_cumulative_hashes_streaming, HostBitmap};
use crate::protocol::openai::extract_openai_routing_view;
use crate::protocol::routing_headers::{CalinixMode, MODE, REQUEST_ID};
use crate::routing::context::{PreparedRequest, RoutingContext};
use crate::routing::RoutingError;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct PrepareInput<'a> {
    pub path: &'a str,
    pub method: &'a str,
    pub headers: &'a HeaderMap,
    pub body: &'a [u8],
}

pub struct PrepareStage {
    pub default_mode: CalinixMode,
    pub block_size: usize,
}

impl PrepareStage {
    pub fn run(&self, input: PrepareInput<'_>) -> Result<PreparedRequest, RoutingError> {
        let openai = extract_openai_routing_view(input.path, input.headers, input.body)?;
        let block_size = self.block_size.max(1);
        let cumulative_hashes = prompt_to_cumulative_hashes_streaming(&openai.prompt_text, block_size);
        let cache_namespace = cache_namespace(openai.model.as_deref(), block_size);
        let mode = requested_mode(input.headers)?.unwrap_or_else(|| self.default_mode.clone());
        let request_id = request_id(input.headers);

        Ok(PreparedRequest {
            ctx: RoutingContext {
                request_id,
                path: input.path.to_string(),
                method: input.method.to_string(),
                mode,
                openai,
                cumulative_hashes,
                cache_namespace,
                candidate_single: HostBitmap::empty(),
                candidate_prefill: HostBitmap::empty(),
                candidate_decode: HostBitmap::empty(),
            },
        })
    }
}

fn requested_mode(headers: &HeaderMap) -> Result<Option<CalinixMode>, RoutingError> {
    let Some(value) = headers.get(MODE) else {
        return Ok(None);
    };
    let mode = value
        .to_str()
        .map_err(|_| RoutingError::InvalidMode("<non-utf8>".to_string()))?
        .trim()
        .to_ascii_lowercase();

    match mode.as_str() {
        "single" => Ok(Some(CalinixMode::Single)),
        "disaggregated" | "dispatch" => Ok(Some(CalinixMode::Disaggregated)),
        _ => Err(RoutingError::InvalidMode(mode)),
    }
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get(REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(new_request_id)
}

fn new_request_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("calinix-{nanos:x}-{counter:x}")
}

fn cache_namespace(model: Option<&str>, block_size: usize) -> String {
    let model = model.unwrap_or("unknown");
    format!("openai:{model}:whitespace-v1:block-{block_size}")
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;

    use super::{PrepareInput, PrepareStage};
    use crate::protocol::routing_headers::CalinixMode;

    #[test]
    fn prepare_builds_routing_context_without_body() {
        let stage = PrepareStage {
            default_mode: CalinixMode::Single,
            block_size: 2,
        };
        let body = br#"{"model":"m","prompt":"one two three four five"}"#;

        let prepared = stage
            .run(PrepareInput {
                path: "/v1/completions",
                method: "POST",
                headers: &HeaderMap::new(),
                body,
            })
            .unwrap();

        assert_eq!(prepared.ctx.cumulative_hashes.len(), 3);
        assert_eq!(
            prepared.ctx.cache_namespace,
            "openai:m:whitespace-v1:block-2"
        );
    }
}
