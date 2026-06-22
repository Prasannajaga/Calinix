use http::HeaderMap;
use http::HeaderValue;

use crate::protocol::routing_headers::{
    inject_routing_headers, CalinixMode, RoutingHeaderValues, DECODE_POD_ID, PREFILL_POD_ID,
    REQUEST_ID, TARGET_POD_ID,
};
use crate::protocol::openai::{extract_openai_routing_view, OpenAiRequestKind};

// --- From protocol/routing_headers.rs ---

#[test]
fn overwrites_calinix_headers_and_preserves_user_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer user"));
    headers.insert(REQUEST_ID, HeaderValue::from_static("old"));

    inject_routing_headers(
        &mut headers,
        &RoutingHeaderValues {
            request_id: "new".to_string(),
            mode: CalinixMode::Single,
            target_pod_id: Some("7".to_string()),
            prefill_pod_id: None,
            decode_pod_id: None,
            cache_hit: true,
            cache_prefix_depth: 3,
            cache_namespace: Some("ns".to_string()),
            route_policy: "default".to_string(),
        },
    )
    .unwrap();

    assert_eq!(headers.get("authorization").unwrap(), "Bearer user");
    assert_eq!(headers.get(REQUEST_ID).unwrap(), "new");
    assert_eq!(headers.get(TARGET_POD_ID).unwrap(), "7");
    assert!(!headers.contains_key(PREFILL_POD_ID));
    assert!(!headers.contains_key(DECODE_POD_ID));
}

// --- From protocol/openai.rs ---

#[test]
fn extracts_chat_message_text_without_rewriting_body() {
    let body = br#"{"model":"gpt-test","messages":[{"role":"system","content":"You are terse."},{"role":"user","content":"Say hi."},{"role":"user","content":[{"type":"text","text":"skip for now"}]}],"stream":true}"#;

    let view =
        extract_openai_routing_view("/v1/chat/completions", &HeaderMap::new(), body).unwrap();

    assert_eq!(view.kind, OpenAiRequestKind::ChatCompletions);
    assert_eq!(view.model.as_deref(), Some("gpt-test"));
    assert_eq!(view.prompt_text, "You are terse.\nSay hi.");
    assert!(view.stream);
}

#[test]
fn extracts_completion_prompt_array() {
    let body = br#"{"model":"gpt-test","prompt":["one","two"]}"#;

    let view = extract_openai_routing_view("/v1/completions", &HeaderMap::new(), body).unwrap();

    assert_eq!(view.kind, OpenAiRequestKind::Completions);
    assert_eq!(view.prompt_text, "one\ntwo");
}

#[test]
fn extracts_embedding_input_array() {
    let body = br#"{"model":"embedding-test","input":["alpha","beta"]}"#;

    let view = extract_openai_routing_view("/v1/embeddings", &HeaderMap::new(), body).unwrap();

    assert_eq!(view.kind, OpenAiRequestKind::Embeddings);
    assert_eq!(view.prompt_text, "alpha\nbeta");
}

#[test]
fn unknown_paths_do_not_require_json_body() {
    let body = b"raw body can still be proxied";

    let view = extract_openai_routing_view("/any/path", &HeaderMap::new(), body).unwrap();

    assert_eq!(view.kind, OpenAiRequestKind::Unknown);
    assert_eq!(view.prompt_text, "");
}
