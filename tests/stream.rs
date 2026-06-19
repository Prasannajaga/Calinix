use std::io;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use serde_json::json;

struct MockPod {
    base_url: String,
}

#[tokio::test]
#[ignore = "requires e2e/routing/docker-compose.yml mock pods"]
async fn mock_pod_streams_sse_chunks_one_by_one() {
    let mock = start_mock_pod().await.expect("mock pod starts");
    let client = reqwest::Client::new();

    let stream_started = Instant::now();
    let mut response = client
        .post(format!("{}/v1/chat/completions", mock.base_url))
        .header("x-mock-stream-token-count", "24")
        .header("x-mock-stream-delay-ms", "80")
        .json(&json!({
            "model": "mock-model",
            "messages": [{"role": "user", "content": "stream slowly"}],
            "stream": true,
            "max_tokens": 24
        }))
        .send()
        .await
        .expect("stream request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream")),
        "streaming response should be text/event-stream"
    );

    let first = response
        .chunk()
        .await
        .expect("first chunk read succeeds")
        .expect("first chunk exists");
    print_stream_chunk(stream_started, &first, 0, "role");
    assert!(
        std::str::from_utf8(&first)
            .expect("first chunk is utf8")
            .contains(r#""role":"assistant""#),
        "first chunk should be the assistant role prelude"
    );

    let before_token = Instant::now();
    let token = response
        .chunk()
        .await
        .expect("token chunk read succeeds")
        .expect("token chunk exists");
    print_stream_chunk(stream_started, &token, 1, "token");
    assert!(
        before_token.elapsed() >= Duration::from_millis(60),
        "token chunk arrived too quickly; mock likely buffered the whole stream"
    );
    assert!(
        std::str::from_utf8(&token)
            .expect("token chunk is utf8")
            .contains(r#""content":" "#),
        "token chunk should contain assistant content"
    );

    let mut body = String::new();
    let mut chunk_index = 2;
    while let Some(chunk) = response
        .chunk()
        .await
        .expect("remaining chunk read succeeds")
    {
        let text = std::str::from_utf8(&chunk).expect("remaining chunk is utf8");
        let kind = if text.contains("[DONE]") {
            "done"
        } else if text.contains(r#""finish_reason":"stop""#) {
            "stop"
        } else {
            "token"
        };
        print_stream_chunk(stream_started, &chunk, chunk_index, kind);
        chunk_index += 1;
        body.push_str(text);
    }
    assert!(
        chunk_index >= 27,
        "expected role + 24 tokens + stop + done chunks"
    );
    assert!(body.contains("data: [DONE]"));
}

fn print_stream_chunk(started: Instant, chunk: &[u8], index: usize, kind: &str) {
    println!(
        "[{:>4}ms chunk #{:<2} {:>5}] {}",
        started.elapsed().as_millis(),
        index,
        kind,
        std::str::from_utf8(chunk)
            .expect("stream chunk is utf8")
            .trim_end()
    );
}

async fn start_mock_pod() -> io::Result<MockPod> {
    let base_url =
        std::env::var("MOCK_POD_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:18101".to_string());
    let mock = MockPod { base_url };
    wait_for_health(&mock.base_url).await?;
    Ok(mock)
}

async fn wait_for_health(base_url: &str) -> io::Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if client
            .get(format!("{base_url}/health"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "mock pod did not become healthy",
    ))
}
