use std::io;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use serde_json::json;

struct MockPod {
    base_url: String,
    child: Child,
}

impl Drop for MockPod {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
#[ignore = "requires local Python deps: fastapi and uvicorn"]
async fn mock_pod_streams_sse_chunks_one_by_one() {
    let mock = start_mock_pod().await.expect("mock pod starts");
    let client = reqwest::Client::new();

    let mut response = client
        .post(format!("{}/v1/chat/completions", mock.base_url))
        .json(&json!({
            "model": "mock-model",
            "messages": [{"role": "user", "content": "stream slowly"}],
            "stream": true,
            "max_tokens": 3
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
    assert!(
        before_token.elapsed() >= Duration::from_millis(40),
        "token chunk arrived too quickly; mock likely buffered the whole stream"
    );
    assert!(
        std::str::from_utf8(&token)
            .expect("token chunk is utf8")
            .contains(r#""content":" "#),
        "token chunk should contain assistant content"
    );

    let mut body = String::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .expect("remaining chunk read succeeds")
    {
        body.push_str(std::str::from_utf8(&chunk).expect("remaining chunk is utf8"));
    }
    assert!(body.contains("data: [DONE]"));
}

async fn start_mock_pod() -> io::Result<MockPod> {
    let port = free_port()?;
    let base_url = format!("http://127.0.0.1:{port}");
    let child = Command::new("python3")
        .args([
            "-m",
            "uvicorn",
            "mock_pod:app",
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .current_dir("e2e/routing")
        .env("SERVICE_NAME", "stream-test")
        .env("MOCK_STREAM_TOKEN_COUNT", "3")
        .env("MOCK_STREAM_DELAY_MS", "50")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let mock = MockPod { base_url, child };
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

fn free_port() -> io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}
