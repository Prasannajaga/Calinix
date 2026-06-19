use std::fmt;

use http::HeaderMap;
use serde::Deserialize;
use serde::de::IgnoredAny;

const SESSION_KEY_HEADER: &str = "x-calinix-session-key";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenAiRequestKind {
    ChatCompletions,
    Completions,
    Embeddings,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenAiRoutingView {
    pub kind: OpenAiRequestKind,
    pub model: Option<String>,
    pub prompt_text: String,
    pub session_key: Option<String>,
    pub stream: bool,
}

#[derive(Debug)]
pub enum OpenAiParseError {
    InvalidJson(serde_json::Error),
    InvalidShape(&'static str),
}

impl fmt::Display for OpenAiParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson(err) => write!(f, "invalid OpenAI-compatible JSON body: {err}"),
            Self::InvalidShape(message) => {
                write!(f, "invalid OpenAI-compatible request: {message}")
            }
        }
    }
}

impl std::error::Error for OpenAiParseError {}

#[derive(Deserialize, Default)]
struct OpenAiRequestDto<'a> {
    #[serde(borrow)]
    model: Option<&'a str>,
    #[serde(default)]
    stream: bool,
    #[serde(borrow)]
    user: Option<&'a str>,

    // For chat completions
    #[serde(borrow, default)]
    messages: Option<Vec<MessageDto<'a>>>,

    // For completions
    #[serde(borrow)]
    prompt: Option<PromptDto<'a>>,

    // For embeddings
    #[serde(borrow)]
    input: Option<PromptDto<'a>>,
}

#[derive(Deserialize)]
struct MessageDto<'a> {
    #[serde(borrow)]
    content: Option<MessageContentDto<'a>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum MessageContentDto<'a> {
    Text(&'a str),
    Other(IgnoredAny),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PromptDto<'a> {
    Text(&'a str),
    Array(Vec<&'a str>),
    Other(IgnoredAny),
}

pub fn extract_openai_routing_view(
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<OpenAiRoutingView, OpenAiParseError> {
    let kind = request_kind(path);
    if kind == OpenAiRequestKind::Unknown {
        return Ok(OpenAiRoutingView {
            kind,
            model: None,
            prompt_text: String::new(),
            session_key: session_key_header(headers),
            stream: false,
        });
    }

    let dto: OpenAiRequestDto = if body.is_empty() {
        OpenAiRequestDto::default()
    } else {
        serde_json::from_slice(body).map_err(OpenAiParseError::InvalidJson)?
    };

    let model = dto.model.map(ToOwned::to_owned);
    let stream = dto.stream;
    let session_key = session_key_header(headers).or_else(|| {
        dto.user
            .filter(|u| !u.is_empty())
            .map(ToOwned::to_owned)
    });

    let prompt_text = match kind {
        OpenAiRequestKind::ChatCompletions => extract_chat_prompt(dto.messages)?,
        OpenAiRequestKind::Completions => extract_completion_prompt(dto.prompt)?,
        OpenAiRequestKind::Embeddings => extract_embedding_input(dto.input)?,
        OpenAiRequestKind::Unknown => String::new(),
    };

    Ok(OpenAiRoutingView {
        kind,
        model,
        prompt_text,
        session_key,
        stream,
    })
}

fn request_kind(path: &str) -> OpenAiRequestKind {
    let path = path.split('?').next().unwrap_or(path);
    match path {
        "/v1/chat/completions" => OpenAiRequestKind::ChatCompletions,
        "/v1/completions" => OpenAiRequestKind::Completions,
        "/v1/embeddings" => OpenAiRequestKind::Embeddings,
        _ => OpenAiRequestKind::Unknown,
    }
}

fn extract_chat_prompt(messages: Option<Vec<MessageDto>>) -> Result<String, OpenAiParseError> {
    let messages = messages.ok_or(OpenAiParseError::InvalidShape(
        "chat completions require messages[]",
    ))?;

    let mut parts = Vec::new();
    for message in messages {
        match message.content {
            Some(MessageContentDto::Text(text)) => parts.push(text),
            Some(MessageContentDto::Other(_)) => {}
            None => {}
        }
    }

    Ok(parts.join("\n"))
}

fn extract_completion_prompt(prompt: Option<PromptDto>) -> Result<String, OpenAiParseError> {
    let prompt = prompt.ok_or(OpenAiParseError::InvalidShape("completions require prompt"))?;

    match prompt {
        PromptDto::Text(text) => Ok(text.to_owned()),
        PromptDto::Array(items) => Ok(items.join("\n")),
        PromptDto::Other(_) => Err(OpenAiParseError::InvalidShape(
            "prompt must be a string or string array",
        )),
    }
}

fn extract_embedding_input(input: Option<PromptDto>) -> Result<String, OpenAiParseError> {
    let input = input.ok_or(OpenAiParseError::InvalidShape("embeddings require input"))?;

    match input {
        PromptDto::Text(text) => Ok(text.to_owned()),
        PromptDto::Array(items) => Ok(items.join("\n")),
        PromptDto::Other(_) => Err(OpenAiParseError::InvalidShape(
            "embedding input must be a string or string array",
        )),
    }
}

fn session_key_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get(SESSION_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;

    use super::{extract_openai_routing_view, OpenAiRequestKind};

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
}
