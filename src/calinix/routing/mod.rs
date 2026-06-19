pub mod context;
pub mod filter;
pub mod pick;
pub mod pipeline;
pub mod plan;
pub mod prepare;
pub mod profiles;
pub mod score;

use std::fmt;

use crate::protocol::openai::OpenAiParseError;

#[derive(Debug)]
pub enum RoutingError {
    InvalidMode(String),
    Parse(OpenAiParseError),
    NoCandidates,
    MissingPod(u16),
}

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMode(mode) => write!(f, "invalid routing mode: {mode}"),
            Self::Parse(err) => write!(f, "{err}"),
            Self::NoCandidates => write!(f, "no routing candidates available"),
            Self::MissingPod(pod_id) => write!(f, "selected pod {pod_id} is not in the catalog"),
        }
    }
}

impl std::error::Error for RoutingError {}

impl From<OpenAiParseError> for RoutingError {
    fn from(value: OpenAiParseError) -> Self {
        Self::Parse(value)
    }
}
