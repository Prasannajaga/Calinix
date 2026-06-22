use std::fmt;

use http::header::InvalidHeaderValue;
use http::{HeaderMap, HeaderName, HeaderValue};

pub const REQUEST_ID: &str = "x-calinix-request-id";
pub const MODE: &str = "x-calinix-mode";
pub const TARGET_POD_ID: &str = "x-calinix-target-pod-id";
pub const PREFILL_POD_ID: &str = "x-calinix-prefill-pod-id";
pub const DECODE_POD_ID: &str = "x-calinix-decode-pod-id";
pub const CACHE_HIT: &str = "x-calinix-cache-hit";
pub const CACHE_PREFIX_DEPTH: &str = "x-calinix-cache-prefix-depth";
pub const CACHE_NAMESPACE: &str = "x-calinix-cache-namespace";
pub const ROUTE_POLICY: &str = "x-calinix-route-policy";

const OWNED_HEADERS: [&str; 9] = [
    REQUEST_ID,
    MODE,
    TARGET_POD_ID,
    PREFILL_POD_ID,
    DECODE_POD_ID,
    CACHE_HIT,
    CACHE_PREFIX_DEPTH,
    CACHE_NAMESPACE,
    ROUTE_POLICY,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CalinixMode {
    Single,
    Disaggregated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutingHeaderValues {
    pub request_id: String,
    pub mode: CalinixMode,
    pub target_pod_id: Option<String>,
    pub prefill_pod_id: Option<String>,
    pub decode_pod_id: Option<String>,
    pub cache_hit: bool,
    pub cache_prefix_depth: usize,
    pub cache_namespace: Option<String>,
    pub route_policy: String,
}

#[derive(Debug)]
pub enum HeaderError {
    MissingRequired(&'static str),
    InvalidCombination(&'static str),
    InvalidValue(InvalidHeaderValue),
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRequired(header) => write!(f, "missing required routing header {header}"),
            Self::InvalidCombination(message) => {
                write!(f, "invalid routing header combination: {message}")
            }
            Self::InvalidValue(err) => write!(f, "invalid routing header value: {err}"),
        }
    }
}

impl std::error::Error for HeaderError {}

pub fn inject_routing_headers(
    headers: &mut HeaderMap,
    values: &RoutingHeaderValues,
) -> Result<(), HeaderError> {
    validate(values)?;

    for header in OWNED_HEADERS {
        headers.remove(HeaderName::from_static(header));
    }

    insert(headers, REQUEST_ID, &values.request_id)?;
    insert(headers, MODE, values.mode.as_str())?;
    insert(
        headers,
        CACHE_HIT,
        if values.cache_hit { "true" } else { "false" },
    )?;
    insert(
        headers,
        CACHE_PREFIX_DEPTH,
        &values.cache_prefix_depth.to_string(),
    )?;
    insert(headers, ROUTE_POLICY, &values.route_policy)?;

    if let Some(namespace) = &values.cache_namespace {
        insert(headers, CACHE_NAMESPACE, namespace)?;
    }

    match values.mode {
        CalinixMode::Single => {
            insert(
                headers,
                TARGET_POD_ID,
                values.target_pod_id.as_deref().unwrap_or_default(),
            )?;
        }
        CalinixMode::Disaggregated => {
            insert(
                headers,
                PREFILL_POD_ID,
                values.prefill_pod_id.as_deref().unwrap_or_default(),
            )?;
            insert(
                headers,
                DECODE_POD_ID,
                values.decode_pod_id.as_deref().unwrap_or_default(),
            )?;
        }
    }

    Ok(())
}

impl CalinixMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Disaggregated => "disaggregated",
        }
    }
}

fn validate(values: &RoutingHeaderValues) -> Result<(), HeaderError> {
    if values.request_id.is_empty() {
        return Err(HeaderError::MissingRequired(REQUEST_ID));
    }
    if values.route_policy.is_empty() {
        return Err(HeaderError::MissingRequired(ROUTE_POLICY));
    }

    match values.mode {
        CalinixMode::Single => {
            if values
                .target_pod_id
                .as_deref()
                .unwrap_or_default()
                .is_empty()
            {
                return Err(HeaderError::MissingRequired(TARGET_POD_ID));
            }
            if values.prefill_pod_id.is_some() || values.decode_pod_id.is_some() {
                return Err(HeaderError::InvalidCombination(
                    "single mode cannot include prefill or decode pod ids",
                ));
            }
        }
        CalinixMode::Disaggregated => {
            if values
                .prefill_pod_id
                .as_deref()
                .unwrap_or_default()
                .is_empty()
            {
                return Err(HeaderError::MissingRequired(PREFILL_POD_ID));
            }
            if values
                .decode_pod_id
                .as_deref()
                .unwrap_or_default()
                .is_empty()
            {
                return Err(HeaderError::MissingRequired(DECODE_POD_ID));
            }
            if values.target_pod_id.is_some() {
                return Err(HeaderError::InvalidCombination(
                    "disaggregated mode cannot include target pod id",
                ));
            }
        }
    }

    Ok(())
}

fn insert(headers: &mut HeaderMap, name: &'static str, value: &str) -> Result<(), HeaderError> {
    let value = HeaderValue::from_str(value).map_err(HeaderError::InvalidValue)?;
    headers.insert(HeaderName::from_static(name), value);
    Ok(())
}


