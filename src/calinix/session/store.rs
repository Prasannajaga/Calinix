use std::collections::HashMap;
use std::sync::RwLock;

use crate::upstream::PodId;

#[derive(Debug, Default)]
pub struct StickyStore {
    by_session: RwLock<HashMap<String, PodId>>,
}

impl StickyStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn previous_pod(&self, session_key: &str) -> Option<PodId> {
        self.by_session
            .read()
            .ok()
            .and_then(|sessions| sessions.get(session_key).copied())
    }

    pub fn remember(&self, session_key: impl Into<String>, pod_id: PodId) {
        if let Ok(mut sessions) = self.by_session.write() {
            sessions.insert(session_key.into(), pod_id);
        }
    }
}
