use crate::cache_registry::HostBitmap;
use crate::upstream::{PodRole, UpstreamCatalog};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequiredRole {
    Single,
    Prefill,
    Decode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutePolicy {
    pub name: String,
    pub single_upstream: String,
    pub prefill_upstream: String,
    pub decode_upstream: String,
    pub require_healthy: bool,
}

impl RoutePolicy {
    pub fn upstream_for_role(&self, role: RequiredRole) -> &str {
        match role {
            RequiredRole::Single => &self.single_upstream,
            RequiredRole::Prefill => &self.prefill_upstream,
            RequiredRole::Decode => &self.decode_upstream,
        }
    }
}

pub struct FilterStage;

impl FilterStage {
    pub fn candidates_for_role(
        &self,
        upstreams: &UpstreamCatalog,
        role: RequiredRole,
        route_policy: &RoutePolicy,
        alive: HostBitmap,
    ) -> HostBitmap {
        let Some(group) = upstreams.group_by_name(route_policy.upstream_for_role(role)) else {
            return HostBitmap::empty();
        };
        if group.role != role.into() {
            return HostBitmap::empty();
        }

        group.pod_bitmap.and(&alive)
    }
}

impl From<RequiredRole> for PodRole {
    fn from(value: RequiredRole) -> Self {
        match value {
            RequiredRole::Single => Self::Single,
            RequiredRole::Prefill => Self::Prefill,
            RequiredRole::Decode => Self::Decode,
        }
    }
}
