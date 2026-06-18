use crate::cache_registry::HostBitmap;
use crate::upstream::{LoadState, PodRole, UpstreamCatalog};

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
        loads: &LoadState,
        role: RequiredRole,
        route_policy: &RoutePolicy,
        alive: HostBitmap,
    ) -> HostBitmap {
        eligible_candidates(
            upstreams,
            loads,
            alive,
            route_policy.upstream_for_role(role),
            role,
        )
    }
}

pub fn eligible_candidates(
    upstreams: &UpstreamCatalog,
    loads: &LoadState,
    alive: HostBitmap,
    group_name: &str,
    required_role: RequiredRole,
) -> HostBitmap {
    let Some(group) = upstreams.group_by_name(group_name) else {
        return HostBitmap::empty();
    };
    if group.role != required_role.into() {
        return HostBitmap::empty();
    }

    let role = required_role.into();
    let mut eligible = HostBitmap::empty();
    group.pod_bitmap.and(&alive).for_each_set_bit(|pod_id| {
        let Ok(pod_id) = u16::try_from(pod_id) else {
            return;
        };
        let Some(pod) = upstreams.pod(pod_id) else {
            return;
        };
        if pod.healthy && !pod.draining && pod.capabilities.supports(role) && loads.can_accept(pod)
        {
            eligible.set(pod_id as usize);
        }
    });
    eligible
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
