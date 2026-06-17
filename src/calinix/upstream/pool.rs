use crate::cache_registry::HostBitmap;
use crate::upstream::pod::{PodEndpoint, PodId, UpstreamId};
use crate::upstream::roles::PodRole;

#[derive(Clone, Debug)]
pub struct UpstreamGroup {
    pub id: UpstreamId,
    pub name: String,
    pub role: PodRole,
    pub pods: Vec<PodId>,
    pub pod_bitmap: HostBitmap,
}

#[derive(Clone, Debug, Default)]
pub struct UpstreamCatalog {
    pub pods: Vec<PodEndpoint>,
    pub groups: Vec<UpstreamGroup>,
}

impl UpstreamCatalog {
    pub fn group_by_name(&self, name: &str) -> Option<&UpstreamGroup> {
        self.groups.iter().find(|group| group.name == name)
    }

    pub fn pod(&self, pod_id: PodId) -> Option<&PodEndpoint> {
        self.pods
            .get(pod_id as usize)
            .filter(|pod| pod.id == pod_id)
    }

    pub fn pods_in_group(&self, group_id: UpstreamId) -> Vec<&PodEndpoint> {
        let Some(group) = self.groups.iter().find(|group| group.id == group_id) else {
            return Vec::new();
        };

        group
            .pods
            .iter()
            .filter_map(|pod_id| self.pod(*pod_id))
            .collect()
    }
}
