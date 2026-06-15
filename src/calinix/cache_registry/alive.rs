use super::host_bitmap::HostBitmap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AliveSet {
    bitmap: HostBitmap,
}

impl AliveSet {
    pub fn new(pod_count: usize) -> Self {
        Self {
            bitmap: HostBitmap::full_for_count(pod_count),
        }
    }

    pub fn bitmap(&self) -> HostBitmap {
        self.bitmap.clone()
    }

    pub fn mark_alive(&mut self, pod_id: usize) {
        self.bitmap.set(pod_id);
    }

    pub fn mark_dead(&mut self, pod_id: usize) {
        self.bitmap.clear(pod_id);
    }

    pub fn contains(&self, pod_id: usize) -> bool {
        self.bitmap.contains(pod_id)
    }
}
