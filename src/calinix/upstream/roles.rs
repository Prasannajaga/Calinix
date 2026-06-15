use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PodRole {
    Single,
    Prefill,
    Decode,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodCapabilities {
    pub single: bool,
    pub prefill: bool,
    pub decode: bool,
}

impl PodCapabilities {
    pub const fn single() -> Self {
        Self {
            single: true,
            prefill: false,
            decode: false,
        }
    }

    pub const fn prefill() -> Self {
        Self {
            single: false,
            prefill: true,
            decode: false,
        }
    }

    pub const fn decode() -> Self {
        Self {
            single: false,
            prefill: false,
            decode: true,
        }
    }

    pub const fn supports(self, role: PodRole) -> bool {
        match role {
            PodRole::Single => self.single,
            PodRole::Prefill => self.prefill,
            PodRole::Decode => self.decode,
        }
    }
}

impl From<PodRole> for PodCapabilities {
    fn from(value: PodRole) -> Self {
        match value {
            PodRole::Single => Self::single(),
            PodRole::Prefill => Self::prefill(),
            PodRole::Decode => Self::decode(),
        }
    }
}
