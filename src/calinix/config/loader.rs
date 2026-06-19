use std::fs;
use std::path::Path;

use super::schema::CalinixConfig;
use super::validation::validate_config;

pub fn load_config(path: impl AsRef<Path>) -> Result<CalinixConfig, String> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read config '{}': {err}", path.display()))?;
    let config = serde_yaml::from_str::<CalinixConfig>(&raw)
        .map_err(|err| format!("failed to parse config '{}': {err}", path.display()))?;
    validate_config(&config)?;
    Ok(config)
}
