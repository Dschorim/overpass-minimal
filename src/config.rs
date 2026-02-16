use serde::Deserialize;
use std::path::PathBuf;
use anyhow::{Context, Result};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub filters: Filters,
    pub storage: Storage,
    pub server: Server,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Filters {
    pub primary_keys: Vec<String>,
    pub attribute_keys: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Storage {
    pub cache_dir: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub host: String,
    pub port: u16,
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {:?}", path))?;
        let config: Config = toml::from_str(&content)
            .with_context(|| "Failed to parse config file (TOML)")?;
        Ok(config)
    }
}
