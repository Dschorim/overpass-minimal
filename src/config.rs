use serde::Deserialize;
use std::path::PathBuf;
use anyhow::{Context, Result};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub filters: Filters,
    pub storage: Storage,
    pub server: Server,
    pub profiling: Profiling,
    #[serde(default)]
    pub runtime: Runtime,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Runtime {
    /// If true, drop the internal `HashMap<String,u32>` after loading the cache to save RAM.
    /// The `pool` + `offsets`/`lengths` are kept so `lookup(id)` still works.
    #[serde(default = "default_drop_interner_map")]
    pub drop_interner_map: bool,
}

fn default_drop_interner_map() -> bool { true }

#[derive(Debug, Deserialize, Clone)]
pub struct Filters {
    pub primary_keys: Vec<String>,
    pub attribute_keys: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Storage {
    pub cache_dir: PathBuf,
    /// zstd compression level used when writing the cache (0-22). Default = 3 (fast).
    #[serde(default = "default_zstd_level")]
    pub zstd_level: u32,
}

fn default_zstd_level() -> u32 { 3 }

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Profiling {
    /// Enable/disable CPU sampling profiler (writes a flamegraph when enabled)
    #[serde(default = "default_profiling_enabled")]
    pub enabled: bool,
    /// Output file path for the flamegraph
    #[serde(default = "default_profiling_out")]
    pub out: String,
    /// Sampling frequency for the profiler (samples per second)
    #[serde(default = "default_profiling_freq")]
    pub frequency: u64,
}

fn default_profiling_enabled() -> bool { false }
fn default_profiling_out() -> String { "profile.svg".into() }
fn default_profiling_freq() -> u64 { 100 }

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {:?}", path))?;
        let config: Config = toml::from_str(&content)
            .with_context(|| "Failed to parse config file (TOML)")?;
        Ok(config)
    }
}
