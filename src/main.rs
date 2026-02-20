mod config;
mod model;
mod preprocessor;
mod api;

use clap::Parser;
use std::path::PathBuf;
use std::fs::File;
use anyhow::{Result, Context};
use tracing::info;
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the TOML configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Path to the OSM PBF file
    #[arg(short, long)]
    input: PathBuf,

    /// Path to the cache directory (overrides config)
    #[arg(short, long)]
    cache: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    
    tracing_subscriber::fmt::init();
    
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(16);
    
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .ok();

    info!("Application starting (using {} threads)", num_threads);

    let args = Args::parse();
    
    let mut config = config::Config::from_file(&args.config)?;
    if let Some(cache_override) = args.cache {
        config.storage.cache_dir = cache_override;
    }

    // Ensure cache directory exists
    if !config.storage.cache_dir.exists() {
        std::fs::create_dir_all(&config.storage.cache_dir)
            .with_context(|| format!("Failed to create cache directory: {:?}", config.storage.cache_dir))?;
    }

    info!("Loading data...");

    // Optional CPU sampling profiler — configurable via `config.toml` ([profiling])
    // Environment variables can still override: OVERPASS_PROFILE, OVERPASS_PROFILE_OUT, OVERPASS_PROFILE_FREQ
    let prof_env_on = std::env::var("OVERPASS_PROFILE").is_ok();
    let profile_enabled = prof_env_on || config.profiling.enabled;
    let profile_freq: u64 = std::env::var("OVERPASS_PROFILE_FREQ").ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(config.profiling.frequency);
    // pprof expects an `i32` frequency; clamp safely into range
    let profile_freq_i32: i32 = if profile_freq == 0 {
        100
    } else {
        let clamped = std::cmp::min(profile_freq, i32::MAX as u64);
        clamped as i32
    };

    let maybe_prof = if profile_enabled {
        match pprof::ProfilerGuard::new(profile_freq_i32) {
            Ok(g) => Some(g),
            Err(e) => {
                tracing::warn!("failed to start profiler: {:?}", e);
                None
            }
        }
    } else { None };

    let cache = preprocessor::load_or_preprocess(&config, &args.input)?;

    // If profiling was enabled, write a flamegraph of the preprocessing stage
    if let Some(guard) = maybe_prof {
        if let Ok(report) = guard.report().build() {
            let out_str = std::env::var("OVERPASS_PROFILE_OUT").unwrap_or_else(|_| config.profiling.out.clone());
            let mut out_path = std::path::PathBuf::from(out_str);
            if out_path.is_relative() {
                out_path = config.storage.cache_dir.join(out_path);
            }
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            if let Ok(mut f) = File::create(&out_path) {
                let _ = report.flamegraph(&mut f);
                info!("Flamegraph written to {}", out_path.display());
            } else {
                tracing::warn!("failed to create flamegraph output file: {}", out_path.display());
            }
        } else {
            tracing::warn!("failed to build profiler report");
        }
    }

    if let preprocessor::LoadedCache::Owned { elements, .. } = &cache {
        info!("Loaded {} elements.", elements.len());
    }

    // Log current RSS (Linux `/proc/self/status` VmRSS) to make it easy to verify memory usage
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        let mb = kb / 1024;
                        info!("Resident memory after load: {} MB", mb);
                    }
                }
                break;
            }
        }
    }
    // Start the API server
    api::start_server(config, cache, start_time).await?;

    Ok(())
}
