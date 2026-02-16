mod config;
mod model;
mod preprocessor;
mod api;

use clap::Parser;
use std::path::PathBuf;
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

    println!("Loading data...");
    let (elements, tag_sets, interner) = preprocessor::load_or_preprocess(&config, &args.input)?;
    println!("Loaded {} elements.", elements.len());

    // Start the API server
    api::start_server(config, elements, tag_sets, interner, start_time).await?;

    Ok(())
}
