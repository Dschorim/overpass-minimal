use crate::config::Config;
use crate::model::{Element, StringInterner, CacheData};
use anyhow::{Result, Context};
use osmpbf::{ElementReader, Element as OsmElement};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tracing::{info, debug};
use roaring::RoaringBitmap;

pub fn load_or_preprocess(config: &Config, pbf_path: &Path) -> Result<(Vec<Element>, StringInterner)> {
    let source_hash = calculate_source_hash(config, pbf_path)?;
    let cache_file = config.storage.cache_dir.join("data.bin");

    if cache_file.exists() {
        let file = File::open(&cache_file)?;
        let reader = BufReader::new(file);
        let cache_data_res: Result<CacheData, _> = bincode::deserialize_from(reader);
        
        if let Ok(cache_data) = cache_data_res {
            if cache_data.source_hash == source_hash {
                info!("Loading data from cache: {:?}", cache_file);
                return Ok((cache_data.elements, cache_data.interner));
            }
        }
        info!("Input file or config changed, re-preprocessing...");
    }

    preprocess(config, pbf_path, source_hash, &cache_file)
}

fn calculate_source_hash(config: &Config, pbf_path: &Path) -> Result<u64> {
    let mut s = DefaultHasher::new();
    config.filters.primary_keys.hash(&mut s);
    config.filters.attribute_keys.hash(&mut s);
    
    let metadata = std::fs::metadata(pbf_path)
        .with_context(|| format!("Failed to get metadata for PBF: {:?}", pbf_path))?;
    
    if let Ok(abs_path) = pbf_path.canonicalize() {
        abs_path.to_string_lossy().hash(&mut s);
    } else {
        pbf_path.to_string_lossy().hash(&mut s);
    }
    
    metadata.len().hash(&mut s);
    if let Ok(modified) = metadata.modified() {
        modified.hash(&mut s);
    }
    
    Ok(s.finish())
}

fn preprocess(config: &Config, pbf_path: &Path, source_hash: u64, cache_file: &Path) -> Result<(Vec<Element>, StringInterner)> {
    info!("Starting Optimized PBF preprocessing: {:?}", pbf_path);
    let primary_keys: HashSet<_> = config.filters.primary_keys.iter().cloned().collect();

    // Pass 1: Identify "Required" Nodes
    info!("Pass 1: Identifying required node IDs...");
    let mut required_nodes = RoaringBitmap::new();
    let mut node_count = 0;
    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| {
        match element {
            OsmElement::Way(way) => {
                let tags: HashMap<_, _> = way.tags().collect();
                if has_primary_key(&tags, &primary_keys) {
                    for node_id in way.refs() {
                        required_nodes.insert(node_id as u32);
                    }
                }
            }
            OsmElement::Node(node) => {
                node_count += 1;
                let tags: HashMap<_, _> = node.tags().collect();
                if has_primary_key(&tags, &primary_keys) {
                    required_nodes.insert(node.id() as u32);
                }
            }
            OsmElement::DenseNode(node) => {
                node_count += 1;
                let tags: HashMap<_, _> = node.tags().collect();
                if has_primary_key(&tags, &primary_keys) {
                    required_nodes.insert(node.id() as u32);
                }
            }
            _ => {}
        }
        if node_count > 0 && node_count % 50_000_000 == 0 {
            info!("  Scanned {}M nodes for requirements...", node_count / 1_000_000);
            node_count += 1; // prevent multiple logs for same count if DensNode chunk? (actually for_each is per element)
        }
    })?;
    info!("Identified {} unique nodes required for filtered data.", required_nodes.len());

    // Pass 2: Collect Coordinates for Required Nodes only
    info!("Pass 2: Collecting coordinates for {} required nodes...", required_nodes.len());
    let mut node_coords = HashMap::with_capacity(required_nodes.len() as usize);
    let mut node_count_pass2 = 0;
    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| {
        match element {
            OsmElement::Node(node) => {
                node_count_pass2 += 1;
                let id = node.id() as u32;
                if required_nodes.contains(id) {
                    node_coords.insert(id, (node.lat() as f32, node.lon() as f32));
                }
            }
            OsmElement::DenseNode(node) => {
                node_count_pass2 += 1;
                let id = node.id() as u32;
                if required_nodes.contains(id) {
                    node_coords.insert(id, (node.lat() as f32, node.lon() as f32));
                }
            }
            _ => {}
        }
        if node_count_pass2 > 0 && node_count_pass2 % 50_000_000 == 0 {
            info!("  Loading coords: {}M nodes inspected...", node_count_pass2 / 1_000_000);
            node_count_pass2 += 1;
        }
    })?;
    info!("Coordinate collection complete. Loaded {} coordinates.", node_coords.len());

    // Pass 3: Extract and Filter
    info!("Pass 3: Final extraction and tag interning...");
    let mut interner = StringInterner::new();
    let mut elements = Vec::new();
    let mut matched_count = 0;
    
    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| {
        match element {
            OsmElement::Node(node) => {
                let tags: HashMap<_, _> = node.tags().collect();
                if has_primary_key(&tags, &primary_keys) {
                    let mut extracted_tags = Vec::new();
                    extract_tags(config, &tags, &mut interner, &mut extracted_tags);
                    elements.push(Element {
                        id: node.id() as u64,
                        coordinate: [node.lat(), node.lon()],
                        tags: extracted_tags,
                    });
                    matched_count += 1;
                }
            }
            OsmElement::DenseNode(node) => {
                let tags: HashMap<_, _> = node.tags().collect();
                if has_primary_key(&tags, &primary_keys) {
                    let mut extracted_tags = Vec::new();
                    extract_tags(config, &tags, &mut interner, &mut extracted_tags);
                    elements.push(Element {
                        id: node.id() as u64,
                        coordinate: [node.lat(), node.lon()],
                        tags: extracted_tags,
                    });
                    matched_count += 1;
                }
            }
            OsmElement::Way(way) => {
                let tags: HashMap<_, _> = way.tags().collect();
                if has_primary_key(&tags, &primary_keys) {
                    let mut lats = 0.0;
                    let mut lons = 0.0;
                    let mut count = 0;
                    for node_id in way.refs() {
                        if let Some(&(lat, lon)) = node_coords.get(&(node_id as u32)) {
                            lats += lat as f64;
                            lons += lon as f64;
                            count += 1;
                        }
                    }
                    
                    if count > 0 {
                        let mut extracted_tags = Vec::new();
                        extract_tags(config, &tags, &mut interner, &mut extracted_tags);
                        elements.push(Element {
                            id: way.id() as u64,
                            coordinate: [lats / count as f64, lons / count as f64],
                            tags: extracted_tags,
                        });
                        matched_count += 1;
                    }
                }
            }
            _ => {}
        }
    })?;
    info!("Extraction complete. Matched {} total elements.", matched_count);

    // Save to cache
    info!("Saving optimized cache to disk...");
    let file = File::create(cache_file)?;
    let writer = BufWriter::new(file);
    let cache_data = CacheData {
        elements: elements.clone(),
        interner: interner.clone(),
        source_hash,
    };
    bincode::serialize_into(writer, &cache_data)?;
    info!("Cache saved successfully.");

    Ok((elements, interner))
}

fn extract_tags(config: &Config, tags: &HashMap<&str, &str>, interner: &mut StringInterner, out: &mut Vec<(u32, u32)>) {
    for key in &config.filters.primary_keys {
        if let Some(val) = tags.get(key.as_str()) {
            let kid = interner.get_or_intern(key);
            let vid = interner.get_or_intern(val);
            out.push((kid, vid));
        }
    }
    for key in &config.filters.attribute_keys {
        if let Some(val) = tags.get(key.as_str()) {
            let kid = interner.get_or_intern(key);
            let vid = interner.get_or_intern(val);
            out.push((kid, vid));
        }
    }
}

fn has_primary_key(tags: &HashMap<&str, &str>, primary_keys: &HashSet<String>) -> bool {
    for k in primary_keys {
        if tags.contains_key(k.as_str()) {
            return true;
        }
    }
    false
}
