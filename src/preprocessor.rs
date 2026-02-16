use crate::config::Config;
use crate::model::{Element, StringInterner, CacheData};
use anyhow::{Result, Context};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tracing::info;
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
    use osmpbf::{ElementReader, Element as OsmElement};
    info!("Starting Optimized PBF preprocessing: {:?}", pbf_path);
    // Pass 1: Identify "Required" Nodes
    info!("Pass 1: Identifying required node IDs...");
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    let node_count = AtomicUsize::new(0);
    let primary_keys_set: HashSet<&str> = config.filters.primary_keys.iter().map(|s| s.as_str()).collect();

    let reader = ElementReader::from_path(pbf_path)?;
    let required_nodes: RoaringBitmap = reader.par_map_reduce(
        |element| {
            let mut local_required = RoaringBitmap::new();
            let mut local_count = 0;
            
            match element {
                OsmElement::Way(way) => {
                    let tags: HashMap<_, _> = way.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        for node_id in way.refs() {
                            local_required.insert(node_id as u32);
                        }
                    }
                }
                OsmElement::Node(node) => {
                    local_count += 1;
                    let tags: HashMap<_, _> = node.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        local_required.insert(node.id() as u32);
                    }
                }
                OsmElement::DenseNode(node) => {
                    local_count += 1;
                    let tags: HashMap<_, _> = node.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        local_required.insert(node.id() as u32);
                    }
                }
                _ => {}
            }
            
            if local_count > 0 {
                let total = node_count.fetch_add(local_count, Ordering::Relaxed);
                if (total + local_count) / 50_000_000 > total / 50_000_000 {
                    info!("  Scanned {}M nodes for requirements...", (total + local_count) / 1_000_000);
                }
            }
            
            local_required
        },
        || RoaringBitmap::new(),
        |mut a, b| {
            a |= b;
            a
        },
    ).map_err(|e| anyhow::anyhow!("PBF Error: {:?}", e))?;

    info!("Identified {} unique nodes required for filtered data.", required_nodes.len());

    // Pass 2: Collect Coordinates for Required Nodes only
    info!("Pass 2: Collecting coordinates for {} required nodes...", required_nodes.len());
    let node_coords = dashmap::DashMap::with_capacity(required_nodes.len() as usize);
    let node_count_pass2 = AtomicUsize::new(0);
    
    let reader_pass2 = ElementReader::from_path(pbf_path)?;
    reader_pass2.par_map_reduce(
        |element| {
            let mut local_count = 0;
            match element {
                OsmElement::Node(node) => {
                    local_count += 1;
                    let id = node.id() as u32;
                    if required_nodes.contains(id) {
                        node_coords.insert(id, (node.lat() as f32, node.lon() as f32));
                    }
                }
                OsmElement::DenseNode(node) => {
                    local_count += 1;
                    let id = node.id() as u32;
                    if required_nodes.contains(id) {
                        node_coords.insert(id, (node.lat() as f32, node.lon() as f32));
                    }
                }
                _ => {}
            }
            
            if local_count > 0 {
                let total = node_count_pass2.fetch_add(local_count, Ordering::Relaxed);
                if (total + local_count) / 50_000_000 > total / 50_000_000 {
                    info!("  Loading coords: {}M nodes inspected...", (total + local_count) / 1_000_000);
                }
            }
        },
        || (),
        |_, _| (),
    ).map_err(|e| anyhow::anyhow!("PBF Error: {:?}", e))?;

    info!("Coordinate collection complete. Loaded {} coordinates.", node_coords.len());

    // Pass 3: Extract and Filter
    info!("Pass 3: Final extraction and tag interning...");
    let interner = StringInterner::new();
    let primary_keys_set: HashSet<&str> = config.filters.primary_keys.iter().map(|s| s.as_str()).collect();
    
    let reader_pass3 = ElementReader::from_path(pbf_path)?;
    let elements = reader_pass3.par_map_reduce(
        |element| {
            let mut local_elements = Vec::new();

            match element {
                OsmElement::Node(node) => {
                    let tags: HashMap<_, _> = node.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        let mut extracted_tags = Vec::new();
                        extract_tags(config, &tags, &interner, &mut extracted_tags);
                        local_elements.push(Element {
                            id: node.id() as u64,
                            coordinates: [[node.lat(), node.lon()], [node.lat(), node.lon()]],
                            tags: extracted_tags,
                        });
                    }
                }
                OsmElement::DenseNode(node) => {
                    let tags: HashMap<_, _> = node.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        let mut extracted_tags = Vec::new();
                        extract_tags(config, &tags, &interner, &mut extracted_tags);
                        local_elements.push(Element {
                            id: node.id() as u64,
                            coordinates: [[node.lat(), node.lon()], [node.lat(), node.lon()]],
                            tags: extracted_tags,
                        });
                    }
                }
                OsmElement::Way(way) => {
                    let tags: HashMap<_, _> = way.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        let way_nodes: Vec<_> = way.refs().collect();
                        for i in 0..way_nodes.len().saturating_sub(1) {
                            if let (Some(c1), Some(c2)) = 
                                (node_coords.get(&(way_nodes[i] as u32)), node_coords.get(&(way_nodes[i+1] as u32))) 
                            {
                                let (lat1, lon1) = *c1;
                                let (lat2, lon2) = *c2;
                                let mut extracted_tags = Vec::new();
                                extract_tags(config, &tags, &interner, &mut extracted_tags);
                                local_elements.push(Element {
                                    id: way.id() as u64,
                                    coordinates: [[lat1 as f64, lon1 as f64], [lat2 as f64, lon2 as f64]],
                                    tags: extracted_tags,
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
            local_elements
        },
        || Vec::new(),
        |mut a: Vec<Element>, mut b| {
            a.append(&mut b);
            a
        },
    ).map_err(|e| anyhow::anyhow!("PBF Error: {:?}", e))?;

    info!("Extraction complete. Matched {} total elements.", elements.len());

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

fn local_has_primary_key(tags: &HashMap<&str, &str>, primary_keys: &HashSet<&str>) -> bool {
    for k in primary_keys {
        if tags.contains_key(k) {
            return true;
        }
    }
    false
}

fn extract_tags(config: &Config, tags: &HashMap<&str, &str>, interner: &StringInterner, out: &mut Vec<(u32, u32)>) {
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

