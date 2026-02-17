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
use roaring::RoaringTreemap;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn load_or_preprocess(config: &Config, pbf_path: &Path) -> Result<(Vec<Element>, Vec<Vec<(u32, u32)>>, StringInterner)> {
    let source_hash = calculate_source_hash(config, pbf_path)?;
    let cache_file = config.storage.cache_dir.join("data.bin");

    if cache_file.exists() {
        let file = File::open(&cache_file)?;
        let reader = BufReader::new(file);
        let cache_data_res: Result<CacheData, _> = bincode::deserialize_from(reader);
        
        if let Ok(cache_data) = cache_data_res {
            if cache_data.source_hash == source_hash {
                info!("Loading data from cache: {:?}", cache_file);
                return Ok((cache_data.elements, cache_data.tag_sets, cache_data.interner));
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

fn preprocess(config: &Config, pbf_path: &Path, source_hash: u64, cache_file: &Path) -> Result<(Vec<Element>, Vec<Vec<(u32, u32)>>, StringInterner)> {
    use osmpbf::{ElementReader, Element as OsmElement};
    info!("Starting Optimized PBF preprocessing: {:?}", pbf_path);
    // Pass 1: Identify "Required" Nodes
    info!("Pass 1: Identifying required node IDs...");
    
    let node_count = AtomicUsize::new(0);
    let primary_keys_set: HashSet<&str> = config.filters.primary_keys.iter().map(|s| s.as_str()).collect();

    let reader = ElementReader::from_path(pbf_path)?;
    let required_nodes: RoaringTreemap = reader.par_map_reduce(
        |element| {
            let mut local_required = RoaringTreemap::new();
            let mut local_count = 0;
            
            match element {
                OsmElement::Way(way) => {
                    let tags: HashMap<_, _> = way.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        for node_id in way.refs() {
                            local_required.insert(node_id as u64);
                        }
                    }
                }
                OsmElement::Node(node) => {
                    local_count += 1;
                    let tags: HashMap<_, _> = node.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        local_required.insert(node.id() as u64);
                    }
                }
                OsmElement::DenseNode(node) => {
                    local_count += 1;
                    let tags: HashMap<_, _> = node.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        local_required.insert(node.id() as u64);
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
        || RoaringTreemap::new(),
        |mut a, b| {
            a |= b;
            a
        },
    ).map_err(|e| anyhow::anyhow!("PBF Error: {:?}", e))?;

    info!("Identified {} unique nodes required for filtered data.", required_nodes.len());

    // Pass 2: Collect Coordinates for Required Nodes only
    info!("Pass 2: Collecting coordinates for {} required nodes...", required_nodes.len());
    let node_coords = Arc::new(dashmap::DashMap::with_capacity(required_nodes.len() as usize));
    let node_count_pass2 = AtomicUsize::new(0);
    let coords_stored = AtomicUsize::new(0);
    
    let reader_pass2 = ElementReader::from_path(pbf_path)?;
    reader_pass2.par_map_reduce(
        |element| {
            let mut local_count = 0;
            match element {
                OsmElement::Node(node) => {
                    local_count += 1;
                    let id = node.id() as u64;
                    if required_nodes.contains(id) {
                        node_coords.insert(id, (node.lat() as f32, node.lon() as f32));
                        coords_stored.fetch_add(1, Ordering::Relaxed);
                    }
                }
                OsmElement::DenseNode(node) => {
                    local_count += 1;
                    let id = node.id() as u64;
                    if required_nodes.contains(id) {
                        node_coords.insert(id, (node.lat() as f32, node.lon() as f32));
                        coords_stored.fetch_add(1, Ordering::Relaxed);
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
    
    let final_coords_stored = coords_stored.load(Ordering::Relaxed) as u64;
    info!("Coordinate collection complete. Loaded {} coordinates (expected {}).", final_coords_stored, required_nodes.len());
    if final_coords_stored < required_nodes.len() {
        info!("  WARNING: {} required nodes were NOT found in the PBF file.", required_nodes.len() - final_coords_stored);
    }

    // Pass 3: Extract and Filter
    info!("Pass 3: Final extraction and tag interning...");
    let interner = Arc::new(StringInterner::new());
    let primary_keys_set: HashSet<&str> = config.filters.primary_keys.iter().map(|s| s.as_str()).collect();
    
    // Tag set interning to save massive RAM for way segments
    let tag_set_interner = DashMap::new();
    let tag_sets_reverse = RwLock::new(Vec::new());

    let reader_pass3 = ElementReader::from_path(pbf_path)?;
    let segments_skipped = AtomicUsize::new(0);
    
    let elements = reader_pass3.par_map_reduce(
        |element| {
            let mut local_elements = Vec::new();
            let mut local_skips = 0;

            match element {
                OsmElement::Node(node) => {
                    let tags: HashMap<_, _> = node.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        let mut extracted_tags = Vec::new();
                        extract_tags(config, &tags, &interner, &mut extracted_tags);
                        
                        let tag_set_id = if let Some(id) = tag_set_interner.get(&extracted_tags) {
                            *id
                        } else {
                            let mut reverse = tag_sets_reverse.write();
                            if let Some(id) = tag_set_interner.get(&extracted_tags) {
                                *id
                            } else {
                                let id = reverse.len() as u32;
                                tag_set_interner.insert(extracted_tags.clone(), id);
                                reverse.push(extracted_tags);
                                id
                            }
                        };

                        local_elements.push(Element {
                            id: node.id() as u64,
                            coordinates: [[node.lat() as f32, node.lon() as f32], [node.lat() as f32, node.lon() as f32]],
                            tag_set_id,
                        });
                    }
                }
                OsmElement::DenseNode(node) => {
                    let tags: HashMap<_, _> = node.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        let mut extracted_tags = Vec::new();
                        extract_tags(config, &tags, &interner, &mut extracted_tags);
                        
                        let tag_set_id = if let Some(id) = tag_set_interner.get(&extracted_tags) {
                            *id
                        } else {
                            let mut reverse = tag_sets_reverse.write();
                            if let Some(id) = tag_set_interner.get(&extracted_tags) {
                                *id
                            } else {
                                let id = reverse.len() as u32;
                                tag_set_interner.insert(extracted_tags.clone(), id);
                                reverse.push(extracted_tags);
                                id
                            }
                        };

                        local_elements.push(Element {
                            id: node.id() as u64,
                            coordinates: [[node.lat() as f32, node.lon() as f32], [node.lat() as f32, node.lon() as f32]],
                            tag_set_id,
                        });
                    }
                }
                OsmElement::Way(way) => {
                    let tags: HashMap<_, _> = way.tags().collect();
                    if local_has_primary_key(&tags, &primary_keys_set) {
                        let mut extracted_tags = Vec::new();
                        extract_tags(config, &tags, &interner, &mut extracted_tags);
                        
                        let tag_set_id = if let Some(id) = tag_set_interner.get(&extracted_tags) {
                            *id
                        } else {
                            let mut reverse = tag_sets_reverse.write();
                            if let Some(id) = tag_set_interner.get(&extracted_tags) {
                                *id
                            } else {
                                let id = reverse.len() as u32;
                                tag_set_interner.insert(extracted_tags.clone(), id);
                                reverse.push(extracted_tags);
                                id
                            }
                        };

                        let way_nodes: Vec<_> = way.refs().collect();
                        let mut segments_added = 0;
                        if way.id() == 276301579 {
                            info!("Processing Way 276301579: {} nodes found in PBF.", way_nodes.len());
                        }
                        for i in 0..way_nodes.len().saturating_sub(1) {
                            if let (Some(c1), Some(c2)) = 
                                (node_coords.get(&(way_nodes[i] as u64)), node_coords.get(&(way_nodes[i+1] as u64))) 
                            {
                                let (lat1, lon1) = *c1;
                                let (lat2, lon2) = *c2;
                                local_elements.push(Element {
                                    id: way.id() as u64,
                                    coordinates: [[lat1, lon1], [lat2, lon2]],
                                    tag_set_id,
                                });
                                segments_added += 1;
                            } else {
                                if way.id() == 276301579 {
                                    // Log exactly which node is missing for this way
                                    let node_idx = i; // either i or i+1
                                    let missing_node_id = if node_idx == i { way_nodes[i] } else { way_nodes[i+1] };
                                    info!("  Way 276301579: Missing coordinate for node ID {} (index {}).", missing_node_id, node_idx);
                                }
                                local_skips += 1;
                            }
                        }
                        if way.id() == 276301579 {
                            info!("Way 276301579: Added {} segments, skipped {}.", segments_added, way_nodes.len().saturating_sub(1) - segments_added);
                        }
                        if segments_added == 0 && !way_nodes.is_empty() {
                            // This is a warning sign - we have a tagged way but couldn't find its nodes
                            // Often happens if the PBF is an extract that doesn't include "uninteresting" nodes
                            // but those nodes are still needed for way geometry.
                            // We don't log every one to avoid spam, but we should know if it happens.
                        }
                    }
                }
                _ => {}
            }
            if local_skips > 0 {
                segments_skipped.fetch_add(local_skips, Ordering::Relaxed);
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
    let final_skips = segments_skipped.load(Ordering::Relaxed);
    if final_skips > 0 {
        info!("  WARNING: {} way segments were skipped due to missing node coordinates.", final_skips);
    }
    let final_tag_sets = tag_sets_reverse.into_inner();
    info!("Total unique tag sets: {}", final_tag_sets.len());

    // Save to cache
    info!("Saving optimized cache to disk...");
    let file = File::create(cache_file)?;
    let writer = BufWriter::new(file);
    let cache_data = CacheData {
        elements: elements.clone(),
        tag_sets: final_tag_sets.clone(),
        interner: (*interner).clone(),
        source_hash,
    };
    bincode::serialize_into(writer, &cache_data)?;
    info!("Cache saved successfully.");
    
    let final_interner = (*interner).clone();
    Ok((elements, final_tag_sets, final_interner))
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

