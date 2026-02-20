use crate::config::Config;
use crate::model::{Element, StringInterner, CacheData, ConcurrentInterner, InternerLike};
use anyhow::{Result, Context};
use std::collections::HashSet;
use rustc_hash::FxHashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tracing::info;
use roaring::RoaringTreemap;
use dashmap::DashMap;
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU32, Ordering};


/// Result of loading/preprocessing — currently always an owned in-memory cache.
pub enum LoadedCache {
    Owned { elements: Vec<Element>, tag_sets: crate::model::FlatTagSets, interner: StringInterner },
}

pub fn load_or_preprocess(config: &Config, pbf_path: &Path) -> Result<LoadedCache> {
    let source_hash = calculate_source_hash(config, pbf_path)?;
    let cache_file_zst = config.storage.cache_dir.join("data.bin.zst");


    // Only use the compressed zst cache (legacy uncompressed cache support removed)
    if cache_file_zst.exists() {
        let file = File::open(&cache_file_zst)?;
        let reader = BufReader::new(file);
        let mut decoder = zstd::stream::read::Decoder::new(reader)?;
        let cache_data_res: Result<CacheData, _> = bincode::deserialize_from(&mut decoder);
        if let Ok(mut cache_data) = cache_data_res {
            if cache_data.source_hash == source_hash {
                info!("Loading data from cache: {:?}", cache_file_zst);

                // optionally clear the runtime-only interner HashMap to save RAM (controlled by config)
                if config.runtime.drop_interner_map {
                    cache_data.interner.map.write().clear();
                }

                return Ok(LoadedCache::Owned { elements: cache_data.elements, tag_sets: cache_data.tag_sets, interner: cache_data.interner });
            }
        }

        info!("Input file or config changed, re-preprocessing...");
    }

    // Write compressed cache to the new zst path
    match preprocess(config, pbf_path, source_hash, &cache_file_zst) {
        Ok((elements, tag_sets, mut interner)) => {


            if config.runtime.drop_interner_map {
                interner.map.write().clear();
            }

            Ok(LoadedCache::Owned { elements, tag_sets, interner })
        }
        Err(e) => Err(e),
    }
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

fn preprocess(config: &Config, pbf_path: &Path, source_hash: u64, cache_file: &Path) -> Result<(Vec<Element>, crate::model::FlatTagSets, StringInterner)> {
    use osmpbf::{ElementReader, Element as OsmElement};
    info!("Starting Optimized PBF preprocessing: {:?}", pbf_path);
    // Pass 1: Identify "Required" Nodes
    info!("Pass 1: Identifying required node IDs...");
    let t1 = std::time::Instant::now();

    let node_count = AtomicUsize::new(0);
    let primary_keys_set: HashSet<&str> = config.filters.primary_keys.iter().map(|s| s.as_str()).collect();

    let reader = ElementReader::from_path(pbf_path)?;
    let required_nodes: RoaringTreemap = reader.par_map_reduce(
        |element| {
            let mut local_required = RoaringTreemap::new();
            let mut local_count = 0;
            
            match element {
                OsmElement::Way(way) => {
                    // avoid allocating a HashMap for every way -- just check the tags iterator
                    if way.tags().any(|(k, _)| primary_keys_set.contains(k)) {
                        for node_id in way.refs() {
                            local_required.insert(node_id as u64);
                        }
                    }
                }
                OsmElement::Node(node) => {
                    local_count += 1;
                    if node.tags().any(|(k, _)| primary_keys_set.contains(k)) {
                        local_required.insert(node.id() as u64);
                    }
                }
                OsmElement::DenseNode(node) => {
                    local_count += 1;
                    if node.tags().any(|(k, _)| primary_keys_set.contains(k)) {
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

    info!("Identified {} unique nodes required for filtered data. (pass1: {:.2?})", required_nodes.len(), t1.elapsed());

    // Pass 2: Collect Coordinates for Required Nodes only
    info!("Pass 2: Collecting coordinates for {} required nodes...", required_nodes.len());
    let t2 = std::time::Instant::now();
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
    info!("Coordinate collection complete. Loaded {} coordinates (expected {}). (pass2: {:.2?})", final_coords_stored, required_nodes.len(), t2.elapsed());
    if final_coords_stored < required_nodes.len() {
        info!("  WARNING: {} required nodes were NOT found in the PBF file.", required_nodes.len() - final_coords_stored);
    }

    // Compact node coordinate store into a FxHashMap to reduce memory overhead and speed reads
    let mut compact_coords: FxHashMap<u64, (f32, f32)> = FxHashMap::default();
    compact_coords.reserve(final_coords_stored as usize);
    for entry in node_coords.iter() {
        compact_coords.insert(*entry.key(), *entry.value());
    }
    // Shadow the previous `node_coords` with a compact, read-only Arc<FxHashMap<..>> used by pass 3
    let node_coords = Arc::new(compact_coords);

    // Pass 3: Extract and Filter
    info!("Pass 3: Final extraction and tag interning...");
    let t3 = std::time::Instant::now();
    // Use a concurrent interner during parallel processing to avoid heavy locking
    let interner = Arc::new(ConcurrentInterner::new());
    let primary_keys_set: HashSet<&str> = config.filters.primary_keys.iter().map(|s| s.as_str()).collect();
    let attribute_keys_set: HashSet<&str> = config.filters.attribute_keys.iter().map(|s| s.as_str()).collect();
    
    // Concurrent tag-set interning: DashMap + atomic counter (avoids a single RwLock<Vec<...>>)
    let tag_set_map: DashMap<Vec<(u32, u32)>, u32> = DashMap::new();
    let tag_set_reverse: DashMap<u32, Vec<(u32, u32)>> = DashMap::new();
    let tag_set_counter = AtomicU32::new(0);

    let get_tag_set_id = |tags: Vec<(u32, u32)>| -> u32 {
        if let Some(id) = tag_set_map.get(&tags) {
            return *id;
        }
        let id = tag_set_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(prev) = tag_set_map.insert(tags.clone(), id) {
            return prev;
        }
        tag_set_reverse.insert(id, tags);
        id
    };

    let reader_pass3 = ElementReader::from_path(pbf_path)?;
    let segments_skipped = AtomicUsize::new(0);
    
    let mut elements = reader_pass3.par_map_reduce(
        |element| {
            let mut local_elements = Vec::new();
            let mut local_skips = 0;

            match element {
                OsmElement::Node(node) => {
                    let mut extracted_tags = Vec::new();
                    let mut has_primary = false;
                    for (k, v) in node.tags() {
                        if primary_keys_set.contains(k) {
                            has_primary = true;
                            let kid = interner.get_or_intern(k);
                            let vid = interner.get_or_intern(v);
                            extracted_tags.push((kid, vid));
                        } else if attribute_keys_set.contains(k) {
                            let kid = interner.get_or_intern(k);
                            let vid = interner.get_or_intern(v);
                            extracted_tags.push((kid, vid));
                        }
                    }

                    if has_primary {
                        // Concurrent-friendly tag-set interning (reduced contention)
                        let tag_set_id = get_tag_set_id(extracted_tags);

                        local_elements.push(Element {
                            id: node.id() as u64,
                            coordinates: [[node.lat() as f32, node.lon() as f32], [node.lat() as f32, node.lon() as f32]],
                            tag_set_id,
                        });
                    }
                }
                OsmElement::DenseNode(node) => {
                    let mut extracted_tags = Vec::new();
                    let mut has_primary = false;
                    for (k, v) in node.tags() {
                        if primary_keys_set.contains(k) {
                            has_primary = true;
                            let kid = interner.get_or_intern(k);
                            let vid = interner.get_or_intern(v);
                            extracted_tags.push((kid, vid));
                        } else if attribute_keys_set.contains(k) {
                            let kid = interner.get_or_intern(k);
                            let vid = interner.get_or_intern(v);
                            extracted_tags.push((kid, vid));
                        }
                    }

                    if has_primary {
                        // Concurrent-friendly tag-set interning (reduced contention)
                        let tag_set_id = get_tag_set_id(extracted_tags);

                        local_elements.push(Element {
                            id: node.id() as u64,
                            coordinates: [[node.lat() as f32, node.lon() as f32], [node.lat() as f32, node.lon() as f32]],
                            tag_set_id,
                        });
                    }
                }
                OsmElement::Way(way) => {
                    let mut extracted_tags = Vec::new();
                    let mut has_primary = false;
                    for (k, v) in way.tags() {
                        if primary_keys_set.contains(k) {
                            has_primary = true;
                            let kid = interner.get_or_intern(k);
                            let vid = interner.get_or_intern(v);
                            extracted_tags.push((kid, vid));
                        } else if attribute_keys_set.contains(k) {
                            let kid = interner.get_or_intern(k);
                            let vid = interner.get_or_intern(v);
                            extracted_tags.push((kid, vid));
                        }
                    }

                    if has_primary {
                        // Concurrent-friendly tag-set interning (reduced contention)
                        let tag_set_id = get_tag_set_id(extracted_tags);

                        let way_nodes: Vec<_> = way.refs().collect();
                        let mut segments_added = 0;
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
                                local_skips += 1;
                            }
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

    info!("Extraction complete. Matched {} total elements. (pass3: {:.2?})", elements.len(), t3.elapsed());
    let final_skips = segments_skipped.load(Ordering::Relaxed);
    if final_skips > 0 {
        info!("  WARNING: {} way segments were skipped due to missing node coordinates.", final_skips);
    }

    // Materialize final tag-sets into a flattened, compact representation
    let tag_set_count = tag_set_counter.load(Ordering::Relaxed) as usize;
    let mut flat_data: Vec<u64> = Vec::new();
    let mut offsets: Vec<u32> = Vec::with_capacity(tag_set_count);
    let mut lengths: Vec<u32> = Vec::with_capacity(tag_set_count);

    for i in 0..(tag_set_count as u32) {
        offsets.push(flat_data.len() as u32);
        if let Some(v) = tag_set_reverse.get(&i) {
            lengths.push(v.len() as u32);
            for (k, val) in v.iter() {
                flat_data.push(((*k as u64) << 32) | (*val as u64));
            }
        } else {
            lengths.push(0);
        }
    }

    let mut final_tag_sets = crate::model::FlatTagSets { data: flat_data, offsets, lengths };
    info!("Total unique tag sets: {}", final_tag_sets.offsets.len());

    // Reduce memory before serializing
    elements.shrink_to_fit();
    final_tag_sets.data.shrink_to_fit();
    final_tag_sets.offsets.shrink_to_fit();
    final_tag_sets.lengths.shrink_to_fit();

    // Convert concurrent interner into the serializable `StringInterner`
    let final_interner = match Arc::try_unwrap(interner) {
        Ok(ci) => ci.into_string_interner(),
        Err(ci_arc) => ci_arc.to_string_interner(),
    };

    // Save to cache (move values into the cache object to avoid cloning large vectors)
    info!("Saving optimized cache to disk (zstd compressed)...");
    let t_cache = std::time::Instant::now();
    let file = File::create(cache_file)?;
    let writer = BufWriter::new(file);
    let mut encoder = zstd::stream::write::Encoder::new(writer, config.storage.zstd_level as i32)?; // configurable zstd level

    let mut cache_data = CacheData {
        elements,
        tag_sets: final_tag_sets,
        interner: final_interner,
        source_hash,
    };

    bincode::serialize_into(&mut encoder, &cache_data)?; // serialize into compressed stream
    encoder.finish()?; // ensure the compression stream is finalized



    info!("Cache saved successfully. (serialize: {:.2?})", t_cache.elapsed());

    // Take the values back out to return them (no extra cloning)
    let elements = std::mem::take(&mut cache_data.elements);
    let tag_sets = std::mem::take(&mut cache_data.tag_sets);
    let interner = std::mem::take(&mut cache_data.interner);

    if config.runtime.drop_interner_map {
        // free the interner HashMap keys (these duplicate the `pool` contents and are not
        // required at runtime because we resolve strings via `pool` + offsets/lengths)
        interner.map.write().clear();
    }

    Ok((elements, tag_sets, interner))
}



