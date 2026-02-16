use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Optimized representation of an OSM element
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Element {
    pub id: u64,
    /// [[lat1, lon1], [lat2, lon2]] stored as f32 for memory efficiency (~1cm precision)
    pub coordinates: [[f32; 2]; 2],
    /// Index into the tag_sets list in CacheData
    pub tag_set_id: u32,
}

use parking_lot::RwLock;

/// A simple string interner to map strings to u32 IDs for memory efficiency
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StringInterner {
    pub map: RwLock<HashMap<String, u32>>,
    pub reverse: RwLock<Vec<String>>,
}

impl Clone for StringInterner {
    fn clone(&self) -> Self {
        Self {
            map: RwLock::new(self.map.read().clone()),
            reverse: RwLock::new(self.reverse.read().clone()),
        }
    }
}

impl StringInterner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_intern(&self, s: &str) -> u32 {
        {
            let map = self.map.read();
            if let Some(&id) = map.get(s) {
                return id;
            }
        }

        let mut map = self.map.write();
        let mut reverse = self.reverse.write();
        
        // Double-check after acquiring write lock
        if let Some(&id) = map.get(s) {
            return id;
        }

        let id = reverse.len() as u32;
        map.insert(s.to_string(), id);
        reverse.push(s.to_string());
        id
    }

    pub fn lookup(&self, id: u32) -> Option<String> {
        self.reverse.read().get(id as usize).cloned()
    }
}

/// The structure saved to the cache file
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheData {
    pub elements: Vec<Element>,
    pub tag_sets: Vec<Vec<(u32, u32)>>,
    pub interner: StringInterner,
    /// Store a hash of the config AND input file metadata to know when to re-preprocess
    pub source_hash: u64,
}
