use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Optimized representation of an OSM element
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Element {
    pub id: u64,
    /// [lat, lon]
    pub coordinate: [f64; 2],
    /// List of (KeyID, ValueID) pairs
    pub tags: Vec<(u32, u32)>,
}

/// A simple string interner to map strings to u32 IDs for memory efficiency
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct StringInterner {
    pub map: HashMap<String, u32>,
    pub reverse: Vec<String>,
}

impl StringInterner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.map.get(s) {
            id
        } else {
            let id = self.reverse.len() as u32;
            self.map.insert(s.to_string(), id);
            self.reverse.push(s.to_string());
            id
        }
    }

    pub fn lookup(&self, id: u32) -> Option<&str> {
        self.reverse.get(id as usize).map(|s| s.as_str())
    }
}

/// The structure saved to the cache file
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheData {
    pub elements: Vec<Element>,
    pub interner: StringInterner,
    /// Store a hash of the config AND input file metadata to know when to re-preprocess
    pub source_hash: u64,
}
