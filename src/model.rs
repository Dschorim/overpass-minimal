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
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, Ordering};

/// A memory-efficient string interner using a single contiguous string pool for reverse lookups.
///
/// - `map` is kept as `HashMap<String,u32>` for fast lookup during insertion.
/// - `pool` stores all strings concatenated (reduces per-String allocation overhead).
/// - `offsets` / `lengths` map id -> (start, len) inside `pool`.
/// Pool storage for interned strings: always an owned `String` (mmapping removed).
#[derive(Debug, Clone)]
pub struct Pool(String);

impl Default for Pool {
    fn default() -> Self { Pool(String::new()) }
}


#[derive(Debug, Default)]
pub struct StringInterner {
    pub map: RwLock<HashMap<String, u32>>,
    pub pool: RwLock<Pool>,
    pub offsets: RwLock<Vec<u32>>,
    pub lengths: RwLock<Vec<u32>>,
}

impl Clone for StringInterner {
    fn clone(&self) -> Self {
        Self {
            map: RwLock::new(self.map.read().clone()),
            pool: RwLock::new(self.pool.read().clone()),
            offsets: RwLock::new(self.offsets.read().clone()),
            lengths: RwLock::new(self.lengths.read().clone()),
        }
    }
}

// Serializable helper used for serde (keeps disk format unchanged)
#[derive(Serialize, Deserialize)]
struct SerializableStringInterner {
    map: HashMap<String, u32>,
    pool: String,
    offsets: Vec<u32>,
    lengths: Vec<u32>,
}

impl serde::Serialize for StringInterner {
    fn serialize<S>(&self, serializer: S) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        let map = self.map.read().clone();
        let offsets = self.offsets.read().clone();
        let lengths = self.lengths.read().clone();
        let pool_str = (&*self.pool.read()).0.clone();

        let ssi = SerializableStringInterner { map, pool: pool_str, offsets, lengths };
        ssi.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for StringInterner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ssi = SerializableStringInterner::deserialize(deserializer)?;
        Ok(StringInterner {
            map: RwLock::new(ssi.map),
            pool: RwLock::new(Pool(ssi.pool)),
            offsets: RwLock::new(ssi.offsets),
            lengths: RwLock::new(ssi.lengths),
        })
    }
}

impl StringInterner {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_intern(&self, s: &str) -> u32 {
        // Fast path: check map under read lock
        {
            let map = self.map.read();
            if let Some(&id) = map.get(s) {
                return id;
            }
        }

        // Acquire write locks (consistent ordering: map -> pool -> offsets -> lengths)
        let mut map = self.map.write();
        // Double-check after acquiring write lock
        if let Some(&id) = map.get(s) {
            return id;
        }

        let mut pool_guard = self.pool.write();
        let mut offsets = self.offsets.write();
        let mut lengths = self.lengths.write();

        let id = offsets.len() as u32;

        // Mutate the owned pool (pool is always owned now)
        let start = pool_guard.0.len();
        pool_guard.0.push_str(s);
        offsets.push(start as u32);
        lengths.push(s.len() as u32);

        map.insert(s.to_string(), id);
        id
    }

    /// Return an owned `String` for the given id (keeps API unchanged).
    pub fn lookup(&self, id: u32) -> Option<String> {
        let offsets = self.offsets.read();
        let idx = id as usize;
        let start = *offsets.get(idx)? as usize;
        let len = *self.lengths.read().get(idx)? as usize;

        let pool_str = &self.pool.read().0;
        Some(pool_str[start..start + len].to_string())
    }
}

/// Concurrent interner used during preprocessing to avoid heavy locking.
/// Converted to `StringInterner` after preprocessing completes.
#[derive(Debug, Default)]
pub struct ConcurrentInterner {
    map: DashMap<String, u32>,
    reverse: DashMap<u32, String>,
    next_id: AtomicU32,
}

impl ConcurrentInterner {
    pub fn new() -> Self {
        Self { map: DashMap::new(), reverse: DashMap::new(), next_id: AtomicU32::new(0) }
    }

    pub fn get_or_intern(&self, s: &str) -> u32 {
        if let Some(id) = self.map.get(s) {
            return *id;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // If another thread inserted concurrently, use its id
        if let Some(prev) = self.map.insert(s.to_string(), id) {
            return prev;
        }
        self.reverse.insert(id, s.to_string());
        id
    }

    /// Convert into the serializable `StringInterner` (called after preprocessing)
    pub fn into_string_interner(self) -> StringInterner {
        let count = self.next_id.load(Ordering::Relaxed) as usize;
        // build ordered reverse vector first (id -> String)
        let mut reverse_vec = vec![String::new(); count];
        for entry in self.reverse.into_iter() {
            reverse_vec[entry.0 as usize] = entry.1;
        }

        // build contiguous pool + offsets/lengths
        let mut pool = String::with_capacity(reverse_vec.iter().map(|s| s.len()).sum());
        let mut offsets = Vec::with_capacity(count);
        let mut lengths = Vec::with_capacity(count);
        for s in reverse_vec.into_iter() {
            offsets.push(pool.len() as u32);
            lengths.push(s.len() as u32);
            pool.push_str(&s);
        }

        let mut map = HashMap::with_capacity(count);
        for entry in self.map.into_iter() {
            map.insert(entry.0, entry.1);
        }

        StringInterner { map: RwLock::new(map), pool: RwLock::new(Pool(pool)), offsets: RwLock::new(offsets), lengths: RwLock::new(lengths) }
    }

    /// Non-consuming conversion (useful when `ConcurrentInterner` is held in an `Arc`)
    pub fn to_string_interner(&self) -> StringInterner {
        let count = self.next_id.load(Ordering::Relaxed) as usize;
        let mut reverse_vec = vec![String::new(); count];

        for entry in self.reverse.iter() {
            reverse_vec[*entry.key() as usize] = entry.value().clone();
        }

        let mut pool = String::with_capacity(reverse_vec.iter().map(|s| s.len()).sum());
        let mut offsets = Vec::with_capacity(count);
        let mut lengths = Vec::with_capacity(count);
        for s in reverse_vec.into_iter() {
            offsets.push(pool.len() as u32);
            lengths.push(s.len() as u32);
            pool.push_str(&s);
        }

        let mut map = HashMap::with_capacity(count);
        for entry in self.map.iter() {
            map.insert(entry.key().clone(), *entry.value());
        }

        StringInterner { map: RwLock::new(map), pool: RwLock::new(Pool(pool)), offsets: RwLock::new(offsets), lengths: RwLock::new(lengths) }
    }
}

/// Trait used by preprocessing so we can accept either a `StringInterner` (single-threaded/runtime)
/// or a `ConcurrentInterner` (used during parallel preprocessing).
pub trait InternerLike: Send + Sync {
    fn get_or_intern(&self, s: &str) -> u32;
}

impl InternerLike for StringInterner {
    fn get_or_intern(&self, s: &str) -> u32 { self.get_or_intern(s) }
}

impl InternerLike for ConcurrentInterner {
    fn get_or_intern(&self, s: &str) -> u32 { self.get_or_intern(s) }
}

// Convenience: allow `Arc<T>` to be used wherever an `InternerLike` is required
impl<T: InternerLike + ?Sized> InternerLike for std::sync::Arc<T> {
    fn get_or_intern(&self, s: &str) -> u32 { (**self).get_or_intern(s) }
}

/// Flattened representation of tag-sets to reduce per-Vec overhead
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct FlatTagSets {
    /// packed pairs: high 32 bits = key id, low 32 bits = value id
    pub data: Vec<u64>,
    /// start index for each tag-set inside `data`
    pub offsets: Vec<u32>,
    /// length (number of pairs) for each tag-set
    pub lengths: Vec<u32>,
}

impl FlatTagSets {
    pub fn get(&self, idx: usize) -> Option<&[u64]> {
        let off = *self.offsets.get(idx)? as usize;
        let len = *self.lengths.get(idx)? as usize;
        Some(&self.data[off..off + len])
    }
}

/// The structure saved to the cache file
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheData {
    pub elements: Vec<Element>,
    pub tag_sets: FlatTagSets,
    pub interner: StringInterner,
    /// Store a hash of the config AND input file metadata to know when to re-preprocess
    pub source_hash: u64,
}
