use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use crate::config::Config;
use crate::model::StringInterner;
use rstar::{RTree, AABB, primitives::Line, PointDistance};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::HashMap;
use tracing::info;

#[derive(Clone)]
struct TagSetsHandle(Arc<crate::model::FlatTagSets>);

impl TagSetsHandle {
    fn get(&self, idx: usize) -> Option<&[u64]> { self.0.get(idx) }
}

#[derive(Clone)]
struct AppState {
    rtree: Option<Arc<RTree<SpatialElement>>>,
    /* if cache was `Owned` and build_rtree=false we store elements here for fallback scanning */
    owned_elements: Option<Arc<Vec<crate::model::Element>>>,
    tag_sets: TagSetsHandle,
    interner: Arc<StringInterner>,
}

#[derive(Clone)]
struct SpatialElement {
    id: u64,
    tag_set_id: u32,
    storage: SegmentStorage,
}

#[derive(Clone)]
enum SegmentStorage {
    Owned(Line<[f32; 2]>),
}

impl SpatialElement {
    fn endpoints(&self) -> ([f32; 2], [f32; 2]) {
        match &self.storage {
            SegmentStorage::Owned(line) => (line.from, line.to),
        }
    }
}

impl rstar::RTreeObject for SpatialElement {
    type Envelope = AABB<[f32; 2]>;
    fn envelope(&self) -> Self::Envelope {
        let (p1, p2) = self.endpoints();
        Line::new(p1, p2).envelope()
    }
}

impl rstar::PointDistance for SpatialElement {
    fn distance_2(&self, point: &[f32; 2]) -> f32 {
        let (p1, p2) = self.endpoints();
        Line::new(p1, p2).distance_2(point)
    }
}

#[derive(Deserialize)]
pub struct QueryParams {
    lat: f64,
    lon: f64,
    radius: f64, 
}

#[derive(Serialize)]
pub struct QueryResponse {
    elements: Vec<ResultElement>,
}

#[derive(Serialize)]
pub struct ResultElement {
    id: u64,
    lat1: f64,
    lon1: f64,
    lat2: f64,
    lon2: f64,
    #[serde(rename = "type")]
    element_type: String,
    tags: HashMap<String, String>,
}

pub async fn start_server(
    config: Config,
    cache: crate::preprocessor::LoadedCache,
    start_time: std::time::Instant,
) -> anyhow::Result<()> {
    // build spatial elements + tag_sets handle + interner from the Owned cache
    // (runtime.build_rtree option has been removed; we always build the in-memory RTree at startup)

    // small helper to read RSS (MB)
    let get_rss_mb = || -> Option<u64> {
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                if line.starts_with("VmRSS:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u64>() {
                            return Some(kb / 1024);
                        }
                    }
                    break;
                }
            }
        }
        None
    };

    match cache {
        crate::preprocessor::LoadedCache::Owned { elements, tag_sets, interner } => {
            let interner_arc = Arc::new(interner);
            let tag_sets_handle = TagSetsHandle(Arc::new(tag_sets));

            info!("Building in-memory RTree for {} elements (this may use a lot of RAM)...", elements.len());
            if let Some(rss) = get_rss_mb() { info!("RSS before building RTree: {} MB", rss); }

            let ses = elements.into_iter().map(|e| SpatialElement {
                id: e.id,
                tag_set_id: e.tag_set_id,
                storage: SegmentStorage::Owned(Line::new(e.coordinates[0], e.coordinates[1])),
            }).collect::<Vec<_>>();

            if let Some(rss) = get_rss_mb() { info!("RSS after preparing SpatialElement vec: {} MB", rss); }
            let rtree = RTree::bulk_load(ses);
            if let Some(rss) = get_rss_mb() { info!("RSS after RTree::bulk_load: {} MB", rss); }

            let state = AppState { rtree: Some(Arc::new(rtree)), owned_elements: None, tag_sets: tag_sets_handle, interner: interner_arc };

            run_server_with_state(config, state, start_time).await
        }
    }
}

async fn run_server_with_state(config: Config, state: AppState, start_time: std::time::Instant) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/api/query", get(handle_query))
        .with_state(state);

    let addr_str = format!("{}:{}", config.server.host, config.server.port);
    let addr: SocketAddr = addr_str.parse()?;

    let listener = tokio::net::TcpListener::bind(addr).await?;

    let elapsed = start_time.elapsed();

    info!("Server listening on {}", addr);
    info!("Total startup time: {:.2?}", elapsed);

    axum::serve(listener, app).await?;

    Ok(())
}


async fn handle_query(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
) -> Json<QueryResponse> {
    let radius_deg = params.radius / 111320.0; 
    let radius_deg_f32 = radius_deg as f32;
    let query_point = [params.lat as f32, params.lon as f32];

    // helper: squared distance from point to segment
    fn point_segment_distance2(px: f32, py: f32, x1: f32, y1: f32, x2: f32, y2: f32) -> f32 {
        let vx = x2 - x1;
        let vy = y2 - y1;
        let wx = px - x1;
        let wy = py - y1;
        let c1 = vx * wx + vy * wy;
        if c1 <= 0.0 { return (px - x1).powi(2) + (py - y1).powi(2); }
        let c2 = vx * vx + vy * vy;
        if c2 <= c1 { return (px - x2).powi(2) + (py - y2).powi(2); }
        let t = c1 / c2;
        let cx = x1 + t * vx;
        let cy = y1 + t * vy;
        (px - cx).powi(2) + (py - cy).powi(2)
    }

    let mut response_elements = Vec::new();

    if let Some(rtree) = &state.rtree {
        // fast path: in-memory RTree
        let results = rtree.locate_within_distance(query_point, radius_deg_f32 * radius_deg_f32);
        for se in results {
            let mut tags = HashMap::new();
            if let Some(packed_slice) = state.tag_sets.get(se.tag_set_id as usize) {
                for &packed in packed_slice {
                    let kid = (packed >> 32) as u32;
                    let vid = (packed & 0xFFFF_FFFF) as u32;
                    if let (Some(k), Some(v)) = (state.interner.lookup(kid), state.interner.lookup(vid)) {
                        tags.insert(k, v);
                    }
                }
            }

            let (p1, p2) = se.endpoints();
            let element_type = if p1 == p2 { "node" } else { "way" }.to_string();
            let dist_deg_sq = se.distance_2(&query_point);

            response_elements.push((dist_deg_sq, ResultElement {
                id: se.id,
                lat1: p1[0] as f64,
                lon1: p1[1] as f64,
                lat2: p2[0] as f64,
                lon2: p2[1] as f64,
                element_type,
                tags,
            }));
        }
    } else if let Some(owned) = &state.owned_elements {
        // fallback for Owned cache when RTree was skipped
        for e in owned.iter() {
            let p1 = e.coordinates[0];
            let p2 = e.coordinates[1];
            let dist2 = point_segment_distance2(query_point[0], query_point[1], p1[0], p1[1], p2[0], p2[1]);
            if dist2 <= radius_deg_f32 * radius_deg_f32 {
                let mut tags = HashMap::new();
                if let Some(packed_slice) = state.tag_sets.get(e.tag_set_id as usize) {
                    for &packed in packed_slice {
                        let kid = (packed >> 32) as u32;
                        let vid = (packed & 0xFFFF_FFFF) as u32;
                        if let (Some(k), Some(v)) = (state.interner.lookup(kid), state.interner.lookup(vid)) {
                            tags.insert(k, v);
                        }
                    }
                }
                let element_type = if p1 == p2 { "node" } else { "way" }.to_string();
                response_elements.push((dist2, ResultElement { id: e.id, lat1: p1[0] as f64, lon1: p1[1] as f64, lat2: p2[0] as f64, lon2: p2[1] as f64, element_type, tags }));
            }
        }
    }

    // Sort by distance (ASC)
    response_elements.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let final_elements: Vec<ResultElement> = response_elements.into_iter().map(|(_, e)| e).collect();

    Json(QueryResponse { elements: final_elements })
}
