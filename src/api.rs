use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use crate::config::Config;
use crate::model::{Element, StringInterner};
use rstar::{RTree, AABB, primitives::Line, PointDistance};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::HashMap;
use tracing::info;

#[derive(Clone)]
struct AppState {
    rtree: Arc<RTree<SpatialElement>>,
    tag_sets: Arc<Vec<Vec<(u32, u32)>>>,
    interner: Arc<StringInterner>,
}

#[derive(Clone)]
struct SpatialElement {
    id: u64,
    segment: Line<[f32; 2]>,
    tag_set_id: u32,
}

impl rstar::RTreeObject for SpatialElement {
    type Envelope = AABB<[f32; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.segment.envelope()
    }
}

impl rstar::PointDistance for SpatialElement {
    fn distance_2(&self, point: &[f32; 2]) -> f32 {
        self.segment.distance_2(point)
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
    tags: HashMap<String, String>,
}

pub async fn start_server(
    config: Config, 
    elements: Vec<Element>, 
    tag_sets: Vec<Vec<(u32, u32)>>,
    interner: StringInterner,
    start_time: std::time::Instant
) -> anyhow::Result<()> {
    let spatial_elements: Vec<SpatialElement> = elements.into_iter().map(|e| {
        SpatialElement {
            id: e.id,
            segment: Line::new(e.coordinates[0], e.coordinates[1]),
            tag_set_id: e.tag_set_id,
        }
    }).collect();

    let rtree = RTree::bulk_load(spatial_elements);
    
    let state = AppState {
        rtree: Arc::new(rtree),
        tag_sets: Arc::new(tag_sets),
        interner: Arc::new(interner),
    };

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
    
    let envelope = AABB::from_corners(
        [(params.lat - radius_deg) as f32, (params.lon - radius_deg) as f32],
        [(params.lat + radius_deg) as f32, (params.lon + radius_deg) as f32],
    );

    let results = state.rtree.locate_in_envelope(&envelope);
    
    let mut response_elements = Vec::new();
    let query_point = [params.lat as f32, params.lon as f32];

    for se in results {
        let dist_deg_sq = se.segment.distance_2(&query_point);
        
        if dist_deg_sq <= radius_deg_f32 * radius_deg_f32 {
            let mut tags = HashMap::new();
            if let Some(tag_set) = state.tag_sets.get(se.tag_set_id as usize) {
                for (kid, vid) in tag_set {
                    if let (Some(k), Some(v)) = (state.interner.lookup(*kid), state.interner.lookup(*vid)) {
                        tags.insert(k, v);
                    }
                }
            }
            
            let p1 = se.segment.from;
            let p2 = se.segment.to;

            response_elements.push(ResultElement {
                id: se.id,
                lat1: p1[0] as f64,
                lon1: p1[1] as f64,
                lat2: p2[0] as f64,
                lon2: p2[1] as f64,
                tags,
            });
        }
    }

    Json(QueryResponse { elements: response_elements })
}
