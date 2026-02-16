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
    interner: Arc<StringInterner>,
}

#[derive(Clone)]
struct SpatialElement {
    id: u64,
    segment: Line<[f64; 2]>,
    tags: Vec<(u32, u32)>,
}

impl rstar::RTreeObject for SpatialElement {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.segment.envelope()
    }
}

impl rstar::PointDistance for SpatialElement {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
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
    interner: StringInterner,
    start_time: std::time::Instant
) -> anyhow::Result<()> {
    let spatial_elements: Vec<SpatialElement> = elements.into_iter().map(|e| {
        SpatialElement {
            id: e.id,
            segment: Line::new(e.coordinates[0], e.coordinates[1]),
            tags: e.tags,
        }
    }).collect();

    let rtree = RTree::bulk_load(spatial_elements);
    
    let state = AppState {
        rtree: Arc::new(rtree),
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
    
    let envelope = AABB::from_corners(
        [params.lat - radius_deg, params.lon - radius_deg],
        [params.lat + radius_deg, params.lon + radius_deg],
    );

    let results = state.rtree.locate_in_envelope(&envelope);
    
    let mut response_elements = Vec::new();
    let query_point = [params.lat, params.lon];

    for se in results {
        // Line::distance_2 requires PointDistance trait in scope
        let dist_deg_sq = se.segment.distance_2(&query_point);
        
        if dist_deg_sq <= radius_deg * radius_deg {
            let mut tags = HashMap::new();
            for (kid, vid) in &se.tags {
                if let (Some(k), Some(v)) = (state.interner.lookup(*kid), state.interner.lookup(*vid)) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
            
            let p1 = se.segment.from;
            let p2 = se.segment.to;

            response_elements.push(ResultElement {
                id: se.id,
                lat1: p1[0],
                lon1: p1[1],
                lat2: p2[0],
                lon2: p2[1],
                tags,
            });
        }
    }

    Json(QueryResponse { elements: response_elements })
}
