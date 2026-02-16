use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use crate::config::Config;
use crate::model::{Element, StringInterner};
use rstar::{RTree, AABB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::net::SocketAddr;

#[derive(Clone)]
struct AppState {
    rtree: Arc<RTree<SpatialElement>>,
    interner: Arc<StringInterner>,
}

#[derive(Debug, Clone)]
struct SpatialElement {
    id: u64,
    coordinate: [f64; 2],
    tags: Vec<(u32, u32)>,
}

impl rstar::RTreeObject for SpatialElement {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.coordinate)
    }
}

impl rstar::PointDistance for SpatialElement {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.coordinate[0] - point[0];
        let dy = self.coordinate[1] - point[1];
        dx * dx + dy * dy
    }
}

#[derive(Deserialize)]
pub struct QueryParams {
    lat: f64,
    lon: f64,
    radius: f64, // in "degree-ish" units for simplicity in this minimal version, or convert to meters
}

#[derive(Serialize)]
pub struct QueryResponse {
    elements: Vec<ResultElement>,
}

#[derive(Serialize)]
pub struct ResultElement {
    id: u64,
    lat: f64,
    lon: f64,
    tags: HashMap<String, String>,
}

use std::collections::HashMap;

pub async fn start_server(config: Config, elements: Vec<Element>, interner: StringInterner) -> anyhow::Result<()> {
    let spatial_elements: Vec<SpatialElement> = elements.into_iter().map(|e| SpatialElement {
        id: e.id,
        coordinate: e.coordinate,
        tags: e.tags,
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
    
    println!("Server listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn handle_query(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
) -> Json<QueryResponse> {
    // radius in degree-ish: roughly 111km per degree.
    // simpler: search within a bounding box first
    let radius_deg = params.radius / 111000.0; 
    
    let envelope = AABB::from_corners(
        [params.lat - radius_deg, params.lon - radius_deg],
        [params.lat + radius_deg, params.lon + radius_deg],
    );

    let results = state.rtree.locate_in_envelope(&envelope);
    
    let mut response_elements = Vec::new();
    for se in results {
        // filter by actual distance for circular radius
        let dx = se.coordinate[0] - params.lat;
        let dy = se.coordinate[1] - params.lon;
        let dist_deg = (dx*dx + dy*dy).sqrt();
        
        if dist_deg <= radius_deg {
            let mut tags = HashMap::new();
            for (kid, vid) in &se.tags {
                if let (Some(k), Some(v)) = (state.interner.lookup(*kid), state.interner.lookup(*vid)) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
            
            response_elements.push(ResultElement {
                id: se.id,
                lat: se.coordinate[0],
                lon: se.coordinate[1],
                tags,
            });
        }
    }

    Json(QueryResponse { elements: response_elements })
}
