use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tantivy::Document;

use crate::indexation::field_to_string;
use crate::person::person_fields;
use crate::server::AppState;

#[derive(Deserialize)]
pub struct SearchPersonQuery {
    query: String,
}

#[derive(Serialize)]
struct SearchPersonResponse {
    id: String,
    email: String,
}

pub async fn search_people(State(state): State<AppState>, search_query: Query<SearchPersonQuery>) -> impl IntoResponse {
    let search_result = state.person_index_handle.search(search_query.query.as_str(), 10).await;

    match search_result {
        Ok(people_docs) => {
            let response: Vec<SearchPersonResponse> = people_docs.iter().map(document_to_person).collect();
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            tracing::error!("failed to search people: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(vec![]))
        }
    }
}

fn document_to_person(doc: &Document) -> SearchPersonResponse {
    let fields = person_fields();

    SearchPersonResponse {
        id: field_to_string(doc, fields.id),
        email: field_to_string(doc, fields.email),
    }
}