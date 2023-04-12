use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use serde::Deserialize;
use tantivy::{doc, Document};

use crate::person::person_fields;
use crate::server::AppState;

#[derive(Deserialize)]
pub struct IndexPerson {
    id: String,
    email: String,
}

#[derive(Deserialize)]
pub struct ReIndexPerson {
    people: Vec<IndexPerson>,
}

fn new_document(person: &IndexPerson) -> Document {
    let fields = person_fields();

    doc!(
        fields.id => person.id.clone(),
        fields.email => person.email.clone())
}

pub async fn index_person(State(state): State<AppState>, Json(payload): Json<IndexPerson>) -> impl IntoResponse {
    tracing::debug!("request received to index a person, id: {}", payload.id);

    state.person_index_handle.index_single(new_document(&payload)).await;

    StatusCode::ACCEPTED
}

pub async fn delete_person(State(state): State<AppState>, Path(person_id): Path<String>) -> impl IntoResponse {
    state.person_index_handle.delete(person_id).await;
    StatusCode::ACCEPTED
}

pub async fn reindex_person(State(state): State<AppState>, Json(payload): Json<ReIndexPerson>) -> impl IntoResponse {
    for p in payload.people {
        state.person_index_handle.index_single(new_document(&p)).await;
    }

    StatusCode::ACCEPTED
}