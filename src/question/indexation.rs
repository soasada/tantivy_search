use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use serde::Deserialize;
use tantivy::{doc, Document};

use crate::question::question_fields;
use crate::server::AppState;

#[derive(Deserialize)]
pub struct IndexQuestion {
    pub id: String,
    pub question: String,
    pub public_employment_name: String,
    pub question_type: String,
    pub created_at: String,
}

#[derive(Deserialize)]
pub struct ReIndexQuestion {
    questions: Vec<IndexQuestion>,
}

pub fn new_document(question: &IndexQuestion) -> Document {
    let fields = question_fields();

    doc!(
        fields.id => question.id.clone(),
        fields.question => question.question.clone(),
        fields.public_employment_name => question.public_employment_name.clone(),
        fields.question_type => question.question_type.clone(),
        fields.created_at => question.created_at.clone(),
    )
}

pub async fn index_question(State(state): State<AppState>, Json(payload): Json<IndexQuestion>) -> impl IntoResponse {
    tracing::debug!("request received to index a question id: {}, question: {}", payload.id, payload.question);

    state.question_index_handle.index_single(new_document(&payload)).await;

    StatusCode::ACCEPTED
}

pub async fn delete_question(State(state): State<AppState>, Path(question_id): Path<String>) -> impl IntoResponse {
    state.question_index_handle.delete(question_id).await;
    StatusCode::ACCEPTED
}

pub async fn reindex_question(State(state): State<AppState>, Json(payload): Json<ReIndexQuestion>) -> impl IntoResponse {
    for q in payload.questions {
        state.question_index_handle.index_single(new_document(&q)).await;
    }
    StatusCode::ACCEPTED
}