use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tantivy::Document;

use crate::indexation::field_to_string;
use crate::question::question_fields;
use crate::server::AppState;

#[derive(Deserialize)]
pub struct SearchQuestionQuery {
    query: String,
}

#[derive(Serialize)]
pub struct SearchQuestionResponse {
    id: String,
    question: String,
    public_employment_name: String,
    question_type: String,
    created_at: String,
}

pub async fn search_questions(State(state): State<AppState>,
                              search_query: Query<SearchQuestionQuery>) -> impl IntoResponse {
    let search_result = state.question_index_handle.search(search_query.query.as_str(), 10).await;

    match search_result {
        Ok(question_docs) => {
            let response: Vec<SearchQuestionResponse> = question_docs.iter().map(document_to_question).collect();
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            tracing::error!("failed to search questions: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(vec![]))
        }
    }
}

pub fn document_to_question(doc: &Document) -> SearchQuestionResponse {
    let fields = question_fields();

    SearchQuestionResponse {
        id: field_to_string(doc, fields.id),
        question: field_to_string(doc, fields.question),
        public_employment_name: field_to_string(doc, fields.public_employment_name),
        question_type: field_to_string(doc, fields.question_type),
        created_at: field_to_string(doc, fields.created_at),
    }
}