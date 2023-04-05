use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use serde::Deserialize;
use tantivy::{doc, Document};
use tantivy::schema::{Field, Schema, STORED, STRING};

use crate::indexation::{Indexer, ngram2_options};
use crate::server::AppState;

struct QuestionIndexer {
    question: IndexQuestion,
}

impl QuestionIndexer {
    fn new(question: IndexQuestion) -> QuestionIndexer {
        QuestionIndexer {
            question
        }
    }
}

pub fn new_question_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    let text_options = ngram2_options();

    schema_builder.add_text_field("id", STRING | STORED);
    schema_builder.add_text_field("question", text_options);

    schema_builder.build()
}

pub fn question_searchable_field() -> Field {
    let schema = new_question_schema();
    schema.get_field("question").unwrap()
}

impl Indexer for QuestionIndexer {
    fn new_document(&self) -> Document {
        let schema = new_question_schema();
        let id_field = schema.get_field("id").unwrap();
        let question_field = schema.get_field("question").unwrap();

        doc!(
        id_field => self.question.id.clone(),
        question_field => self.question.question.clone())
    }
}

#[derive(Deserialize)]
pub struct IndexQuestion {
    id: String,
    question: String,
}

pub async fn index_question(State(state): State<AppState>, Json(payload): Json<IndexQuestion>) -> impl IntoResponse {
    tracing::debug!("request received to index a question id: {}, question: {}", payload.id, payload.question);

    let indexer = QuestionIndexer::new(payload);
    state.question_index_handle.index_single(indexer.new_document(), new_question_schema()).await;

    StatusCode::ACCEPTED
}

pub async fn search_question(State(state): State<AppState>) -> impl IntoResponse {
    state.question_index_handle.search(question_searchable_field(), "hola", new_question_schema()).unwrap();
    StatusCode::ACCEPTED
}