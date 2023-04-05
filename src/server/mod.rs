use std::fs;

use axum::{
    Router, routing::get, routing::post,
};
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use tantivy::TantivyError;

use crate::indexation::IndexActorHandle;
use crate::question::{index_question, new_question_schema, search_question};

#[derive(Clone)]
pub struct AppState {
    pub question_index_handle: IndexActorHandle,
}

pub fn new_router() -> Result<Router, TantivyError> {
    // Init indexers
    let question_index_handle = new_index_actor("idx_questions", new_question_schema)?;

    // Init app state
    let app_state = AppState {
        question_index_handle,
    };

    Ok(Router::new()
        .route("/questions", get(search_question).post(index_question))
        .with_state(app_state))
}

fn new_index_actor(path: &str, new_schema: fn() -> Schema) -> Result<IndexActorHandle, TantivyError> {
    fs::create_dir_all(path).unwrap();
    let dir = MmapDirectory::open(path).unwrap();
    IndexActorHandle::new(dir, new_schema)
}