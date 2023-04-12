use std::fs;

use axum::{
    Router, routing::delete, routing::get, routing::post,
};
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use tantivy::TantivyError;

use crate::AppEnv;
use crate::indexation::handle::IndexActorHandle;
use crate::person::indexation::{delete_person, index_person, reindex_person};
use crate::person::new_person_schema;
use crate::person::search::search_people;
use crate::question::indexation::{delete_question, index_question, reindex_question};
use crate::question::new_question_schema;
use crate::question::search::search_questions;

/// Only one index writer and one reader is allowed for the entire lifetime of the server.
/// For each, we spawn a regular OS thread with std::thread::spawn.
/// We use tokio channels to communicate with the indexers.
#[derive(Clone)]
pub struct AppState {
    pub question_index_handle: IndexActorHandle,
    pub person_index_handle: IndexActorHandle,
    pub backend_env: AppEnv,
}

pub async fn new_router(backend_env: AppEnv) -> Result<Router, TantivyError> {
    // Init indexers
    let question_index_handle = new_index_actor("idx_questions", new_question_schema(), String::from("questions"), backend_env.clone()).await?;
    let person_index_handle = new_index_actor("idx_people", new_person_schema(), String::from("people"), backend_env.clone()).await?;

    // Init app state
    let app_state = AppState {
        question_index_handle,
        person_index_handle,
        backend_env,
    };

    Ok(Router::new()
        .route("/questions", get(search_questions).post(index_question))
        .route("/questions/reindex", post(reindex_question))
        .route("/questions/:question_id", delete(delete_question))
        .route("/people", get(search_people).post(index_person))
        .route("/people/reindex", post(reindex_person))
        .route("/people/:person_id", delete(delete_person))
        .with_state(app_state))
}

async fn new_index_actor(path: &str, schema: Schema, index_name: String, backend_env: AppEnv) -> Result<IndexActorHandle, TantivyError> {
    fs::create_dir_all(path).unwrap();
    let dir = MmapDirectory::open(path).unwrap();
    IndexActorHandle::new(dir, schema, index_name, backend_env).await
}