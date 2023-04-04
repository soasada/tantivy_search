use std::env;
use std::net::SocketAddr;

use crate::server::new_router;

mod indexation;
mod question;
mod server;

#[tokio::main]
async fn main() {
    let default_env = "development";
    let backend_env = match env::var("BACKEND_SEARCH_ENV") {
        Ok(env_var) => env_var,
        Err(_) => String::from(default_env),
    };

    if backend_env.eq_ignore_ascii_case(default_env) {
        env::set_var("RUST_LOG", "tantivy_search=debug");
    } else {
        env::set_var("RUST_LOG", "tantivy_search=info");
    }

    // install global collector configured based on RUST_LOG env var. By default only logs WARN and up
    tracing_subscriber::fmt::init();

    let app_router = match new_router() {
        Ok(r) => r,
        Err(e) => panic!("Error creating router: {:?}", e)
    };

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app_router.into_make_service())
        .await
        .unwrap();
}
