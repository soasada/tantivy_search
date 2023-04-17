use std::env;
use std::net::SocketAddr;
use tokio::signal;

use tracing_subscriber::EnvFilter;

use crate::server::new_router;

mod indexation;
mod person;
mod question;
mod server;

#[derive(Debug, Clone)]
pub struct AppEnv {
    backend_env: String,
}

impl AppEnv {
    fn new(backend_env: String) -> Self {
        AppEnv {
            backend_env
        }
    }

    fn is_prod(&self) -> bool {
        self.backend_env.eq_ignore_ascii_case("prod")
    }
}

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::main]
async fn main() {
    #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

    let default_env = "development";
    let backend_env = match env::var("BACKEND_SEARCH_ENV") {
        Ok(env_var) => env_var,
        Err(_) => String::from(default_env),
    };

    let app_env = AppEnv::new(backend_env);

    if app_env.is_prod() {
        env::set_var("RUST_LOG", "info");
    } else {
        env::set_var("RUST_LOG", "tantivy_search=debug");
    }

    // install global collector configured based on RUST_LOG env var. By default only logs WARN and up
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let app_router = match new_router(app_env).await {
        Ok(r) => r,
        Err(e) => panic!("Error creating router: {:?}", e)
    };

    let addr = SocketAddr::from(([0, 0, 0, 0], 8079));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app_router.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    println!("signal received, starting graceful shutdown");
}