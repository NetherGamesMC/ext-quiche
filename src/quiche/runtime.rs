use std::sync::OnceLock;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use crate::quiche::error::QuicheError;

/// https://github.com/BSN4/grpc-php-rs/blob/main/src/runtime.rs
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn get_runtime() -> Result<&'static Runtime, QuicheError> {
    // OnceLock::get_or_try_init is unstable, so we use a two-step approach:
    // 1. Fast path: already initialized
    if let Some(rt) = RUNTIME.get() {
        return Ok(rt);
    }

    // TODO: Use php.ini to configure the number of threads
    let rt = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("quiche-event-io")
        .build()
        .map_err(QuicheError::RuntimeInit)?;

    // If another thread beat us, our `rt` is dropped (harmless).
    let _ = RUNTIME.set(rt);
    RUNTIME
        .get()
        .ok_or_else(|| QuicheError::RuntimeInit(std::io::Error::other("runtime init failed")))
}
