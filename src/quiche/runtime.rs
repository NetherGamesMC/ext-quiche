use std::sync::OnceLock;
use std::thread::available_parallelism;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use crate::quiche::error::QuicheError;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Floor for the worker thread count. The QUIC I/O worker plus a couple of
/// driver tasks already saturate one thread under load, so two is the minimum
/// that keeps the listener responsive while a connection is busy.
const MIN_WORKER_THREADS: usize = 2;

/// Resolve the worker thread count.
/// Order of precedence:
/// 1. `QUICHE_WORKER_THREADS` environment variable
/// 2. `std::thread::available_parallelism()`
/// 3. `MIN_WORKER_THREADS`
fn worker_thread_count() -> usize {
    if let Ok(s) = std::env::var("QUICHE_WORKER_THREADS") {
        if let Ok(n) = s.parse::<usize>()
            && n >= 1
        {
            return n;
        }
        log::warn!(
            "QUICHE_WORKER_THREADS={s:?} is not a positive integer; falling back to detection"
        );
    }

    available_parallelism()
        .map(|n| n.get().max(MIN_WORKER_THREADS))
        .unwrap_or(MIN_WORKER_THREADS)
}

pub fn get_runtime() -> Result<&'static Runtime, QuicheError> {
    if let Some(rt) = RUNTIME.get() {
        return Ok(rt);
    }

    let workers = worker_thread_count();
    log::info!("Initializing Tokio runtime with {workers} worker thread(s)");

    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
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
