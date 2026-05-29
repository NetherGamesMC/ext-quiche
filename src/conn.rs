use crate::config::Config;
use crate::quiche::error::QuicheError;
use crate::quiche::runtime::get_runtime;
use crate::quiche::socket::QuicheServerSocketImpl;
use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;
use parking_lot::Mutex;
use std::os::fd::{FromRawFd, RawFd};
use std::sync::Arc;
use std::time::Instant;
use tokio_eventfd::EventFd;

#[php_class]
#[php(name = "NetherGames\\Quiche\\QuicheServerSocket")]
pub struct QuicheServerSocket {
    inner: QuicheServerSocketImpl,
    event_fd: Option<EventFd>,
}

#[php_impl]
impl QuicheServerSocket {
    // PHP signature:
    // public function __construct(array $sockets, Config $config, int $event_fd_id = -1, callable $on_stream)
    pub fn __construct(
        sockets: Vec<&SocketAddress>,
        config: &Config,
        event_fd_id: i32,
        on_stream: &Zval,
    ) -> PhpResult<Self> {
        if !on_stream.is_callable() {
            let received_type = match on_stream.get_type() {
                ext_php_rs::types::ZvalTypeFlags::Null => "null",
                ext_php_rs::types::ZvalTypeFlags::Bool => "bool",
                ext_php_rs::types::ZvalTypeFlags::Long => "int",
                ext_php_rs::types::ZvalTypeFlags::Double => "float",
                ext_php_rs::types::ZvalTypeFlags::String => "string",
                ext_php_rs::types::ZvalTypeFlags::Array => "array",
                ext_php_rs::types::ZvalTypeFlags::Object => "object",
                ext_php_rs::types::ZvalTypeFlags::Resource => "resource",
                _ => "unknown",
            };

            return Err(PhpException::custom(
                "InvalidArgumentException",
                format!(
                    "The $on_stream argument must be a callable (function, closure, or invokable object). Received: {}",
                    received_type
                ),
            ));
        }

        let rt = get_runtime().map_err(PhpException::from)?;

        // Zval is !Send, but Arc<Mutex<>> lets us share across the PHP object boundary.
        // The callable is ONLY invoked on the PHP thread (inside block_on).
        #[allow(clippy::arc_with_non_send_sync)]
        let cb = Arc::new(Mutex::new(Some(on_stream.shallow_clone())));

        let addrs: Vec<String> = sockets.iter().map(|s| s.get_address_result()).collect();
        let (server, event_fd) = rt.block_on(async {
            let addr_strs: Vec<&str> = addrs.iter().map(String::as_str).collect();
            let server = QuicheServerSocketImpl::bind(&addr_strs, config, cb).await;
            let event_fd: Option<EventFd> = if event_fd_id != -1 {
                // Duplicate the caller's fd so this object owns an independent
                // fd that refers to the same underlying eventfd. That gives
                // PHP main a clean ownership story:
                //
                //   * Main owns the original fd it created with
                //     network_eventfd_create(); it can close that fd at any
                //     time (after joining the worker is the safest order).
                //   * The worker thread's QuicheServerSocket owns the dup;
                //     close() / Drop closes the dup.
                //
                // Both fds are independent file table entries that share
                // the same kernel eventfd object (refcounted), so a write
                // on either is visible to a read on the other.
                let dup_fd = unsafe { libc::dup(event_fd_id as RawFd) };
                if dup_fd < 0 {
                    let e = std::io::Error::last_os_error();
                    log::error!("dup(eventfd) failed: {e}");
                    None
                } else {
                    log::info!(
                        "QuicheServerSocket: duplicated eventfd {} → {} (worker owns dup)",
                        event_fd_id, dup_fd
                    );
                    // SAFETY: dup_fd is freshly allocated by libc::dup and
                    // owned solely by this EventFd from here on.
                    Some(unsafe { EventFd::from_raw_fd(dup_fd) })
                }
            } else {
                None
            };
            (server, event_fd)
        });

        Ok(Self {
            event_fd,
            inner: server.map_err(QuicheError::RuntimeInit)?,
        })
    }

    /// Drive one iteration of the event loop.
    ///
    /// Internally runs a `tokio::select!` across new and existing connections
    /// plus the optional EventFd timer.  Returns when the timer fires (or when
    /// `event_fd_id == -1` and a channel arm is exhausted).
    pub fn tick(&mut self) -> PhpResult<()> {
        let rt = get_runtime().map_err(PhpException::from)?;

        log::debug!(
            "PHP tick() called on thread={:?} pid={} tid={} (eventfd={})",
            std::thread::current().id(),
            std::process::id(),
            gettid::gettid(),
            self.event_fd.is_some()
        );

        let start = Instant::now();
        
        let (inner, event_fd) = (&mut self.inner, &mut self.event_fd);
        rt.block_on(inner.tick(event_fd.as_mut()));

        let duration = start.elapsed();
        
        if duration.as_millis() > 100 {
            log::warn!("PHP tick() took {:?} (slow event loop detected)", duration);
        } else {
            log::debug!("PHP tick() completed in {:?}", duration);
        }

        log::info!("PHP tick() returning to caller");
        Ok(())
    }

    /// Shut down the server and release all resources.
    pub fn close(&mut self) {
        log::info!("PHP close() invoked");
        self.inner.close();
        
        if let Some(fd) = self.event_fd.take() {
            // Dropping the EventFd here closes the underlying file descriptor
            // exactly once. Callers must NOT call libc::close() on the fd
            // separately — that would race the worker thread's last poll and
            // cause it to miss the wake-up.
            drop(fd);
            log::info!("EventFd closed successfully");
        } else {
            log::warn!("PHP close() called on already closed socket (double close detected)");
        }
        
        log::info!("PHP close() returned");
    }
}

/// An implementation of a QUIC socket address.
///
/// Initialized before instantiation of the Quiche socket.
#[php_class]
#[php(name = "NetherGames\\Quiche\\SocketAddress")]
pub struct SocketAddress {
    pub address: String,
    pub port: u16,
}

#[php_impl]
impl SocketAddress {
    pub fn __construct(address: String, port: u16) -> Self {
        Self { address, port }
    }

    #[php(getter)]
    pub fn get_address(&self) -> String {
        self.address.clone()
    }

    #[php(setter)]
    pub fn set_address(&mut self, address: String) {
        self.address = address;
    }

    #[php(getter)]
    pub fn get_port(&self) -> u16 {
        self.port
    }

    #[php(setter)]
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    #[php(getter)]
    pub fn get_address_result(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::quiche::socket::QuicheServerSocketImpl;
    use parking_lot::Mutex;
    use rcgen::CertifiedKey;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio_quiche::settings::QuicSettings;

    struct TestCerts {
        cert: PathBuf,
        key: PathBuf,
        _dir: tempfile::TempDir, // keeps temp dir alive
    }

    fn make_certs() -> TestCerts {
        let CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, signing_key.serialize_pem()).unwrap();

        TestCerts {
            cert: cert_path,
            key: key_path,
            _dir: dir,
        }
    }

    #[tokio::test]
    async fn quiche_server_socket_tick() {
        let _ = env_logger::builder().is_test(true).try_init();

        let certs = make_certs();
        let mut conf_default = QuicSettings::default();
        let mut vec: Vec<String> = Vec::new();
        vec.push("rquic".to_string());
        conf_default.alpn = vec.into_iter().map(|s| s.into_bytes()).collect();
        conf_default.verify_peer = false;

        let config = Config {
            inner: conf_default,
            cert_path: certs.cert.to_str().unwrap().to_string(),
            key_path: certs.key.to_str().unwrap().to_string(),
        };
        let addr_strs = vec!["127.0.0.1:19132"];

        #[allow(clippy::arc_with_non_send_sync)]
        let mut server =
            QuicheServerSocketImpl::bind(&addr_strs, &config, Arc::new(Mutex::new(None)))
                .await
                .unwrap();

        loop {
            server.tick(None).await;
        }
    }
}
