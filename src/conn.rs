use crate::config::Config;
use crate::quiche::error::QuicheError;
use crate::quiche::runtime::get_runtime;
use crate::quiche::socket::QuicheServerSocketImpl;
use ext_php_rs::prelude::*;
use std::os::fd::{FromRawFd, RawFd};
use tokio_eventfd::EventFd;

#[php_class]
#[php(name = "NetherGames\\Quiche\\QuicheServerSocket")]
pub struct QuicheServerSocket {
    inner: QuicheServerSocketImpl,
    event_fd: Option<EventFd>,
}

#[php_impl]
impl QuicheServerSocket {
    pub fn __construct(
        sockets: Vec<&SocketAddress>,
        config: &Config,
        event_fd_id: i32,
    ) -> PhpResult<Self> {
        let rt = get_runtime().map_err(PhpException::from)?;

        let addrs: Vec<String> = sockets.iter().map(|s| s.get_address_result()).collect();
        let (server, event_fd) = rt.block_on(async move {
            let addr_strs: Vec<&str> = addrs.iter().map(String::as_str).collect();
            let server = QuicheServerSocketImpl::bind(&addr_strs, config).await;
            let event_fd: Option<EventFd> = if event_fd_id != -1 {
                // This is unsafe because we do not control the lifecycle of this raw fd, and since this is controlled
                // by the PHP thread, we cannot guarantee that the fd is valid. The developer is responsible for ensuring
                // that the fd is valid and is not closed by the time we try to use it.
                Some(unsafe { EventFd::from_raw_fd(event_fd_id as RawFd) })
            } else {
                None
            };
            (server, event_fd)
        });

        Ok(Self {
            inner: server.map_err(QuicheError::RuntimeInit)?,
            event_fd,
        })
    }

    /// Drive one iteration of the event loop.
    ///
    /// Internally runs a `tokio::select!` across: new and existing connections
    /// alongside the optional EventFd timer.
    pub fn tick(&mut self) -> PhpResult<()> {
        let rt = get_runtime().map_err(PhpException::from)?;

        // Split borrows explicitly so the borrow checker can see that
        // `inner` and `event_fd` are independent fields.
        let (inner, event_fd) = (&mut self.inner, &mut self.event_fd);
        rt.block_on(inner.tick(event_fd.as_mut()));

        Ok(())
    }

    /// Shut down the server and release all resources.
    pub fn close(&mut self) {
        self.inner.close();
    }
}

/// An implementation of a QUIC socket address.
///
/// Initialized before instantiation of the Quiche socket
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
    use rcgen::CertifiedKey;
    use std::path::PathBuf;
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

        // Tick the server socket.
        let mut server = QuicheServerSocketImpl::bind(&addr_strs, &config)
            .await
            .unwrap();

        loop {
            server.tick(None).await;
        }
    }
}
