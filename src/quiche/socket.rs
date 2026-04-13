use crate::config::Config;
use crate::quiche::conn::QuicheConnection;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncReadExt;
use tokio::net::UdpSocket;
use tokio_eventfd::EventFd;
use tokio_quiche::{
    listen, metrics::DefaultMetrics, settings::{CertificateKind, Hooks, TlsCertificatePaths}, ConnectionParams,
    InitialQuicConnection,
    QuicConnectionStream,
};

pub struct QuicheServerSocketImpl {
    listeners: Vec<QuicConnectionStream<DefaultMetrics>>,

    /// Per-connection handles kept alive so that stream routing tables and
    /// write channels are not dropped while connections are active.
    connections: HashMap<u64, QuicheConnection>,
    id_counter: AtomicU64,
}

impl QuicheServerSocketImpl {
    pub async fn bind(addrs: &[&str], config: &Config) -> std::io::Result<Self> {
        let sockets: Vec<UdpSocket> =
            try_join_all(addrs.iter().map(|addr| UdpSocket::bind(addr))).await?;

        let settings = &config.inner;
        let params = ConnectionParams::new_server(
            settings.to_owned(),
            TlsCertificatePaths {
                cert: config.get_cert_path(),
                private_key: config.get_key_path(),
                kind: CertificateKind::X509,
            },
            Hooks::default(),
        );

        let listeners = listen(sockets, params, DefaultMetrics)?;

        Ok(Self {
            listeners,
            connections: HashMap::new(),
            id_counter: AtomicU64::new(0),
        })
    }

    /// Close the server: drop all listeners and connections.
    pub fn close(&mut self) {
        self.listeners.clear();
        self.connections.clear();
    }

    /// Run the event loop until the `event_fd` fires.
    ///
    /// Each iteration processes an initial quic handshake event, then
    /// binds a StreamQuicDriver to handle stream events received by upstream server.
    ///
    /// This process iterates through all active connections, then reads or handles
    /// new streams, whichever comes first, during each connection tick.
    ///
    /// The loop only returns once `event_fd` produces a value (or is `None`, in which case
    /// the loop runs until one of the channel arms is exhausted).
    pub async fn tick(&mut self, mut event_fd: Option<&mut EventFd>) {
        log::info!("Starting QUIC server loop");

        enum Event {
            NewConn(Option<std::io::Result<InitialQuicConnection<UdpSocket, DefaultMetrics>>>),
            NewConnEvent(Option<(u64, bool)>),
            Timer,
        }

        loop {
            let event = {
                let efd = event_fd.as_deref_mut();
                let connections = &mut self.connections.values_mut();

                tokio::select! {
                    Some(conn) = async {
                        let mut fut = FuturesUnordered::new();
                        for listener in &mut self.listeners {
                            fut.push(listener.next());
                        }

                        if !fut.is_empty() {
                            fut.next().await
                        } else {
                            std::future::pending::<()>().await;
                            None
                        }
                    } => Event::NewConn(conn),
                    conn_id = async {
                        let mut fut = FuturesUnordered::new();
                        for conn in connections {
                            fut.push(conn.tick());
                        }

                         if !fut.is_empty() {
                            fut.next().await
                        } else {
                            std::future::pending::<()>().await;
                            None
                        }
                    } => Event::NewConnEvent(conn_id),
                    _ = async {
                        if let Some(efd) = efd {
                            let _ = efd.read_u64().await;
                        } else {
                            std::future::pending::<()>().await
                        }
                    } => Event::Timer,
                }
            };

            match event {
                Event::NewConn(Some(Ok(conn))) => {
                    let conn_id_fn = || self.id_counter.fetch_add(1, Ordering::Relaxed);
                    let conn_id = conn_id_fn();

                    let peer_addr = conn.peer_addr();
                    let (handle, driver) = QuicheConnection::build(conn_id, peer_addr);
                    conn.start(driver);

                    self.connections.insert(conn_id, handle);

                    log::info!("[{conn_id}] New QUIC connection accepted from {peer_addr}");
                }
                Event::NewConn(Some(Err(e))) => {
                    log::warn!("Connection accept error: {e:?}");
                }
                Event::NewConn(None) => {
                    log::warn!("QUIC listener stream ended");
                }
                Event::NewConnEvent(Some((conn_id, closed))) => {
                    if closed {
                        log::info!("[{conn_id}] QUIC connection closed");
                        self.connections.remove(&conn_id);
                    }

                    // TODO: Notify PHP
                }
                Event::NewConnEvent(None) => {
                    log::warn!("Unexpected None event received, ignoring");
                }
                Event::Timer => {
                    log::info!("EventFd/timer fired, returning to PHP");
                    break;
                }
            }
        }
    }
}
