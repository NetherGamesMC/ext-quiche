use crate::quiche::data::{ReadHalf, WriteHalf};
use crate::quiche::driver::StreamQuicDriver;
use crate::quiche::stream::{
    AcceptedRemoteStream, BiDirectionalQuicheStream, WriteableQuicheStream,
};
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Notify};

/// General data type for a quiche known types
pub type StreamId = u64;
pub type StreamData = Option<Bytes>;
pub type StreamFin = bool;
pub type StreamClosed = bool;

/// Common data type for stream data with stream id, optional data bytes, and fin flag
pub type StreamWriter = (StreamId, StreamData, StreamFin);
pub type StreamReader = (StreamData, StreamFin, StreamClosed);

/// Map of stream id => multi-producer sender queue
pub type StreamRoutes = HashMap<StreamId, mpsc::Sender<StreamReader>>;

/// Map of connection id => stream routes guarded by a mutex and ref-counted
pub type ConnectionMap = Arc<Mutex<StreamRoutes>>;

/// Per-connection handle retained by the server.
///
/// New stream notifications are aggregated at the connection level via the shared
/// `new_stream_tx` channel, so this handle only needs to keep the writing path
/// and stream-routing table alive and to provide server-initiated stream opening.
#[derive(Debug)]
pub struct QuicheConnection {
    /// The unique connection ID of the current connection state.
    conn_id: u64,
    /// The remote address of this connection's peer.
    peer_addr: SocketAddr,

    stream_event_send: mpsc::Sender<StreamWriter>,
    stream_event_send_notify: Arc<Notify>,
    connections: ConnectionMap,

    stream_create_recv: mpsc::Receiver<AcceptedRemoteStream>,

    /// All currently open streams accepted from any connection.
    active_streams: Vec<AcceptedRemoteStream>,
}

impl QuicheConnection {
    pub(crate) const STREAM_BUFFER_SIZE: usize = 65_536;

    /// Build a connection handle and its paired driver.
    ///
    /// * `new_stream_tx` / `closed_stream_tx` are server-wide channels so that
    ///   all connections funnel their stream events into a single
    ///   `tokio::select!` in [`QuicheServerSocket::tick`].
    /// * `peer_addr` is the remote address of the peer and is embedded in every
    ///   stream opened through this connection.
    pub fn build(conn_id: u64, peer_addr: SocketAddr) -> (Self, StreamQuicDriver) {
        let (stream_event_send, stream_event_recv) = mpsc::channel(128);
        let (stream_create_send, stream_create_recv) = mpsc::channel(16);

        let write_notify = Arc::new(Notify::new());
        let connections: ConnectionMap = Arc::new(Mutex::new(HashMap::new()));

        let driver = StreamQuicDriver {
            conn_id,
            stream_event_recv,
            new_stream_tx: stream_create_send.clone(),
            connections: connections.clone(),
            stream_event_send: stream_event_send.clone(),
            write_notify: write_notify.clone(),
            established: false,
            io_worker_buf: vec![0u8; QuicheConnection::STREAM_BUFFER_SIZE],
            peer_addr,
        };

        let handle = Self {
            conn_id,
            peer_addr,
            stream_event_send,
            stream_event_send_notify: write_notify,
            connections,
            stream_create_recv,
            active_streams: Vec::new(),
        };

        (handle, driver)
    }

    /// Open a **bidirectional** stream (both sides can read and write).
    ///
    /// Use IDs matching your role:
    /// - server-initiated: 1, 5, 9, …
    /// - client-initiated: 0, 4, 8, …
    pub fn open_bidi_stream(&self, stream_id: u64) -> BiDirectionalQuicheStream {
        let (read_tx, read_rx) = mpsc::channel(256);
        let mut connections = self.connections.lock().unwrap();
        connections.insert(stream_id, read_tx);
        BiDirectionalQuicheStream {
            read: ReadHalf {
                stream_id,
                read_rx,
                conn_id: self.conn_id,
                peer_addr: self.peer_addr,
            },
            write: WriteHalf {
                stream_id,
                conn_id: self.conn_id,
                peer_addr: self.peer_addr,
                write_tx: self.stream_event_send.clone(),
                write_notify: self.stream_event_send_notify.clone(),
            },
        }
    }

    /// Open a **unidirectional** stream — you write, the remote reads.
    ///
    /// Use IDs matching your role:
    /// - server-initiated uni: 3, 7, 11, …
    /// - client-initiated uni: 2, 6, 10, …
    pub fn open_uni_stream(&self, stream_id: u64) -> WriteableQuicheStream {
        WriteableQuicheStream {
            inner: WriteHalf {
                stream_id,
                conn_id: self.conn_id,
                peer_addr: self.peer_addr,
                write_tx: self.stream_event_send.clone(),
                write_notify: self.stream_event_send_notify.clone(),
            },
        }
    }

    pub async fn tick(&mut self) -> (u64, bool) {
        let mut closed = false;

        tokio::select! {
            Some(stream) = self.stream_create_recv.recv() => {
                self.active_streams.push(stream);

                // TODO: process new streams
            },
            Some(Some(event)) = async {
                let mut fut = FuturesUnordered::new();
                for stream in &mut self.active_streams {
                    fut.push(stream.read());
                }

                if !fut.is_empty() {
                    fut.next().await
                } else {
                    std::future::pending::<()>().await;
                    None
                }
            } => {
                log::info!("[{:?}] Stream data received — {event}", self.conn_id);

                if event.fin {
                    self.active_streams.retain(|s| s.stream_id() != event.stream_id);

                    log::info!(
                        "[{:?}] Stream {:?} closed, removing from tracked list, left = {:?}",
                        self.conn_id,
                        event.stream_id,
                        self.active_streams.len()
                    );

                    if event.closed {
                        closed = true;
                    }
                } else {
                    // TODO: process stream writes - push into a closure maybe?
                }
            }
        }

        (self.conn_id, closed)
    }
}
