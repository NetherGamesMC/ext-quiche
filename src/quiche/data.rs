use crate::quiche::conn::{StreamReader, StreamWriter};
use crate::quiche::error::StreamError;
use bytes::Bytes;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

/// A single chunk of data received on an incoming stream.
///
/// Encapsulates which stream the data arrived on, which peer sent it,
/// the raw payload, and whether this is the final chunk (FIN).
#[derive(Debug, Clone)]
pub struct StreamDataEvent {
    pub conn_id: u64,
    /// The QUIC stream ID.
    pub stream_id: u64,
    /// The remote peer address that established this connection.
    pub peer_addr: SocketAddr,
    /// The received payload.
    pub data: Option<Bytes>,
    /// `true` if this is the last chunk on the stream (QUIC FIN flag).
    pub fin: bool,
    /// `true` if this connection is closed.
    pub closed: bool,
}

impl fmt::Display for StreamDataEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[stream={}][peer={}][fin={}] {} byte(s): {:?}",
            self.stream_id,
            self.peer_addr,
            self.fin,
            self.data.as_ref().map(|d| d.len()).unwrap_or(0),
            self.data,
        )
    }
}

#[derive(Debug)]
pub struct ReadHalf {
    pub conn_id: u64,
    pub stream_id: u64,
    pub peer_addr: SocketAddr,
    pub read_rx: mpsc::Receiver<StreamReader>,
}

#[derive(Debug)]
pub struct WriteHalf {
    pub conn_id: u64,
    pub stream_id: u64,
    pub peer_addr: SocketAddr,
    pub write_tx: mpsc::Sender<StreamWriter>,
    pub write_notify: Arc<Notify>,
}

impl ReadHalf {
    pub async fn read(&mut self) -> Option<StreamDataEvent> {
        self.read_rx
            .recv()
            .await
            .map(|(data, fin, closed)| StreamDataEvent {
                conn_id: self.conn_id,
                stream_id: self.stream_id,
                peer_addr: self.peer_addr,
                data,
                fin,
                closed,
            })
    }
}

impl WriteHalf {
    pub async fn write(&self, data: Bytes, fin: bool) -> Result<(), StreamError> {
        self.write_tx
            .send((self.stream_id, Some(data), fin))
            .await?;
        self.write_notify.notify_one();
        Ok(())
    }
}
