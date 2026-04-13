use std::net::SocketAddr;

use crate::quiche::data::{ReadHalf, StreamDataEvent, WriteHalf};
use crate::quiche::error::StreamError;
use bytes::Bytes;

#[derive(Debug)]
pub enum AcceptedRemoteStream {
    /// Remote opened a **unidirectional** stream — we can only read.
    Readable(ReadableQuicheStream),
    /// Remote opened a **bidirectional** stream — we can read and write.
    BiDirectional(BiDirectionalQuicheStream),
}

impl AcceptedRemoteStream {
    /// Return the stream ID regardless of variant.
    pub fn stream_id(&self) -> u64 {
        match self {
            Self::Readable(s) => s.inner.stream_id,
            Self::BiDirectional(s) => s.read.stream_id,
        }
    }

    /// Return the peer address of the connection that opened this stream.
    pub fn peer_addr(&self) -> SocketAddr {
        match self {
            Self::Readable(s) => s.inner.peer_addr,
            Self::BiDirectional(s) => s.read.peer_addr,
        }
    }

    /// Receive the next chunk from whichever variant this stream is.
    ///
    /// Returns `None` when the stream is closed.  This is a concrete `async fn`
    /// (not a trait method) so all variants produce the **same future type**,
    /// making it safe to push into `FuturesUnordered`.
    pub async fn read(&mut self) -> Option<StreamDataEvent> {
        match self {
            Self::Readable(s) => s.read().await,
            Self::BiDirectional(s) => s.read().await,
        }
    }
}

impl BiDirectionalQuicheStream {
    /// Consume this stream and return its (readable, writeable) halves.
    pub fn split(self) -> (ReadableQuicheStream, WriteableQuicheStream) {
        (
            ReadableQuicheStream { inner: self.read },
            WriteableQuicheStream { inner: self.write },
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Read and Write traits
// ─────────────────────────────────────────────────────────────────────────────

/// A stream you can **only read** from (the remote side writes).
///
/// Typically obtained by accepting a remote-initiated unidirectional stream.
#[derive(Debug)]
pub struct ReadableQuicheStream {
    pub inner: ReadHalf,
}

/// A stream you can **only write** to (the remote side reads).
///
/// Typically obtained by opening a local unidirectional stream.
#[derive(Debug)]
pub struct WriteableQuicheStream {
    pub inner: WriteHalf,
}

#[derive(Debug)]
pub struct BiDirectionalQuicheStream {
    pub read: ReadHalf,
    pub write: WriteHalf,
}

impl QuicheStream for ReadableQuicheStream {
    fn conn_id(&self) -> u64 {
        self.inner.conn_id
    }
    fn stream_id(&self) -> u64 {
        self.inner.stream_id
    }
}

impl QuicheReadable for ReadableQuicheStream {
    async fn read(&mut self) -> Option<StreamDataEvent> {
        self.inner.read().await
    }
}

impl QuicheStream for WriteableQuicheStream {
    fn conn_id(&self) -> u64 {
        self.inner.conn_id
    }
    fn stream_id(&self) -> u64 {
        self.inner.stream_id
    }
}

impl QuicheWriteable for WriteableQuicheStream {
    async fn write(&self, data: Bytes, fin: bool) -> Result<(), StreamError> {
        self.inner.write(data, fin).await
    }
}

impl QuicheStream for BiDirectionalQuicheStream {
    fn conn_id(&self) -> u64 {
        self.read.conn_id
    }
    fn stream_id(&self) -> u64 {
        self.read.stream_id
    }
}

impl QuicheReadable for BiDirectionalQuicheStream {
    async fn read(&mut self) -> Option<StreamDataEvent> {
        self.read.read().await
    }
}

impl QuicheWriteable for BiDirectionalQuicheStream {
    async fn write(&self, data: Bytes, fin: bool) -> Result<(), StreamError> {
        self.write.write(data, fin).await
    }
}

pub trait QuicheStream: Send {
    fn conn_id(&self) -> u64;
    fn stream_id(&self) -> u64;
}

pub trait QuicheReadable: QuicheStream {
    /// Receive the next chunk.  Returns `None` when the stream is closed.
    /// The `bool` is the QUIC FIN flag (last chunk on this stream).
    fn read(&mut self) -> impl Future<Output = Option<StreamDataEvent>> + Send;
}

pub trait QuicheWriteable: QuicheStream {
    /// Send `data`.  `fin = true` closes the send side of the stream.
    fn write(&self, data: Bytes, fin: bool)
    -> impl Future<Output = Result<(), StreamError>> + Send;
}
