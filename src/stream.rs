use crate::quiche::conn::{CallbackSlot, ConnOpenHandle, StreamPriority, StreamWriter};
use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

/// quiche stores urgency in a `u8`; the type itself caps the upper bound.
/// Streams default to urgency `127` if `setPriority` is never called
/// (see quiche `DEFAULT_URGENCY` in `quiche/src/stream/mod.rs`).
const MAX_URGENCY: u32 = u8::MAX as u32;

/// Shared helper: validate a callable and store it in the slot.
fn install_slot(slot: &CallbackSlot, callback: &Zval, name: &str) -> PhpResult<()> {
    if !callback.is_callable() {
        return Err(PhpException::default(format!(
            "{name} expects a callable"
        )));
    }
    *slot.lock() = Some(callback.shallow_clone());
    Ok(())
}

/// Shared helper: enqueue a priority change for the driver to apply.
fn submit_priority(
    priority_tx: &mpsc::Sender<StreamPriority>,
    write_notify: &Arc<Notify>,
    stream_id: u64,
    urgency: u32,
    incremental: bool,
) -> PhpResult<()> {
    if urgency > MAX_URGENCY {
        return Err(PhpException::default(format!(
            "setPriority: urgency must be 0..=255 (got {urgency})"
        )));
    }
    priority_tx
        .try_send((stream_id, urgency as u8, incremental))
        .map_err(|e| PhpException::default(format!("setPriority failed: {e}")))?;
    // Wake the driver so the priority applies before the next write batch.
    write_notify.notify_one();
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// IncomingBidiStream — client opened a bidi stream; server can read + write
// ─────────────────────────────────────────────────────────────────────────────

/// A bidirectional stream opened by the remote peer.
///
/// - Call `setOnData` to receive incoming chunks.
/// - Call `setOnClose` to be notified when the stream/connection ends.
/// - Call `write` to send data back.
/// - Call `openBidiStream` / `openUniStream` to initiate new streams on the
///   same connection.
#[php_class]
#[php(name = "NetherGames\\Quiche\\IncomingBidiStream")]
pub struct IncomingBidiStream {
    pub conn_id: u64,
    pub stream_id: u64,
    pub peer_addr: String,
    pub(crate) write_tx: mpsc::Sender<StreamWriter>,
    pub(crate) write_notify: Arc<Notify>,
    pub(crate) priority_tx: mpsc::Sender<StreamPriority>,
    pub(crate) on_data: CallbackSlot,
    pub(crate) on_close: CallbackSlot,
    pub(crate) conn_handle: ConnOpenHandle,
}

#[php_impl]
impl IncomingBidiStream {
    /// Register a closure invoked for every data chunk on this stream.
    ///
    /// Signature: `function(string $data, bool $fin): void`
    pub fn set_on_data(&mut self, callback: &Zval) -> PhpResult<()> {
        install_slot(&self.on_data, callback, "setOnData")
    }

    /// Register a closure invoked exactly once when this stream is no
    /// longer usable.
    ///
    /// Signature: `function(bool $peerClosed): void`
    /// - `peerClosed = true`  → the peer initiated the close (FIN, peer
    ///   `CONNECTION_CLOSE`, or peer-side timeout).
    /// - `peerClosed = false` → the local server initiated the close.
    pub fn set_on_close(&mut self, callback: &Zval) -> PhpResult<()> {
        install_slot(&self.on_close, callback, "setOnClose")
    }

    /// Write `$data` to the remote peer.  Set `$fin = true` to close the stream.
    ///
    /// Uses a non-blocking channel send so it is safe to call from inside the
    /// `setOnData` closure (which runs within the event loop tick).
    pub fn write(&self, data: String, fin: bool) -> PhpResult<()> {
        let bytes = bytes::Bytes::from(data.into_bytes());
        self.write_tx
            .try_send((self.stream_id, Some(bytes), fin))
            .map_err(|e| PhpException::default(format!("write failed: {e}")))?;
        self.write_notify.notify_one();
        Ok(())
    }

    /// Close the send side of the stream (sends an empty FIN frame).
    pub fn close(&self) -> PhpResult<()> {
        self.write_tx
            .try_send((self.stream_id, Some(bytes::Bytes::new()), true))
            .map_err(|e| PhpException::default(format!("close failed: {e}")))?;
        self.write_notify.notify_one();
        Ok(())
    }

    /// Open a new **bidirectional** stream toward the client on this connection.
    ///
    /// The returned `BidiStream` has `setOnData`, `setOnClose`, and `write`.
    pub fn open_bidi_stream(&self, stream_id: u64) -> PhpResult<BidiStream> {
        let (write_tx, write_notify, on_data, on_close, priority_tx) =
            self.conn_handle.prepare_bidi_stream(stream_id);
        Ok(BidiStream {
            conn_id: self.conn_handle.conn_id,
            stream_id,
            write_tx,
            write_notify,
            priority_tx,
            on_data,
            on_close,
        })
    }

    /// Open a new **unidirectional** stream toward the client on this connection.
    ///
    /// The returned `UniStream` exposes `write` and `setOnClose`.
    pub fn open_uni_stream(&self, stream_id: u64) -> PhpResult<UniStream> {
        let (write_tx, write_notify, on_close, priority_tx) =
            self.conn_handle.prepare_uni_stream(stream_id);
        Ok(UniStream {
            conn_id: self.conn_handle.conn_id,
            stream_id,
            write_tx,
            write_notify,
            priority_tx,
            on_close,
        })
    }

    /// Set this stream's send-priority in quiche's internal scheduler.
    ///
    /// `urgency` is a `u8` (0..=255). **Lower values are sent first**;
    /// streams default to `127` if this is never called. The encoding mirrors
    /// the direction of RFC 9218 HTTP priorities but spans the full byte
    /// rather than the 0..=7 HTTP space.
    ///
    /// `incremental = true` tells the scheduler to round-robin this stream
    /// with peers at the same urgency, instead of draining streams in arrival
    /// order.
    pub fn set_priority(&self, urgency: u32, incremental: bool) -> PhpResult<()> {
        submit_priority(
            &self.priority_tx,
            &self.write_notify,
            self.stream_id,
            urgency,
            incremental,
        )
    }

    #[php(getter)]
    pub fn get_conn_id(&self) -> u64 {
        self.conn_id
    }

    #[php(getter)]
    pub fn get_stream_id(&self) -> u64 {
        self.stream_id
    }

    #[php(getter)]
    pub fn get_peer_addr(&self) -> String {
        self.peer_addr.clone()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IncomingUniStream — client opened a uni stream; server reads only
// ─────────────────────────────────────────────────────────────────────────────

/// A unidirectional stream opened by the remote peer (server reads only).
///
/// - Call `setOnData` to receive incoming chunks.
/// - Call `setOnClose` to be notified when the stream/connection ends.
/// - Call `openBidiStream` / `openUniStream` to initiate new streams toward
///   the same client.
#[php_class]
#[php(name = "NetherGames\\Quiche\\IncomingUniStream")]
pub struct IncomingUniStream {
    pub conn_id: u64,
    pub stream_id: u64,
    pub peer_addr: String,
    pub(crate) priority_tx: mpsc::Sender<StreamPriority>,
    pub(crate) on_data: CallbackSlot,
    pub(crate) on_close: CallbackSlot,
    pub(crate) conn_handle: ConnOpenHandle,
}

#[php_impl]
impl IncomingUniStream {
    /// Register a closure invoked for every data chunk on this stream.
    ///
    /// Signature: `function(string $data, bool $fin): void`
    pub fn set_on_data(&mut self, callback: &Zval) -> PhpResult<()> {
        install_slot(&self.on_data, callback, "setOnData")
    }

    /// Register a closure invoked exactly once when this stream is no
    /// longer usable.
    ///
    /// Signature: `function(bool $peerClosed): void`
    pub fn set_on_close(&mut self, callback: &Zval) -> PhpResult<()> {
        install_slot(&self.on_close, callback, "setOnClose")
    }

    /// Open a new **bidirectional** stream toward the client on this connection.
    pub fn open_bidi_stream(&self, stream_id: u64) -> PhpResult<BidiStream> {
        let (write_tx, write_notify, on_data, on_close, priority_tx) =
            self.conn_handle.prepare_bidi_stream(stream_id);
        Ok(BidiStream {
            conn_id: self.conn_handle.conn_id,
            stream_id,
            write_tx,
            write_notify,
            priority_tx,
            on_data,
            on_close,
        })
    }

    /// Open a new **unidirectional** stream toward the client on this connection.
    pub fn open_uni_stream(&self, stream_id: u64) -> PhpResult<UniStream> {
        let (write_tx, write_notify, on_close, priority_tx) =
            self.conn_handle.prepare_uni_stream(stream_id);
        Ok(UniStream {
            conn_id: self.conn_handle.conn_id,
            stream_id,
            write_tx,
            write_notify,
            priority_tx,
            on_close,
        })
    }

    /// Set this incoming stream's priority in quiche's scheduler.
    /// `urgency` is 0..=255 (lower = higher priority, 127 default).
    /// The priority influences how quiche orders writes when multiple
    /// streams are ready and how it schedules read flow-control credit.
    pub fn set_priority(&self, urgency: u32, incremental: bool) -> PhpResult<()> {
        submit_priority(
            &self.priority_tx,
            &self.conn_handle.write_notify,
            self.stream_id,
            urgency,
            incremental,
        )
    }

    #[php(getter)]
    pub fn get_conn_id(&self) -> u64 {
        self.conn_id
    }

    #[php(getter)]
    pub fn get_stream_id(&self) -> u64 {
        self.stream_id
    }

    #[php(getter)]
    pub fn get_peer_addr(&self) -> String {
        self.peer_addr.clone()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BidiStream — server-initiated bidi stream; server writes + receives via closure
// ─────────────────────────────────────────────────────────────────────────────

/// A bidirectional stream initiated by the server.
///
/// - Call `setOnData` to receive chunks sent by the client on this stream.
/// - Call `setOnClose` to be notified when the stream/connection ends.
/// - Call `write` to send data to the client.
#[php_class]
#[php(name = "NetherGames\\Quiche\\BidiStream")]
pub struct BidiStream {
    pub conn_id: u64,
    pub stream_id: u64,
    pub(crate) write_tx: mpsc::Sender<StreamWriter>,
    pub(crate) write_notify: Arc<Notify>,
    pub(crate) priority_tx: mpsc::Sender<StreamPriority>,
    pub(crate) on_data: CallbackSlot,
    pub(crate) on_close: CallbackSlot,
}

#[php_impl]
impl BidiStream {
    /// Register a closure invoked when the client sends data on this stream.
    ///
    /// Signature: `function(string $data, bool $fin): void`
    pub fn set_on_data(&mut self, callback: &Zval) -> PhpResult<()> {
        install_slot(&self.on_data, callback, "setOnData")
    }

    /// Register a closure invoked exactly once when this stream is no
    /// longer usable.
    ///
    /// Signature: `function(bool $peerClosed): void`
    pub fn set_on_close(&mut self, callback: &Zval) -> PhpResult<()> {
        install_slot(&self.on_close, callback, "setOnClose")
    }

    /// Write `$data` to the client.  Set `$fin = true` to close the stream.
    pub fn write(&self, data: String, fin: bool) -> PhpResult<()> {
        let bytes = bytes::Bytes::from(data.into_bytes());
        self.write_tx
            .try_send((self.stream_id, Some(bytes), fin))
            .map_err(|e| PhpException::default(format!("write failed: {e}")))?;
        self.write_notify.notify_one();
        Ok(())
    }

    /// Close the stream (sends an empty FIN frame).
    pub fn close(&self) -> PhpResult<()> {
        self.write_tx
            .try_send((self.stream_id, Some(bytes::Bytes::new()), true))
            .map_err(|e| PhpException::default(format!("close failed: {e}")))?;
        self.write_notify.notify_one();
        Ok(())
    }

    /// Set this stream's priority in quiche's scheduler.
    /// `urgency` is 0..=255 (lower = higher priority, 127 default).
    pub fn set_priority(&self, urgency: u32, incremental: bool) -> PhpResult<()> {
        submit_priority(
            &self.priority_tx,
            &self.write_notify,
            self.stream_id,
            urgency,
            incremental,
        )
    }

    #[php(getter)]
    pub fn get_conn_id(&self) -> u64 {
        self.conn_id
    }

    #[php(getter)]
    pub fn get_stream_id(&self) -> u64 {
        self.stream_id
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UniStream — server-initiated uni stream; server writes only
// ─────────────────────────────────────────────────────────────────────────────

/// A unidirectional stream initiated by the server (server writes only).
#[php_class]
#[php(name = "NetherGames\\Quiche\\UniStream")]
pub struct UniStream {
    pub conn_id: u64,
    pub stream_id: u64,
    pub(crate) write_tx: mpsc::Sender<StreamWriter>,
    pub(crate) write_notify: Arc<Notify>,
    pub(crate) priority_tx: mpsc::Sender<StreamPriority>,
    pub(crate) on_close: CallbackSlot,
}

#[php_impl]
impl UniStream {
    /// Register a closure invoked exactly once when this stream is no
    /// longer usable.
    ///
    /// Signature: `function(bool $peerClosed): void`
    pub fn set_on_close(&mut self, callback: &Zval) -> PhpResult<()> {
        install_slot(&self.on_close, callback, "setOnClose")
    }

    /// Write `$data` to the client.  Set `$fin = true` to close the stream.
    pub fn write(&self, data: String, fin: bool) -> PhpResult<()> {
        let bytes = bytes::Bytes::from(data.into_bytes());
        self.write_tx
            .try_send((self.stream_id, Some(bytes), fin))
            .map_err(|e| PhpException::default(format!("write failed: {e}")))?;
        self.write_notify.notify_one();
        Ok(())
    }

    /// Close the stream (sends an empty FIN frame).
    pub fn close(&self) -> PhpResult<()> {
        self.write_tx
            .try_send((self.stream_id, Some(bytes::Bytes::new()), true))
            .map_err(|e| PhpException::default(format!("close failed: {e}")))?;
        self.write_notify.notify_one();
        Ok(())
    }

    /// Set this stream's priority in quiche's scheduler.
    /// `urgency` is 0..=255 (lower = higher priority, 127 default).
    pub fn set_priority(&self, urgency: u32, incremental: bool) -> PhpResult<()> {
        submit_priority(
            &self.priority_tx,
            &self.write_notify,
            self.stream_id,
            urgency,
            incremental,
        )
    }

    #[php(getter)]
    pub fn get_conn_id(&self) -> u64 {
        self.conn_id
    }

    #[php(getter)]
    pub fn get_stream_id(&self) -> u64 {
        self.stream_id
    }
}
