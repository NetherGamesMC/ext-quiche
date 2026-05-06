use crate::quiche::conn::{CallbackSlot, ConnOpenHandle, StreamWriter};
use ext_php_rs::binary::Binary;
use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

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
    pub fn write(&self, data: Binary<u8>, fin: bool) -> PhpResult<()> {
        let bytes = bytes::Bytes::copy_from_slice(&data);
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
        let (write_tx, write_notify, on_data, on_close) =
            self.conn_handle.prepare_bidi_stream(stream_id);
        Ok(BidiStream {
            conn_id: self.conn_handle.conn_id,
            stream_id,
            write_tx,
            write_notify,
            on_data,
            on_close,
        })
    }

    /// Open a new **unidirectional** stream toward the client on this connection.
    ///
    /// The returned `UniStream` exposes `write` and `setOnClose`.
    pub fn open_uni_stream(&self, stream_id: u64) -> PhpResult<UniStream> {
        let (write_tx, write_notify, on_close) =
            self.conn_handle.prepare_uni_stream(stream_id);
        Ok(UniStream {
            conn_id: self.conn_handle.conn_id,
            stream_id,
            write_tx,
            write_notify,
            on_close,
        })
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
        let (write_tx, write_notify, on_data, on_close) =
            self.conn_handle.prepare_bidi_stream(stream_id);
        Ok(BidiStream {
            conn_id: self.conn_handle.conn_id,
            stream_id,
            write_tx,
            write_notify,
            on_data,
            on_close,
        })
    }

    /// Open a new **unidirectional** stream toward the client on this connection.
    pub fn open_uni_stream(&self, stream_id: u64) -> PhpResult<UniStream> {
        let (write_tx, write_notify, on_close) =
            self.conn_handle.prepare_uni_stream(stream_id);
        Ok(UniStream {
            conn_id: self.conn_handle.conn_id,
            stream_id,
            write_tx,
            write_notify,
            on_close,
        })
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
    pub fn write(&self, data: Binary<u8>, fin: bool) -> PhpResult<()> {
        let bytes = bytes::Bytes::copy_from_slice(&data);
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
    pub fn write(&self, data: Binary<u8>, fin: bool) -> PhpResult<()> {
        let bytes = bytes::Bytes::copy_from_slice(&data);
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

    #[php(getter)]
    pub fn get_conn_id(&self) -> u64 {
        self.conn_id
    }

    #[php(getter)]
    pub fn get_stream_id(&self) -> u64 {
        self.stream_id
    }
}
