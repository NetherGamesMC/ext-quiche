use crate::quiche::driver::StreamQuicDriver;
use bytes::Bytes;
use ext_php_rs::types::Zval;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

pub type StreamId = u64;
pub type StreamData = Option<Bytes>;
pub type StreamFin = bool;

/// Outbound chunk: the PHP layer sends these into the per-connection write
/// channel; the driver pulls them off in `process_writes`.
pub type StreamWriter = (StreamId, StreamData, StreamFin);

/// A single PHP-callable slot. `None` until PHP registers a callback.
pub type CallbackSlot = Arc<parking_lot::Mutex<Option<Zval>>>;

/// Per-stream callback slots. Each stream has its own pair so PHP can hook
/// data delivery and disconnect notification independently.
pub struct StreamSlots {
    pub on_data: CallbackSlot,
    pub on_close: CallbackSlot,
}

/// Map of stream_id → callback slots. Owned by the server (PHP thread);
/// a clone lives on each `ConnOpenHandle` so PHP stream objects can
/// register their own slot when opening server-initiated streams.
pub type PhpStreamCallbacks =
    Arc<parking_lot::Mutex<HashMap<StreamId, StreamSlots>>>;

pub(crate) const STREAM_BUFFER_SIZE: usize = 65_536;

// ─────────────────────────────────────────────────────────────────────────────
// DriverEvent — emitted by every driver into the server's shared event channel
// ─────────────────────────────────────────────────────────────────────────────

/// Events emitted by `StreamQuicDriver` into the server's shared event mpsc.
///
/// Decoupling the driver from the server means we no longer need a per-tick
/// `FuturesUnordered` over connections: each driver wakes the server only
/// when it has work, and idle connections cost zero CPU.
#[derive(Debug)]
pub enum DriverEvent {
    /// The remote peer opened a new bidirectional stream.
    NewBidi(StreamId),
    /// The remote peer opened a new unidirectional stream.
    NewUni(StreamId),
    /// A data chunk arrived on `stream_id`.
    Data {
        stream_id: StreamId,
        data: Bytes,
        fin: bool,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// ConnOpenHandle — clonable handle held by PHP stream objects
// ─────────────────────────────────────────────────────────────────────────────

/// Lightweight handle cloned into PHP stream objects so they can open new
/// streams on the same connection without retaining the full `ConnState`.
///
/// The `php_stream_callbacks` field wraps `Zval` (`!Send`); the handle is
/// only ever accessed from the PHP thread, so this is sound.
#[derive(Clone)]
pub struct ConnOpenHandle {
    pub conn_id: u64,
    pub peer_addr: SocketAddr,
    pub(crate) write_tx: mpsc::Sender<StreamWriter>,
    pub(crate) write_notify: Arc<Notify>,
    pub(crate) php_stream_callbacks: PhpStreamCallbacks,
}

impl ConnOpenHandle {
    /// Register slots for a server-initiated bidirectional stream.
    ///
    /// No async wiring is needed: when the peer responds on this stream the
    /// driver will route the data through the shared event channel, and the
    /// dispatcher uses `php_stream_callbacks[stream_id]` to find the slots.
    pub fn prepare_bidi_stream(
        &self,
        stream_id: StreamId,
    ) -> (
        mpsc::Sender<StreamWriter>,
        Arc<Notify>,
        CallbackSlot,
        CallbackSlot,
    ) {
        let (on_data, on_close) = make_slot_pair();
        self.php_stream_callbacks.lock().insert(
            stream_id,
            StreamSlots {
                on_data: on_data.clone(),
                on_close: on_close.clone(),
            },
        );
        (
            self.write_tx.clone(),
            self.write_notify.clone(),
            on_data,
            on_close,
        )
    }

    /// Register slots for a server-initiated unidirectional stream. The
    /// `on_data` slot is allocated for uniformity but never invoked (the
    /// peer cannot send data back on a server-initiated uni stream).
    pub fn prepare_uni_stream(
        &self,
        stream_id: StreamId,
    ) -> (mpsc::Sender<StreamWriter>, Arc<Notify>, CallbackSlot) {
        let (on_data, on_close) = make_slot_pair();
        self.php_stream_callbacks.lock().insert(
            stream_id,
            StreamSlots {
                on_data,
                on_close: on_close.clone(),
            },
        );
        (self.write_tx.clone(), self.write_notify.clone(), on_close)
    }
}

#[allow(clippy::arc_with_non_send_sync)]
pub fn make_slot_pair() -> (CallbackSlot, CallbackSlot) {
    (
        Arc::new(parking_lot::Mutex::new(None)),
        Arc::new(parking_lot::Mutex::new(None)),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// ConnState — per-connection record retained by the server
// ─────────────────────────────────────────────────────────────────────────────

/// Server-side per-connection record. There is no per-connection task or
/// `tick()` anymore — the driver pumps events into a shared channel and the
/// server's main loop dispatches them.
pub struct ConnState {
    pub peer_addr: SocketAddr,
    pub php_stream_callbacks: PhpStreamCallbacks,
    pub open_handle: ConnOpenHandle,
}

impl ConnState {
    /// Build the per-connection record + its driver, wired to the shared
    /// `server_event_tx` so reads route directly into the server.
    ///
    /// `conn_close_tx` carries terminal close signals on a separate small
    /// channel so a saturated event channel can never lose a close event.
    pub fn build(
        conn_id: u64,
        peer_addr: SocketAddr,
        server_event_tx: mpsc::Sender<(u64, DriverEvent)>,
        conn_close_tx: mpsc::Sender<(u64, bool)>,
    ) -> (Self, StreamQuicDriver) {
        let (write_tx, stream_event_recv) = mpsc::channel(256);
        let write_notify = Arc::new(Notify::new());

        #[allow(clippy::arc_with_non_send_sync)]
        let php_stream_callbacks: PhpStreamCallbacks =
            Arc::new(parking_lot::Mutex::new(HashMap::new()));

        let open_handle = ConnOpenHandle {
            conn_id,
            peer_addr,
            write_tx: write_tx.clone(),
            write_notify: write_notify.clone(),
            php_stream_callbacks: php_stream_callbacks.clone(),
        };

        let driver = StreamQuicDriver {
            conn_id,
            stream_event_recv,
            event_tx: server_event_tx,
            write_notify: write_notify.clone(),
            established: false,
            io_worker_buf: vec![0u8; STREAM_BUFFER_SIZE],
            pending_writes: HashMap::new(),
            seen_streams: HashSet::new(),
            conn_close_tx,
        };

        let state = Self {
            peer_addr,
            php_stream_callbacks,
            open_handle,
        };

        (state, driver)
    }
}
