use crate::config::Config;
use crate::quiche::conn::{CallbackSlot, ConnState, DriverEvent, StreamSlots, make_slot_pair};
use crate::stream::{IncomingBidiStream, IncomingUniStream};
use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::{IntoZval, IntoZvalDyn};
use ext_php_rs::types::{ZendCallable, ZendClassObject, ZendObject, Zval};
use futures::stream::SelectAll;
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio_eventfd::EventFd;
use tokio_quiche::{
    ConnectionParams, InitialQuicConnection, QuicConnectionStream,
    listen,
    metrics::DefaultMetrics,
    settings::{CertificateKind, Hooks, TlsCertificatePaths},
};

/// Shared driver→server channel capacity. Sized generously so a slow PHP
/// drain doesn't trigger backpressure under bursty workloads. With ~60
/// connections each producing single-digit in-flight events between PHP
/// ticks, 8 192 leaves substantial headroom.
const SERVER_EVENT_CHANNEL_CAPACITY: usize = 8192;

/// Dedicated terminal-close channel. Separate from the event channel so a
/// saturated event channel cannot drop a close signal and leak a connection.
const CONN_CLOSE_CHANNEL_CAPACITY: usize = 1024;

pub struct QuicheServerSocketImpl {
    /// Merged stream of all listeners, built once at bind. `next()` is awaited
    /// directly in the select! — no per-tick allocation.
    listener_stream: SelectAll<QuicConnectionStream<DefaultMetrics>>,

    /// Per-connection records keyed by conn_id.
    connections: HashMap<u64, ConnState>,
    id_counter: AtomicU64,

    /// PHP `function($incomingStream): void` callback fired when the peer
    /// opens a new stream.
    #[allow(clippy::arc_with_non_send_sync)]
    stream_callback: Arc<Mutex<Option<Zval>>>,

    /// Cloned into every driver. Drivers tag events with their conn_id.
    server_event_tx: mpsc::Sender<(u64, DriverEvent)>,
    server_event_rx: mpsc::Receiver<(u64, DriverEvent)>,

    /// Cloned into every driver. Fired from `on_conn_close` so the server
    /// prunes its handle even when the connection had no streams. The
    /// `bool` is the peer-initiated flag forwarded to PHP `on_close`.
    conn_close_tx: mpsc::Sender<(u64, bool)>,
    conn_close_rx: mpsc::Receiver<(u64, bool)>,
}

impl QuicheServerSocketImpl {
    pub async fn bind(
        addrs: &[&str],
        config: &Config,
        #[allow(clippy::arc_with_non_send_sync)] callback: Arc<Mutex<Option<Zval>>>,
    ) -> std::io::Result<Self> {
        if addrs.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "bind requires at least one socket address",
            ));
        }

        let sockets: Vec<UdpSocket> =
            futures::future::try_join_all(addrs.iter().map(|addr| UdpSocket::bind(addr))).await?;

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
        // SelectAll panics on empty iterators in some versions; we guarded
        // against empty addrs above.
        let listener_stream = futures::stream::select_all(listeners);

        let (server_event_tx, server_event_rx) =
            mpsc::channel(SERVER_EVENT_CHANNEL_CAPACITY);
        let (conn_close_tx, conn_close_rx) = mpsc::channel(CONN_CLOSE_CHANNEL_CAPACITY);

        Ok(Self {
            listener_stream,
            connections: HashMap::new(),
            id_counter: AtomicU64::new(0),
            stream_callback: callback,
            server_event_tx,
            server_event_rx,
            conn_close_tx,
            conn_close_rx,
        })
    }

    /// Close the server: fire `on_close(peer_closed=false)` for every live
    /// stream, drop all per-connection state, and stop accepting new
    /// connections.
    pub fn close(&mut self) {
        let initial = self.connections.len();
        log::info!("close: invoked (active_conns={initial})");

        for (conn_id, state) in self.connections.drain() {
            let mut slots_map = state.php_stream_callbacks.lock();
            let n = slots_map.len();
            log::info!(
                "close: firing on_close(false) for {n} stream(s) on conn={conn_id}"
            );
            for (_stream_id, slots) in slots_map.drain() {
                Self::fire_on_close(&slots.on_close, false);
            }
        }
        // `select_all(empty_iter)` panics in some futures versions;
        // `SelectAll::new()` is the supported empty constructor.
        self.listener_stream = SelectAll::new();

        log::info!("close: complete");
    }

    /// Run the event loop until the `event_fd` fires.
    ///
    /// The loop has exactly four select arms — listener, driver event,
    /// connection close, timer — none of which allocate per iteration.
    pub async fn tick(&mut self, mut event_fd: Option<&mut EventFd>) {
        log::info!(
            "tick: entering server loop (active_conns={}, has_eventfd={})",
            self.connections.len(),
            event_fd.is_some()
        );

        // The InitialQuicConnection variant is large (~16 KiB); box it so the
        // enum stays small and the hot driver/close paths don't pay for it.
        enum Event {
            NewConn(Option<std::io::Result<Box<InitialQuicConnection<UdpSocket, DefaultMetrics>>>>),
            Driver(Option<(u64, DriverEvent)>),
            ConnClosed(Option<(u64, bool)>),
            Timer,
        }

        loop {
            let event = {
                let efd = event_fd.as_deref_mut();

                tokio::select! {
                    biased;

                    // Drain ready events first under contention so PHP sees
                    // backpressure relief promptly.
                    Some((conn_id, ev)) = self.server_event_rx.recv() => {
                        Event::Driver(Some((conn_id, ev)))
                    }

                    Some(close_msg) = self.conn_close_rx.recv() => {
                        Event::ConnClosed(Some(close_msg))
                    }

                    Some(conn) = self.listener_stream.next() => {
                        Event::NewConn(Some(conn.map(Box::new)))
                    }

                    _ = async {
                        if let Some(efd) = efd {
                            match efd.read_u64().await {
                                Ok(v) => log::info!("tick: eventfd readable, counter={v}"),
                                Err(e) => log::warn!("tick: eventfd read error: {e}"),
                            }
                        } else {
                            std::future::pending::<()>().await
                        }
                    } => Event::Timer,
                }
            };

            match event {
                Event::NewConn(Some(Ok(conn_box))) => {
                    let conn = *conn_box;
                    let conn_id = self.id_counter.fetch_add(1, Ordering::Relaxed);
                    let peer_addr = conn.peer_addr();
                    let (state, driver) = ConnState::build(
                        conn_id,
                        peer_addr,
                        self.server_event_tx.clone(),
                        self.conn_close_tx.clone(),
                    );
                    conn.start(driver);
                    self.connections.insert(conn_id, state);
                    log::info!(
                        "[{conn_id}] New QUIC connection accepted from {peer_addr} (active={})",
                        self.connections.len()
                    );
                }
                Event::NewConn(Some(Err(e))) => {
                    log::warn!("Connection accept error: {e:?}");
                }
                Event::NewConn(None) => {
                    log::warn!("QUIC listener stream ended");
                }

                Event::ConnClosed(Some((conn_id, peer_initiated))) => {
                    if let Some(state) = self.connections.remove(&conn_id) {
                        // Fire on_close for each remaining stream so PHP
                        // sees a disconnect for every active stream.
                        let mut slots_map = state.php_stream_callbacks.lock();
                        for (_stream_id, slots) in slots_map.drain() {
                            Self::fire_on_close(&slots.on_close, peer_initiated);
                        }
                        log::info!(
                            "[{conn_id}] Connection closed peer_initiated={peer_initiated} (active={})",
                            self.connections.len()
                        );
                    }
                }
                Event::ConnClosed(None) => {
                    log::warn!("conn_close_rx unexpectedly closed");
                }

                Event::Driver(Some((conn_id, ev))) => {
                    self.dispatch_driver_event(conn_id, ev);
                }
                Event::Driver(None) => {
                    log::warn!("server_event_rx unexpectedly closed");
                }

                Event::Timer => {
                    log::info!("tick: timer arm fired, exiting server loop");
                    break;
                }
            }
        }

        log::info!(
            "tick: exited (active_conns={})",
            self.connections.len()
        );
    }

    /// Translate a single `DriverEvent` into the corresponding PHP-side
    /// dispatch. All Zval interaction happens here on the PHP thread.
    fn dispatch_driver_event(&mut self, conn_id: u64, ev: DriverEvent) {
        let Some(state) = self.connections.get(&conn_id) else {
            // Late event for an already-closed connection — drop it.
            return;
        };

        match ev {
            DriverEvent::NewBidi(stream_id) => {
                let (on_data, on_close) = make_slot_pair();
                state.php_stream_callbacks.lock().insert(
                    stream_id,
                    StreamSlots {
                        on_data: on_data.clone(),
                        on_close: on_close.clone(),
                    },
                );

                let incoming = IncomingBidiStream {
                    conn_id,
                    stream_id,
                    peer_addr: state.peer_addr.to_string(),
                    write_tx: state.open_handle.write_tx.clone(),
                    write_notify: state.open_handle.write_notify.clone(),
                    on_data,
                    on_close,
                    conn_handle: state.open_handle.clone(),
                };
                Self::call_stream_callback(
                    &self.stream_callback,
                    ZendClassObject::new(incoming).into(),
                );
            }

            DriverEvent::NewUni(stream_id) => {
                let (on_data, on_close) = make_slot_pair();
                state.php_stream_callbacks.lock().insert(
                    stream_id,
                    StreamSlots {
                        on_data: on_data.clone(),
                        on_close: on_close.clone(),
                    },
                );

                let incoming = IncomingUniStream {
                    conn_id,
                    stream_id,
                    peer_addr: state.peer_addr.to_string(),
                    on_data,
                    on_close,
                    conn_handle: state.open_handle.clone(),
                };
                Self::call_stream_callback(
                    &self.stream_callback,
                    ZendClassObject::new(incoming).into(),
                );
            }

            DriverEvent::Data {
                stream_id,
                data,
                fin,
            } => {
                let on_data_slot = {
                    let cbs = state.php_stream_callbacks.lock();
                    cbs.get(&stream_id).map(|s| s.on_data.clone())
                };

                if let Some(slot) = on_data_slot {
                    let on_data = slot.lock();
                    if let Some(ref cb_zval) = *on_data
                        && let Ok(callable) = ZendCallable::new(cb_zval)
                    {
                        let data_str: &str = unsafe { std::str::from_utf8_unchecked(&data) };
                        let _ = callable.try_call(vec![
                            &data_str as &dyn IntoZvalDyn,
                            &fin as &dyn IntoZvalDyn,
                        ]);
                    }
                }

                // Peer FIN: stream is done from peer's side; fire on_close
                // and remove the slot. peer_closed = true because the FIN
                // came from the remote side.
                if fin {
                    let removed = state.php_stream_callbacks.lock().remove(&stream_id);
                    if let Some(slots) = removed {
                        Self::fire_on_close(&slots.on_close, true);
                    }
                }
            }
        }
    }

    /// Invoke a stream's `on_close` callback exactly once with the peer-
    /// closed flag. Errors are swallowed so a misbehaving callback can't
    /// take down the loop.
    fn fire_on_close(slot: &CallbackSlot, peer_closed: bool) {
        let cb = slot.lock();
        if let Some(ref cb_zval) = *cb
            && let Ok(callable) = ZendCallable::new(cb_zval)
        {
            let _ = callable.try_call(vec![&peer_closed as &dyn IntoZvalDyn]);
        }
    }

    /// Converts `obj` (a `ZBox<ZendObject>`) into a `Zval` and calls the
    /// stream callback with it. Silently ignores errors so a bad callback
    /// does not crash the server loop.
    fn call_stream_callback(
        #[allow(clippy::arc_with_non_send_sync)] callback: &Arc<Mutex<Option<Zval>>>,
        obj: ZBox<ZendObject>,
    ) {
        if let Ok(obj_zval) = obj.into_zval(false) {
            let cb = callback.lock();
            if let Some(ref cb_zval) = *cb {
                if let Ok(callable) = ZendCallable::new(cb_zval) {
                    let _ = callable.try_call(vec![&obj_zval as &dyn IntoZvalDyn]);
                }
            }
        }
    }
}
