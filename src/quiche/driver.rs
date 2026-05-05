use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::quiche::conn::{DriverEvent, StreamPriority, StreamWriter};
use bytes::Bytes;
use tokio::sync::{mpsc, Notify};
use tokio_quiche::metrics::Metrics;
use tokio_quiche::{
    ApplicationOverQuic, QuicResult,
    quic::{HandshakeInfo, QuicheConnection},
};

/// Server-initiated stream IDs have bit 0 == 1 (from the server's perspective);
/// client-initiated IDs have bit 0 == 0. We only fire `NewBidi`/`NewUni` for
/// the latter — server-initiated streams are already known to the PHP layer
/// at the time it called `openBidiStream`/`openUniStream`.
const STREAM_ID_INITIATOR_MASK: u64 = 0b01;
const STREAM_ID_DIRECTION_MASK: u64 = 0b10;

pub struct StreamQuicDriver {
    pub conn_id: u64,

    /// Outbound chunks pushed by PHP via the per-connection write channel.
    pub stream_event_recv: mpsc::Receiver<StreamWriter>,

    /// Stream priority commands pushed by PHP via `setPriority`. Drained at
    /// the top of `process_writes` so a priority change applies before the
    /// pending writes for the same stream are flushed.
    pub priority_recv: mpsc::Receiver<StreamPriority>,

    /// Shared event channel into the server. Tagged with `conn_id` so the
    /// server can find the right `ConnState` without per-connection routing.
    pub event_tx: mpsc::Sender<(u64, DriverEvent)>,

    /// Dedicated close channel — separate from `event_tx` so a saturated
    /// event channel can never drop a connection's terminal close signal.
    /// The bool indicates whether the peer initiated the close (true) or
    /// the close came from our side (false).
    pub conn_close_tx: mpsc::Sender<(u64, bool)>,

    pub established: bool,
    pub io_worker_buf: Vec<u8>,
    pub write_notify: Arc<Notify>,

    /// Writes that quiche could not accept yet (flow-controlled or partial).
    /// Per-stream queues keep one blocked stream from starving the others.
    pub pending_writes: HashMap<u64, VecDeque<(Bytes, bool)>>,

    /// Stream IDs we've already announced to the server. Prevents duplicate
    /// `NewBidi`/`NewUni` events on subsequent readability for the same stream.
    pub seen_streams: HashSet<u64>,
}

impl ApplicationOverQuic for StreamQuicDriver {
    fn on_conn_established(
        &mut self,
        conn: &mut QuicheConnection,
        _info: &HandshakeInfo,
    ) -> QuicResult<()> {
        log::debug!("[{:?}] QUIC handshake complete.", conn.source_id());
        self.established = true;
        Ok(())
    }

    fn should_act(&self) -> bool {
        self.established
    }

    fn buffer(&mut self) -> &mut [u8] {
        &mut self.io_worker_buf
    }

    async fn wait_for_data(&mut self, _conn: &mut QuicheConnection) -> QuicResult<()> {
        self.write_notify.notified().await;
        Ok(())
    }

    fn process_reads(&mut self, conn: &mut QuicheConnection) -> QuicResult<()> {
        let conn_id = self.conn_id;

        'outer: while let Some(stream_id) = conn.stream_readable_next() {
            // Announce client-initiated streams the first time we see them.
            // Server-initiated streams are already known to PHP.
            let client_initiated = stream_id & STREAM_ID_INITIATOR_MASK == 0;
            if client_initiated && !self.seen_streams.contains(&stream_id) {
                let permit = match self.event_tx.try_reserve() {
                    Ok(p) => p,
                    Err(_) => break 'outer,
                };
                let event = if stream_id & STREAM_ID_DIRECTION_MASK == 0 {
                    DriverEvent::NewBidi(stream_id)
                } else {
                    DriverEvent::NewUni(stream_id)
                };
                permit.send((conn_id, event));
                self.seen_streams.insert(stream_id);
            }

            // Drain the readable bytes for this stream, applying back-pressure
            // by holding the data in quiche's buffer if the event channel is
            // full. quiche's per-stream flow control then propagates upstream
            // to the peer.
            loop {
                let permit = match self.event_tx.try_reserve() {
                    Ok(p) => p,
                    Err(_) => break 'outer,
                };
                match conn.stream_recv(stream_id, &mut self.io_worker_buf) {
                    Ok((n, fin)) => {
                        let data = Bytes::copy_from_slice(&self.io_worker_buf[..n]);
                        permit.send((
                            conn_id,
                            DriverEvent::Data {
                                stream_id,
                                data,
                                fin,
                            },
                        ));
                        if fin {
                            self.seen_streams.remove(&stream_id);
                        }
                    }
                    Err(tokio_quiche::quiche::Error::Done) => break,
                    Err(e) => {
                        log::warn!(
                            "[{conn_id}] stream_recv error on stream {stream_id}: {e:?}"
                        );
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn process_writes(&mut self, conn: &mut QuicheConnection) -> QuicResult<()> {
        let conn_id = self.conn_id;

        // Apply any pending priority changes first so they take effect for
        // the writes in this same pass.
        while let Ok((stream_id, urgency, incremental)) = self.priority_recv.try_recv() {
            match conn.stream_priority(stream_id, urgency, incremental) {
                Ok(()) => log::debug!(
                    "[{conn_id}] stream {stream_id} priority urgency={urgency} \
                     incremental={incremental}"
                ),
                Err(e) => log::warn!(
                    "[{conn_id}] stream_priority({stream_id}, {urgency}, {incremental}) \
                     failed: {e:?}"
                ),
            }
        }

        // Drain new chunks from PHP into per-stream queues first so we can
        // attempt them in the same pass.
        while let Ok((stream_id, data, fin)) = self.stream_event_recv.try_recv() {
            let bytes = data.unwrap_or_else(Bytes::new);
            self.pending_writes
                .entry(stream_id)
                .or_default()
                .push_back((bytes, fin));
        }

        // Per-stream flush: stop on the first stream that quiche won't accept,
        // but continue trying other streams (their flow control is independent).
        self.pending_writes.retain(|stream_id, queue| {
            while let Some((bytes, fin)) = queue.front().cloned() {
                match conn.stream_send(*stream_id, &bytes, fin) {
                    Ok(n) if n == bytes.len() => {
                        queue.pop_front();
                    }
                    Ok(n) => {
                        // Partial write — buffer the remainder at the head.
                        queue[0] = (bytes.slice(n..), fin);
                        return true;
                    }
                    Err(tokio_quiche::quiche::Error::Done) => {
                        // No window right now; retry on next process_writes.
                        return true;
                    }
                    Err(e) => {
                        log::warn!(
                            "[{conn_id}] stream_send error on stream {stream_id}: {e:?}"
                        );
                        queue.pop_front();
                    }
                }
            }
            !queue.is_empty()
        });

        Ok(())
    }

    fn on_conn_close<M: Metrics>(
        &mut self,
        conn: &mut QuicheConnection,
        _metrics: &M,
        connection_result: &QuicResult<()>,
    ) {
        // Determine which side initiated the termination so PHP's on_close
        // callbacks can report the correct `peer_closed` flag.
        // - peer_error.is_some() → remote sent CONNECTION_CLOSE
        // - otherwise (idle timeout, local close, transport error from us)
        //   → treat as locally initiated.
        let peer_initiated = conn.peer_error().is_some();
        let conn_id = self.conn_id;
        log::info!(
            "[{conn_id}] driver on_conn_close peer_initiated={peer_initiated} result={connection_result:?}"
        );
        match self.conn_close_tx.try_send((conn_id, peer_initiated)) {
            Ok(()) => {}
            Err(e) => log::warn!(
                "[{conn_id}] failed to deliver close to server: {e:?}"
            ),
        }
    }
}
