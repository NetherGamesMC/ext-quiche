use std::net::SocketAddr;
use std::sync::Arc;

use crate::quiche::conn::{ConnectionMap, StreamWriter};
use crate::quiche::data::{ReadHalf, WriteHalf};
use crate::quiche::stream::{
    AcceptedRemoteStream, BiDirectionalQuicheStream, ReadableQuicheStream,
};
use bytes::Bytes;
use tokio::sync::{mpsc, Notify};
use tokio_quiche::metrics::Metrics;
use tokio_quiche::{
    quic::{HandshakeInfo, QuicheConnection}, ApplicationOverQuic,
    QuicResult,
};

pub struct StreamQuicDriver {
    pub conn_id: u64,

    // Process writes events from a Writable stream data
    pub stream_event_recv: mpsc::Receiver<StreamWriter>,
    pub stream_event_send: mpsc::Sender<StreamWriter>,

    // Sender for new streams
    pub new_stream_tx: mpsc::Sender<AcceptedRemoteStream>,

    // List of all active connections
    pub connections: ConnectionMap,
    pub established: bool,

    /// The buffer used to interact with the underlying IoWorker.
    pub io_worker_buf: Vec<u8>,

    /// Remote peer address for this connection. Known at construction time.
    pub peer_addr: SocketAddr,

    /// Notification channel that is signaled when the driver is ready to send data
    /// to the remote peer. This is used for immediate signaling of write readiness.
    pub write_notify: Arc<Notify>,
}

impl StreamQuicDriver {
    fn ensure_stream(&mut self, stream_id: u64) {
        let mut streams = self.connections.lock().unwrap();
        if streams.contains_key(&stream_id) {
            return;
        }

        let (read_tx, read_rx) = mpsc::channel(256);
        streams.insert(stream_id, read_tx);

        // Bit 1 of stream_id: 0 = bidi, 1 = uni
        let incoming = if stream_id & 0x2 == 0 {
            AcceptedRemoteStream::BiDirectional(BiDirectionalQuicheStream {
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
                    write_notify: self.write_notify.clone(),
                },
            })
        } else {
            AcceptedRemoteStream::Readable(ReadableQuicheStream {
                inner: ReadHalf {
                    stream_id,
                    conn_id: self.conn_id,
                    peer_addr: self.peer_addr,
                    read_rx,
                },
            })
        };

        let _ = self.new_stream_tx.try_send(incoming);
    }
}

impl ApplicationOverQuic for StreamQuicDriver {
    fn on_conn_established(
        &mut self,
        conn: &mut QuicheConnection,
        _info: &HandshakeInfo,
    ) -> QuicResult<()> {
        log::info!("[{:?}] QUIC handshake complete.", conn.source_id());
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
        while let Some(stream_id) = conn.stream_readable_next() {
            self.ensure_stream(stream_id);
            let buf = &mut self.io_worker_buf
                [0..crate::quiche::conn::QuicheConnection::STREAM_BUFFER_SIZE];
            loop {
                match conn.stream_recv(stream_id, buf) {
                    Ok((n, fin)) => {
                        let streams = self.connections.lock().unwrap();

                        if let Some(tx) = streams.get(&stream_id) {
                            let _ =
                                tx.try_send((Some(Bytes::copy_from_slice(&buf[..n])), fin, false));
                        }
                    }
                    Err(tokio_quiche::quiche::Error::Done) => break,
                    Err(e) => {
                        log::warn!("stream_recv error on stream {stream_id}: {e:?}");
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn process_writes(&mut self, conn: &mut QuicheConnection) -> QuicResult<()> {
        while let Ok((stream_id, data, fin)) = self.stream_event_recv.try_recv() {
            match conn.stream_send(stream_id, &data.unwrap(), fin) {
                Ok(_) => {}
                Err(tokio_quiche::quiche::Error::Done) => {
                    log::debug!("stream {stream_id} flow-controlled, chunk dropped (prototype)");
                }
                Err(e) => {
                    log::warn!("stream_send error on stream {stream_id}: {e:?}");
                }
            }
        }
        Ok(())
    }

    fn on_conn_close<M: Metrics>(
        &mut self,
        _conn: &mut QuicheConnection,
        _metrics: &M,
        _connection_result: &QuicResult<()>,
    ) {
        let mut streams = self.connections.lock().unwrap();
        for route in streams.values_mut() {
            let _ = route.try_send((None, true, true));
        }

        streams.clear();
    }
}
