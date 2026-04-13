use ext_php_rs::prelude::*;
use std::time::Duration;
use tokio_quiche::settings::QuicSettings;

#[php_class]
#[php(name = "NetherGames\\Quiche\\Config")]
pub struct Config {
    pub inner: QuicSettings,
    pub cert_path: String,
    pub key_path: String,
}

#[php_impl]
impl Config {
    pub fn __construct(cert_path: String, key_path: String) -> PhpResult<Self> {
        let inner = QuicSettings::default();

        Ok(Self {
            inner,
            cert_path,
            key_path,
        })
    }

    pub fn get_alpn(&self) -> Vec<String> {
        self.inner
            .alpn
            .iter()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .collect()
    }

    pub fn set_alpn(&mut self, alpn: Vec<String>) {
        self.inner.alpn = alpn.into_iter().map(|s| s.into_bytes()).collect();
    }

    pub fn get_enable_dgram(&self) -> bool {
        self.inner.enable_dgram
    }

    pub fn set_enable_dgram(&mut self, enable_dgram: bool) {
        self.inner.enable_dgram = enable_dgram;
    }

    pub fn get_dgram_recv_max_queue_len(&self) -> usize {
        self.inner.dgram_recv_max_queue_len
    }

    pub fn set_dgram_recv_max_queue_len(&mut self, len: usize) {
        self.inner.dgram_recv_max_queue_len = len;
    }

    pub fn get_dgram_send_max_queue_len(&self) -> usize {
        self.inner.dgram_send_max_queue_len
    }

    pub fn set_dgram_send_max_queue_len(&mut self, len: usize) {
        self.inner.dgram_send_max_queue_len = len;
    }

    pub fn get_enable_early_data(&self) -> bool {
        self.inner.enable_early_data
    }

    pub fn set_enable_early_data(&mut self, enable: bool) {
        self.inner.enable_early_data = enable;
    }

    pub fn get_initial_max_data(&self) -> u64 {
        self.inner.initial_max_data
    }

    pub fn set_initial_max_data(&mut self, data: u64) {
        self.inner.initial_max_data = data;
    }

    pub fn get_initial_max_stream_data_bidi_local(&self) -> u64 {
        self.inner.initial_max_stream_data_bidi_local
    }

    pub fn set_initial_max_stream_data_bidi_local(&mut self, data: u64) {
        self.inner.initial_max_stream_data_bidi_local = data;
    }

    pub fn get_initial_max_stream_data_bidi_remote(&self) -> u64 {
        self.inner.initial_max_stream_data_bidi_remote
    }

    pub fn set_initial_max_stream_data_bidi_remote(&mut self, data: u64) {
        self.inner.initial_max_stream_data_bidi_remote = data;
    }

    pub fn get_initial_max_stream_data_uni(&self) -> u64 {
        self.inner.initial_max_stream_data_uni
    }

    pub fn set_initial_max_stream_data_uni(&mut self, data: u64) {
        self.inner.initial_max_stream_data_uni = data;
    }

    pub fn get_initial_max_streams_bidi(&self) -> u64 {
        self.inner.initial_max_streams_bidi
    }

    pub fn set_initial_max_streams_bidi(&mut self, streams: u64) {
        self.inner.initial_max_streams_bidi = streams;
    }

    pub fn get_initial_max_streams_uni(&self) -> u64 {
        self.inner.initial_max_streams_uni
    }

    pub fn set_initial_max_streams_uni(&mut self, streams: u64) {
        self.inner.initial_max_streams_uni = streams;
    }

    pub fn get_max_idle_timeout(&self) -> Option<u64> {
        self.inner.max_idle_timeout.map(|d| d.as_millis() as u64)
    }

    pub fn set_max_idle_timeout(&mut self, timeout: Option<u64>) {
        self.inner.max_idle_timeout = timeout.map(Duration::from_millis);
    }

    pub fn get_disable_active_migration(&self) -> bool {
        self.inner.disable_active_migration
    }

    pub fn set_disable_active_migration(&mut self, disable: bool) {
        self.inner.disable_active_migration = disable;
    }

    pub fn get_active_connection_id_limit(&self) -> u64 {
        self.inner.active_connection_id_limit
    }

    pub fn set_active_connection_id_limit(&mut self, limit: u64) {
        self.inner.active_connection_id_limit = limit;
    }

    pub fn get_max_recv_udp_payload_size(&self) -> usize {
        self.inner.max_recv_udp_payload_size
    }

    pub fn set_max_recv_udp_payload_size(&mut self, size: usize) {
        self.inner.max_recv_udp_payload_size = size;
    }

    pub fn get_max_send_udp_payload_size(&self) -> usize {
        self.inner.max_send_udp_payload_size
    }

    pub fn set_max_send_udp_payload_size(&mut self, size: usize) {
        self.inner.max_send_udp_payload_size = size;
    }

    pub fn get_disable_client_ip_validation(&self) -> bool {
        self.inner.disable_client_ip_validation
    }

    pub fn set_disable_client_ip_validation(&mut self, disable: bool) {
        self.inner.disable_client_ip_validation = disable;
    }

    pub fn get_keylog_file(&self) -> Option<String> {
        self.inner.keylog_file.clone()
    }

    pub fn set_keylog_file(&mut self, file: Option<String>) {
        self.inner.keylog_file = file;
    }

    pub fn get_qlog_dir(&self) -> Option<String> {
        self.inner.qlog_dir.clone()
    }

    pub fn set_qlog_dir(&mut self, dir: Option<String>) {
        self.inner.qlog_dir = dir;
    }

    pub fn get_cc_algorithm(&self) -> String {
        self.inner.cc_algorithm.clone()
    }

    pub fn set_cc_algorithm(&mut self, algo: String) {
        self.inner.cc_algorithm = algo;
    }

    pub fn get_initial_congestion_window_packets(&self) -> usize {
        self.inner.initial_congestion_window_packets
    }

    pub fn set_initial_congestion_window_packets(&mut self, packets: usize) {
        self.inner.initial_congestion_window_packets = packets;
    }

    pub fn get_enable_relaxed_loss_threshold(&self) -> bool {
        self.inner.enable_relaxed_loss_threshold
    }

    pub fn set_enable_relaxed_loss_threshold(&mut self, enable: bool) {
        self.inner.enable_relaxed_loss_threshold = enable;
    }

    pub fn get_discover_path_mtu(&self) -> bool {
        self.inner.discover_path_mtu
    }

    pub fn set_discover_path_mtu(&mut self, enable: bool) {
        self.inner.discover_path_mtu = enable;
    }

    pub fn get_pmtud_max_probes(&self) -> u8 {
        self.inner.pmtud_max_probes
    }

    pub fn set_pmtud_max_probes(&mut self, probes: u8) {
        self.inner.pmtud_max_probes = probes;
    }

    pub fn get_enable_hystart(&self) -> bool {
        self.inner.enable_hystart
    }

    pub fn set_enable_hystart(&mut self, enable: bool) {
        self.inner.enable_hystart = enable;
    }

    pub fn get_enable_pacing(&self) -> bool {
        self.inner.enable_pacing
    }

    pub fn set_enable_pacing(&mut self, enable: bool) {
        self.inner.enable_pacing = enable;
    }

    pub fn get_max_pacing_rate(&self) -> Option<u64> {
        self.inner.max_pacing_rate
    }

    pub fn set_max_pacing_rate(&mut self, rate: Option<u64>) {
        self.inner.max_pacing_rate = rate;
    }

    pub fn get_enable_expensive_packet_count_metrics(&self) -> bool {
        self.inner.enable_expensive_packet_count_metrics
    }

    pub fn set_enable_expensive_packet_count_metrics(&mut self, enable: bool) {
        self.inner.enable_expensive_packet_count_metrics = enable;
    }

    pub fn get_capture_quiche_logs(&self) -> bool {
        self.inner.capture_quiche_logs
    }

    pub fn set_capture_quiche_logs(&mut self, capture: bool) {
        self.inner.capture_quiche_logs = capture;
    }

    pub fn get_handshake_timeout(&self) -> Option<u64> {
        self.inner.handshake_timeout.map(|d| d.as_millis() as u64)
    }

    pub fn set_handshake_timeout(&mut self, timeout: Option<u64>) {
        self.inner.handshake_timeout = timeout.map(Duration::from_millis);
    }

    pub fn get_listen_backlog(&self) -> usize {
        self.inner.listen_backlog
    }

    pub fn set_listen_backlog(&mut self, backlog: usize) {
        self.inner.listen_backlog = backlog;
    }

    pub fn get_verify_peer(&self) -> bool {
        self.inner.verify_peer
    }

    pub fn set_verify_peer(&mut self, verify: bool) {
        self.inner.verify_peer = verify;
    }

    pub fn get_max_connection_window(&self) -> u64 {
        self.inner.max_connection_window
    }

    pub fn set_max_connection_window(&mut self, window: u64) {
        self.inner.max_connection_window = window;
    }

    pub fn get_max_stream_window(&self) -> u64 {
        self.inner.max_stream_window
    }

    pub fn set_max_stream_window(&mut self, window: u64) {
        self.inner.max_stream_window = window;
    }

    pub fn get_enable_send_streams_blocked(&self) -> bool {
        self.inner.enable_send_streams_blocked
    }

    pub fn set_enable_send_streams_blocked(&mut self, enable: bool) {
        self.inner.enable_send_streams_blocked = enable;
    }

    pub fn get_grease(&self) -> bool {
        self.inner.grease
    }

    pub fn set_grease(&mut self, grease: bool) {
        self.inner.grease = grease;
    }

    pub fn get_max_amplification_factor(&self) -> usize {
        self.inner.max_amplification_factor
    }

    pub fn set_max_amplification_factor(&mut self, factor: usize) {
        self.inner.max_amplification_factor = factor;
    }

    pub fn get_send_capacity_factor(&self) -> f64 {
        self.inner.send_capacity_factor
    }

    pub fn set_send_capacity_factor(&mut self, factor: f64) {
        self.inner.send_capacity_factor = factor;
    }

    pub fn get_ack_delay_exponent(&self) -> u64 {
        self.inner.ack_delay_exponent
    }

    pub fn set_ack_delay_exponent(&mut self, exponent: u64) {
        self.inner.ack_delay_exponent = exponent;
    }

    pub fn get_max_ack_delay(&self) -> u64 {
        self.inner.max_ack_delay
    }

    pub fn set_max_ack_delay(&mut self, delay: u64) {
        self.inner.max_ack_delay = delay;
    }

    pub fn get_max_path_challenge_recv_queue_len(&self) -> usize {
        self.inner.max_path_challenge_recv_queue_len
    }

    pub fn set_max_path_challenge_recv_queue_len(&mut self, len: usize) {
        self.inner.max_path_challenge_recv_queue_len = len;
    }

    pub fn get_stateless_reset_token(&self) -> Option<String> {
        self.inner.stateless_reset_token.map(|t| format!("{:x}", t))
    }

    pub fn set_stateless_reset_token(&mut self, token: Option<String>) {
        if let Some(token_str) = token {
            if let Ok(t) = u128::from_str_radix(&token_str, 16) {
                self.inner.stateless_reset_token = Some(t);
            }
        } else {
            self.inner.stateless_reset_token = None;
        }
    }

    pub fn get_disable_dcid_reuse(&self) -> bool {
        self.inner.disable_dcid_reuse
    }

    pub fn set_disable_dcid_reuse(&mut self, disable: bool) {
        self.inner.disable_dcid_reuse = disable;
    }

    pub fn get_track_unknown_transport_parameters(&self) -> Option<usize> {
        self.inner.track_unknown_transport_parameters
    }

    pub fn set_track_unknown_transport_parameters(&mut self, len: Option<usize>) {
        self.inner.track_unknown_transport_parameters = len;
    }

    pub fn get_key_path(&self) -> &str {
        &self.key_path
    }

    pub fn set_key_path(&mut self, key_path: String) {
        self.key_path = key_path;
    }

    pub fn get_cert_path(&self) -> &str {
        &self.cert_path
    }

    pub fn set_cert_path(&mut self, cert_path: String) {
        self.cert_path = cert_path;
    }
}
