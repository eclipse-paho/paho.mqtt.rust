// paho-mqtt/src/runtime/smol_rt.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2024-2026 Frank Pagliughi <fpagliughi@mindspring.com>
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Frank Pagliughi - initial implementation and documentation
 *******************************************************************************/

//! Smol-based async transport and event loop for the MQTT client.
//!
//! Provides [`TcpConnection`] and other transport implementations of the
//! shared [`Connection`] trait, [`RuntimeHandle`] for cross-thread
//! communication, and the internal [`Session`] event loop.
//!
//! TLS is supported via the `smol-tls` feature (uses `futures-rustls`).
//! WebSocket support is deferred (`smol-websocket` feature, future work).

use std::{
    collections::{HashMap, VecDeque},
    io,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use bytes::BytesMut;
use paho_mqtt_codec::{
    ConnAck, Disconnect, MqttVersion, PubAck, QoS, RetainHandling as CodecRetainHandling,
    Subscribe, Subscription, Unsubscribe,
    packet::{Packet, RawPacket},
};
use smol::io::split;
use smol::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use smol::net::TcpStream;
#[cfg(unix)]
use smol::net::unix::UnixStream;
#[cfg(feature = "smol-tls")]
use std::sync::Arc as StdArc;

use super::{
    commands::{BufConfig, Command, ConnectParams, Event, ServerAddr},
    connection::Connection,
};
use crate::{
    Error, Message, Result,
    persistence::ClientPersistence,
    responses::{ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse},
    token::Completer,
};

/////////////////////////////////////////////////////////////////////////////

/// A plain TCP transport backed by [`smol::net::TcpStream`].
pub(crate) struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
    /// Opens a TCP connection to `host` on `port`.
    pub(crate) async fn connect(host: &str, port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }
}

/// Implements `AsyncRead`, `AsyncWrite`, and `Connection` for a newtype struct
/// whose only field is named `stream` and already implements those traits
/// via the `futures-io` traits.
macro_rules! impl_connection {
    ($ty:ty) => {
        impl AsyncRead for $ty {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.stream).poll_read(cx, buf)
            }
        }

        impl AsyncWrite for $ty {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.stream).poll_write(cx, buf)
            }

            fn poll_flush(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_flush(cx)
            }

            fn poll_close(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_close(cx)
            }
        }

        impl Connection for $ty {}
    };
}

impl_connection!(TcpConnection);

/////////////////////////////////////////////////////////////////////////////

/// A Unix-domain socket transport backed by [`smol::net::unix::UnixStream`].
#[cfg(unix)]
pub(crate) struct UnixConnection {
    stream: UnixStream,
}

#[cfg(unix)]
impl UnixConnection {
    /// Opens a Unix-domain socket connection to `path`.
    pub(crate) async fn connect(path: &str) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self { stream })
    }
}

#[cfg(unix)]
impl_connection!(UnixConnection);

/////////////////////////////////////////////////////////////////////////////

/// A TLS transport backed by [`futures_rustls`] (requires the `smol-tls` feature).
#[cfg(feature = "smol-tls")]
pub(crate) struct TlsConnection {
    stream: futures_rustls::client::TlsStream<TcpStream>,
}

#[cfg(feature = "smol-tls")]
impl_connection!(TlsConnection);

#[cfg(feature = "smol-tls")]
type TlsResult<T> = std::result::Result<T, rustls::Error>;

/////////////////////////////////////////////////////////////////////////////

/// A [`rustls::client::ServerCertVerifier`] that accepts any certificate.
///
/// Used when `TlsOptions::enable_server_cert_auth` is `false`.
#[cfg(feature = "smol-tls")]
#[derive(Debug)]
struct NoCertVerifier;

#[cfg(feature = "smol-tls")]
impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> TlsResult<rustls::client::danger::ServerCertVerified> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> TlsResult<rustls::client::danger::HandshakeSignatureValid> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> TlsResult<rustls::client::danger::HandshakeSignatureValid> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA1,
            ECDSA_SHA1_Legacy,
            RSA_PKCS1_SHA256,
            ECDSA_NISTP256_SHA256,
            RSA_PKCS1_SHA384,
            ECDSA_NISTP384_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            ED25519,
            ED448,
        ]
    }
}

/// Filters the ring provider's cipher suites to those named in `spec`.
///
/// `spec` is a colon- or comma-separated list of rustls suite names.
/// Matching is case-insensitive.  Returns an error if no names match.
#[cfg(feature = "smol-tls")]
fn filter_cipher_suites(spec: &str) -> Result<Vec<rustls::SupportedCipherSuite>> {
    let provider = rustls::crypto::ring::default_provider();
    let requested: std::collections::HashSet<String> = spec
        .split(|c: char| c == ':' || c == ',')
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    let filtered: Vec<rustls::SupportedCipherSuite> = provider
        .cipher_suites
        .iter()
        .filter(|cs| {
            let name = format!("{:?}", cs.suite()).to_ascii_uppercase();
            requested.contains(&name)
        })
        .copied()
        .collect();

    if filtered.is_empty() {
        return Err(Error::GeneralString(format!(
            "no recognized cipher suites in '{spec}'; \
            use rustls names such as TLS13_AES_256_GCM_SHA384"
        )));
    }
    Ok(filtered)
}

/// Builds a [`rustls::ClientConfig`] from [`TlsOptions`].
///
/// Identical logic to the tokio backend's `make_tls_config`; only the
/// async wrapper crate differs (`futures-rustls` vs `tokio-rustls`).
#[cfg(feature = "smol-tls")]
fn make_tls_config(tls: &crate::TlsOptions) -> Result<StdArc<rustls::ClientConfig>> {
    use crate::TlsVersion;
    use rustls::RootCertStore;
    use std::io::BufReader;

    if !tls.private_key_password.is_empty() {
        return Err(Error::General(
            "encrypted private keys are not supported with the rustls TLS backend",
        ));
    }

    // ---- Root certificate store ----
    let mut root_store = RootCertStore::empty();

    if !tls.disable_default_trust_store {
        let native_certs = rustls_native_certs::load_native_certs().map_err(Error::Io)?;
        for cert in native_certs {
            let _ = root_store.add(cert);
        }
    }

    if !tls.trust_store.as_os_str().is_empty() {
        let f = std::fs::File::open(&tls.trust_store).map_err(Error::Io)?;
        let mut r = BufReader::new(f);
        for cert in rustls_pemfile::certs(&mut r) {
            root_store
                .add(cert.map_err(Error::Io)?)
                .map_err(|e| Error::GeneralString(e.to_string()))?;
        }
    }

    if !tls.ca_path.as_os_str().is_empty() {
        for entry in std::fs::read_dir(&tls.ca_path).map_err(Error::Io)? {
            let path = entry.map_err(Error::Io)?.path();
            if path.is_file() {
                if let Ok(f) = std::fs::File::open(&path) {
                    let mut r = BufReader::new(f);
                    for cert in rustls_pemfile::certs(&mut r).flatten() {
                        let _ = root_store.add(cert);
                    }
                }
            }
        }
    }

    // ---- Crypto provider (cipher suites) ----
    let mut provider = rustls::crypto::ring::default_provider();
    if !tls.enabled_cipher_suites.is_empty() {
        provider.cipher_suites = filter_cipher_suites(&tls.enabled_cipher_suites)?;
    }

    // ---- Protocol versions ----
    let versions: &[&'static rustls::SupportedProtocolVersion] = match tls.version {
        TlsVersion::Default | TlsVersion::Tls1_2 => {
            &[&rustls::version::TLS12, &rustls::version::TLS13]
        }
        TlsVersion::Tls1_0 | TlsVersion::Tls1_1 => {
            log::warn!(
                "TLS 1.0/1.1 are not supported by the rustls backend; \
                 using TLS 1.2 as the minimum version"
            );
            &[&rustls::version::TLS12, &rustls::version::TLS13]
        }
    };

    // ---- Config builder ----
    let builder = rustls::ClientConfig::builder_with_provider(StdArc::new(provider))
        .with_protocol_versions(versions)
        .map_err(|e| Error::GeneralString(e.to_string()))?;

    // ---- Certificate verification ----
    let wants_client = if tls.enable_server_cert_auth {
        builder.with_root_certificates(root_store)
    }
    else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(StdArc::new(NoCertVerifier))
    };

    // ---- Client authentication ----
    let mut config = if !tls.key_store.as_os_str().is_empty() {
        let cert_f = std::fs::File::open(&tls.key_store).map_err(Error::Io)?;
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(cert_f))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Error::Io)?;

        let key_path = if !tls.private_key.as_os_str().is_empty() {
            &tls.private_key
        }
        else {
            &tls.key_store
        };
        let key_f = std::fs::File::open(key_path).map_err(Error::Io)?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_f))
            .map_err(Error::Io)?
            .ok_or_else(|| Error::General("no private key found in key store"))?;

        wants_client
            .with_client_auth_cert(certs, key)
            .map_err(|e| Error::GeneralString(e.to_string()))?
    }
    else {
        wants_client.with_no_client_auth()
    };

    // ---- ALPN protocols ----
    if !tls.protos.is_empty() {
        config.alpn_protocols = tls.protos.iter().map(|p| p.as_bytes().to_vec()).collect();
    }

    Ok(StdArc::new(config))
}

/////////////////////////////////////////////////////////////////////////////
// Runtime management

/// Tracks whether this client owns the smol executor or merely uses it.
///
/// Stored in [`InnerAsyncClient`] to keep the executor alive.
///
/// [`InnerAsyncClient`]: crate::async_client::InnerAsyncClient
pub(crate) enum SmolOwnership {
    /// The library manages the shared executor; the `Arc` keeps it alive.
    #[allow(dead_code)] // held purely for its drop effect
    Shared(Arc<smol::Executor<'static>>),
}

/// Process-wide shared smol executor.
///
/// Created on first use and kept alive for the process lifetime.  A
/// background OS thread drives the executor by running
/// `executor.run(future::pending())` indefinitely.
static SHARED_EXECUTOR: OnceLock<Arc<smol::Executor<'static>>> = OnceLock::new();

/// Returns the process-wide shared executor, creating it on first call.
pub(crate) fn get_or_create_shared_executor() -> Arc<smol::Executor<'static>> {
    SHARED_EXECUTOR
        .get_or_init(|| {
            let ex = Arc::new(smol::Executor::new());
            let ex2 = ex.clone();
            std::thread::Builder::new()
                .name("mqtt-smol".to_string())
                .spawn(move || smol::block_on(ex2.run(std::future::pending::<()>())))
                .expect("spawn mqtt-smol executor thread");
            ex
        })
        .clone()
}

/// Channels connecting the public API to the internal event loop.
///
/// Created by [`RuntimeHandle::spawn`].
pub(crate) struct RuntimeHandle {
    cmd_tx: async_channel::Sender<Command>,
    event_rx: async_channel::Receiver<Event>,
}

impl RuntimeHandle {
    /// Spawns the MQTT event loop as a task on `executor` and returns the
    /// channel endpoints for communicating with it.
    pub(crate) fn spawn(executor: &smol::Executor<'static>, buf_cfg: BufConfig) -> Self {
        let (cmd_tx, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, event_rx) = async_channel::unbounded::<Event>();
        executor
            .spawn(run_event_loop(cmd_rx, event_tx, buf_cfg))
            .detach();
        Self { cmd_tx, event_rx }
    }

    /// Consumes the handle, returning the underlying sender and receiver.
    pub(crate) fn into_parts(
        self,
    ) -> (
        async_channel::Sender<Command>,
        async_channel::Receiver<Event>,
    ) {
        (self.cmd_tx, self.event_rx)
    }
}

/////////////////////////////////////////////////////////////////////////////
// Internal event loop helpers

/// Reads bytes until at least one complete MQTT packet is available, then
/// returns the decoded CONNACK.  Returns an error if any other packet arrives
/// first or if the connection closes.
async fn recv_connack(
    reader: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
    ver: MqttVersion,
) -> Result<ConnAck> {
    let mut tmp = [0u8; 512];
    loop {
        let n = reader.read(&mut tmp).await?;
        if n == 0 {
            return Err(Error::Disconnected);
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(raw) = RawPacket::decode(buf)? {
            match Packet::from_raw(raw, ver)? {
                Packet::ConnAck(ack) => return Ok(ack),
                _ => return Err(Error::General("expected CONNACK")),
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Session

/// Holds all mutable state for a connected MQTT session.
struct Session {
    /// Write half of the split transport.
    writer: WriteHalf<Box<dyn Connection>>,
    /// Negotiated MQTT version.
    ver: MqttVersion,
    /// Keep-alive interval.
    keep_alive: Duration,
    /// Reusable scratch buffer for packet encoding.
    write_buf: BytesMut,
    /// Monotonically increasing packet-ID counter (wraps; skips 0).
    next_id: u16,
    /// Pending QoS 1 outbound publishes awaiting PUBACK.
    pending_pub1: HashMap<u16, Completer<PublishResponse>>,
    /// Pending QoS 2 outbound publishes awaiting PUBCOMP.
    pending_pub2: HashMap<u16, Completer<PublishResponse>>,
    /// Pending inbound QoS 2 messages awaiting PUBREL (packet_id → message).
    pending_pub2_in: HashMap<u16, Message>,
    /// Pending SUBSCRIBE operations awaiting SUBACK.
    pending_sub: HashMap<u16, Completer<SubscribeResponse>>,
    /// Pending UNSUBSCRIBE operations awaiting UNSUBACK.
    pending_unsub: HashMap<u16, Completer<UnsubscribeResponse>>,
    /// Sender to deliver inbound events to the public layer.
    event_tx: async_channel::Sender<Event>,
    /// True when a PINGREQ has been sent and PINGRESP has not yet arrived.
    ping_outstanding: bool,
    /// Persistence store for in-flight QoS 1/2 messages (if any).
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
    /// Maximum concurrent outbound QoS 1/2 publishes; 0 means unlimited.
    max_inflight: usize,
    /// QoS 1/2 publish commands deferred because the in-flight limit was reached.
    inflight_deferred: VecDeque<Command>,
    /// Set when the broker sends a DISCONNECT packet; suppresses ConnectionLost.
    broker_disconnected: bool,
    /// Set by `send()` to indicate a packet was written since the last
    /// keep-alive timer reset.
    sent_packet: bool,
}

impl Session {
    fn new(
        writer: WriteHalf<Box<dyn Connection>>,
        ver: MqttVersion,
        keep_alive: Duration,
        event_tx: async_channel::Sender<Event>,
        persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
        max_inflight: usize,
    ) -> Self {
        Self {
            writer,
            ver,
            keep_alive,
            write_buf: BytesMut::with_capacity(256),
            next_id: 0,
            pending_pub1: HashMap::new(),
            pending_pub2: HashMap::new(),
            pending_pub2_in: HashMap::new(),
            pending_sub: HashMap::new(),
            pending_unsub: HashMap::new(),
            event_tx,
            ping_outstanding: false,
            persist,
            max_inflight,
            inflight_deferred: VecDeque::new(),
            broker_disconnected: false,
            sent_packet: false,
        }
    }

    fn has_pending(&self) -> bool {
        !self.pending_pub1.is_empty()
            || !self.pending_pub2.is_empty()
            || !self.pending_sub.is_empty()
            || !self.pending_unsub.is_empty()
            || !self.inflight_deferred.is_empty()
    }

    fn inflight_count(&self) -> usize {
        self.pending_pub1.len() + self.pending_pub2.len()
    }

    async fn drain_deferred(&mut self) -> Result<()> {
        loop {
            if self.max_inflight != 0 && self.inflight_count() >= self.max_inflight {
                break;
            }
            let Some(cmd) = self.inflight_deferred.pop_front()
            else {
                break;
            };
            self.handle_command(cmd).await?;
        }
        Ok(())
    }

    fn persist_put(&self, key: &str, data: &[u8]) {
        if let Some(p) = &self.persist {
            let _ = p.lock().unwrap().put(key, vec![data]);
        }
    }

    fn persist_remove(&self, key: &str) {
        if let Some(p) = &self.persist {
            let _ = p.lock().unwrap().remove(key);
        }
    }

    fn alloc_id(&mut self) -> u16 {
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1;
        }
        self.next_id
    }

    async fn send(&mut self, packet: &Packet) -> Result<()> {
        self.write_buf.clear();
        packet.encode(&mut self.write_buf, self.ver)?;
        self.writer.write_all(&self.write_buf).await?;
        self.writer.flush().await?;
        self.sent_packet = true;
        Ok(())
    }

    /// Restores in-flight QoS 1/2 messages from the persistence store after a
    /// successful connect.
    async fn restore(&mut self) -> Result<()> {
        let keys = {
            let Some(persist) = &self.persist
            else {
                return Ok(());
            };
            match persist.lock().unwrap().keys() {
                Ok(k) => k,
                Err(_) => return Ok(()),
            }
        };

        for key in &keys {
            let data = {
                let Some(persist) = &self.persist
                else {
                    return Ok(());
                };
                match persist.lock().unwrap().get(key) {
                    Ok(d) => d,
                    Err(_) => continue,
                }
            };

            let mut buf = BytesMut::from(data.as_slice());
            let raw = match RawPacket::decode(&mut buf) {
                Ok(Some(r)) => r,
                _ => continue,
            };
            let pkt = match Packet::from_raw(raw, self.ver) {
                Ok(p) => p,
                Err(_) => continue,
            };

            if key.starts_with('s') {
                if let Packet::Publish(mut pub_pkt) = pkt {
                    let Some(pid) = pub_pkt.packet_id
                    else {
                        continue;
                    };
                    let qos = pub_pkt.qos;
                    pub_pkt.dup = true;
                    self.send(&Packet::Publish(pub_pkt)).await?;
                    if pid > self.next_id {
                        self.next_id = pid;
                    }
                    let comp = Completer::orphan();
                    match qos {
                        QoS::QoS1 => {
                            self.pending_pub1.insert(pid, comp);
                        }
                        QoS::QoS2 => {
                            self.pending_pub2.insert(pid, comp);
                        }
                        _ => {}
                    }
                }
            }
            else if key.starts_with('r') {
                if let Packet::Publish(pub_pkt) = pkt {
                    let id = pub_pkt.packet_id.unwrap_or(0);
                    let msg = Message::from(pub_pkt);
                    self.pending_pub2_in.insert(id, msg);
                }
            }
        }

        Ok(())
    }

    /// Main event loop: read from `reader`, dispatch commands from `cmd_rx`,
    /// and service keep-alive until an error or clean disconnect.
    async fn run(
        &mut self,
        reader: &mut (impl AsyncRead + Unpin),
        cmd_rx: &async_channel::Receiver<Command>,
    ) -> Result<()> {
        let keep_alive = self.keep_alive;
        let ka_enabled = !keep_alive.is_zero();

        // Track the next expected keep-alive deadline; reset on outbound packets.
        let mut ka_deadline = Instant::now() + keep_alive;

        let mut read_buf = BytesMut::with_capacity(4096);
        let mut tmp = [0u8; 4096];

        loop {
            // Three-way race: network read vs. command vs. keep-alive timer.
            // Fresh futures are created each iteration; smol::future::race takes
            // ownership of async blocks (reborrows of reader/tmp are released
            // when race completes, allowing next-iteration reborrows).
            enum LoopEv {
                Net(io::Result<usize>),
                Cmd(std::result::Result<Command, async_channel::RecvError>),
                Ka,
            }

            let ka_dl = ka_deadline;
            let ev = smol::future::race(
                smol::future::race(
                    async { LoopEv::Net(reader.read(&mut tmp).await) },
                    async { LoopEv::Cmd(cmd_rx.recv().await) },
                ),
                async move {
                    if ka_enabled {
                        smol::Timer::at(ka_dl).await;
                    }
                    else {
                        std::future::pending::<()>().await;
                    }
                    LoopEv::Ka
                },
            )
            .await;

            match ev {
                LoopEv::Net(result) => {
                    let n = result?;
                    if n == 0 {
                        return Err(Error::Disconnected);
                    }
                    read_buf.extend_from_slice(&tmp[..n]);
                    while let Some(raw) = RawPacket::decode(&mut read_buf)? {
                        let packet = Packet::from_raw(raw, self.ver)?;
                        self.handle_packet(packet).await?;
                    }
                    if ka_enabled && self.sent_packet {
                        self.sent_packet = false;
                        ka_deadline = Instant::now() + keep_alive;
                    }
                }

                LoopEv::Cmd(Err(_)) => return Ok(()),

                LoopEv::Cmd(Ok(Command::Disconnect {
                    timeout,
                    reason_code,
                    props,
                    tx,
                    connected_flag,
                })) => {
                    if !timeout.is_zero() && self.has_pending() {
                        let drain_deadline = Instant::now() + timeout;
                        'drain: loop {
                            if !self.has_pending() {
                                break;
                            }
                            enum DrainEv {
                                Net(io::Result<usize>),
                                Timeout,
                            }
                            let dl = drain_deadline;
                            let dev = smol::future::race(
                                async { DrainEv::Net(reader.read(&mut tmp).await) },
                                async move {
                                    smol::Timer::at(dl).await;
                                    DrainEv::Timeout
                                },
                            )
                            .await;
                            match dev {
                                DrainEv::Timeout => break 'drain,
                                DrainEv::Net(Ok(0)) | DrainEv::Net(Err(_)) => break 'drain,
                                DrainEv::Net(Ok(n)) => {
                                    read_buf.extend_from_slice(&tmp[..n]);
                                    while let Ok(Some(raw)) =
                                        RawPacket::decode(&mut read_buf)
                                    {
                                        match Packet::from_raw(raw, self.ver) {
                                            Ok(pkt) => {
                                                let _ = self.handle_packet(pkt).await;
                                            }
                                            Err(_) => break 'drain,
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let disc = Disconnect {
                        reason_code,
                        properties: props,
                    };
                    let _ = self.send(&Packet::Disconnect(disc)).await;
                    connected_flag.store(false, Ordering::SeqCst);
                    tx.complete(Ok(()));
                    return Ok(());
                }

                LoopEv::Cmd(Ok(cmd)) => {
                    if self.handle_command(cmd).await? {
                        return Ok(());
                    }
                    if ka_enabled && self.sent_packet {
                        self.sent_packet = false;
                        ka_deadline = Instant::now() + keep_alive;
                    }
                }

                LoopEv::Ka => {
                    if self.ping_outstanding {
                        return Err(Error::Timeout);
                    }
                    self.send(&Packet::PingReq).await?;
                    self.ping_outstanding = true;
                    ka_deadline = Instant::now() + keep_alive;
                }
            }
        }
    }

    async fn handle_packet(&mut self, packet: Packet) -> Result<()> {
        match packet {
            Packet::PubAck(ack) => {
                if let Some(tx) = self.pending_pub1.remove(&ack.packet_id) {
                    self.persist_remove(&format!("s{}", ack.packet_id));
                    let rsp = PublishResponse::new(ack.reason_code, ack.properties);
                    tx.complete(Ok(rsp));
                }
                self.drain_deferred().await?;
            }

            Packet::PubRec(rec) => {
                if rec.reason_code as u8 >= 0x80 {
                    if let Some(tx) = self.pending_pub2.remove(&rec.packet_id) {
                        tx.complete(Err(Error::ReasonCode(
                            rec.reason_code,
                            rec.properties,
                        )));
                    }
                    return Ok(());
                }
                let rel = PubAck::new(rec.packet_id);
                self.send(&Packet::PubRel(rel)).await?;
            }

            Packet::PubComp(comp) => {
                if let Some(tx) = self.pending_pub2.remove(&comp.packet_id) {
                    self.persist_remove(&format!("s{}", comp.packet_id));
                    let rsp = PublishResponse::new(comp.reason_code, comp.properties);
                    tx.complete(Ok(rsp));
                }
                self.drain_deferred().await?;
            }

            Packet::SubAck(ack) => {
                if let Some(tx) = self.pending_sub.remove(&ack.packet_id) {
                    let rsp = SubscribeResponse::new(ack.reason_codes, ack.properties);
                    tx.complete(Ok(rsp));
                }
            }

            Packet::UnsubAck(ack) => {
                if let Some(tx) = self.pending_unsub.remove(&ack.packet_id) {
                    let rsp = UnsubscribeResponse::new(ack.reason_codes, ack.properties);
                    tx.complete(Ok(rsp));
                }
            }

            Packet::Publish(pub_pkt) => match pub_pkt.qos {
                QoS::QoS0 => {
                    let msg = Message::from(pub_pkt);
                    let _ = self.event_tx.send(Event::MessageArrived(msg)).await;
                }
                QoS::QoS1 => {
                    let id = pub_pkt.packet_id.unwrap_or(0);
                    let msg = Message::from(pub_pkt);
                    let _ = self.event_tx.send(Event::MessageArrived(msg)).await;
                    let ack = PubAck::new(id);
                    self.send(&Packet::PubAck(ack)).await?;
                }
                QoS::QoS2 => {
                    let id = pub_pkt.packet_id.unwrap_or(0);
                    let mut enc_buf = BytesMut::with_capacity(256);
                    let packet = Packet::Publish(pub_pkt);
                    if packet.encode(&mut enc_buf, self.ver).is_ok() {
                        self.persist_put(&format!("r{id}"), &enc_buf);
                    }
                    let Packet::Publish(pub_pkt) = packet
                    else {
                        unreachable!()
                    };
                    let msg = Message::from(pub_pkt);
                    self.pending_pub2_in.insert(id, msg);
                    let rec = PubAck::new(id);
                    self.send(&Packet::PubRec(rec)).await?;
                }
            },

            Packet::PubRel(rel) => {
                if let Some(msg) = self.pending_pub2_in.remove(&rel.packet_id) {
                    self.persist_remove(&format!("r{}", rel.packet_id));
                    let _ = self.event_tx.send(Event::MessageArrived(msg)).await;
                }
                let comp = PubAck::new(rel.packet_id);
                self.send(&Packet::PubComp(comp)).await?;
            }

            Packet::PingResp => {
                self.ping_outstanding = false;
            }

            Packet::Disconnect(disc) => {
                self.broker_disconnected = true;
                let _ = self
                    .event_tx
                    .send(Event::Disconnected {
                        reason_code: disc.reason_code,
                        props: disc.properties,
                    })
                    .await;
                return Err(Error::Disconnected);
            }

            _ => {}
        }
        Ok(())
    }

    async fn handle_command(&mut self, cmd: Command) -> Result<bool> {
        match cmd {
            Command::Connect { tx, .. } => {
                tx.complete(Err(Error::General("already connected")));
            }

            Command::Publish {
                topic,
                payload,
                qos,
                retain,
                props,
                tx,
            } => {
                if qos != QoS::QoS0
                    && self.max_inflight != 0
                    && self.inflight_count() >= self.max_inflight
                {
                    self.inflight_deferred.push_back(Command::Publish {
                        topic,
                        payload,
                        qos,
                        retain,
                        props,
                        tx,
                    });
                    return Ok(false);
                }

                let id = match qos {
                    QoS::QoS0 => None,
                    _ => Some(self.alloc_id()),
                };

                let pub_pkt = paho_mqtt_codec::Publish {
                    dup: false,
                    qos,
                    retain,
                    topic,
                    packet_id: id,
                    payload,
                    properties: props,
                };

                let packet = Packet::Publish(pub_pkt);

                if let Some(pid) = id {
                    let mut enc_buf = BytesMut::with_capacity(256);
                    if packet.encode(&mut enc_buf, self.ver).is_ok() {
                        self.persist_put(&format!("s{pid}"), &enc_buf);
                    }
                }

                self.send(&packet).await?;

                match qos {
                    QoS::QoS0 => {
                        tx.complete(Ok(PublishResponse::default()));
                    }
                    QoS::QoS1 => {
                        if let Some(id) = id {
                            self.pending_pub1.insert(id, tx);
                        }
                    }
                    QoS::QoS2 => {
                        if let Some(id) = id {
                            self.pending_pub2.insert(id, tx);
                        }
                    }
                }
            }

            Command::Subscribe { filters, props, tx } => {
                let id = self.alloc_id();
                let subscriptions = filters
                    .into_iter()
                    .map(|(f, q, opts)| {
                        use crate::subscribe_options::RetainHandling as RH;
                        Subscription {
                            topic_filter: f,
                            qos: q,
                            no_local: opts.no_local,
                            retain_as_published: opts.retain_as_published,
                            retain_handling: match opts.retain_handling {
                                RH::SendRetainedOnSubscribe => {
                                    CodecRetainHandling::SendOnSubscribe
                                }
                                RH::SendRetainedOnNew => CodecRetainHandling::SendIfNew,
                                RH::DontSendRetained => CodecRetainHandling::DoNotSend,
                            },
                        }
                    })
                    .collect();
                let sub = Subscribe {
                    packet_id: id,
                    subscriptions,
                    properties: props,
                };
                self.send(&Packet::Subscribe(sub)).await?;
                self.pending_sub.insert(id, tx);
            }

            Command::Unsubscribe { filters, props, tx } => {
                let id = self.alloc_id();
                let unsub = Unsubscribe {
                    packet_id: id,
                    topic_filters: filters,
                    properties: props,
                };
                self.send(&Packet::Unsubscribe(unsub)).await?;
                self.pending_unsub.insert(id, tx);
            }

            Command::Disconnect { .. } => unreachable!(),
        }
        Ok(false)
    }
}

/////////////////////////////////////////////////////////////////////////////
// Auto-reconnect helpers

struct ConnectJob {
    params: Box<ConnectParams>,
    server_idx: usize,
    connected_flag: Arc<AtomicBool>,
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
}

impl ConnectJob {
    fn extract(
        cmd: Command,
    ) -> std::result::Result<(Self, Completer<ConnectResponse>), Command> {
        match cmd {
            Command::Connect {
                params,
                tx,
                connected_flag,
                persist,
            } => Ok((
                Self {
                    params,
                    server_idx: 0,
                    connected_flag,
                    persist,
                },
                tx,
            )),
            other => Err(other),
        }
    }
}

/// Opens the transport, opens persistence, sends CONNECT, waits for CONNACK.
async fn do_connect(
    job: &ConnectJob,
) -> Result<(
    ReadHalf<Box<dyn Connection>>,
    WriteHalf<Box<dyn Connection>>,
    ConnAck,
)> {
    let p = &job.params;
    let (addr, server_uri) = &p.servers[job.server_idx];

    if let Some(store) = &job.persist {
        store.lock().unwrap().open(&p.client_id, server_uri)?;
    }

    let conn: Box<dyn Connection> = match addr {
        ServerAddr::Tcp { host, port } => {
            Box::new(TcpConnection::connect(host, *port).await?)
        }
        #[cfg(feature = "smol-tls")]
        ServerAddr::Tls { host, port } => {
            let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
            let config = make_tls_config(tls)?;
            let connector = futures_rustls::TlsConnector::from(config);
            let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                .map_err(|_| {
                    Error::GeneralString(format!("invalid TLS server name: {}", host))
                })?
                .to_owned();
            let tcp = TcpStream::connect((host.as_str(), *port)).await?;
            tcp.set_nodelay(true)?;
            let tls_stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| Error::Tls(e.to_string()))?;
            Box::new(TlsConnection { stream: tls_stream })
        }
        #[cfg(not(feature = "smol-tls"))]
        #[cfg(any(feature = "tls", feature = "openssl"))]
        ServerAddr::Tls { .. } => {
            return Err(Error::General(
                "TLS is not yet supported with the smol runtime",
            ));
        }
        #[cfg(unix)]
        ServerAddr::Unix { path } => Box::new(UnixConnection::connect(path).await?),
        #[cfg(feature = "websocket")]
        ServerAddr::Ws { .. } => {
            return Err(Error::General(
                "WebSocket is not yet supported with the smol runtime",
            ));
        }
        #[cfg(all(feature = "websocket", any(feature = "tls", feature = "openssl")))]
        ServerAddr::Wss { .. } => {
            return Err(Error::General(
                "WebSocket+TLS is not yet supported with the smol runtime",
            ));
        }
    };

    let (mut reader, mut writer) = split(conn);

    let connect_pkt = paho_mqtt_codec::Connect {
        clean_start: p.clean_start,
        keep_alive: p.keep_alive_secs,
        client_id: p.client_id.clone(),
        username: p.user_name.clone(),
        password: p.password.clone(),
        will: p.will.clone(),
        properties: p.props.clone(),
    };

    let mut buf = BytesMut::with_capacity(256);
    Packet::Connect(connect_pkt).encode(&mut buf, p.ver)?;
    writer.write_all(&buf).await?;
    writer.flush().await?;

    let mut read_buf = BytesMut::with_capacity(256);
    let connack = recv_connack(&mut reader, &mut read_buf, p.ver).await?;

    Ok((reader, writer, connack))
}

fn buf_push(buf: &mut VecDeque<Command>, cmd: Command, cfg: &BufConfig) {
    if buf.len() < cfg.max_buffered_msgs {
        buf.push_back(cmd);
    }
    else if cfg.delete_oldest {
        buf.pop_front();
        buf.push_back(cmd);
    }
}

enum WaitOutcome {
    Retry,
    Abort,
    Replace(Box<ConnectJob>, Completer<ConnectResponse>),
}

/// Waits for `delay` while draining the command queue.
async fn reconnect_wait(
    delay: Duration,
    cmd_rx: &async_channel::Receiver<Command>,
    buf: &mut VecDeque<Command>,
    buf_cfg: &BufConfig,
) -> WaitOutcome {
    let deadline = Instant::now() + delay;

    loop {
        enum WaitEv {
            Timeout,
            Cmd(std::result::Result<Command, async_channel::RecvError>),
        }

        let ev = smol::future::race(
            async move {
                smol::Timer::at(deadline).await;
                WaitEv::Timeout
            },
            async { WaitEv::Cmd(cmd_rx.recv().await) },
        )
        .await;

        match ev {
            WaitEv::Timeout => return WaitOutcome::Retry,
            WaitEv::Cmd(Err(_)) => return WaitOutcome::Abort,
            WaitEv::Cmd(Ok(cmd)) => match cmd {
                Command::Connect {
                    params,
                    tx,
                    connected_flag,
                    persist,
                } => {
                    return WaitOutcome::Replace(
                        Box::new(ConnectJob {
                            params,
                            server_idx: 0,
                            connected_flag,
                            persist,
                        }),
                        tx,
                    );
                }
                Command::Disconnect {
                    tx, connected_flag, ..
                } => {
                    connected_flag.store(false, Ordering::SeqCst);
                    tx.complete(Ok(()));
                    return WaitOutcome::Abort;
                }
                Command::Publish {
                    qos: QoS::QoS0, tx, ..
                } if buf_cfg.send_while_disconnected && !buf_cfg.persist_qos0 => {
                    tx.complete(Ok(PublishResponse::default()));
                }
                cmd @ Command::Publish { .. } if buf_cfg.send_while_disconnected => {
                    buf_push(buf, cmd, buf_cfg);
                }
                Command::Publish { tx, .. } => {
                    tx.complete(Err(Error::Disconnected));
                }
                Command::Subscribe { tx, .. } => {
                    tx.complete(Err(Error::Disconnected));
                }
                Command::Unsubscribe { tx, .. } => {
                    tx.complete(Err(Error::Disconnected));
                }
            },
        }
    }
}

fn build_connect_response(job: &ConnectJob, connack: ConnAck) -> ConnectResponse {
    let server_uri = job.params.servers[job.server_idx].1.clone();
    ConnectResponse {
        server_uri,
        mqtt_version: if job.params.ver == MqttVersion::V5 {
            5
        }
        else {
            4
        },
        session_present: connack.session_present,
        reason_code: connack.reason_code,
        properties: connack.properties,
    }
}

/////////////////////////////////////////////////////////////////////////////
// Top-level event loop

/// Drives the MQTT event loop on behalf of a [`RuntimeHandle`].
///
/// Semantics are identical to the tokio backend; see `tokio_rt::run_event_loop`
/// for full documentation.
pub(crate) async fn run_event_loop(
    cmd_rx: async_channel::Receiver<Command>,
    event_tx: async_channel::Sender<Event>,
    buf_cfg: BufConfig,
) {
    let mut buf: VecDeque<Command> = VecDeque::new();
    let mut ever_connected = false;

    'outer: loop {
        let (mut job, initial_tx) = loop {
            let cmd = match cmd_rx.recv().await {
                Ok(cmd) => cmd,
                Err(_) => return,
            };
            match ConnectJob::extract(cmd) {
                Ok(pair) => break pair,
                Err(cmd) => {
                    let should_buf = buf_cfg.allow_send_any_time
                        || (ever_connected && buf_cfg.send_while_disconnected);
                    match cmd {
                        Command::Publish {
                            qos: QoS::QoS0, tx, ..
                        } if should_buf && !buf_cfg.persist_qos0 => {
                            tx.complete(Ok(PublishResponse::default()));
                        }
                        cmd @ Command::Publish { .. } if should_buf => {
                            buf_push(&mut buf, cmd, &buf_cfg);
                        }
                        Command::Publish { tx, .. } => {
                            tx.complete(Err(Error::Disconnected));
                        }
                        Command::Subscribe { tx, .. } => {
                            tx.complete(Err(Error::Disconnected));
                        }
                        Command::Unsubscribe { tx, .. } => {
                            tx.complete(Err(Error::Disconnected));
                        }
                        Command::Disconnect {
                            tx, connected_flag, ..
                        } => {
                            connected_flag.store(false, Ordering::SeqCst);
                            tx.complete(Ok(()));
                        }
                        Command::Connect { .. } => unreachable!(),
                    }
                }
            }
        };

        let n = job.params.servers.len();
        let mut last_err: Option<Error> = None;
        let mut initial_result = None;

        for idx in 0..n {
            job.server_idx = idx;
            match do_connect(&job).await {
                Err(e) => last_err = Some(e),
                Ok((reader, writer, connack)) => {
                    if connack.reason_code as u8 >= 0x80 {
                        last_err = Some(Error::ReasonCode(
                            connack.reason_code,
                            connack.properties,
                        ));
                    }
                    else {
                        initial_result = Some((reader, writer, connack));
                        break;
                    }
                }
            }
        }

        let (mut reader, writer, connack) = match initial_result {
            Some(t) => t,
            None => {
                let err = last_err.unwrap_or(Error::General("no servers configured"));
                if matches!(err, Error::Tls(_)) {
                    let _ = event_tx.send(Event::TlsError(err.to_string())).await;
                }
                initial_tx.complete(Err(err));
                continue 'outer;
            }
        };

        let response = build_connect_response(&job, connack);
        job.connected_flag.store(true, Ordering::SeqCst);
        ever_connected = true;
        let _ = event_tx.send(Event::Connected(response.clone())).await;
        initial_tx.complete(Ok(response));

        let mut session = Session::new(
            writer,
            job.params.ver,
            job.params.keep_alive_duration(),
            event_tx.clone(),
            job.persist.clone(),
            job.params.max_inflight,
        );

        if buf_cfg.restore_msgs && session.restore().await.is_err() {
            continue 'outer;
        }
        while let Some(cmd) = buf.pop_front() {
            if session.handle_command(cmd).await.is_err() {
                buf.clear();
                break;
            }
        }

        let first_result = session.run(&mut reader, &cmd_rx).await;
        let broker_disc = session.broker_disconnected;
        if let Some(p) = &session.persist {
            let _ = p.lock().unwrap().close();
        }

        match first_result {
            Ok(()) => continue 'outer,
            Err(_) if broker_disc => {
                job.connected_flag.store(false, Ordering::SeqCst);
                continue 'outer;
            }
            Err(e) => {
                job.connected_flag.store(false, Ordering::SeqCst);
                let _ = event_tx.send(Event::ConnectionLost(e)).await;

                if !job.params.auto_reconnect {
                    continue 'outer;
                }
            }
        }

        let mut retry_delay = job.params.min_retry_interval;

        'retry: loop {
            let tx_opt: Option<Completer<ConnectResponse>> =
                match reconnect_wait(retry_delay, &cmd_rx, &mut buf, &buf_cfg).await {
                    WaitOutcome::Abort => break 'retry,
                    WaitOutcome::Retry => {
                        let n = job.params.servers.len();
                        job.server_idx = (job.server_idx + 1) % n;
                        None
                    }
                    WaitOutcome::Replace(new_job, new_tx) => {
                        job = *new_job;
                        retry_delay = job.params.min_retry_interval;
                        Some(new_tx)
                    }
                };

            match do_connect(&job).await {
                Err(e) => {
                    if matches!(e, Error::Tls(_)) {
                        let _ = event_tx.send(Event::TlsError(e.to_string())).await;
                    }
                    if let Some(tx) = tx_opt {
                        tx.complete(Err(e));
                    }
                    retry_delay = (retry_delay * 2).min(job.params.max_retry_interval);
                }
                Ok((mut reader, writer, connack)) => {
                    if connack.reason_code as u8 >= 0x80 {
                        let e = Error::ReasonCode(connack.reason_code, connack.properties);
                        if let Some(tx) = tx_opt {
                            tx.complete(Err(e));
                        }
                        retry_delay = (retry_delay * 2).min(job.params.max_retry_interval);
                        continue 'retry;
                    }

                    let response = build_connect_response(&job, connack);
                    job.connected_flag.store(true, Ordering::SeqCst);
                    let _ = event_tx.send(Event::Connected(response.clone())).await;
                    if let Some(tx) = tx_opt {
                        tx.complete(Ok(response));
                    }

                    let mut session = Session::new(
                        writer,
                        job.params.ver,
                        job.params.keep_alive_duration(),
                        event_tx.clone(),
                        job.persist.clone(),
                        job.params.max_inflight,
                    );

                    if buf_cfg.restore_msgs && session.restore().await.is_err() {
                        retry_delay = (retry_delay * 2).min(job.params.max_retry_interval);
                        continue 'retry;
                    }
                    while let Some(cmd) = buf.pop_front() {
                        if session.handle_command(cmd).await.is_err() {
                            buf.clear();
                            break;
                        }
                    }

                    let result = session.run(&mut reader, &cmd_rx).await;
                    let broker_disc = session.broker_disconnected;
                    if let Some(p) = &session.persist {
                        let _ = p.lock().unwrap().close();
                    }

                    match result {
                        Ok(()) => break 'retry,
                        Err(_) if broker_disc => {
                            job.connected_flag.store(false, Ordering::SeqCst);
                            break 'retry;
                        }
                        Err(e) => {
                            job.connected_flag.store(false, Ordering::SeqCst);
                            let _ = event_tx.send(Event::ConnectionLost(e)).await;
                            retry_delay = job.params.min_retry_interval;
                        }
                    }
                }
            }
        } // 'retry
    } // 'outer
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_connection<C: Connection>() {}
    fn assert_send<F: Send + 'static>(_: F) {}

    #[test]
    fn test_tcp_connection_implements_connection() {
        assert_connection::<TcpConnection>();
    }

    #[test]
    fn test_run_event_loop_is_send() {
        let (_, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, _) = async_channel::unbounded::<Event>();
        assert_send(run_event_loop(
            cmd_rx,
            event_tx,
            BufConfig {
                send_while_disconnected: false,
                max_buffered_msgs: 100,
                allow_send_any_time: false,
                delete_oldest: false,
                restore_msgs: false,
                persist_qos0: true,
            },
        ));
    }
}
