// paho-mqtt/src/runtime/tokio_rt.rs
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

//! Tokio-based async transport and event loop for the MQTT client.
//!
//! Provides the [`Connection`] trait, [`TcpConnection`] implementation,
//! [`RuntimeHandle`] for cross-thread communication, and the internal
//! [`Session`] event loop.

use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::BytesMut;
use paho_mqtt_codec::{
    ConnAck, Disconnect, MqttVersion, PubAck, QoS, RetainHandling as CodecRetainHandling,
    Subscribe, Subscription, Unsubscribe,
    packet::{Packet, RawPacket},
};
#[cfg(feature = "tls")]
use std::sync::Arc as StdArc;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::{
    io::{
        AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf, ReadHalf, WriteHalf,
    },
    net::TcpStream,
    time,
};
#[cfg(feature = "tls")]
use tokio_rustls::TlsConnector;

use super::commands::{Command, ConnectParams, Event, ServerAddr};
use crate::{
    Error, Message, Result,
    persistence::ClientPersistence,
    responses::{ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse},
    token::Completer,
};

/////////////////////////////////////////////////////////////////////////////

/// A bidirectional async transport used by the internal event loop.
///
/// Any type that implements `AsyncRead + AsyncWrite + Send + Unpin` can serve
/// as a transport; this marker trait bundles those bounds into a single name.
pub(crate) trait Connection:
    AsyncRead + AsyncWrite + Send + Unpin + 'static
{
}

/////////////////////////////////////////////////////////////////////////////

/// A plain TCP transport backed by [`tokio::net::TcpStream`].
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

impl AsyncRead for TcpConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl Connection for TcpConnection {}

/////////////////////////////////////////////////////////////////////////////

/// A Unix-domain socket transport backed by [`tokio::net::UnixStream`].
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
impl AsyncRead for UnixConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

#[cfg(unix)]
impl AsyncWrite for UnixConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[cfg(unix)]
impl Connection for UnixConnection {}

/////////////////////////////////////////////////////////////////////////////

/// A TLS transport backed by [`tokio_rustls`] (requires the `tls` feature).
#[cfg(feature = "tls")]
pub(crate) struct TlsConnection {
    stream: tokio_rustls::client::TlsStream<TcpStream>,
}

#[cfg(feature = "tls")]
impl AsyncRead for TlsConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

#[cfg(feature = "tls")]
impl AsyncWrite for TlsConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[cfg(feature = "tls")]
impl Connection for TlsConnection {}

#[cfg(feature = "tls")]
type TlsResult<T> = std::result::Result<T, rustls::Error>;

/// A [`rustls::client::ServerCertVerifier`] that accepts any certificate.
///
/// Used when `SslOptions::enable_server_cert_auth` is `false`.
#[cfg(feature = "tls")]
#[derive(Debug)]
struct NoCertVerifier;

#[cfg(feature = "tls")]
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

/// Builds a [`rustls::ClientConfig`] from [`SslOptions`].
#[cfg(feature = "tls")]
fn make_tls_config(ssl: &crate::SslOptions) -> Result<StdArc<rustls::ClientConfig>> {
    use rustls::RootCertStore;
    use std::io::BufReader;

    let mut root_store = RootCertStore::empty();

    if !ssl.disable_default_trust_store {
        let native_certs = rustls_native_certs::load_native_certs().map_err(Error::Io)?;
        for cert in native_certs {
            let _ = root_store.add(cert);
        }
    }

    if !ssl.trust_store.as_os_str().is_empty() {
        let f = std::fs::File::open(&ssl.trust_store).map_err(Error::Io)?;
        let mut r = BufReader::new(f);
        for cert in rustls_pemfile::certs(&mut r) {
            root_store
                .add(cert.map_err(Error::Io)?)
                .map_err(|e| Error::GeneralString(e.to_string()))?;
        }
    }

    let wants_client = if ssl.enable_server_cert_auth {
        rustls::ClientConfig::builder().with_root_certificates(root_store)
    }
    else {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(StdArc::new(NoCertVerifier))
    };

    let mut config = if !ssl.key_store.as_os_str().is_empty() {
        let cert_f = std::fs::File::open(&ssl.key_store).map_err(Error::Io)?;
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(cert_f))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Error::Io)?;

        let key_path = if !ssl.private_key.as_os_str().is_empty() {
            &ssl.private_key
        }
        else {
            &ssl.key_store
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

    if !ssl.protos.is_empty() {
        config.alpn_protocols = ssl.protos.iter().map(|p| p.as_bytes().to_vec()).collect();
    }

    Ok(StdArc::new(config))
}

/////////////////////////////////////////////////////////////////////////////

/// A handle to the internal async runtime.
///
/// Holds the two `async_channel`s that bridge the public (sync) API and the
/// internal tokio event loop. Created by [`RuntimeHandle::spawn`].
pub(crate) struct RuntimeHandle {
    cmd_tx: async_channel::Sender<Command>,
    event_rx: async_channel::Receiver<Event>,
}

impl RuntimeHandle {
    /// Spawns a dedicated OS thread with a single-threaded tokio runtime and
    /// starts the MQTT event loop on it.
    ///
    /// Returns a handle that can send commands to and receive events from the
    /// loop.
    pub(crate) fn spawn() -> Self {
        let (cmd_tx, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, event_rx) = async_channel::unbounded::<Event>();

        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime")
                .block_on(run_event_loop(cmd_rx, event_tx));
        });

        Self { cmd_tx, event_rx }
    }

    /// Sends a command to the event loop asynchronously.
    pub(crate) async fn send_cmd(&self, cmd: Command) -> Result<()> {
        self.cmd_tx.send(cmd).await.map_err(|_| Error::Disconnected)
    }

    /// Sends a command to the event loop from a synchronous context.
    pub(crate) fn send_cmd_blocking(&self, cmd: Command) -> Result<()> {
        self.cmd_tx
            .send_blocking(cmd)
            .map_err(|_| Error::Disconnected)
    }

    /// Returns a reference to the inbound event receiver.
    pub(crate) fn event_rx(&self) -> &async_channel::Receiver<Event> {
        &self.event_rx
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
/// returns the decoded CONNACK. Returns an error if any other packet arrives
/// first or if the connection closes.
async fn recv_connack(
    reader: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
    ver: MqttVersion,
) -> Result<ConnAck> {
    loop {
        reader.read_buf(buf).await?;
        while let Some(raw) = RawPacket::decode(buf)? {
            match Packet::from_raw(raw, ver)? {
                Packet::ConnAck(ack) => return Ok(ack),
                _ => return Err(Error::General("expected CONNACK")),
            }
        }
    }
}

/// Encodes `packet` into `buf` and writes it to `writer`.
async fn send_packet(
    writer: &mut (impl AsyncWrite + Unpin),
    packet: &Packet,
    ver: MqttVersion,
    buf: &mut BytesMut,
) -> Result<()> {
    buf.clear();
    packet.encode(buf, ver)?;
    writer.write_all(buf).await?;
    Ok(())
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
    /// Shared flag cleared before the disconnect token resolves.
    connected_flag: Arc<AtomicBool>,
    /// Persistence store for in-flight QoS 1/2 messages (if any).
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
}

impl Session {
    fn new(
        writer: WriteHalf<Box<dyn Connection>>,
        ver: MqttVersion,
        keep_alive: Duration,
        event_tx: async_channel::Sender<Event>,
        connected_flag: Arc<AtomicBool>,
        persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
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
            connected_flag,
            persist,
        }
    }

    /// Returns `true` if any outbound operations are waiting for broker ACKs.
    fn has_pending(&self) -> bool {
        !self.pending_pub1.is_empty()
            || !self.pending_pub2.is_empty()
            || !self.pending_sub.is_empty()
            || !self.pending_unsub.is_empty()
    }

    /// Persists `data` under `key`, silently ignoring errors.
    fn persist_put(&self, key: &str, data: &[u8]) {
        if let Some(p) = &self.persist {
            let _ = p.lock().unwrap().put(key, vec![data]);
        }
    }

    /// Removes the persistence entry for `key`, silently ignoring errors.
    fn persist_remove(&self, key: &str) {
        if let Some(p) = &self.persist {
            let _ = p.lock().unwrap().remove(key);
        }
    }

    /// Allocates the next available packet identifier (1 – 65535, wrapping).
    fn alloc_id(&mut self) -> u16 {
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1;
        }
        self.next_id
    }

    /// Encodes and sends a packet on the wire.
    async fn send(&mut self, packet: &Packet) -> Result<()> {
        self.write_buf.clear();
        packet.encode(&mut self.write_buf, self.ver)?;
        self.writer.write_all(&self.write_buf).await?;
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
        // keep_alive == ZERO means disabled; the select arm is gated on this flag.
        let ka_enabled = !keep_alive.is_zero();

        // Resettable sleep: deadline advances on every outbound packet so that
        // PINGREQs are only sent when the connection has been truly idle.
        let ka_sleep = time::sleep(keep_alive);
        tokio::pin!(ka_sleep);

        let mut read_buf = BytesMut::with_capacity(4096);

        loop {
            tokio::select! {
                result = reader.read_buf(&mut read_buf) => {
                    let n = result?;
                    if n == 0 {
                        return Err(Error::Disconnected);
                    }
                    while let Some(raw) = RawPacket::decode(&mut read_buf)? {
                        let packet = Packet::from_raw(raw, self.ver)?;
                        self.handle_packet(packet).await?;
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Err(_) => return Ok(()),
                        Ok(Command::Disconnect {
                            timeout,
                            reason_code,
                            props,
                            tx,
                            connected_flag,
                        }) => {
                            // If requested, wait for in-flight ops to complete
                            // before sending DISCONNECT.
                            if !timeout.is_zero() && self.has_pending() {
                                let deadline = time::Instant::now() + timeout;
                                'drain: loop {
                                    if !self.has_pending() {
                                        break;
                                    }
                                    tokio::select! {
                                        result = reader.read_buf(&mut read_buf) => {
                                            match result {
                                                Ok(0) | Err(_) => break 'drain,
                                                Ok(_) => {
                                                    while let Ok(Some(raw)) =
                                                        RawPacket::decode(&mut read_buf)
                                                    {
                                                        match Packet::from_raw(raw, self.ver) {
                                                            Ok(pkt) => {
                                                                let _ =
                                                                    self.handle_packet(pkt).await;
                                                            }
                                                            Err(_) => break 'drain,
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        _ = time::sleep_until(deadline) => break 'drain,
                                    }
                                }
                            }
                            let disc = Disconnect { reason_code, properties: props };
                            let _ = self.send(&Packet::Disconnect(disc)).await;
                            connected_flag.store(false, Ordering::SeqCst);
                            tx.complete(Ok(()));
                            return Ok(());
                        }
                        Ok(cmd) => {
                            if self.handle_command(cmd).await? {
                                return Ok(());
                            }
                            // Any outbound packet resets the keep-alive deadline.
                            if ka_enabled {
                                ka_sleep.as_mut().reset(time::Instant::now() + keep_alive);
                            }
                        }
                    }
                }
                _ = &mut ka_sleep, if ka_enabled => {
                    if self.ping_outstanding {
                        return Err(Error::Timeout);
                    }
                    self.send(&Packet::PingReq).await?;
                    self.ping_outstanding = true;
                    ka_sleep.as_mut().reset(time::Instant::now() + keep_alive);
                }
            }
        }
    }

    /// Handles one inbound packet from the broker.
    async fn handle_packet(&mut self, packet: Packet) -> Result<()> {
        match packet {
            // ---- Publish acknowledgements ----
            Packet::PubAck(ack) => {
                if let Some(tx) = self.pending_pub1.remove(&ack.packet_id) {
                    self.persist_remove(&format!("s{}", ack.packet_id));
                    let rsp = PublishResponse::new(ack.reason_code, ack.properties);
                    tx.complete(Ok(rsp));
                }
            }

            // ---- QoS 2 outbound: PUBREC → send PUBREL ----
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

            // ---- QoS 2 outbound: PUBCOMP → resolve token ----
            Packet::PubComp(comp) => {
                if let Some(tx) = self.pending_pub2.remove(&comp.packet_id) {
                    self.persist_remove(&format!("s{}", comp.packet_id));
                    let rsp = PublishResponse::new(comp.reason_code, comp.properties);
                    tx.complete(Ok(rsp));
                }
            }

            // ---- Subscription acknowledgement ----
            Packet::SubAck(ack) => {
                if let Some(tx) = self.pending_sub.remove(&ack.packet_id) {
                    let rsp = SubscribeResponse::new(ack.reason_codes, ack.properties);
                    tx.complete(Ok(rsp));
                }
            }

            // ---- Unsubscription acknowledgement ----
            Packet::UnsubAck(ack) => {
                if let Some(tx) = self.pending_unsub.remove(&ack.packet_id) {
                    let rsp = UnsubscribeResponse::new(ack.reason_codes, ack.properties);
                    tx.complete(Ok(rsp));
                }
            }

            // ---- Inbound application message ----
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
                    // Persist the inbound QoS 2 PUBLISH before acknowledging
                    // so we can redeliver the message on reconnect.
                    let mut enc_buf = BytesMut::with_capacity(256);
                    if Packet::Publish(pub_pkt.clone())
                        .encode(&mut enc_buf, self.ver)
                        .is_ok()
                    {
                        self.persist_put(&format!("r{id}"), &enc_buf);
                    }
                    let msg = Message::from(pub_pkt);
                    self.pending_pub2_in.insert(id, msg);
                    let rec = PubAck::new(id);
                    self.send(&Packet::PubRec(rec)).await?;
                }
            },

            // ---- QoS 2 inbound: PUBREL → send PUBCOMP, deliver message ----
            Packet::PubRel(rel) => {
                if let Some(msg) = self.pending_pub2_in.remove(&rel.packet_id) {
                    self.persist_remove(&format!("r{}", rel.packet_id));
                    let _ = self.event_tx.send(Event::MessageArrived(msg)).await;
                }
                let comp = PubAck::new(rel.packet_id);
                self.send(&Packet::PubComp(comp)).await?;
            }

            // ---- Keep-alive response ----
            Packet::PingResp => {
                self.ping_outstanding = false;
            }

            // ---- Broker-initiated disconnect (v5) ----
            Packet::Disconnect(_) => {
                return Err(Error::Disconnected);
            }

            _ => {} // ignore unexpected packets
        }
        Ok(())
    }

    /// Handles one command from the public API.
    ///
    /// `Command::Disconnect` is intercepted by `run()` before this is called
    /// and should never reach here.
    async fn handle_command(&mut self, cmd: Command) -> Result<bool> {
        match cmd {
            Command::Connect { tx, .. } => {
                // Already connected; reject duplicate connect.
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

                // Persist QoS 1/2 PUBLISH before sending so we can
                // re-transmit after a crash.
                if let Some(pid) = id {
                    let mut enc_buf = BytesMut::with_capacity(256);
                    if Packet::Publish(pub_pkt.clone())
                        .encode(&mut enc_buf, self.ver)
                        .is_ok()
                    {
                        self.persist_put(&format!("s{pid}"), &enc_buf);
                    }
                }

                self.send(&Packet::Publish(pub_pkt)).await?;

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

            // Intercepted in run() before this method is called.
            Command::Disconnect { .. } => unreachable!(),
        }
        Ok(false)
    }
}

/////////////////////////////////////////////////////////////////////////////
// Auto-reconnect helpers

/// All parameters needed to establish one MQTT session.
///
/// Extracted from [`Command::Connect`] so they can be retained across
/// reconnect attempts without re-receiving the command.
struct ConnectJob {
    params: ConnectParams,
    /// Index into `params.servers` for the current connection attempt.
    server_idx: usize,
    connected_flag: Arc<AtomicBool>,
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
}

impl ConnectJob {
    /// Extracts a `ConnectJob` from a `Command::Connect`, returning `None` for
    /// any other command variant.
    fn extract(cmd: Command) -> Option<(Self, Completer<ConnectResponse>)> {
        match cmd {
            Command::Connect {
                params,
                tx,
                connected_flag,
                persist,
            } => Some((
                Self {
                    params,
                    server_idx: 0,
                    connected_flag,
                    persist,
                },
                tx,
            )),
            _ => None,
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

    // ---- Open persistence ----
    if let Some(store) = &job.persist {
        store.lock().unwrap().open(&p.client_id, server_uri)?;
    }

    // ---- Transport connect ----
    let conn: Box<dyn Connection> = match addr {
        ServerAddr::Tcp { host, port } => {
            Box::new(TcpConnection::connect(host, *port).await?)
        }
        #[cfg(feature = "tls")]
        ServerAddr::Tls { host, port } => {
            let ssl = p.ssl.as_ref().ok_or(Error::MissingSslOptions)?;
            let config = make_tls_config(ssl)?;
            let connector = TlsConnector::from(config);
            let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                .map_err(|_| {
                    Error::GeneralString(format!("invalid TLS server name: {}", host))
                })?
                .to_owned();
            let tcp = TcpStream::connect((host.as_str(), *port)).await?;
            tcp.set_nodelay(true)?;
            let tls = connector.connect(server_name, tcp).await?;
            Box::new(TlsConnection { stream: tls })
        }
        #[cfg(unix)]
        ServerAddr::Unix { path } => Box::new(UnixConnection::connect(path).await?),
    };

    let (mut reader, mut writer) = tokio::io::split(conn);

    // ---- Build and send CONNECT packet ----
    let connect_pkt = paho_mqtt_codec::Connect {
        clean_start: p.clean_start,
        keep_alive: p.keep_alive_secs,
        client_id: p.client_id.clone(),
        username: p.user_name.clone(),
        password: p.password.clone(),
        will: p.will.clone(),
        properties: p.props.clone(),
        ..Default::default()
    };

    let mut buf = BytesMut::with_capacity(256);
    send_packet(&mut writer, &Packet::Connect(connect_pkt), p.ver, &mut buf).await?;

    // ---- Wait for CONNACK ----
    let mut read_buf = BytesMut::with_capacity(256);
    let connack = recv_connack(&mut reader, &mut read_buf, p.ver).await?;

    Ok((reader, writer, connack))
}

/// Result of waiting for the reconnect timer to fire.
enum WaitOutcome {
    /// Timer expired — proceed with the current [`ConnectJob`].
    Retry,
    /// A `Disconnect` command arrived; the token has been resolved and the
    /// reconnect loop should be abandoned.
    Abort,
    /// A new `Connect` command arrived with updated parameters; use these
    /// for the next attempt and resolve the supplied token with the result.
    Replace(ConnectJob, Completer<ConnectResponse>),
}

/// Waits for `delay` to elapse while draining the command queue.
///
/// * `Disconnect` → completes the token (already disconnected) and returns
///   [`WaitOutcome::Abort`].
/// * `Connect` → captures the new parameters and returns
///   [`WaitOutcome::Replace`] immediately (skips remaining delay).
/// * Any other command → completes the completer with
///   [`Error::Disconnected`] and keeps waiting.
async fn reconnect_wait(
    delay: Duration,
    cmd_rx: &async_channel::Receiver<Command>,
) -> WaitOutcome {
    let sleep = time::sleep(delay);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            _ = &mut sleep => return WaitOutcome::Retry,
            result = cmd_rx.recv() => {
                match result {
                    Err(_) => return WaitOutcome::Abort,
                    Ok(cmd) => match cmd {
                        Command::Connect {
                            params,
                            tx,
                            connected_flag,
                            persist,
                        } => {
                            return WaitOutcome::Replace(
                                ConnectJob {
                                    params,
                                    server_idx: 0,
                                    connected_flag,
                                    persist,
                                },
                                tx,
                            );
                        }
                        Command::Disconnect {
                            tx,
                            connected_flag,
                            ..
                        } => {
                            // We are already disconnected; satisfy the caller.
                            connected_flag.store(false, Ordering::SeqCst);
                            tx.complete(Ok(()));
                            return WaitOutcome::Abort;
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
    }
}

/// Builds a [`ConnectResponse`] from a successful CONNACK and job metadata.
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
/// Runs indefinitely on a dedicated tokio runtime thread.  Waits for a
/// [`Command::Connect`], calls [`do_connect`] to perform the TCP + MQTT
/// handshake, then delegates to [`Session::run`] for the lifetime of the
/// connection.
///
/// If `auto_reconnect` is set in the connect options and the session ends
/// unexpectedly, the loop retries with exponential backoff (capped at
/// `max_retry_interval`) without re-waiting for a new `Command::Connect`.
/// An explicit `Command::Disconnect` during the backoff wait aborts the
/// reconnect loop.
pub(crate) async fn run_event_loop(
    cmd_rx: async_channel::Receiver<Command>,
    event_tx: async_channel::Sender<Event>,
) {
    'outer: loop {
        // ---- Wait for a Connect command ----
        let cmd = match cmd_rx.recv().await {
            Ok(cmd) => cmd,
            Err(_) => return,
        };

        let Some((mut job, initial_tx)) = ConnectJob::extract(cmd)
        else {
            continue 'outer;
        };

        // ---- First connection attempt: try each server in order ----
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
                initial_tx.complete(Err(
                    last_err.unwrap_or(Error::General("no servers configured"))
                ));
                continue 'outer;
            }
        };

        let response = build_connect_response(&job, connack);
        // Set the flag before resolving the token so `is_connected()` is true
        // as soon as `wait()` / `.await` returns in the caller.
        job.connected_flag.store(true, Ordering::SeqCst);
        let _ = event_tx.send(Event::Connected(response.clone())).await;
        initial_tx.complete(Ok(response));

        // ---- Run the session ----
        let mut session = Session::new(
            writer,
            job.params.ver,
            job.params.keep_alive_duration(),
            event_tx.clone(),
            job.connected_flag.clone(),
            job.persist.clone(),
        );

        let first_result = session.run(&mut reader, &cmd_rx).await;
        if let Some(p) = &session.persist {
            let _ = p.lock().unwrap().close();
        }

        match first_result {
            Ok(()) => continue 'outer, // clean disconnect
            Err(e) => {
                job.connected_flag.store(false, Ordering::SeqCst);
                let _ = event_tx.send(Event::ConnectionLost(e)).await;

                if !job.params.auto_reconnect {
                    continue 'outer;
                }
            }
        }

        // ---- Auto-reconnect loop ----
        let mut retry_delay = job.params.min_retry_interval;

        'retry: loop {
            // Wait for the backoff delay, draining the command queue.
            let tx_opt: Option<Completer<ConnectResponse>> =
                match reconnect_wait(retry_delay, &cmd_rx).await {
                    WaitOutcome::Abort => break 'retry,
                    WaitOutcome::Retry => {
                        // Advance to the next server in round-robin order.
                        let n = job.params.servers.len();
                        job.server_idx = (job.server_idx + 1) % n;
                        None
                    }
                    WaitOutcome::Replace(new_job, new_tx) => {
                        // Explicit reconnect/connect: use the new params and
                        // reset the backoff.
                        job = new_job;
                        retry_delay = job.params.min_retry_interval;
                        Some(new_tx)
                    }
                };

            match do_connect(&job).await {
                Err(e) => {
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
                        job.connected_flag.clone(),
                        job.persist.clone(),
                    );

                    let result = session.run(&mut reader, &cmd_rx).await;
                    if let Some(p) = &session.persist {
                        let _ = p.lock().unwrap().close();
                    }

                    match result {
                        Ok(()) => break 'retry, // clean disconnect
                        Err(e) => {
                            job.connected_flag.store(false, Ordering::SeqCst);
                            let _ = event_tx.send(Event::ConnectionLost(e)).await;
                            // Reset backoff on each successful-then-lost connection.
                            retry_delay = job.params.min_retry_interval;
                        }
                    }
                }
            }
        } // 'retry
    } // 'outer
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_connection<C: Connection>() {}

    #[test]
    fn test_tcp_connection_implements_connection() {
        assert_connection::<TcpConnection>();
    }
}
