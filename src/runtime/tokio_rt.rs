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

use bytes::{Bytes, BytesMut};
use paho_mqtt_codec::{
    ConnAck, Disconnect, MqttVersion, PubAck, QoS, RetainHandling as CodecRetainHandling,
    Subscribe, Subscription, Unsubscribe,
    packet::{Packet, RawPacket},
};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf, WriteHalf},
    net::TcpStream,
    time,
};

use super::commands::{Command, Event, ReceivedMessage, ServerAddr};
use crate::{
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
    pub(crate) async fn send_cmd(&self, cmd: Command) -> crate::Result<()> {
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| crate::Error::Disconnected)
    }

    /// Sends a command to the event loop from a synchronous context.
    pub(crate) fn send_cmd_blocking(&self, cmd: Command) -> crate::Result<()> {
        self.cmd_tx
            .send_blocking(cmd)
            .map_err(|_| crate::Error::Disconnected)
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
) -> crate::Result<ConnAck> {
    loop {
        reader.read_buf(buf).await?;
        while let Some(raw) = RawPacket::decode(buf)? {
            match Packet::from_raw(raw, ver)? {
                Packet::ConnAck(ack) => return Ok(ack),
                _ => return Err(crate::Error::General("expected CONNACK")),
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
) -> crate::Result<()> {
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
    pending_pub2_in: HashMap<u16, ReceivedMessage>,
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
    async fn send(&mut self, packet: &Packet) -> crate::Result<()> {
        self.write_buf.clear();
        packet.encode(&mut self.write_buf, self.ver)?;
        self.writer.write_all(&self.write_buf).await?;
        Ok(())
    }

    /// Main event loop: read from `reader`, dispatch commands from `cmd_rx`,
    /// and service keep-alive ticks until an error or clean disconnect.
    async fn run(
        &mut self,
        reader: &mut (impl AsyncRead + Unpin),
        cmd_rx: &async_channel::Receiver<Command>,
    ) -> crate::Result<()> {
        let keep_alive = self.keep_alive;
        let mut ka = time::interval(keep_alive);
        ka.tick().await; // consume the immediate first tick

        let mut read_buf = BytesMut::with_capacity(4096);

        loop {
            tokio::select! {
                result = reader.read_buf(&mut read_buf) => {
                    let n = result?;
                    if n == 0 {
                        return Err(crate::Error::Disconnected);
                    }
                    while let Some(raw) = RawPacket::decode(&mut read_buf)? {
                        let packet = Packet::from_raw(raw, self.ver)?;
                        self.handle_packet(packet).await?;
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Ok(cmd) => {
                            if self.handle_command(cmd).await? {
                                return Ok(());
                            }
                        }
                        Err(_) => return Ok(()),
                    }
                }
                _ = ka.tick() => {
                    if self.ping_outstanding {
                        return Err(crate::Error::Timeout);
                    }
                    self.send(&Packet::PingReq).await?;
                    self.ping_outstanding = true;
                }
            }
        }
    }

    /// Handles one inbound packet from the broker.
    async fn handle_packet(&mut self, packet: Packet) -> crate::Result<()> {
        match packet {
            // ---- Publish acknowledgements ----
            Packet::PubAck(ack) => {
                if let Some(tx) = self.pending_pub1.remove(&ack.packet_id) {
                    self.persist_remove(&format!("s{}", ack.packet_id));
                    let rsp = PublishResponse {
                        reason_code: ack.reason_code,
                        properties: ack.properties,
                    };
                    tx.complete(Ok(rsp));
                }
            }

            // ---- QoS 2 outbound: PUBREC → send PUBREL ----
            Packet::PubRec(rec) => {
                if rec.reason_code as u8 >= 0x80 {
                    if let Some(tx) = self.pending_pub2.remove(&rec.packet_id) {
                        tx.complete(Err(crate::Error::ReasonCode(
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
                    let rsp = PublishResponse {
                        reason_code: comp.reason_code,
                        properties: comp.properties,
                    };
                    tx.complete(Ok(rsp));
                }
            }

            // ---- Subscription acknowledgement ----
            Packet::SubAck(ack) => {
                if let Some(tx) = self.pending_sub.remove(&ack.packet_id) {
                    let rsp = SubscribeResponse {
                        reason_codes: ack.reason_codes,
                        properties: ack.properties,
                    };
                    tx.complete(Ok(rsp));
                }
            }

            // ---- Unsubscription acknowledgement ----
            Packet::UnsubAck(ack) => {
                if let Some(tx) = self.pending_unsub.remove(&ack.packet_id) {
                    let rsp = UnsubscribeResponse {
                        reason_codes: ack.reason_codes,
                        properties: ack.properties,
                    };
                    tx.complete(Ok(rsp));
                }
            }

            // ---- Inbound application message ----
            Packet::Publish(pub_pkt) => match pub_pkt.qos {
                QoS::QoS0 => {
                    let msg = ReceivedMessage {
                        topic: pub_pkt.topic,
                        payload: pub_pkt.payload,
                        qos: pub_pkt.qos,
                        retain: pub_pkt.retain,
                        dup: pub_pkt.dup,
                        packet_id: None,
                        props: pub_pkt.properties,
                    };
                    let _ = self.event_tx.send(Event::MessageArrived(msg)).await;
                }
                QoS::QoS1 => {
                    let id = pub_pkt.packet_id.unwrap_or(0);
                    let msg = ReceivedMessage {
                        topic: pub_pkt.topic,
                        payload: pub_pkt.payload,
                        qos: pub_pkt.qos,
                        retain: pub_pkt.retain,
                        dup: pub_pkt.dup,
                        packet_id: Some(id),
                        props: pub_pkt.properties,
                    };
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
                    let msg = ReceivedMessage {
                        topic: pub_pkt.topic,
                        payload: pub_pkt.payload,
                        qos: pub_pkt.qos,
                        retain: pub_pkt.retain,
                        dup: pub_pkt.dup,
                        packet_id: Some(id),
                        props: pub_pkt.properties,
                    };
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
                return Err(crate::Error::Disconnected);
            }

            _ => {} // ignore unexpected packets
        }
        Ok(())
    }

    /// Handles one command from the public API.
    ///
    /// Returns `true` when a `Disconnect` command signals a clean shutdown.
    async fn handle_command(&mut self, cmd: Command) -> crate::Result<bool> {
        match cmd {
            Command::Connect { tx, .. } => {
                // Already connected; reject duplicate connect.
                tx.complete(Err(crate::Error::General("already connected")));
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

            Command::Disconnect {
                reason_code,
                props,
                tx,
                connected_flag,
            } => {
                let disc = Disconnect {
                    reason_code,
                    properties: props,
                };
                self.send(&Packet::Disconnect(disc)).await?;
                // Clear the flag before resolving the token so `is_connected()`
                // returns false as soon as `wait()` returns in the caller.
                connected_flag.store(false, Ordering::SeqCst);
                tx.complete(Ok(()));
                return Ok(true); // signal clean shutdown
            }
        }
        Ok(false)
    }
}

/////////////////////////////////////////////////////////////////////////////
// Top-level event loop

/// Drives the MQTT event loop on behalf of a [`RuntimeHandle`].
///
/// This function runs indefinitely on a dedicated tokio runtime thread. It
/// waits for a [`Command::Connect`], establishes the TCP + MQTT handshake,
/// and then delegates to [`Session::run`] for the lifetime of the connection.
/// If the session ends it loops back and waits for the next connect command.
pub(crate) async fn run_event_loop(
    cmd_rx: async_channel::Receiver<Command>,
    event_tx: async_channel::Sender<Event>,
) {
    loop {
        // ---- Wait for a Connect command ----
        let cmd = match cmd_rx.recv().await {
            Ok(cmd) => cmd,
            Err(_) => return,
        };

        let (
            addr,
            server_uri,
            client_id,
            clean_start,
            keep_alive_secs,
            user_name,
            password,
            will,
            ver,
            props,
            tx,
            connected_flag,
            persist,
        ) = match cmd {
            Command::Connect {
                addr,
                server_uri,
                client_id,
                clean_start,
                keep_alive_secs,
                user_name,
                password,
                will,
                ver,
                props,
                tx,
                connected_flag,
                persist,
            } => (
                addr,
                server_uri,
                client_id,
                clean_start,
                keep_alive_secs,
                user_name,
                password,
                will,
                ver,
                props,
                tx,
                connected_flag,
                persist,
            ),
            _ => continue, // ignore non-Connect commands before connecting
        };

        // ---- Transport connect ----
        let conn: Box<dyn Connection> = match addr {
            ServerAddr::Tcp { host, port } => {
                match TcpConnection::connect(&host, port).await {
                    Ok(c) => Box::new(c),
                    Err(e) => {
                        tx.complete(Err(e.into()));
                        continue;
                    }
                }
            }
            #[cfg(unix)]
            ServerAddr::Unix { path } => match UnixConnection::connect(&path).await {
                Ok(c) => Box::new(c),
                Err(e) => {
                    tx.complete(Err(e.into()));
                    continue;
                }
            },
        };

        // ---- Open persistence for this connection ----
        if let Some(p) = &persist {
            if let Err(e) = p.lock().unwrap().open(&client_id, &server_uri) {
                tx.complete(Err(e));
                continue;
            }
        }

        // ---- Build codec CONNECT packet ----
        let connect_pkt = paho_mqtt_codec::Connect {
            clean_start,
            keep_alive: keep_alive_secs,
            client_id,
            username: user_name,
            password: password.map(Bytes::from),
            will,
            properties: props,
            ..Default::default()
        };

        // ---- Split and send CONNECT ----
        let (mut reader, mut writer) = tokio::io::split(conn);

        let mut buf = BytesMut::with_capacity(256);
        if let Err(e) =
            send_packet(&mut writer, &Packet::Connect(connect_pkt), ver, &mut buf).await
        {
            tx.complete(Err(e));
            continue;
        }

        // ---- Wait for CONNACK ----
        let mut read_buf = BytesMut::with_capacity(256);
        let connack = match recv_connack(&mut reader, &mut read_buf, ver).await {
            Ok(c) => c,
            Err(e) => {
                tx.complete(Err(e));
                continue;
            }
        };

        if connack.reason_code as u8 >= 0x80 {
            tx.complete(Err(crate::Error::ReasonCode(
                connack.reason_code,
                connack.properties,
            )));
            continue;
        }

        let mqtt_version = match ver {
            MqttVersion::V5 => 5u32,
            _ => 4,
        };

        let response = ConnectResponse {
            server_uri,
            mqtt_version,
            session_present: connack.session_present,
            reason_code: connack.reason_code,
            properties: connack.properties,
        };
        // Set the flag before resolving the token so `is_connected()` is
        // true as soon as `wait()` / `.await` returns in the caller.
        connected_flag.store(true, Ordering::SeqCst);
        let _ = event_tx.send(Event::Connected(response.clone())).await;
        tx.complete(Ok(response));

        // ---- Run the session ----
        let keep_alive = if keep_alive_secs > 0 {
            Duration::from_secs(keep_alive_secs as u64)
        }
        else {
            Duration::from_secs(3600)
        };

        let mut session = Session::new(
            writer,
            ver,
            keep_alive,
            event_tx.clone(),
            connected_flag,
            persist,
        );
        if let Err(e) = session.run(&mut reader, &cmd_rx).await {
            session.connected_flag.store(false, Ordering::SeqCst);
            let _ = event_tx.send(Event::ConnectionLost(e)).await;
        }
        if let Some(p) = &session.persist {
            let _ = p.lock().unwrap().close();
        }
    }
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
