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

//! Tokio-based async event loop for the MQTT client.
//!
//! Provides [`RuntimeHandle`] for cross-thread communication and the
//! internal [`Session`] event loop.  Transport types live in
//! [`super::tokio_conn`].

#[cfg(all(feature = "openssl", not(feature = "tls")))]
use std::pin::Pin;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use paho_mqtt_codec::{
    ConnAck, Disconnect, MqttVersion, PubAck, QoS, RetainHandling as CodecRetainHandling,
    Subscribe, Subscription, Unsubscribe,
    packet::{Packet, RawPacket},
};
#[cfg(any(feature = "tls", feature = "openssl", feature = "websocket"))]
use tokio::net::TcpStream;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    time,
};
#[cfg(feature = "tls")]
use tokio_rustls::TlsConnector;

#[cfg(unix)]
use super::tokio_conn::UnixConnection;
#[cfg(all(feature = "openssl", not(feature = "tls")))]
use super::tokio_conn::{OpenSslConnection, make_openssl_config};
#[cfg(feature = "tls")]
use super::tokio_conn::{TlsConnection, make_tls_config};
#[cfg(feature = "websocket")]
use super::tokio_conn::{WsConnection, build_ws_request};
use super::{
    commands::{BufConfig, Command, ConnectParams, Event, ServerAddr},
    connection::Connection,
    tokio_conn::TcpConnection,
};

use crate::{
    Error, Message, Result,
    persistence::ClientPersistence,
    responses::{ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse},
    token::Completer,
};

/////////////////////////////////////////////////////////////////////////////

/// Tracks whether this client owns the Tokio runtime or merely borrows it.
///
/// Stored in [`InnerAsyncClient`] to keep the owned runtime alive until the
/// last client using it is dropped.
///
/// [`InnerAsyncClient`]: crate::async_client::InnerAsyncClient
pub(crate) enum RuntimeOwnership {
    /// The library created this runtime; the `Arc` keeps it alive until the
    /// last client that shares it is dropped.
    #[allow(dead_code)] // held purely for its Drop effect
    Owned(Arc<tokio::runtime::Runtime>),
    /// The application owns the runtime; the library only holds a `Handle`.
    Borrowed,
}

/// Process-wide shared runtime registry.
///
/// Holds a `Weak` reference so the runtime is dropped automatically when
/// the last owned client is dropped.
static SHARED_RT: Mutex<Option<Weak<tokio::runtime::Runtime>>> = Mutex::new(None);

/// Returns a `Handle` to the appropriate runtime and the corresponding
/// ownership record, based on `mode`.
pub(crate) fn resolve_runtime(
    mode: &crate::create_options::RuntimeMode,
) -> (tokio::runtime::Handle, RuntimeOwnership) {
    use crate::create_options::RuntimeMode;
    match mode {
        RuntimeMode::Auto => {
            if let Ok(h) = tokio::runtime::Handle::try_current() {
                return (h, RuntimeOwnership::Borrowed);
            }
            let rt = get_or_create_shared_runtime(0);
            let h = rt.handle().clone();
            (h, RuntimeOwnership::Owned(rt))
        }
        RuntimeMode::Shared(n) => {
            let rt = get_or_create_shared_runtime(*n);
            let h = rt.handle().clone();
            (h, RuntimeOwnership::Owned(rt))
        }
        RuntimeMode::ApplicationRuntime => {
            let h = tokio::runtime::Handle::try_current()
                .expect("RuntimeMode::ApplicationRuntime requires a current Tokio runtime");
            (h, RuntimeOwnership::Borrowed)
        }
    }
}

/// Returns the process-wide shared runtime, creating it if necessary.
///
/// `num_threads` is only honoured on first creation. If the runtime already
/// exists and the requested thread count differs, a warning is logged and
/// the existing runtime is returned unchanged.
fn get_or_create_shared_runtime(num_threads: usize) -> Arc<tokio::runtime::Runtime> {
    let mut guard = SHARED_RT.lock().unwrap();
    if let Some(weak) = guard.as_ref()
        && let Some(rt) = weak.upgrade()
    {
        let existing = rt.metrics().num_workers();
        let requested = num_threads.max(1);
        if requested != existing {
            warn!(
                "Ignoring num_threads={} — shared runtime already has {} worker(s)",
                num_threads, existing
            );
        }
        return rt;
    }
    let rt = Arc::new(build_runtime(num_threads));
    *guard = Some(Arc::downgrade(&rt));
    rt
}

fn build_runtime(num_threads: usize) -> tokio::runtime::Runtime {
    // Always use multi_thread: spawned tasks need background worker threads
    // to drive them. current_thread only runs tasks when block_on is called,
    // which does not apply to the library-owned shared runtime.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads.max(1))
        .enable_all()
        .build()
        .expect("build tokio runtime")
}

/// Channels connecting the public API to the internal event loop.
/// Created by [`RuntimeHandle::spawn`].
pub(crate) struct RuntimeHandle {
    cmd_tx: async_channel::Sender<Command>,
    event_rx: async_channel::Receiver<Event>,
}

impl RuntimeHandle {
    /// Spawns the MQTT event loop as a task on `handle` and returns the
    /// channel endpoints for communicating with it.
    pub(crate) fn spawn(handle: &tokio::runtime::Handle, buf_cfg: BufConfig) -> Self {
        let (cmd_tx, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, event_rx) = async_channel::unbounded::<Event>();
        handle.spawn(run_event_loop(cmd_rx, event_tx, buf_cfg));
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
/// returns the decoded CONNACK. Returns an error if any other packet arrives
/// first or if the connection closes.
async fn recv_connack(
    reader: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
    ver: MqttVersion,
) -> Result<ConnAck> {
    loop {
        reader.read_buf(buf).await?;
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
    /// QoS 1/2 publish commands deferred because the in-flight limit was
    /// reached.  Drained one-for-one as PUBACK/PUBCOMP ACKs arrive.
    inflight_deferred: VecDeque<Command>,
    /// Set by `send()` to indicate a packet was written since the last
    /// keep-alive timer reset. Checked in `run()` to reset the deadline.
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
            sent_packet: false,
        }
    }

    /// Returns `true` if any outbound operations are waiting for broker ACKs
    /// or are queued in the in-flight deferred buffer.
    fn has_pending(&self) -> bool {
        !self.pending_pub1.is_empty()
            || !self.pending_pub2.is_empty()
            || !self.pending_sub.is_empty()
            || !self.pending_unsub.is_empty()
            || !self.inflight_deferred.is_empty()
    }

    /// Number of outbound QoS 1/2 publishes currently awaiting broker ACKs.
    fn inflight_count(&self) -> usize {
        self.pending_pub1.len() + self.pending_pub2.len()
    }

    /// Drains deferred publish commands into the session while the in-flight
    /// count is below `max_inflight`.  A transport error propagates immediately.
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
        self.writer.flush().await?;
        self.sent_packet = true;
        Ok(())
    }

    /// Restores in-flight QoS 1/2 messages from the persistence store after a
    /// successful connect.
    ///
    /// Outbound messages (persistence keys `s{id}`) are re-decoded, marked
    /// DUP, and retransmitted.  An orphan [`Completer`] is registered for each
    /// so that the arriving PUBACK/PUBCOMP removes the persistence entry
    /// without resolving any user token (the original token is long gone).
    /// `next_id` is advanced past all restored packet IDs to prevent
    /// collisions with new allocations.
    ///
    /// Inbound QoS 2 messages awaiting PUBREL (keys `r{id}`) are reloaded
    /// into `pending_pub2_in`; when the broker re-sends PUBREL the message
    /// is delivered and PUBCOMP is sent normally.
    ///
    /// Returns early without error if there is no persistence store.  Any
    /// individual key that fails to decode is silently skipped; a transport
    /// error aborts the restore and bubbles up via `Err`.
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
                // Outbound QoS 1/2 PUBLISH — retransmit with DUP=true.
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
                // Inbound QoS 2 awaiting PUBREL — reload so PUBREL is handled.
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
                    if ka_enabled && self.sent_packet {
                        self.sent_packet = false;
                        ka_sleep.as_mut().reset(time::Instant::now() + keep_alive);
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
                            if ka_enabled && self.sent_packet {
                                self.sent_packet = false;
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
                self.drain_deferred().await?;
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
                self.drain_deferred().await?;
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
            // As in the C library, this also fires ConnectionLost and
            // triggers auto-reconnect once the session loop unwinds.
            Packet::Disconnect(disc) => {
                let _ = self
                    .event_tx
                    .send(Event::Disconnected {
                        reason_code: disc.reason_code,
                        props: disc.properties,
                    })
                    .await;
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
                // QoS 1/2 publishes are deferred when the in-flight limit is
                // reached.  QoS 0 is never gated (no ACK, no packet ID).
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

                // Persist QoS 1/2 PUBLISH before sending so we can
                // re-transmit after a crash.
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
    params: Box<ConnectParams>,
    /// Index into `params.servers` for the current connection attempt.
    server_idx: usize,
    connected_flag: Arc<AtomicBool>,
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
}

impl ConnectJob {
    /// Extracts a `ConnectJob` from a `Command::Connect`.
    ///
    /// Returns `Ok((job, tx))` on success or `Err(cmd)` if `cmd` is not a
    /// `Connect` variant, giving the caller back the original command.
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
            let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
            let config = make_tls_config(tls)?;
            let connector = TlsConnector::from(config);
            let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                .map_err(|_| {
                    Error::GeneralString(format!("invalid TLS server name: {}", host))
                })?
                .to_owned();
            let tcp = TcpStream::connect((host.as_str(), *port)).await?;
            tcp.set_nodelay(true)?;
            let tls = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| Error::Tls(e.to_string()))?;
            Box::new(TlsConnection::new(tls))
        }
        #[cfg(all(not(feature = "tls"), feature = "openssl"))]
        ServerAddr::Tls { host, port } => {
            let tls_opts = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
            let connector = make_openssl_config(tls_opts)?;
            let ssl = connector
                .configure()
                .and_then(|c| c.into_ssl(host.as_str()))
                .map_err(|e| Error::GeneralString(e.to_string()))?;
            let tcp = TcpStream::connect((host.as_str(), *port)).await?;
            tcp.set_nodelay(true)?;
            let mut stream = tokio_openssl::SslStream::new(ssl, tcp)
                .map_err(|e| Error::GeneralString(e.to_string()))?;
            Pin::new(&mut stream)
                .connect()
                .await
                .map_err(|e| Error::Tls(e.to_string()))?;
            Box::new(OpenSslConnection::new(stream))
        }
        #[cfg(unix)]
        ServerAddr::Unix { path } => Box::new(UnixConnection::connect(path).await?),
        #[cfg(feature = "websocket")]
        ServerAddr::Ws { host, port, path } => {
            let url = format!("ws://{}:{}{}", host, port, path);
            let request = build_ws_request(&url, &p.http_headers)?;
            let tcp = TcpStream::connect((host.as_str(), *port)).await?;
            tcp.set_nodelay(true)?;
            let (ws, _) = tokio_tungstenite::client_async(request, tcp)
                .await
                .map_err(|e| Error::GeneralString(e.to_string()))?;
            Box::new(WsConnection::new(ws))
        }
        #[cfg(all(feature = "websocket", feature = "tls"))]
        ServerAddr::Wss { host, port, path } => {
            let tls = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
            let config = make_tls_config(tls)?;
            let connector = TlsConnector::from(config);
            let server_name = rustls::pki_types::ServerName::try_from(host.as_str())
                .map_err(|_| {
                    Error::GeneralString(format!("invalid TLS server name: {}", host))
                })?
                .to_owned();
            let tcp = TcpStream::connect((host.as_str(), *port)).await?;
            tcp.set_nodelay(true)?;
            let tls = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| Error::Tls(e.to_string()))?;
            let url = format!("wss://{}:{}{}", host, port, path);
            let request = build_ws_request(&url, &p.http_headers)?;
            let (ws, _) = tokio_tungstenite::client_async(request, tls)
                .await
                .map_err(|e| Error::GeneralString(e.to_string()))?;
            Box::new(WsConnection::new(ws))
        }
        #[cfg(all(feature = "websocket", not(feature = "tls"), feature = "openssl"))]
        ServerAddr::Wss { host, port, path } => {
            let tls_opts = p.tls.as_ref().ok_or(Error::MissingTlsOptions)?;
            let connector = make_openssl_config(tls_opts)?;
            let ssl = connector
                .configure()
                .and_then(|c| c.into_ssl(host.as_str()))
                .map_err(|e| Error::GeneralString(e.to_string()))?;
            let tcp = TcpStream::connect((host.as_str(), *port)).await?;
            tcp.set_nodelay(true)?;
            let mut stream = tokio_openssl::SslStream::new(ssl, tcp)
                .map_err(|e| Error::GeneralString(e.to_string()))?;
            Pin::new(&mut stream)
                .connect()
                .await
                .map_err(|e| Error::Tls(e.to_string()))?;
            let url = format!("wss://{}:{}{}", host, port, path);
            let request = build_ws_request(&url, &p.http_headers)?;
            let (ws, _) = tokio_tungstenite::client_async(request, stream)
                .await
                .map_err(|e| Error::GeneralString(e.to_string()))?;
            Box::new(WsConnection::new(ws))
        }
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
    };

    let mut buf = BytesMut::with_capacity(256);
    buf.clear();
    Packet::Connect(connect_pkt).encode(&mut buf, p.ver)?;
    writer.write_all(&buf).await?;
    writer.flush().await?;

    // ---- Wait for CONNACK ----
    let mut read_buf = BytesMut::with_capacity(256);
    let connack = recv_connack(&mut reader, &mut read_buf, p.ver).await?;

    Ok((reader, writer, connack))
}

/// Pushes a bufferable command into the send-while-disconnected buffer.
///
/// If the buffer is already at capacity and `cfg.delete_oldest` is set, the
/// front entry is evicted (its `Completer` drops → `Error::Disconnected`).
/// If `delete_oldest` is not set and the buffer is full, the incoming command
/// is dropped instead (same effect on its completer).
fn buf_push(buf: &mut VecDeque<Command>, cmd: Command, cfg: &BufConfig) {
    if buf.len() < cfg.max_buffered_msgs {
        buf.push_back(cmd);
    }
    else if cfg.delete_oldest {
        buf.pop_front(); // oldest Completer drops → resolves with Disconnected
        buf.push_back(cmd);
    }
    // else: buffer full, new cmd is dropped → its Completer resolves with Disconnected
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
    Replace(Box<ConnectJob>, Completer<ConnectResponse>),
}

/// Waits for `delay` to elapse while draining the command queue.
///
/// * `Disconnect` → completes the token (already disconnected) and returns
///   [`WaitOutcome::Abort`].
/// * `Connect` → captures the new parameters and returns
///   [`WaitOutcome::Replace`] immediately (skips remaining delay).
/// * `Publish` with `send_while_disconnected` set → pushed into `buf`.
/// * Any other command → completes the completer with
///   [`Error::Disconnected`] and keeps waiting.
async fn reconnect_wait(
    delay: Duration,
    cmd_rx: &async_channel::Receiver<Command>,
    buf: &mut VecDeque<Command>,
    buf_cfg: &BufConfig,
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
                            tx,
                            connected_flag,
                            ..
                        } => {
                            // We are already disconnected; satisfy the caller.
                            connected_flag.store(false, Ordering::SeqCst);
                            tx.complete(Ok(()));
                            return WaitOutcome::Abort;
                        }
                        Command::Publish { qos: QoS::QoS0, tx, .. }
                            if buf_cfg.send_while_disconnected && !buf_cfg.persist_qos0 =>
                        {
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
///
/// `buf_cfg` controls the send-while-disconnected buffer.  When enabled,
/// `Publish` commands received while the client is disconnected are queued
/// and flushed into the session immediately after a successful (re)connect.
pub(crate) async fn run_event_loop(
    cmd_rx: async_channel::Receiver<Command>,
    event_tx: async_channel::Sender<Event>,
    buf_cfg: BufConfig,
) {
    // Buffer for Publish commands received while disconnected.
    let mut buf: VecDeque<Command> = VecDeque::new();
    // True once the first successful CONNACK has been received.
    let mut ever_connected = false;

    'outer: loop {
        // ---- Wait for a Connect command ----
        // While waiting, optionally buffer Publish commands.
        let (mut job, initial_tx) = loop {
            let cmd = match cmd_rx.recv().await {
                Ok(cmd) => cmd,
                Err(_) => return,
            };
            match ConnectJob::extract(cmd) {
                Ok(pair) => break pair,
                Err(cmd) => {
                    // Non-Connect command while not connected.
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
                let err = last_err.unwrap_or(Error::General("no servers configured"));
                if matches!(err, Error::Tls(_)) {
                    let _ = event_tx.send(Event::TlsError(err.to_string())).await;
                }
                initial_tx.complete(Err(err));
                continue 'outer;
            }
        };

        let response = build_connect_response(&job, connack);
        // Set the flag before resolving the token so `is_connected()` is true
        // as soon as `wait()` / `.await` returns in the caller.
        job.connected_flag.store(true, Ordering::SeqCst);
        ever_connected = true;
        let _ = event_tx.send(Event::Connected(response.clone())).await;
        initial_tx.complete(Ok(response));

        // ---- Run the session ----
        let mut session = Session::new(
            writer,
            job.params.ver,
            job.params.keep_alive_duration(),
            event_tx.clone(),
            job.persist.clone(),
            job.params.max_inflight,
        );

        // Restore persisted in-flight messages, then flush the disconnected
        // buffer into the newly connected session.
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
                match reconnect_wait(retry_delay, &cmd_rx, &mut buf, &buf_cfg).await {
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

                    // Restore persisted in-flight messages, then flush the
                    // disconnected buffer into the reconnected session.
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

    fn assert_send<F: Send + 'static>(_: F) {}

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
