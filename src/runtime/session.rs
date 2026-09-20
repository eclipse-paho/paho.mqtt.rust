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
//! Runtime-independent MQTT session and event loop.
//!
//! Everything that is MQTT protocol logic rather than I/O mechanics lives
//! here, generic over a [`Backend`]: the [`Session`] state machine, packet
//! and command handling, session restore, the top-level connect/reconnect
//! loop, and the send-while-disconnected buffer.  A backend supplies only the
//! transport (open, read, write, flush) and a resettable timer; see
//! `tokio_rt` and `smol_rt`.

use std::{
    collections::{HashMap, VecDeque},
    io,
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::{FutureExt, select};
use paho_mqtt_codec::{
    ConnAck, Disconnect, MqttVersion, Packet, Properties, PubAck, QoS, RawPacket, ReasonCode,
    RetainHandling as CodecRetainHandling, Subscribe, Subscription, Unsubscribe,
};

use super::{
    backend::{Backend, Sleep},
    commands::{BufConfig, Command, ConnectParams, Event, SessionLimits},
};
use crate::{
    Error, Message, Result,
    persistence::ClientPersistence,
    responses::{ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse},
    token::Completer,
};

/////////////////////////////////////////////////////////////////////////////
// Runtime handle

/// Channels connecting the public API to the internal event loop.
///
/// Created by the backend's `RuntimeHandle::spawn`, which builds the channels
/// with [`RuntimeHandle::new`] and spawns the returned event-loop future on
/// its executor.
pub(crate) struct RuntimeHandle {
    cmd_tx: async_channel::Sender<Command>,
    event_rx: async_channel::Receiver<Event>,
}

impl RuntimeHandle {
    /// Creates the command and event channels and the event-loop future that
    /// services them.  The caller must spawn the future on its executor.
    pub(crate) fn new<B: Backend>(
        buf_cfg: BufConfig,
    ) -> (Self, impl Future<Output = ()> + Send + 'static) {
        let (cmd_tx, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, event_rx) = async_channel::unbounded::<Event>();
        let task = run_event_loop::<B>(cmd_rx, event_tx, buf_cfg);
        (Self { cmd_tx, event_rx }, task)
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
async fn recv_connack<B: Backend>(
    reader: &mut B::Reader,
    buf: &mut BytesMut,
    ver: MqttVersion,
    max_packet_size: u32,
) -> Result<ConnAck> {
    loop {
        if B::read_buf(reader, buf).await? == 0 {
            return Err(Error::Disconnected);
        }
        if let Some(raw) = RawPacket::decode_max(buf, max_packet_size)? {
            match Packet::from_raw(raw, ver)? {
                Packet::ConnAck(ack) => return Ok(ack),
                _ => return Err(Error::General("expected CONNACK")),
            }
        }
    }
}

/// What woke the session loop.
enum LoopEvent {
    /// The transport read completed (byte count or error).
    Read(io::Result<usize>),
    /// A command arrived from the public API (or the channel closed).
    Command(std::result::Result<Command, async_channel::RecvError>),
    /// The keep-alive timer fired.
    KeepAlive,
}

// Session

/// A QoS 1/2 publish waiting for an in-flight slot.
enum Deferred {
    /// A `Command::Publish` from the API, not yet assigned a packet ID.
    Command(Command),
    /// A persisted PUBLISH awaiting re-transmission under its original ID.
    Restored(paho_mqtt_codec::Publish),
}

/// Resolves a publish token from its PUBACK/PUBCOMP: `Ok` for a success
/// code, `Err(ReasonCode)` for an error code.
fn publish_outcome(rc: ReasonCode, props: Properties) -> Result<PublishResponse> {
    if rc.is_err() {
        Err(Error::ReasonCode(rc, props))
    } else {
        Ok(PublishResponse::new(rc, props))
    }
}

/// Resolves a (un)subscribe token from its acknowledgement. For a `single`
/// operation an error code fails the token; a many-topic operation always
/// resolves `Ok` so partial success is visible per filter.
fn single_outcome<R>(
    single: bool,
    rsp: R,
    codes: impl Fn(&R) -> &Vec<ReasonCode>,
    props: impl FnOnce(R) -> Properties,
) -> Result<R> {
    if single
        && let Some(rc) = codes(&rsp).first().copied()
        && rc.is_err()
    {
        return Err(Error::ReasonCode(rc, props(rsp)));
    }
    Ok(rsp)
}

/// PUBLISH payloads up to this size are copied into the header buffer and
/// sent as one contiguous write; larger ones are written from their own
/// buffer to avoid the copy.
const INLINE_PAYLOAD_MAX: usize = 16 * 1024;

/// Holds all mutable state for a connected MQTT session.
struct Session<B: Backend> {
    /// Write half of the split transport.
    writer: B::Writer,
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
    /// Pending SUBSCRIBE operations awaiting SUBACK, with the `single` flag
    /// that decides how an error code in the SUBACK is reported.
    pending_sub: HashMap<u16, (Completer<SubscribeResponse>, bool)>,
    /// Pending UNSUBSCRIBE operations awaiting UNSUBACK, likewise.
    pending_unsub: HashMap<u16, (Completer<UnsubscribeResponse>, bool)>,
    /// Sender to deliver inbound events to the public layer.
    event_tx: async_channel::Sender<Event>,
    /// True when a PINGREQ has been sent and PINGRESP has not yet arrived.
    ping_outstanding: bool,
    /// Persistence store for in-flight QoS 1/2 messages (if any).
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
    /// Limits fixed once the CONNACK arrived.
    limits: SessionLimits,
    /// QoS 1/2 publishes deferred because the in-flight limit was reached —
    /// new commands and persisted messages awaiting re-transmission alike.
    /// Drained one-for-one as PUBACK/PUBCOMP ACKs arrive.
    inflight_deferred: VecDeque<Deferred>,
    /// Set by `send()` to indicate a packet was written since the last
    /// keep-alive timer reset. Checked in `run()` to reset the deadline.
    sent_packet: bool,
}

impl<B: Backend> Session<B> {
    fn new(
        writer: B::Writer,
        ver: MqttVersion,
        keep_alive: Duration,
        event_tx: async_channel::Sender<Event>,
        persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
        limits: SessionLimits,
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
            limits,
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

    /// `true` when another QoS 1/2 publish may go out now.
    fn inflight_slot_free(&self) -> bool {
        self.limits.max_inflight == 0 || self.inflight_count() < self.limits.max_inflight
    }

    /// Drains deferred publishes into the session while the in-flight count
    /// is below `max_inflight`.  A transport error propagates immediately.
    async fn drain_deferred(&mut self) -> Result<()> {
        while self.inflight_slot_free() {
            match self.inflight_deferred.pop_front() {
                Some(Deferred::Command(cmd)) => {
                    self.handle_command(cmd).await?;
                }
                Some(Deferred::Restored(pkt)) => self.send_restored(pkt).await?,
                None => break,
            }
        }
        Ok(())
    }

    /// Re-transmits a persisted QoS 1/2 PUBLISH under its original packet
    /// identifier with DUP set, registering an orphan completer so the ACK
    /// cleans up the persistence entry without resolving any user token.
    async fn send_restored(&mut self, mut pub_pkt: paho_mqtt_codec::Publish) -> Result<()> {
        let Some(pid) = pub_pkt.packet_id else {
            return Ok(());
        };
        let qos = pub_pkt.qos;
        pub_pkt.dup = true;
        self.send(&Packet::Publish(pub_pkt)).await?;
        let comp = Completer::orphan();
        match qos {
            QoS::QoS1 => {
                self.pending_pub1.insert(pid, comp);
            }
            QoS::QoS2 => {
                self.pending_pub2.insert(pid, comp);
            }
            QoS::QoS0 => {}
        }
        Ok(())
    }

    /// `true` if `id` is currently in use by any outstanding operation.
    fn id_in_use(&self, id: u16) -> bool {
        self.pending_pub1.contains_key(&id)
            || self.pending_pub2.contains_key(&id)
            || self.pending_sub.contains_key(&id)
            || self.pending_unsub.contains_key(&id)
    }

    /// Persists `data` under `key`, silently ignoring errors.
    fn persist_put(&self, key: &str, data: Vec<&[u8]>) {
        if let Some(p) = &self.persist {
            let _ = p.lock().unwrap().put(key, data);
        }
    }

    /// Removes the persistence entry for `key`, silently ignoring errors.
    fn persist_remove(&self, key: &str) {
        if let Some(p) = &self.persist {
            let _ = p.lock().unwrap().remove(key);
        }
    }

    /// Allocates the next free packet identifier (1 – 65535, wrapping).
    ///
    /// Identifiers still awaiting an acknowledgement are skipped: after a
    /// session restore `next_id` sits past the highest persisted identifier,
    /// so a wrap-around could otherwise land on one that is still in flight
    /// and silently replace its completer.
    fn alloc_id(&mut self) -> u16 {
        loop {
            self.next_id = self.next_id.wrapping_add(1);
            if self.next_id == 0 {
                self.next_id = 1;
            }
            if !self.id_in_use(self.next_id) {
                return self.next_id;
            }
        }
    }

    /// Encodes and sends a packet on the wire.
    async fn send(&mut self, packet: &Packet) -> Result<()> {
        self.write_buf.clear();
        let payload = packet.encode_split(&mut self.write_buf, self.ver)?;
        self.check_outbound_size(payload.as_ref().map_or(0, Bytes::len))?;
        self.write_frame(payload).await
    }

    /// Refuses a frame larger than the server's advertised maximum packet
    /// size. The header is already in `write_buf`; `payload_len` is the
    /// separately held payload, if any.
    fn check_outbound_size(&self, payload_len: usize) -> Result<()> {
        if let Some(max) = self.limits.server_max_packet_size
            && self.write_buf.len() + payload_len > max as usize
        {
            return Err(paho_mqtt_codec::Error::PacketTooLarge.into());
        }
        Ok(())
    }

    /// Writes the packet header in `write_buf`, followed by `payload` if
    /// present, as one flushed frame.
    ///
    /// Small payloads are appended to the header so the frame goes out in a
    /// single write; larger ones are written from their own buffer (via a
    /// vectored write where the transport supports it) to avoid copying.
    async fn write_frame(&mut self, payload: Option<Bytes>) -> Result<()> {
        match payload {
            Some(p) if p.len() > INLINE_PAYLOAD_MAX => {
                B::write_frame(&mut self.writer, &self.write_buf, p).await?;
            }
            Some(p) => {
                self.write_buf.extend_from_slice(&p);
                B::write_all(&mut self.writer, &self.write_buf).await?;
            }
            None => B::write_all(&mut self.writer, &self.write_buf).await?,
        }
        B::flush(&mut self.writer).await?;
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
            let Some(persist) = &self.persist else {
                return Ok(());
            };
            match persist.lock().unwrap().keys() {
                Ok(k) => k,
                Err(_) => return Ok(()),
            }
        };

        for key in &keys {
            let data = {
                let Some(persist) = &self.persist else {
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
                // Outbound QoS 1/2 PUBLISH — retransmit with DUP=true, subject
                // to the same in-flight limit as new publishes.
                if let Packet::Publish(pub_pkt) = pkt {
                    let Some(pid) = pub_pkt.packet_id else {
                        continue;
                    };
                    if pid > self.next_id {
                        self.next_id = pid;
                    }
                    if self.inflight_slot_free() {
                        self.send_restored(pub_pkt).await?;
                    } else {
                        self.inflight_deferred
                            .push_back(Deferred::Restored(pub_pkt));
                    }
                }
            } else if key.starts_with('r') {
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

    /// Brings a freshly connected session up and runs it to completion:
    /// restores persisted in-flight messages (if `restore`), flushes the
    /// commands buffered while disconnected, then services the connection
    /// until it ends.
    ///
    /// Any failure — restore, flush, or run — surfaces as one `Err`, so the
    /// caller tears the session down the same way in every case. Commands
    /// still in `buf` when a flush fails stay there for the next connection.
    async fn serve(
        &mut self,
        reader: &mut B::Reader,
        cmd_rx: &async_channel::Receiver<Command>,
        buf: &mut VecDeque<Command>,
        restore: bool,
    ) -> Result<()> {
        if restore {
            self.restore().await?;
        }
        while let Some(cmd) = buf.pop_front() {
            self.handle_command(cmd).await?;
        }
        self.run(reader, cmd_rx).await
    }

    /// Main event loop: read from `reader`, dispatch commands from `cmd_rx`,
    /// and service keep-alive until an error or clean disconnect.
    async fn run(
        &mut self,
        reader: &mut B::Reader,
        cmd_rx: &async_channel::Receiver<Command>,
    ) -> Result<()> {
        let keep_alive = self.keep_alive;
        // keep_alive == ZERO means disabled: a never-firing sleep reports
        // itself terminated, so `select!` skips that branch entirely.
        let ka_enabled = !keep_alive.is_zero();

        // Resettable sleep: the deadline advances on every outbound packet so
        // that PINGREQs are only sent when the connection has been truly idle.
        let mut ka_sleep = if ka_enabled {
            B::Sleep::new(keep_alive)
        } else {
            B::Sleep::never()
        };

        let mut read_buf = BytesMut::with_capacity(4096);

        loop {
            let ev = select! {
                res = B::read_buf(reader, &mut read_buf).fuse() => LoopEvent::Read(res),
                cmd = cmd_rx.recv().fuse() => LoopEvent::Command(cmd),
                _ = ka_sleep => LoopEvent::KeepAlive,
            };

            match ev {
                LoopEvent::Read(res) => {
                    let n = res?;
                    if n == 0 {
                        return Err(Error::Disconnected);
                    }
                    while let Some(raw) =
                        RawPacket::decode_max(&mut read_buf, self.limits.max_packet_size)?
                    {
                        let packet = Packet::from_raw(raw, self.ver)?;
                        self.handle_packet(packet).await?;
                    }
                    if ka_enabled && self.sent_packet {
                        self.sent_packet = false;
                        ka_sleep.reset(keep_alive);
                    }
                }
                LoopEvent::Command(Err(_)) => return Ok(()),
                LoopEvent::Command(Ok(Command::Disconnect {
                    timeout,
                    reason_code,
                    props,
                    tx,
                    connected_flag,
                })) => {
                    // If requested, wait for in-flight ops to complete
                    // before sending DISCONNECT.
                    if !timeout.is_zero() && self.has_pending() {
                        self.drain_pending(reader, &mut read_buf, timeout).await;
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
                LoopEvent::Command(Ok(cmd)) => {
                    if self.handle_command(cmd).await? {
                        return Ok(());
                    }
                    if ka_enabled && self.sent_packet {
                        self.sent_packet = false;
                        ka_sleep.reset(keep_alive);
                    }
                }
                LoopEvent::KeepAlive => {
                    if self.ping_outstanding {
                        return Err(Error::Timeout);
                    }
                    self.send(&Packet::PingReq).await?;
                    self.ping_outstanding = true;
                    ka_sleep.reset(keep_alive);
                }
            }
        }
    }

    /// Keeps servicing inbound packets until every pending operation has been
    /// acknowledged or `timeout` elapses, whichever comes first.
    ///
    /// Used by an orderly disconnect so in-flight QoS 1/2 publishes and
    /// (un)subscribes get their ACKs before DISCONNECT goes out.  A closed
    /// connection, read error, or undecodable packet ends the drain early;
    /// the caller sends DISCONNECT regardless.
    async fn drain_pending(
        &mut self,
        reader: &mut B::Reader,
        read_buf: &mut BytesMut,
        timeout: Duration,
    ) {
        let mut deadline = B::Sleep::new(timeout);

        while self.has_pending() {
            let res = select! {
                res = B::read_buf(reader, read_buf).fuse() => res,
                _ = deadline => return,
            };
            match res {
                Ok(0) | Err(_) => return,
                Ok(_) => {
                    while let Ok(Some(raw)) =
                        RawPacket::decode_max(read_buf, self.limits.max_packet_size)
                    {
                        match Packet::from_raw(raw, self.ver) {
                            Ok(pkt) => {
                                let _ = self.handle_packet(pkt).await;
                            }
                            Err(_) => return,
                        }
                    }
                }
            }
        }
    }

    /// Handles one inbound packet from the broker.
    async fn handle_packet(&mut self, packet: Packet) -> Result<()> {
        match packet {
            // ---- Publish acknowledgements ----
            // A publish is one operation with one outcome: an error reason
            // code fails its token, mirroring how a refused CONNACK fails
            // `connect()`. The persisted copy is removed either way — the
            // broker has answered, so there is nothing left to retry.
            Packet::PubAck(ack) => {
                if let Some(tx) = self.pending_pub1.remove(&ack.packet_id) {
                    self.persist_remove(&format!("s{}", ack.packet_id));
                    tx.complete(publish_outcome(ack.reason_code, ack.properties));
                }
                self.drain_deferred().await?;
            }

            // ---- QoS 2 outbound: PUBREC → send PUBREL ----
            Packet::PubRec(rec) => {
                if rec.reason_code.is_err() {
                    if let Some(tx) = self.pending_pub2.remove(&rec.packet_id) {
                        self.persist_remove(&format!("s{}", rec.packet_id));
                        tx.complete(Err(Error::ReasonCode(rec.reason_code, rec.properties)));
                    }
                    self.drain_deferred().await?;
                    return Ok(());
                }
                let rel = PubAck::new(rec.packet_id);
                self.send(&Packet::PubRel(rel)).await?;
            }

            // ---- QoS 2 outbound: PUBCOMP → resolve token ----
            Packet::PubComp(comp) => {
                if let Some(tx) = self.pending_pub2.remove(&comp.packet_id) {
                    self.persist_remove(&format!("s{}", comp.packet_id));
                    tx.complete(publish_outcome(comp.reason_code, comp.properties));
                }
                self.drain_deferred().await?;
            }

            // ---- Subscription acknowledgement ----
            // `subscribe()` has one filter, so its one code decides the
            // outcome. `subscribe_many()` may succeed partially, so it always
            // resolves `Ok` and the caller inspects the per-filter codes.
            Packet::SubAck(ack) => {
                if let Some((tx, single)) = self.pending_sub.remove(&ack.packet_id) {
                    let rsp = SubscribeResponse::new(ack.reason_codes, ack.properties);
                    tx.complete(single_outcome(
                        single,
                        rsp,
                        |r| &r.reason_codes,
                        |r| r.properties,
                    ));
                }
            }

            // ---- Unsubscription acknowledgement ----
            Packet::UnsubAck(ack) => {
                if let Some((tx, single)) = self.pending_unsub.remove(&ack.packet_id) {
                    let rsp = UnsubscribeResponse::new(ack.reason_codes, ack.properties);
                    tx.complete(single_outcome(
                        single,
                        rsp,
                        |r| &r.reason_codes,
                        |r| r.properties,
                    ));
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
                        self.persist_put(&format!("r{id}"), vec![&enc_buf]);
                    }
                    let Packet::Publish(pub_pkt) = packet else {
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
                if qos != QoS::QoS0 && !self.inflight_slot_free() {
                    self.inflight_deferred
                        .push_back(Deferred::Command(Command::Publish {
                            topic,
                            payload,
                            qos,
                            retain,
                            props,
                            tx,
                        }));
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

                // Encode once: the header lands in `write_buf` and the payload
                // is shared by reference, so persisting and sending both use
                // the same bytes without a second encode or a copy.
                //
                // A publish that cannot be encoded, or that exceeds the
                // server's maximum packet size, is this publish's failure,
                // not the connection's: fail its token and carry on.
                self.write_buf.clear();
                let payload = match packet.encode_split(&mut self.write_buf, self.ver) {
                    Ok(p) => p,
                    Err(e) => {
                        tx.complete(Err(e.into()));
                        return Ok(false);
                    }
                };
                if let Err(e) = self.check_outbound_size(payload.as_ref().map_or(0, Bytes::len)) {
                    tx.complete(Err(e));
                    return Ok(false);
                }

                // Persist QoS 1/2 PUBLISH before sending so we can
                // re-transmit after a crash.
                if let Some(pid) = id {
                    let mut parts = vec![&self.write_buf[..]];
                    if let Some(p) = &payload {
                        parts.push(&p[..]);
                    }
                    self.persist_put(&format!("s{pid}"), parts);
                }

                self.write_frame(payload).await?;

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

            Command::Subscribe {
                single,
                filters,
                props,
                tx,
            } => {
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
                                RH::SendRetainedOnSubscribe => CodecRetainHandling::SendOnSubscribe,
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
                self.pending_sub.insert(id, (tx, single));
            }

            Command::Unsubscribe {
                single,
                filters,
                props,
                tx,
            } => {
                let id = self.alloc_id();
                let unsub = Unsubscribe {
                    packet_id: id,
                    topic_filters: filters,
                    properties: props,
                };
                self.send(&Packet::Unsubscribe(unsub)).await?;
                self.pending_unsub.insert(id, (tx, single));
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
/// Built from a [`Command::Connect`] by [`handle_offline_command`] and
/// retained across reconnect attempts.
struct ConnectJob {
    params: Box<ConnectParams>,
    /// Index into `params.servers` for the current connection attempt.
    server_idx: usize,
    connected_flag: Arc<AtomicBool>,
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
}

/// Opens persistence and the transport, sends CONNECT, waits for CONNACK.
async fn do_connect<B: Backend>(job: &ConnectJob) -> Result<(B::Reader, B::Writer, ConnAck)> {
    let p = &job.params;
    let (addr, server_uri) = &p.servers[job.server_idx];

    // ---- Open persistence ----
    if let Some(store) = &job.persist {
        store.lock().unwrap().open(&p.client_id, server_uri)?;
    }

    // ---- Transport connect ----
    let (mut reader, mut writer) = B::open(addr, p).await?;

    // ---- Build and send CONNECT packet ----
    let connect_pkt = paho_mqtt_codec::Connect {
        version: p.ver,
        clean_start: p.clean_start,
        keep_alive: p.keep_alive_secs,
        client_id: p.client_id.clone(),
        username: p.user_name.clone(),
        password: p.password.clone(),
        will: p.will.clone(),
        properties: p.props.clone(),
    };

    let mut buf = BytesMut::with_capacity(256);
    Packet::Connect(Box::new(connect_pkt)).encode(&mut buf, p.ver)?;
    B::write_all(&mut writer, &buf).await?;
    B::flush(&mut writer).await?;

    // ---- Wait for CONNACK ----
    let mut read_buf = BytesMut::with_capacity(256);
    let connack = recv_connack::<B>(&mut reader, &mut read_buf, p.ver, p.max_packet_size()).await?;

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
    } else if cfg.delete_oldest {
        buf.pop_front(); // oldest Completer drops → resolves with Disconnected
        buf.push_back(cmd);
    }
    // else: buffer full, new cmd is dropped → its Completer resolves with Disconnected
}

/// What became of a command that arrived while there was no session.
enum Offline {
    /// Buffered, or its token was resolved; nothing else to do.
    Handled,
    /// `disconnect()` was called. Its token is resolved and the connected
    /// flag cleared; whatever the caller was doing (waiting, connecting,
    /// backing off) should stop.
    Disconnect,
    /// `connect()` was called: new parameters and the token to resolve.
    Connect(Box<ConnectJob>, Completer<ConnectResponse>),
}

/// Disposes of a command received while disconnected.
///
/// `Publish` is buffered when `should_buf` allows it (QoS 0 is dropped as
/// `Ok` when `persist_qos0` is off); otherwise it, `Subscribe`, and
/// `Unsubscribe` fail with [`Error::Disconnected`]. `Disconnect` is satisfied
/// on the spot. `Connect` is handed back to the caller.
fn handle_offline_command(
    cmd: Command,
    buf: &mut VecDeque<Command>,
    cfg: &BufConfig,
    should_buf: bool,
) -> Offline {
    match cmd {
        Command::Connect {
            params,
            tx,
            connected_flag,
            persist,
        } => Offline::Connect(
            Box::new(ConnectJob {
                params,
                server_idx: 0,
                connected_flag,
                persist,
            }),
            tx,
        ),
        Command::Disconnect {
            tx, connected_flag, ..
        } => {
            connected_flag.store(false, Ordering::SeqCst);
            tx.complete(Ok(()));
            Offline::Disconnect
        }
        Command::Publish {
            qos: QoS::QoS0, tx, ..
        } if should_buf && !cfg.persist_qos0 => {
            tx.complete(Ok(PublishResponse::default()));
            Offline::Handled
        }
        cmd @ Command::Publish { .. } if should_buf => {
            buf_push(buf, cmd, cfg);
            Offline::Handled
        }
        Command::Publish { tx, .. } => {
            tx.complete(Err(Error::Disconnected));
            Offline::Handled
        }
        Command::Subscribe { tx, .. } => {
            tx.complete(Err(Error::Disconnected));
            Offline::Handled
        }
        Command::Unsubscribe { tx, .. } => {
            tx.complete(Err(Error::Disconnected));
            Offline::Handled
        }
    }
}

/// Outcome of one connection attempt against one server.
enum Attempt<B: Backend> {
    /// Transport up and CONNACK received (its reason code not yet examined).
    Connected(B::Reader, B::Writer, ConnAck),
    /// The attempt failed or timed out.
    Failed(Error),
    /// `disconnect()` was called mid-attempt (or the API was dropped); the
    /// half-open connection was discarded.
    Aborted,
}

/// Runs [`do_connect`] for `job.server_idx` under the job's connect timeout
/// while continuing to service the command channel.
///
/// A `Disconnect` aborts the attempt. A `Connect` during an attempt fails its
/// own token — one connect at a time. Everything else is handled as it is
/// while disconnected.
async fn attempt_connect<B: Backend>(
    job: &ConnectJob,
    cmd_rx: &async_channel::Receiver<Command>,
    buf: &mut VecDeque<Command>,
    buf_cfg: &BufConfig,
    should_buf: bool,
) -> Attempt<B> {
    let timeout = job.params.connect_timeout;
    let mut deadline = if timeout.is_zero() {
        B::Sleep::never()
    } else {
        B::Sleep::new(timeout)
    };
    let mut conn = pin!(do_connect::<B>(job).fuse());

    loop {
        let cmd = select! {
            res = conn => {
                return match res {
                    Ok((r, w, ack)) => Attempt::Connected(r, w, ack),
                    Err(e) => Attempt::Failed(e),
                };
            }
            _ = deadline => return Attempt::Failed(Error::Timeout),
            cmd = cmd_rx.recv().fuse() => cmd,
        };
        match cmd {
            // API dropped: nothing will ever use the connection.
            Err(_) => return Attempt::Aborted,
            Ok(Command::Connect { tx, .. }) => {
                tx.complete(Err(Error::General("connect already in progress")));
            }
            Ok(cmd) => match handle_offline_command(cmd, buf, buf_cfg, should_buf) {
                Offline::Handled => {}
                Offline::Disconnect => return Attempt::Aborted,
                Offline::Connect(..) => unreachable!("Connect is matched above"),
            },
        }
    }
}

/// Result of waiting for the reconnect timer to fire.
enum WaitOutcome {
    /// Timer expired — proceed with the current [`ConnectJob`].
    Retry,
    /// A `Disconnect` command arrived (or the API was dropped); abandon the
    /// reconnect loop.
    Abort,
    /// A new `Connect` command arrived with updated parameters; use these
    /// for the next attempt and resolve the supplied token with the result.
    Replace(Box<ConnectJob>, Completer<ConnectResponse>),
}

/// Waits for `delay` to elapse while draining the command queue; see
/// [`handle_offline_command`] for how each command is treated. A `Connect`
/// ends the wait early.
async fn reconnect_wait<B: Backend>(
    delay: Duration,
    cmd_rx: &async_channel::Receiver<Command>,
    buf: &mut VecDeque<Command>,
    buf_cfg: &BufConfig,
) -> WaitOutcome {
    let mut sleep = B::Sleep::new(delay);

    loop {
        let result = select! {
            _ = sleep => return WaitOutcome::Retry,
            res = cmd_rx.recv().fuse() => res,
        };
        match result {
            Err(_) => return WaitOutcome::Abort,
            Ok(cmd) => {
                match handle_offline_command(cmd, buf, buf_cfg, buf_cfg.send_while_disconnected) {
                    Offline::Handled => {}
                    Offline::Disconnect => return WaitOutcome::Abort,
                    Offline::Connect(job, tx) => return WaitOutcome::Replace(job, tx),
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
        mqtt_version: job.params.ver as u32,
        session_present: connack.session_present,
        reason_code: connack.reason_code,
        properties: connack.properties,
    }
}

/////////////////////////////////////////////////////////////////////////////
// Top-level event loop

/// Drives the MQTT event loop on behalf of a [`RuntimeHandle`].
///
/// Runs indefinitely as a task on the backend's executor.  Waits for a
/// [`Command::Connect`], tries each configured server in turn under the
/// connect timeout, then delegates to [`Session::serve`] for the lifetime of
/// the connection.
///
/// If `auto_reconnect` is set in the connect options and an established
/// session ends unexpectedly, the loop retries with exponential backoff
/// (capped at `max_retry_interval`), rotating through the server list,
/// without re-waiting for a new `Command::Connect`. A failed *initial*
/// connect is reported to the caller and not retried. An explicit
/// `Command::Disconnect` at any point — waiting, connecting, backing off —
/// stops the loop and returns it to waiting for a `Connect`.
///
/// `buf_cfg` controls the send-while-disconnected buffer.  When enabled,
/// `Publish` commands received while the client is disconnected are queued
/// and flushed into the session immediately after a successful (re)connect.
pub(crate) async fn run_event_loop<B: Backend>(
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
        let (mut job, mut tx_opt) = loop {
            let cmd = match cmd_rx.recv().await {
                Ok(cmd) => cmd,
                Err(_) => return,
            };
            let should_buf =
                buf_cfg.allow_send_any_time || (ever_connected && buf_cfg.send_while_disconnected);
            match handle_offline_command(cmd, &mut buf, &buf_cfg, should_buf) {
                Offline::Handled | Offline::Disconnect => {}
                Offline::Connect(job, tx) => break (job, Some(tx)),
            }
        };

        // `false` for the initial connect (each server tried once, then the
        // caller is told); `true` once a session has been lost and
        // `auto_reconnect` is retrying with backoff.
        let mut reconnecting = false;
        let mut retry_delay = job.params.min_retry_interval;

        'retry: loop {
            let should_buf =
                buf_cfg.allow_send_any_time || (ever_connected && buf_cfg.send_while_disconnected);

            // ---- One connection attempt (or one pass over the server list) ----
            let attempt: Attempt<B> = if !reconnecting {
                let n = job.params.servers.len();
                let mut last_err = None;
                let mut result = None;
                for idx in 0..n {
                    job.server_idx = idx;
                    match attempt_connect::<B>(&job, &cmd_rx, &mut buf, &buf_cfg, should_buf).await
                    {
                        Attempt::Connected(_, _, ack) if ack.reason_code.is_err() => {
                            last_err = Some(Error::ReasonCode(ack.reason_code, ack.properties));
                        }
                        Attempt::Connected(r, w, ack) => {
                            result = Some((r, w, ack));
                            break;
                        }
                        Attempt::Failed(e) => last_err = Some(e),
                        Attempt::Aborted => continue 'outer,
                    }
                }
                match result {
                    Some((r, w, ack)) => Attempt::Connected(r, w, ack),
                    None => {
                        Attempt::Failed(last_err.unwrap_or(Error::General("no servers configured")))
                    }
                }
            } else {
                match reconnect_wait::<B>(retry_delay, &cmd_rx, &mut buf, &buf_cfg).await {
                    WaitOutcome::Abort => break 'retry,
                    WaitOutcome::Retry => {
                        // Advance to the next server in round-robin order.
                        let n = job.params.servers.len();
                        job.server_idx = (job.server_idx + 1) % n;
                    }
                    WaitOutcome::Replace(new_job, new_tx) => {
                        // Explicit reconnect/connect: use the new params and
                        // reset the backoff.
                        job = new_job;
                        retry_delay = job.params.min_retry_interval;
                        tx_opt = Some(new_tx);
                    }
                }
                match attempt_connect::<B>(&job, &cmd_rx, &mut buf, &buf_cfg, should_buf).await {
                    Attempt::Connected(_, _, ack) if ack.reason_code.is_err() => {
                        Attempt::Failed(Error::ReasonCode(ack.reason_code, ack.properties))
                    }
                    other => other,
                }
            };

            let (mut reader, writer, connack) = match attempt {
                Attempt::Aborted => break 'retry,
                Attempt::Failed(err) => {
                    if matches!(err, Error::Tls(_)) {
                        let _ = event_tx.send(Event::TlsError(err.to_string())).await;
                    }
                    if let Some(tx) = tx_opt.take() {
                        tx.complete(Err(err));
                    }
                    if !reconnecting {
                        continue 'outer;
                    }
                    retry_delay = (retry_delay * 2).min(job.params.max_retry_interval);
                    continue 'retry;
                }
                Attempt::Connected(r, w, ack) => (r, w, ack),
            };

            // ---- Connected ----
            let limits = SessionLimits::new(&job.params, &connack);
            let response = build_connect_response(&job, connack);
            // Set the flag before resolving the token so `is_connected()` is
            // true as soon as `wait()` / `.await` returns in the caller.
            job.connected_flag.store(true, Ordering::SeqCst);
            ever_connected = true;
            let _ = event_tx.send(Event::Connected(response.clone())).await;
            if let Some(tx) = tx_opt.take() {
                tx.complete(Ok(response));
            }

            let mut session = Session::<B>::new(
                writer,
                job.params.ver,
                job.params.keep_alive_duration(),
                event_tx.clone(),
                job.persist.clone(),
                limits,
            );
            let result = session
                .serve(&mut reader, &cmd_rx, &mut buf, buf_cfg.restore_msgs)
                .await;
            if let Some(p) = &session.persist {
                let _ = p.lock().unwrap().close();
            }

            match result {
                // Clean disconnect: back to waiting for a Connect.
                Ok(()) => continue 'outer,
                Err(e) => {
                    job.connected_flag.store(false, Ordering::SeqCst);
                    let _ = event_tx.send(Event::ConnectionLost(e)).await;
                    if !job.params.auto_reconnect {
                        continue 'outer;
                    }
                    // Start (or restart) backing off from the minimum.
                    reconnecting = true;
                    retry_delay = job.params.min_retry_interval;
                }
            }
        } // 'retry
    } // 'outer
}
