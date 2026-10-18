// paho-mqtt/src/runtime/tokio_rt.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2026 Frank Pagliughi <fpagliughi@mindspring.com>
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
//!
//! # Writing
//!
//! Packet handlers never touch the transport. They encode into the session's
//! [`Outbox`] and return, which keeps them synchronous; writing is one branch
//! of the run loop's `select!`, beside reading, commands and keep-alive. A
//! packet queued on an idle connection goes out on the loop's next poll, at
//! the cost of a lone write. Under load, whatever queues up while a write is
//! in progress goes out together in the next vectored write, and a write
//! blocked on a full socket no longer stops reads.

use std::{
    collections::{HashMap, VecDeque},
    io::{self, IoSlice},
    pin::pin,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::BytesMut;
use futures::{FutureExt, future::pending, select, select_biased, task::AtomicWaker};
use log::{debug, warn};
use paho_mqtt_codec::{
    Auth, ConnAck, Disconnect, MqttVersion, Packet, Properties, Property, PropertyId, PubAck, QoS,
    RawPacket, ReasonCode, RetainHandling as CodecRetainHandling, Subscribe, Subscription,
    Unsubscribe,
};

use super::{
    backend::{Backend, Sleep},
    commands::{
        BufConfig, Command, ConnectParams, Event, MAX_PACKET_IDS, SessionLimits,
        assigned_client_id, negotiated_keep_alive,
    },
    outbox::Outbox,
    sinks::{Parked, Sinks, chain, disconnected_event},
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
    dispatch: Dispatch,
}

/// What the event-dispatcher thread and the event loop share.
///
/// Messages reach the application two ways. Streams are fed by the loop
/// itself (see [`Sinks`]); callbacks and the synchronous consumers are fed
/// through the event channel by the dispatcher thread, because a callback
/// may block and a `crossbeam` receiver cannot be awaited.
#[derive(Clone)]
pub(crate) struct Dispatch {
    /// Bounds the messages waiting for the dispatcher thread.
    pub(crate) gate: Arc<InboundGate>,
    /// Whether anything on the dispatcher side wants messages at all — an
    /// application message callback, or a synchronous consumer.
    ///
    /// While this is false the session delivers to the streams alone and
    /// never touches the event channel for a message, which is the thread
    /// hop gone. Connection events always go to the dispatcher, whatever
    /// this says, because the callbacks for them may be registered at any
    /// time.
    pub(crate) wants_messages: Arc<AtomicBool>,
}

impl Dispatch {
    /// A link with nothing registered on the dispatcher side yet.
    pub(crate) fn new() -> Self {
        Self {
            gate: InboundGate::new(INBOUND_MSG_CAP),
            wants_messages: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether a message should be handed to the dispatcher thread.
    fn wanted(&self) -> bool {
        self.wants_messages.load(Ordering::Relaxed)
    }
}

impl RuntimeHandle {
    /// Creates the command and event channels and the event-loop future that
    /// services them.  The caller must spawn the future on its executor.
    pub(crate) fn new<B: Backend>(
        buf_cfg: BufConfig,
    ) -> (Self, impl Future<Output = ()> + Send + 'static) {
        let (cmd_tx, cmd_rx) = async_channel::unbounded::<Command>();
        let (event_tx, event_rx) = async_channel::unbounded::<Event>();
        let dispatch = Dispatch::new();
        let task = run_event_loop::<B>(cmd_rx, event_tx, dispatch.clone(), buf_cfg);
        (
            Self {
                cmd_tx,
                event_rx,
                dispatch,
            },
            task,
        )
    }

    /// Consumes the handle, returning the command sender, the event
    /// receiver, and the link the event dispatcher shares with the loop.
    pub(crate) fn into_parts(
        self,
    ) -> (
        async_channel::Sender<Command>,
        async_channel::Receiver<Event>,
        Dispatch,
    ) {
        (self.cmd_tx, self.event_rx, self.dispatch)
    }
}

/// How many inbound messages may wait for the event dispatcher before the
/// session holds further ones back.
const INBOUND_MSG_CAP: usize = 1024;

/// Bounds the inbound messages waiting for the event dispatcher.
///
/// The event channel itself is unbounded, so that connection events
/// (`Connected`, `ConnectionLost`, ...) never wait: a callback blocked on a
/// token could otherwise deadlock the session's teardown. Messages are
/// counted here instead. The session hands one over only while the count is
/// below capacity; beyond that it holds them, and stops reading the socket
/// (see `Session::reads_allowed`). The dispatcher calls
/// [`consumed`](Self::consumed) once a message's callback has returned,
/// which wakes the session to hand over the next.
pub(crate) struct InboundGate {
    queued: AtomicUsize,
    capacity: usize,
    waker: AtomicWaker,
}

impl InboundGate {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            queued: AtomicUsize::new(0),
            capacity,
            waker: AtomicWaker::new(),
        })
    }

    /// `true` while another message may be handed to the dispatcher.
    fn has_room(&self) -> bool {
        self.queued.load(Ordering::Acquire) < self.capacity
    }

    /// Records a message handed to the dispatcher.
    fn sent(&self) {
        self.queued.fetch_add(1, Ordering::AcqRel);
    }

    /// Records that the dispatcher has finished with one message, and wakes
    /// the session if it is waiting for room.
    pub(crate) fn consumed(&self) {
        self.queued.fetch_sub(1, Ordering::AcqRel);
        self.waker.wake();
    }

    /// Ready once there is room. Registers the waker before checking again,
    /// so a `consumed` between the two is not missed.
    fn poll_room(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.has_room() {
            return Poll::Ready(());
        }
        self.waker.register(cx.waker());
        if self.has_room() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Internal event loop helpers

/// Reads bytes until at least one complete MQTT packet is available, then
/// returns the decoded CONNACK. Returns an error if any other packet arrives
/// first or if the connection closes.
async fn recv_connack<B: Backend>(
    reader: &mut B::Reader,
    writer: &mut B::Writer,
    buf: &mut BytesMut,
    p: &ConnectParams,
) -> Result<ConnAck> {
    loop {
        if B::read_buf(reader, buf).await? == 0 {
            return Err(Error::Disconnected);
        }
        while let Some(raw) = RawPacket::decode_max(buf, p.max_packet_size())? {
            match Packet::from_raw(raw, p.ver)? {
                Packet::ConnAck(ack) => return Ok(ack),
                // Enhanced authentication: the broker challenges, the
                // handler answers, and this repeats until a CONNACK
                // arrives or either side gives up (§4.12).
                Packet::Auth(auth) if auth.reason_code == ReasonCode::ContinueAuthentication => {
                    let reply = auth_response(p, &auth.properties)?;
                    let mut out = BytesMut::with_capacity(64);
                    Packet::Auth(reply).encode(&mut out, p.ver)?;
                    B::write_all(writer, &out).await?;
                    B::flush(writer).await?;
                }
                Packet::Auth(_) => return Err(Error::General("unexpected AUTH reason code")),
                _ => return Err(Error::General("expected CONNACK")),
            }
        }
    }
}

/// Builds the client's answer to one authentication challenge.
///
/// The method is taken from our own CONNECT properties rather than the
/// broker's echo of it, and is always included: §3.15.2.2.2 requires every
/// AUTH to name the method, and it must match the one from CONNECT.
fn auth_response(p: &ConnectParams, challenge_props: &Properties) -> Result<Auth> {
    let (Some(handler), Some(method)) = (&p.auth, crate::auth::auth_method(&p.props)) else {
        // `ConnectOptions::check` rejects this combination before we get
        // here; a broker that sends AUTH unasked reaches it.
        return Err(Error::General(
            "broker requested authentication but none was configured",
        ));
    };

    let data = handler.respond(&crate::AuthChallenge {
        method,
        data: crate::auth::auth_data(challenge_props),
        props: challenge_props,
    })?;

    let mut props = Properties::new();
    props.push(Property::AuthenticationMethod(method.into()));
    if !data.is_empty() {
        props.push(Property::AuthenticationData(data.into()));
    }

    Ok(Auth::new(ReasonCode::ContinueAuthentication, props))
}

/// What woke the session loop.
enum LoopEvent {
    /// The writer made progress (or failed).
    Writer(io::Result<Step>),
    /// The transport read completed (byte count or error).
    Read(io::Result<usize>),
    /// A command arrived from the public API (or the channel closed).
    Command(std::result::Result<Command, async_channel::RecvError>),
    /// The keep-alive timer fired.
    KeepAlive,
    /// The event dispatcher has room for a held message.
    Room,
    /// A fan-out that had blocked on a full stream has completed.
    Delivered,
}

// Session

/// The reason code to put in a DISCONNECT when a session ends because of a
/// protocol fault, or `None` when there is nothing to tell the peer.
///
/// Only faults we attribute to the peer are reported: a packet that would
/// not decode or broke a rule ([`Error::Protocol`], carrying the codec's own
/// classification) and a violation we detected ourselves
/// ([`Error::ReasonCode`]). An I/O failure, a keep-alive timeout, or a closed
/// socket leave nothing to send to.
fn disconnect_reason(err: &Error) -> Option<ReasonCode> {
    match err {
        Error::Protocol(e) => Some(e.reason_code()),
        Error::ReasonCode(rc, _) => Some(*rc),
        _ => None,
    }
}

/// Whether the server resumed the session we asked it to.
///
/// v3.1.1 and v5 say so in the CONNACK. v3.1 has no Session Present flag, so
/// there is nothing to go by but our own request: a clean session certainly
/// started fresh, and anything else is assumed to have continued — the
/// forgiving reading, since treating a live session as lost would throw away
/// messages the server is still waiting to have completed.
fn session_resumed(params: &ConnectParams, connack: &ConnAck) -> bool {
    match params.ver {
        MqttVersion::V3_1 => !params.clean_start,
        _ => connack.session_present,
    }
}

/// Rebuilds session state from a persistence store written by an earlier run
/// of the program.
///
/// Outbound entries (`s{id}`) hold either the original PUBLISH, if it has not
/// been acknowledged at all, or a PUBREL once the server has taken delivery
/// (§4.4); each is reinstated in the phase its stored packet implies, with an
/// orphan completer, since the token that started it belonged to a process
/// that is no longer running. Inbound entries (`r{id}`) are QoS 2 messages
/// that were PUBREC'd but not yet released.
///
/// Nothing is sent here: [`Session::resume`] decides what goes back on the
/// wire once the CONNACK says whether the session survived. An entry that
/// cannot be decoded is skipped rather than failing the connection — it is
/// one message lost, against a store that may be from an older version of
/// the program or a different protocol level.
fn load_persisted(
    state: &mut SessionState,
    persist: Option<&Arc<Mutex<Box<dyn ClientPersistence>>>>,
    ver: MqttVersion,
) {
    let Some(persist) = persist else {
        return;
    };
    let Ok(keys) = persist.lock().unwrap().keys() else {
        return;
    };

    for key in &keys {
        let Ok(data) = persist.lock().unwrap().get(key) else {
            continue;
        };
        let mut buf = BytesMut::from(data.as_slice());
        let pkt = match RawPacket::decode(&mut buf) {
            Ok(Some(raw)) => match Packet::from_raw(raw, ver) {
                Ok(p) => p,
                Err(_) => continue,
            },
            _ => continue,
        };

        match (key.as_bytes().first(), pkt) {
            (Some(b's'), Packet::Publish(pub_pkt)) => {
                let Some(id) = pub_pkt.packet_id else {
                    continue;
                };
                state.next_id = state.next_id.max(id);
                state.insert(
                    id,
                    Completer::orphan(),
                    PublishPhase::AwaitingAck(Box::new(pub_pkt)),
                    false,
                );
            }
            (Some(b's'), Packet::PubRel(rel)) => {
                state.next_id = state.next_id.max(rel.packet_id);
                state.insert(
                    rel.packet_id,
                    Completer::orphan(),
                    PublishPhase::AwaitingPubComp,
                    false,
                );
            }
            (Some(b'r'), Packet::Publish(pub_pkt)) => {
                let id = pub_pkt.packet_id.unwrap_or(0);
                state.next_id = state.next_id.max(id);
                state.pending_pub2_in.insert(id, Message::from(pub_pkt));
            }
            _ => {}
        }
    }
}

/// A persistence key, built on the stack: `s` (an outbound message) or `r`
/// (an inbound QoS 2 message awaiting PUBREL) followed by the packet
/// identifier in decimal, as in `s42`. The format is what stores written by
/// earlier versions hold, so it must not change.
struct PersistKey {
    buf: [u8; 6],
    len: usize,
}

impl PersistKey {
    fn new(kind: u8, id: u16) -> Self {
        let mut digits = [0u8; 5];
        let mut n = id;
        let mut i = digits.len();
        loop {
            i -= 1;
            digits[i] = b'0' + (n % 10) as u8;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        let mut buf = [0u8; 6];
        buf[0] = kind;
        let len = 1 + digits.len() - i;
        buf[1..len].copy_from_slice(&digits[i..]);
        Self { buf, len }
    }

    fn as_str(&self) -> &str {
        // ASCII by construction: `kind` is always `b's'` or `b'r'`.
        std::str::from_utf8(&self.buf[..self.len]).unwrap_or_default()
    }
}

/// An operation waiting for a slot in the in-flight window, or for a free
/// packet identifier.
enum Deferred {
    /// A QoS 1/2 `Command::Publish`, `Subscribe` or `Unsubscribe` from the
    /// API, not yet assigned a packet ID.
    Command(Command),
    /// An established in-flight message, awaiting (re-)transmission under the
    /// identifier it already holds in [`SessionState::inflight`].
    Retransmit(u16),
}

/// How far a QoS 1/2 publish has got, and what has to be re-sent if the
/// session is resumed on a new connection (§4.4).
enum PublishPhase {
    /// Awaiting PUBACK (QoS 1) or PUBREC (QoS 2). The PUBLISH is kept so it
    /// can be re-sent with DUP set.
    AwaitingAck(Box<paho_mqtt_codec::Publish>),
    /// PUBREC arrived and PUBREL went out; awaiting PUBCOMP. The message
    /// itself is finished with — only the PUBREL is re-sent, never the
    /// PUBLISH, since the server has already taken delivery of it.
    AwaitingPubComp,
}

/// One outbound QoS 1/2 publish that has not yet completed.
struct InflightPublish {
    /// Resolved when the final acknowledgement arrives, or when the message
    /// is abandoned. An orphan completer for a message recovered from
    /// persistence, whose original token is long gone.
    tx: Completer<PublishResponse>,
    phase: PublishPhase,
    /// `false` while the message is queued for a free window slot; only sent
    /// messages count against the in-flight limit.
    sent: bool,
    /// Order the message was first handed to the session, so a
    /// re-transmission preserves it (§4.6).
    seq: u64,
}

impl InflightPublish {
    /// The QoS of the underlying message, or `None` once only a PUBREL is
    /// outstanding.
    fn qos(&self) -> Option<QoS> {
        match &self.phase {
            PublishPhase::AwaitingAck(p) => Some(p.qos),
            PublishPhase::AwaitingPubComp => None,
        }
    }
}

/// The message state of an MQTT session, which outlives any one connection.
///
/// A session is the client's and server's shared memory of what is still in
/// flight. It survives a dropped connection — that is the whole point of
/// resuming rather than starting clean — so it lives here rather than in
/// [`Session`], and is handed from one connection to the next.
///
/// Persistence mirrors it for crash recovery: everything here can be rebuilt
/// by [`load_persisted`] from a store written by a previous run of the
/// program.
#[derive(Default)]
struct SessionState {
    /// Outbound QoS 1/2 publishes awaiting acknowledgement, by packet ID.
    /// Changed only through [`insert`](Self::insert),
    /// [`remove`](Self::remove), [`mark_sent`](Self::mark_sent) and
    /// [`mark_unsent`](Self::mark_unsent), which keep `sent_count` in step.
    inflight: HashMap<u16, InflightPublish>,
    /// How many `inflight` records are sent: the in-flight window's
    /// occupancy, kept so it need not be counted on every publish and ACK.
    sent_count: usize,
    /// Inbound QoS 2 messages that have been PUBREC'd, held until PUBREL
    /// says to deliver them.
    pending_pub2_in: HashMap<u16, Message>,
    /// Last packet identifier handed out.
    next_id: u16,
    /// Counter behind [`InflightPublish::seq`].
    next_seq: u64,
    /// Whether a persistence store has already been read into this state;
    /// only the first connection of a session needs to.
    loaded: bool,
}

impl SessionState {
    /// Records `record` under `id`, stamped with the next sequence number.
    fn insert(&mut self, id: u16, tx: Completer<PublishResponse>, phase: PublishPhase, sent: bool) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let rec = InflightPublish {
            tx,
            phase,
            sent,
            seq,
        };
        if let Some(old) = self.inflight.insert(id, rec)
            && old.sent
        {
            self.sent_count -= 1;
        }
        if sent {
            self.sent_count += 1;
        }
        self.check_sent_count();
    }

    /// Removes and returns the record held under `id`.
    fn remove(&mut self, id: u16) -> Option<InflightPublish> {
        let rec = self.inflight.remove(&id)?;
        if rec.sent {
            self.sent_count -= 1;
        }
        self.check_sent_count();
        Some(rec)
    }

    /// Marks the record under `id` as sent, so it occupies a window slot,
    /// and returns it.
    fn mark_sent(&mut self, id: u16) -> Option<&mut InflightPublish> {
        let rec = self.inflight.get_mut(&id)?;
        if !rec.sent {
            rec.sent = true;
            self.sent_count += 1;
        }
        Some(rec)
    }

    /// Marks the record under `id` as waiting for a window slot.
    fn mark_unsent(&mut self, id: u16) {
        if let Some(rec) = self.inflight.get_mut(&id)
            && rec.sent
        {
            rec.sent = false;
            self.sent_count -= 1;
        }
    }

    /// Number of records occupying a window slot.
    fn sent_count(&self) -> usize {
        self.sent_count
    }

    /// In the library's own unit tests, checks `sent_count` against a full
    /// count. Only there, and only for small maps: the count is O(n), the
    /// integration tests run debug builds with tens of thousands of messages
    /// in flight, and one unit test fills all 65 535 identifiers.
    fn check_sent_count(&self) {
        if cfg!(test) && self.inflight.len() <= 1024 {
            let n = self.inflight.values().filter(|r| r.sent).count();
            assert_eq!(self.sent_count, n, "sent_count out of step");
        }
    }

    /// Packet IDs of every in-flight message, oldest first.
    fn ids_in_order(&self) -> Vec<u16> {
        let mut ids: Vec<_> = self.inflight.iter().map(|(id, r)| (r.seq, *id)).collect();
        ids.sort_unstable();
        ids.into_iter().map(|(_, id)| id).collect()
    }
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

/// Most bytes the run loop queues from commands it finds waiting before it
/// lets the writer have them. Batching is opportunistic, never timed: this
/// only bounds how long the first of them is held back.
const BATCH_BYTES: usize = 16 * 1024;

/// Most commands taken in one opportunistic batch, for commands that queue
/// no bytes (publishes deferred by the in-flight window).
const BATCH_CMDS: usize = 256;

/// The run loop stops taking commands while this many bytes are waiting to
/// be written; they wait in the command channel instead.
const HIGH_WATER: usize = 512 * 1024;

/// Bound on a best-effort final write: what is queued when the session ends,
/// including a DISCONNECT.
const CLOSE_CAP: Duration = Duration::from_secs(1);

/// Most slices handed to one vectored write.
const MAX_IOV: usize = 64;

/// The run loop yields to the executor after this many passes.
///
/// Under load every branch of its `select!` can be ready at once (commands
/// waiting, the socket writable) for as long as the load lasts, so the loop
/// would never return `Pending`. On smol, where every client's loop shares
/// one executor thread and nothing forces a yield, that starves the other
/// clients: their keep-alive timers stop firing. Tokio has its own
/// cooperative budget; yielding here as well costs it one reschedule per
/// this many passes.
const YIELD_EVERY: u32 = 64;

/// Initial capacity of the inbound byte buffer. One buffer serves a whole
/// connection — from the CONNACK read through to the last packet — and grows
/// to fit anything larger.
const READ_BUF_CAPACITY: usize = 4096;

/// What the CONNECT exchange settled, fixed for the life of the connection.
///
/// Each of these is read from the connect options and the CONNACK together —
/// the version in force, the keep-alive after any Server Keep Alive override,
/// and the limits the two sides agreed — so they are gathered rather than
/// passed one by one.
struct Negotiated {
    ver: MqttVersion,
    keep_alive: Duration,
    limits: SessionLimits,
}

/// Where inbound messages go, and how many may be outstanding there.
///
/// The channel carries every event to the dispatcher; the gate counts only
/// the messages among them, so that a slow consumer holds the session back
/// without connection events ever having to wait. The two are always used
/// together, so they travel together.
struct Delivery {
    event_tx: async_channel::Sender<Event>,
    dispatch: Dispatch,
    /// The application's own streams, lent to the session for the life of
    /// the connection and taken back when it ends.
    sinks: Sinks,
}

/// Holds all mutable state for a connected MQTT session.
struct Session<B: Backend> {
    /// Write half of the split transport.
    writer: B::Writer,
    /// Negotiated MQTT version.
    ver: MqttVersion,
    /// Keep-alive interval.
    keep_alive: Duration,
    /// Encoded packets not yet accepted by the transport.
    out: Outbox,
    /// Inbound bytes not yet parsed into packets. Starts out holding
    /// whatever arrived with the CONNACK behind it; see [`Connected`].
    read_buf: BytesMut,
    /// Message state carried across the connections of one session.
    state: SessionState,
    /// Pending SUBSCRIBE operations awaiting SUBACK, with the `single` flag
    /// that decides how an error code in the SUBACK is reported.
    pending_sub: HashMap<u16, (Completer<SubscribeResponse>, bool)>,
    /// Pending UNSUBSCRIBE operations awaiting UNSUBACK, likewise.
    pending_unsub: HashMap<u16, (Completer<UnsubscribeResponse>, bool)>,
    /// Sender to deliver inbound events to the dispatcher thread, which
    /// runs the application's callbacks and feeds its sync consumers.
    event_tx: async_channel::Sender<Event>,
    /// The dispatcher's gate, and whether it wants messages at all.
    dispatch: Dispatch,
    /// The application's streams, fed by this loop without the thread hop.
    sinks: Sinks,
    /// Inbound messages decoded but not yet delivered, for want of room at
    /// one of their destinations, each with the acknowledgement it is owed
    /// once it has been (PUBACK for QoS 1, PUBCOMP for QoS 2). In arrival
    /// order.
    held: VecDeque<(Message, Option<Packet>)>,
    /// A fan-out that blocked on a full stream, and the acknowledgement
    /// waiting on it. Nothing else is delivered until it completes, so
    /// every destination keeps its order.
    pending: Option<Parked>,
    /// The acknowledgement owed once `pending` completes.
    pending_ack: Option<Packet>,
    /// When the application last stopped taking messages, so that a stall
    /// long enough to look like a hang leaves a trace in the log.
    stalled_since: Option<std::time::Instant>,
    /// True when a PINGREQ has been sent and PINGRESP has not yet arrived.
    ping_outstanding: bool,
    /// Persistence store for in-flight QoS 1/2 messages (if any).
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
    /// Limits fixed once the CONNACK arrived.
    limits: SessionLimits,
    /// Topic alias → topic name, for inbound PUBLISH packets that carry an
    /// alias instead of (or as well as) the name. Valid only for this
    /// connection, so a new session starts with an empty map (§3.3.2.3.4).
    topic_aliases: HashMap<u16, String>,
    /// Operations deferred because the in-flight limit was reached or no
    /// packet identifier was free: new publishes and messages awaiting
    /// re-transmission alike, and (un)subscribes. Drained in order as
    /// acknowledgements free slots and identifiers.
    inflight_deferred: VecDeque<Deferred>,
}

impl<B: Backend> Session<B> {
    /// `flush_needed` says whether the transport holds written bytes until a
    /// flush (TLS, WebSocket); see [`ServerAddr::buffers_writes`].
    ///
    /// [`ServerAddr::buffers_writes`]: super::ServerAddr::buffers_writes
    fn new(
        writer: B::Writer,
        flush_needed: bool,
        read_buf: BytesMut,
        negotiated: Negotiated,
        delivery: Delivery,
        persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
        state: SessionState,
    ) -> Self {
        let Negotiated {
            ver,
            keep_alive,
            limits,
        } = negotiated;
        let Delivery {
            event_tx,
            dispatch,
            sinks,
        } = delivery;
        Self {
            writer,
            ver,
            keep_alive,
            out: Outbox::new(flush_needed),
            read_buf,
            state,
            pending_sub: HashMap::new(),
            pending_unsub: HashMap::new(),
            event_tx,
            dispatch,
            sinks,
            held: VecDeque::new(),
            pending: None,
            pending_ack: None,
            stalled_since: None,
            ping_outstanding: false,
            persist,
            limits,
            topic_aliases: HashMap::new(),
            inflight_deferred: VecDeque::new(),
        }
    }

    /// Returns `true` if any outbound operations are waiting for broker ACKs
    /// or are queued in the in-flight deferred buffer.
    fn has_pending(&self) -> bool {
        !self.state.inflight.is_empty()
            || !self.pending_sub.is_empty()
            || !self.pending_unsub.is_empty()
            || !self.inflight_deferred.is_empty()
    }

    /// Whether to read more from the socket.
    ///
    /// Not while messages are held back for want of room at one of their
    /// destinations: stopping lets TCP flow control push back on the broker,
    /// rather than buffer without limit. Except while one of our own
    /// operations awaits an acknowledgement: that may be in the unread
    /// bytes, behind the backlog, and a consumer waiting on it (a callback
    /// that publishes and waits, say) would then never make room, a
    /// deadlock. So reading goes on, held messages accumulate, and QoS 1/2
    /// among them stay unacknowledged, which lets the broker's in-flight
    /// window bound them.
    fn reads_allowed(&self) -> bool {
        (self.held.is_empty() && self.pending.is_none()) || self.has_pending()
    }

    /// Whether a message can be handed over now: no fan-out is still in
    /// progress, and the dispatcher — if it wants messages at all — has
    /// room for one.
    fn room_to_deliver(&self) -> bool {
        self.pending.is_none() && (!self.dispatch.wanted() || self.dispatch.gate.has_room())
    }

    /// Whether a newly arrived message can go straight out: there is room,
    /// and nothing is queued ahead of it.
    fn can_deliver(&self) -> bool {
        self.held.is_empty() && self.room_to_deliver()
    }

    /// Delivers an inbound message to every destination, queuing `ack` once
    /// they all have it; or holds both, in order, until they can take it.
    fn deliver_msg(&mut self, msg: Message, ack: Option<Packet>) -> Result<()> {
        if self.can_deliver() {
            self.hand_over(msg, ack)
        } else {
            self.held.push_back((msg, ack));
            Ok(())
        }
    }

    /// Hands `msg` to the dispatcher thread, if anything there wants it, and
    /// to each of the application's streams, then queues the acknowledgement
    /// it was owed.
    ///
    /// The last destination takes the message by move, so one consumer — the
    /// usual case — costs no clone. If a stream was full the fan-out is
    /// parked and the acknowledgement waits with it: nothing is acknowledged
    /// to the broker before the application has it. With no destination at
    /// all the message has nowhere to go and is acknowledged all the same,
    /// as it always was.
    fn hand_over(&mut self, msg: Message, ack: Option<Packet>) -> Result<()> {
        let mut msg = Some(msg);
        if self.dispatch.wanted() {
            let m = if self.sinks.is_empty() {
                msg.take().unwrap()
            } else {
                msg.as_ref().unwrap().clone()
            };
            if self.event_tx.try_send(Event::MessageArrived(m)).is_ok() {
                self.dispatch.gate.sent();
            }
        }
        if let Some(m) = msg {
            self.pending = self.sinks.deliver_message(m);
        }
        match ack {
            Some(ack) if self.pending.is_some() => self.pending_ack = Some(ack),
            Some(ack) => {
                self.queue(&ack)?;
            }
            None => {}
        }
        Ok(())
    }

    /// Hands over held messages, in order, while they can be taken.
    fn release_held(&mut self) -> Result<()> {
        while self.room_to_deliver() {
            let Some((msg, ack)) = self.held.pop_front() else {
                break;
            };
            self.hand_over(msg, ack)?;
        }
        Ok(())
    }

    /// Hands every held message to its destinations regardless of room, as
    /// the session ends: they were received, and are delivered rather than
    /// lost. Their acknowledgements are not sent; the broker re-sends
    /// unacknowledged QoS 1/2 messages if the session resumes.
    ///
    /// Returns whatever could not be handed over at once, for the caller to
    /// await outside the session.
    fn flush_held(&mut self) -> Option<Parked> {
        let mut parked: Vec<Parked> = self.pending.take().into_iter().collect();
        for (msg, _) in std::mem::take(&mut self.held) {
            let mut msg = Some(msg);
            if self.dispatch.wanted() {
                let m = if self.sinks.is_empty() {
                    msg.take().unwrap()
                } else {
                    msg.as_ref().unwrap().clone()
                };
                if self.event_tx.try_send(Event::MessageArrived(m)).is_ok() {
                    self.dispatch.gate.sent();
                }
            }
            if let Some(m) = msg
                && let Some(p) = self.sinks.deliver_message(m)
            {
                parked.push(p);
            }
        }
        if parked.is_empty() {
            return None;
        }
        Some(Box::pin(async move {
            for p in parked {
                p.await;
            }
        }))
    }

    /// Number of outbound QoS 1/2 publishes occupying the in-flight window.
    ///
    /// A message queued for a free slot does not count until it is sent; one
    /// awaiting PUBCOMP still does, since its packet identifier is not free
    /// until the exchange finishes.
    fn inflight_count(&self) -> usize {
        self.state.sent_count()
    }

    /// `true` when the in-flight window has room for another QoS 1/2
    /// publish. (A new one also needs a packet identifier: [`id_free`].)
    ///
    /// [`id_free`]: Self::id_free
    fn inflight_slot_free(&self) -> bool {
        self.inflight_count() < self.limits.max_inflight
    }

    /// `true` when a packet identifier is free for a new QoS 1/2 publish,
    /// SUBSCRIBE or UNSUBSCRIBE.
    ///
    /// Every outstanding exchange holds one, including in-flight messages
    /// still waiting for a window slot, so the window having room does not
    /// imply this: with a window of 65 535, pending (un)subscribes and
    /// messages awaiting re-transmission can use up the last of them.
    fn id_free(&self) -> bool {
        self.state.inflight.len() + self.pending_sub.len() + self.pending_unsub.len()
            < MAX_PACKET_IDS
    }

    /// Whether the deferred operation `d` can go ahead now.
    fn can_proceed(&self, d: &Deferred) -> bool {
        match d {
            // Already holds its identifier.
            Deferred::Retransmit(_) => self.inflight_slot_free(),
            Deferred::Command(Command::Publish { .. }) => {
                self.inflight_slot_free() && self.id_free()
            }
            Deferred::Command(_) => self.id_free(),
        }
    }

    /// Takes deferred operations, in order, for as long as the one at the
    /// front can go ahead. Called whenever an acknowledgement frees a window
    /// slot or a packet identifier. An encoding error propagates immediately.
    fn drain_deferred(&mut self) -> Result<()> {
        while let Some(d) = self.inflight_deferred.front() {
            if !self.can_proceed(d) {
                break;
            }
            match self.inflight_deferred.pop_front() {
                Some(Deferred::Command(cmd)) => {
                    self.handle_command(cmd)?;
                }
                Some(Deferred::Retransmit(id)) => self.send_inflight(id)?,
                None => break,
            }
        }
        Ok(())
    }

    /// Queues (or re-queues) the in-flight message held under `id`.
    ///
    /// A message still awaiting its first acknowledgement goes out as a
    /// PUBLISH with DUP set; one that has been PUBREC'd goes out as a PUBREL.
    /// Marks the record as occupying a window slot.
    fn send_inflight(&mut self, id: u16) -> Result<()> {
        let Some(rec) = self.state.mark_sent(id) else {
            return Ok(());
        };
        let max = self.limits.server_max_packet_size;
        let res = match std::mem::replace(&mut rec.phase, PublishPhase::AwaitingPubComp) {
            // The record's own copy is encoded and put back, not cloned. Any
            // later re-send is a duplicate too, so DUP stays set.
            PublishPhase::AwaitingAck(mut pub_pkt) => {
                pub_pkt.dup = true;
                let packet = Packet::Publish(*pub_pkt);
                let res = self.out.push(&packet, self.ver, max);
                let Packet::Publish(pub_pkt) = packet else {
                    unreachable!()
                };
                rec.phase = PublishPhase::AwaitingAck(Box::new(pub_pkt));
                res
            }
            PublishPhase::AwaitingPubComp => {
                self.out
                    .push(&Packet::PubRel(PubAck::new(id)), self.ver, max)
            }
        };
        res.map(|_| ())
    }

    /// `true` if `id` is currently in use by any outstanding operation.
    fn id_in_use(&self, id: u16) -> bool {
        self.state.inflight.contains_key(&id)
            || self.pending_sub.contains_key(&id)
            || self.pending_unsub.contains_key(&id)
    }

    /// Persists `parts` under the key for `kind` and `id` (see
    /// [`PersistKey`]), silently ignoring errors. Without a store this does
    /// nothing at all, not even build the key.
    fn persist_put(&self, kind: u8, id: u16, parts: &[&[u8]]) {
        if let Some(p) = &self.persist {
            let key = PersistKey::new(kind, id);
            let _ = p.lock().unwrap().put(key.as_str(), parts.to_vec());
        }
    }

    /// Persists the frame just queued (see [`Outbox::last_frame`]) under the
    /// key for `kind` and `id`. Nothing queued is written before the handler
    /// returns, so the frame is stored before it is sent.
    fn persist_last_frame(&self, kind: u8, id: u16) {
        if self.persist.is_some() {
            let [hdr, payload] = self.out.last_frame();
            if payload.is_empty() {
                self.persist_put(kind, id, &[hdr]);
            } else {
                self.persist_put(kind, id, &[hdr, payload]);
            }
        }
    }

    /// Removes the persistence entry for `kind` and `id`, silently ignoring
    /// errors. Without a store this does nothing.
    fn persist_remove(&self, kind: u8, id: u16) {
        if let Some(p) = &self.persist {
            let key = PersistKey::new(kind, id);
            let _ = p.lock().unwrap().remove(key.as_str());
        }
    }

    /// Allocates the next free packet identifier (1 – 65535, wrapping).
    ///
    /// Identifiers still awaiting an acknowledgement are skipped: after a
    /// session restore `next_id` sits past the highest persisted identifier,
    /// so a wrap-around could otherwise land on one that is still in flight
    /// and silently replace its completer.
    ///
    /// The caller must have checked [`id_free`](Self::id_free): with every
    /// identifier in use this would search forever.
    fn alloc_id(&mut self) -> u16 {
        debug_assert!(
            self.id_free(),
            "alloc_id() with every packet identifier in use"
        );
        loop {
            self.state.next_id = self.state.next_id.wrapping_add(1);
            if self.state.next_id == 0 {
                self.state.next_id = 1;
            }
            if !self.id_in_use(self.state.next_id) {
                return self.state.next_id;
            }
        }
    }

    /// Queues `packet` for the writer and returns the offset at which its
    /// frame ends. Nothing is written until the handler returns to the run
    /// loop. A packet over the server's maximum packet size is refused.
    fn queue(&mut self, packet: &Packet) -> Result<u64> {
        self.out
            .push(packet, self.ver, self.limits.server_max_packet_size)
    }

    /// Brings the carried session state into line with what the server
    /// remembers, then puts everything outstanding back on the wire.
    ///
    /// When `resumed` is false the server has no memory of this session —
    /// either we asked for a clean start, or it lost ours. What that means
    /// per message:
    ///
    /// * **QoS 1** is re-sent. Its contract is at-least-once, and a message
    ///   the server never acknowledged may never have arrived.
    /// * **QoS 2 is abandoned**, and its token fails. Exactly-once is a
    ///   guarantee made *within* a session: once the server has forgotten
    ///   the exchange, re-sending could duplicate a message it already
    ///   delivered, and staying silent would leave the caller waiting for a
    ///   PUBCOMP that can never come. Quietly downgrading to at-least-once
    ///   would break the contract the caller asked for, so the failure is
    ///   reported instead.
    /// * **Inbound QoS 2** state is dropped: the server will not re-send the
    ///   PUBRELs those messages are waiting for.
    fn resume(&mut self, resumed: bool) -> Result<()> {
        if !resumed {
            self.discard_unresumable();
        }

        // Re-send in the order the messages were first published (§4.6),
        // respecting the in-flight window as new publishes do.
        for id in self.state.ids_in_order() {
            if self.inflight_slot_free() {
                self.send_inflight(id)?;
            } else {
                self.state.mark_unsent(id);
                self.inflight_deferred.push_back(Deferred::Retransmit(id));
            }
        }
        Ok(())
    }

    /// Drops the state a server that did not resume our session cannot
    /// honour, failing the tokens that can no longer be completed.
    fn discard_unresumable(&mut self) {
        let abandon: Vec<u16> = self
            .state
            .inflight
            .iter()
            .filter(|(_, r)| r.qos() != Some(QoS::QoS1))
            .map(|(id, _)| *id)
            .collect();

        for id in abandon {
            if let Some(rec) = self.state.remove(id) {
                self.persist_remove(b's', id);
                rec.tx.complete(Err(Error::General(
                    "server did not resume the session; QoS 2 delivery abandoned",
                )));
            }
        }

        for id in self
            .state
            .pending_pub2_in
            .keys()
            .copied()
            .collect::<Vec<_>>()
        {
            self.persist_remove(b'r', id);
        }
        self.state.pending_pub2_in.clear();
    }

    /// Brings a freshly connected session up and runs it to completion:
    /// re-sends whatever the session still has outstanding, flushes the
    /// commands buffered while disconnected, then services the connection
    /// until it ends. `resumed` says whether the server kept our session.
    ///
    /// Any failure — resume, flush, or run — surfaces as one `Err`, so the
    /// caller tears the session down the same way in every case. Commands
    /// still in `buf` when a flush fails stay there for the next connection.
    async fn serve(
        &mut self,
        reader: &mut B::Reader,
        cmd_rx: &async_channel::Receiver<Command>,
        buf: &mut VecDeque<Command>,
        resumed: bool,
    ) -> Result<()> {
        let res = self.serve_inner(reader, cmd_rx, buf, resumed).await;

        // A v5 receiver that rejects a packet must say why before closing
        // (§4.13). Best-effort, and bounded: the connection may already be
        // gone, and the original error is what the caller hears either way.
        if let Err(e) = &res
            && self.ver.is_v5()
            && let Some(rc) = disconnect_reason(e)
            && self
                .queue(&Packet::Disconnect(Disconnect::with_properties(rc, None)))
                .is_ok()
        {
            self.drain_outbox(reader, CLOSE_CAP).await;
        }
        res
    }

    async fn serve_inner(
        &mut self,
        reader: &mut B::Reader,
        cmd_rx: &async_channel::Receiver<Command>,
        buf: &mut VecDeque<Command>,
        resumed: bool,
    ) -> Result<()> {
        // Both only queue: the run loop's first pass writes the result. A
        // long outage can leave a lot here, well past `HIGH_WATER`; the loop
        // then takes no new commands until it has drained.
        self.resume(resumed)?;
        while let Some(cmd) = buf.pop_front() {
            // Only publishes are ever buffered.
            self.handle_command(cmd)?;
        }
        self.run(reader, cmd_rx).await
    }

    /// Resolves the topic of an inbound PUBLISH that carries a topic alias,
    /// and records the mapping (§3.3.2.3.4).
    ///
    /// A packet with a name *and* an alias establishes or replaces the
    /// mapping; one with an alias and an empty name uses it. An alias of 0,
    /// one above the maximum we advertised, or one we have never been given
    /// a name for is a protocol error, reported as `TopicAliasInvalid` so
    /// the DISCONNECT carries the code the specification asks for.
    fn resolve_topic_alias(&mut self, pub_pkt: &mut paho_mqtt_codec::Publish) -> Result<()> {
        let Some(alias) = pub_pkt.properties.get_u16(PropertyId::TopicAlias) else {
            return Ok(());
        };
        let invalid = || Error::ReasonCode(ReasonCode::TopicAliasInvalid, Properties::new());

        if alias == 0 || alias > self.limits.topic_alias_max {
            return Err(invalid());
        }
        if pub_pkt.topic.is_empty() {
            match self.topic_aliases.get(&alias) {
                Some(topic) => pub_pkt.topic = topic.clone(),
                None => return Err(invalid()),
            }
        } else {
            self.topic_aliases.insert(alias, pub_pkt.topic.clone());
        }
        Ok(())
    }

    /// Handles every complete packet now in `read_buf`.
    ///
    /// One read can carry any number of packets, and the first `read_buf` of
    /// a connection can arrive already full of them, so this is driven by
    /// what the buffer holds rather than by what a single read returned.
    ///
    /// It stops early, leaving the rest buffered, once messages are being
    /// held back and reading should pause (see [`reads_allowed`]).
    ///
    /// [`reads_allowed`]: Self::reads_allowed
    fn handle_buffered(&mut self) -> Result<()> {
        while self.reads_allowed() {
            let Some(raw) = RawPacket::decode_max(&mut self.read_buf, self.limits.max_packet_size)?
            else {
                break;
            };
            let packet = Packet::from_raw(raw, self.ver)?;
            self.handle_packet(packet)?;
        }
        Ok(())
    }

    /// Main event loop: write what the outbox holds, read from `reader`,
    /// take commands from `cmd_rx`, and service keep-alive, until an error
    /// or a clean disconnect.
    ///
    /// The `select!` is biased, in that order. The writer goes first, so a
    /// packet queued on the last pass goes out on this one; it cannot starve
    /// the others, since it is enabled only while there is something to write
    /// or flush, one vectored write normally empties the outbox, and a full
    /// socket makes it pend. Reads come before commands, so inbound ACKs,
    /// which free in-flight slots, are seen before more work is accepted.
    async fn run(
        &mut self,
        reader: &mut B::Reader,
        cmd_rx: &async_channel::Receiver<Command>,
    ) -> Result<()> {
        let keep_alive = self.keep_alive;
        // keep_alive == ZERO means disabled: a never-firing sleep reports
        // itself terminated, so `select!` skips that branch entirely.
        let ka_enabled = !keep_alive.is_zero();

        // Resettable sleep: the deadline advances on every write, so that
        // PINGREQs are only sent when the connection has been truly idle.
        let mut ka_sleep = if ka_enabled {
            B::Sleep::new(keep_alive)
        } else {
            B::Sleep::never()
        };

        // Packets the broker pipelined behind the CONNACK are in `read_buf`
        // already. Nothing further is owed on this connection, so waiting on
        // a read before handling them could wait indefinitely.
        self.handle_buffered()?;

        let mut passes = 0u32;
        loop {
            passes += 1;
            if passes.is_multiple_of(YIELD_EVERY) {
                yield_now().await;
            }

            self.out.prepare_write();
            let take_cmds = self.out.len() < HIGH_WATER;
            let reading = self.reads_allowed();
            self.note_stall();
            // Only wait on the gate when something is held *and* it is the
            // gate holding it up; a fan-out in progress is waited on by its
            // own branch.
            let holding = !self.held.is_empty() && self.pending.is_none();

            let ev = select_biased! {
                res = writer_step::<B>(&mut self.writer, &self.out).fuse() => {
                    LoopEvent::Writer(res)
                }
                res = read_when::<B>(reading, reader, &mut self.read_buf).fuse() => {
                    LoopEvent::Read(res)
                }
                _ = room_when(holding, &self.dispatch).fuse() => LoopEvent::Room,
                _ = deliver_when(&mut self.pending).fuse() => LoopEvent::Delivered,
                cmd = recv_when(take_cmds, cmd_rx).fuse() => LoopEvent::Command(cmd),
                _ = ka_sleep => LoopEvent::KeepAlive,
            };

            match ev {
                LoopEvent::Writer(res) => {
                    if self.writer_done(res)? && ka_enabled {
                        ka_sleep.reset(keep_alive);
                    }
                }
                LoopEvent::Read(res) => {
                    if res? == 0 {
                        return Err(Error::Disconnected);
                    }
                    self.handle_buffered()?;
                    if self.write_now()? && ka_enabled {
                        ka_sleep.reset(keep_alive);
                    }
                }
                // The dispatcher made room: hand over what was held, then
                // carry on with any bytes left buffered when reading paused.
                LoopEvent::Room => {
                    self.release_held()?;
                    self.handle_buffered()?;
                    if self.write_now()? && ka_enabled {
                        ka_sleep.reset(keep_alive);
                    }
                }
                // A stream that was full has taken the message. Only now is
                // it acknowledged to the broker, and only now may the next
                // message be delivered.
                LoopEvent::Delivered => {
                    self.pending = None;
                    if let Some(ack) = self.pending_ack.take() {
                        self.queue(&ack)?;
                    }
                    self.release_held()?;
                    self.handle_buffered()?;
                    if self.write_now()? && ka_enabled {
                        ka_sleep.reset(keep_alive);
                    }
                }
                // The API was dropped: send what is already queued, then stop.
                LoopEvent::Command(Err(_)) => {
                    self.drain_outbox(reader, CLOSE_CAP).await;
                    return Ok(());
                }
                LoopEvent::Command(Ok(cmd)) => {
                    // Take whatever else is already waiting, so it goes out
                    // in the same write, but only up to a bound, so the
                    // first packet is not held back for long.
                    let mut flow = self.handle_command(cmd)?;
                    let mut n = 1;
                    while matches!(flow, Flow::Continue)
                        && n < BATCH_CMDS
                        && self.out.len() < BATCH_BYTES
                    {
                        let Ok(cmd) = cmd_rx.try_recv() else {
                            break;
                        };
                        flow = self.handle_command(cmd)?;
                        n += 1;
                    }
                    if let Flow::Disconnect(req) = flow {
                        self.disconnect(req, reader).await;
                        return Ok(());
                    }
                    if self.write_now()? && ka_enabled {
                        ka_sleep.reset(keep_alive);
                    }
                }
                LoopEvent::KeepAlive => {
                    // A PINGRESP cannot be seen while reading is paused, so
                    // its absence then proves nothing; ping again instead.
                    if self.ping_outstanding && self.reads_allowed() {
                        return Err(Error::Timeout);
                    }
                    self.queue(&Packet::PingReq)?;
                    self.ping_outstanding = true;
                    ka_sleep.reset(keep_alive);
                }
            }
        }
    }

    /// Logs when the application stops taking messages, and how long for.
    ///
    /// Backpressure is deliberate, but from the outside a client that has
    /// stopped reading looks like one that has hung, so a stall past
    /// [`STALL_WARN`] says so when it ends. Nothing is logged per message.
    fn note_stall(&mut self) {
        let blocked = !self.held.is_empty() || self.pending.is_some();
        match (blocked, self.stalled_since) {
            (true, None) => {
                debug!(
                    "Inbound paused: the application has not taken {} message(s)",
                    self.held.len() + usize::from(self.pending.is_some())
                );
                self.stalled_since = Some(std::time::Instant::now());
            }
            (false, Some(since)) => {
                self.stalled_since = None;
                let waited = since.elapsed();
                if waited >= STALL_WARN {
                    warn!(
                        "Inbound was paused for {:.1} s waiting for the application to take \
                         messages; reading has resumed",
                        waited.as_secs_f32()
                    );
                } else {
                    debug!("Inbound resumed after {} ms", waited.as_millis());
                }
            }
            _ => {}
        }
    }

    /// Writes what a handler just queued, if the transport takes it at once,
    /// rather than on the loop's next pass. Returns `true` if bytes went out.
    ///
    /// On an idle connection that saves a whole pass of the `select!` per
    /// packet, which is most of what a lone publish costs. It is one poll
    /// of [`writer_step`], so it never waits: if the transport is not ready,
    /// the writer branch carries on as usual, and what it registered for
    /// wakeups here is replaced when that branch polls with the task's own
    /// waker.
    fn write_now(&mut self) -> Result<bool> {
        if self.out.is_empty() && !self.out.needs_flush() {
            return Ok(false);
        }
        self.out.prepare_write();
        match writer_step::<B>(&mut self.writer, &self.out).now_or_never() {
            Some(res) => self.writer_done(res),
            None => Ok(false),
        }
    }

    /// Applies the outcome of a [`writer_step`] to the outbox. Returns `true`
    /// if bytes went out, which is what restarts the keep-alive interval.
    fn writer_done(&mut self, res: io::Result<Step>) -> Result<bool> {
        match res? {
            Step::Wrote(0) => Err(io::Error::from(io::ErrorKind::WriteZero).into()),
            Step::Wrote(n) => {
                self.out.advance(n);
                Ok(true)
            }
            Step::Flushed => {
                self.out.flushed();
                Ok(false)
            }
        }
    }

    /// Performs an orderly disconnect, then resolves its token.
    ///
    /// With a timeout, in-flight operations get up to that long to be
    /// acknowledged first. The DISCONNECT then goes out behind everything
    /// already queued, given up to [`CLOSE_CAP`] to be written.
    async fn disconnect(&mut self, req: DisconnectReq, reader: &mut B::Reader) {
        if !req.timeout.is_zero() && self.has_pending() {
            self.drain_pending(reader, req.timeout).await;
        }
        let disc = Disconnect::with_properties(req.reason_code, req.props);
        if self.queue(&Packet::Disconnect(disc)).is_ok() {
            self.drain_outbox(reader, CLOSE_CAP).await;
        }
        req.connected_flag.store(false, Ordering::SeqCst);
        req.tx.complete(Ok(()));
    }

    /// Writes and flushes what the outbox holds, for up to `cap`, as the
    /// session ends. Best effort: a write error or the deadline just stops it.
    ///
    /// It keeps reading, since a peer that is itself blocked writing to us
    /// could otherwise deadlock the two of us, but discards what it reads:
    /// anything that came of handling it would be queued behind a DISCONNECT.
    async fn drain_outbox(&mut self, reader: &mut B::Reader, cap: Duration) {
        let mut deadline = B::Sleep::new(cap);
        loop {
            self.out.prepare_write();
            if self.out.is_empty() && !self.out.needs_flush() {
                return;
            }
            let ev = select_biased! {
                res = writer_step::<B>(&mut self.writer, &self.out).fuse() => {
                    LoopEvent::Writer(res)
                }
                res = B::read_buf(reader, &mut self.read_buf).fuse() => LoopEvent::Read(res),
                _ = deadline => return,
            };
            match ev {
                LoopEvent::Writer(res) => {
                    if self.writer_done(res).is_err() {
                        return;
                    }
                }
                LoopEvent::Read(Ok(n)) if n > 0 => self.read_buf.clear(),
                _ => return,
            }
        }
    }

    /// Keeps servicing the connection until every pending operation has been
    /// acknowledged or `timeout` elapses, whichever comes first.
    ///
    /// Used by an orderly disconnect so in-flight QoS 1/2 publishes and
    /// (un)subscribes get their ACKs before DISCONNECT goes out. It writes
    /// too: the packets awaiting those ACKs, and what handling them queues,
    /// may not have gone out yet. A closed connection, I/O error, or
    /// undecodable packet ends the drain early; the caller sends DISCONNECT
    /// regardless.
    async fn drain_pending(&mut self, reader: &mut B::Reader, timeout: Duration) {
        let mut deadline = B::Sleep::new(timeout);

        let mut passes = 0u32;
        while self.has_pending() {
            passes += 1;
            if passes.is_multiple_of(YIELD_EVERY) {
                yield_now().await;
            }
            self.out.prepare_write();
            let ev = select_biased! {
                res = writer_step::<B>(&mut self.writer, &self.out).fuse() => {
                    LoopEvent::Writer(res)
                }
                res = B::read_buf(reader, &mut self.read_buf).fuse() => LoopEvent::Read(res),
                _ = deadline => return,
            };
            match ev {
                LoopEvent::Writer(res) => {
                    if self.writer_done(res).is_err() {
                        return;
                    }
                }
                LoopEvent::Read(Ok(n)) if n > 0 => {
                    while let Ok(Some(raw)) =
                        RawPacket::decode_max(&mut self.read_buf, self.limits.max_packet_size)
                    {
                        match Packet::from_raw(raw, self.ver) {
                            Ok(pkt) => {
                                let _ = self.handle_packet(pkt);
                            }
                            Err(_) => return,
                        }
                    }
                    let _ = self.release_held();
                }
                _ => return,
            }
        }
    }

    /// Handles one inbound packet from the broker. Any reply is queued.
    fn handle_packet(&mut self, packet: Packet) -> Result<()> {
        match packet {
            // ---- Publish acknowledgements ----
            // A publish is one operation with one outcome: an error reason
            // code fails its token, mirroring how a refused CONNACK fails
            // `connect()`. The persisted copy is removed either way — the
            // broker has answered, so there is nothing left to retry.
            Packet::PubAck(ack) => {
                if let Some(rec) = self.state.remove(ack.packet_id) {
                    self.persist_remove(b's', ack.packet_id);
                    rec.tx
                        .complete(publish_outcome(ack.reason_code, ack.properties));
                }
                self.drain_deferred()?;
            }

            // ---- QoS 2 outbound: PUBREC → send PUBREL ----
            Packet::PubRec(rec) => {
                if rec.reason_code.is_err() {
                    if let Some(inflight) = self.state.remove(rec.packet_id) {
                        self.persist_remove(b's', rec.packet_id);
                        inflight
                            .tx
                            .complete(Err(Error::ReasonCode(rec.reason_code, rec.properties)));
                    }
                    self.drain_deferred()?;
                    return Ok(());
                }

                // The server has taken delivery, so the message itself is
                // finished with: from here only a PUBREL is owed, and that
                // is what a resumed session must re-send (§4.4). Replacing
                // the persisted PUBLISH with the PUBREL keeps the store in
                // step, so a crash now cannot resurrect the message.
                if let Some(inflight) = self.state.mark_sent(rec.packet_id) {
                    inflight.phase = PublishPhase::AwaitingPubComp;
                }
                self.queue(&Packet::PubRel(PubAck::new(rec.packet_id)))?;
                self.persist_last_frame(b's', rec.packet_id);
            }

            // ---- QoS 2 outbound: PUBCOMP → resolve token ----
            Packet::PubComp(comp) => {
                if let Some(rec) = self.state.remove(comp.packet_id) {
                    self.persist_remove(b's', comp.packet_id);
                    rec.tx
                        .complete(publish_outcome(comp.reason_code, comp.properties));
                }
                self.drain_deferred()?;
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
                // Its packet identifier is free again.
                self.drain_deferred()?;
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
                // Its packet identifier is free again.
                self.drain_deferred()?;
            }

            // ---- Inbound application message ----
            Packet::Publish(mut pub_pkt) => {
                self.resolve_topic_alias(&mut pub_pkt)?;
                match pub_pkt.qos {
                    QoS::QoS0 => {
                        let msg = Message::from(pub_pkt);
                        self.deliver_msg(msg, None)?;
                    }
                    // Acknowledged only once handed over, so a backlog is
                    // bounded by the broker's in-flight window.
                    QoS::QoS1 => {
                        let id = pub_pkt.packet_id.unwrap_or(0);
                        let msg = Message::from(pub_pkt);
                        self.deliver_msg(msg, Some(Packet::PubAck(PubAck::new(id))))?;
                    }
                    QoS::QoS2 => {
                        let id = pub_pkt.packet_id.unwrap_or(0);
                        // Persist the inbound QoS 2 PUBLISH before acknowledging
                        // so we can redeliver the message on reconnect. The
                        // payload is stored from its own buffer, not copied.
                        let packet = Packet::Publish(pub_pkt);
                        if self.persist.is_some() {
                            let mut hdr = BytesMut::with_capacity(64);
                            if let Ok(payload) = packet.encode_split(&mut hdr, self.ver) {
                                match &payload {
                                    Some(p) => self.persist_put(b'r', id, &[&hdr, p]),
                                    None => self.persist_put(b'r', id, &[&hdr]),
                                }
                            }
                        }
                        let Packet::Publish(pub_pkt) = packet else {
                            unreachable!()
                        };
                        let msg = Message::from(pub_pkt);
                        self.state.pending_pub2_in.insert(id, msg);
                        let rec = PubAck::new(id);
                        self.queue(&Packet::PubRec(rec))?;
                    }
                }
            }

            // ---- QoS 2 inbound: PUBREL → send PUBCOMP, deliver message ----
            Packet::PubRel(rel) => {
                let comp = Packet::PubComp(PubAck::new(rel.packet_id));
                match self.state.pending_pub2_in.remove(&rel.packet_id) {
                    Some(msg) => {
                        self.persist_remove(b'r', rel.packet_id);
                        self.deliver_msg(msg, Some(comp))?;
                    }
                    // Already delivered, its PUBCOMP lost: just complete.
                    None => {
                        self.queue(&comp)?;
                    }
                }
            }

            // ---- Enhanced authentication ----
            // Re-authentication (§4.12.1) is always started by the client,
            // and this client never starts one, so an AUTH here is the
            // broker breaking the protocol.
            Packet::Auth(_) => {
                return Err(Error::ReasonCode(
                    ReasonCode::ProtocolError,
                    Properties::new(),
                ));
            }

            // ---- Keep-alive response ----
            Packet::PingResp => {
                self.ping_outstanding = false;
            }

            // ---- Broker-initiated disconnect (v5) ----
            // As in the C library, this also fires ConnectionLost and
            // triggers auto-reconnect once the session loop unwinds.
            Packet::Disconnect(disc) => {
                // Messages received before the DISCONNECT are delivered
                // before it.
                let flushed = self.flush_held();
                let (reason_code, props) = (disc.reason_code, disc.properties);
                self.deliver(Event::Disconnected {
                    reason_code,
                    props: props.clone(),
                });
                let to_streams = self
                    .sinks
                    .deliver_event(disconnected_event(reason_code, props));
                // Awaited by the loop once the session has unwound.
                self.pending = chain(flushed, to_streams);
                return Err(Error::Disconnected);
            }

            _ => {} // ignore unexpected packets
        }
        Ok(())
    }

    /// Hands an event to the public layer.
    ///
    /// The event channel is unbounded, so this fails only once the API side
    /// has gone, when there is no one left to tell.
    fn deliver(&self, ev: Event) {
        let _ = self.event_tx.try_send(ev);
    }

    /// Handles one command from the public API. Any packet it produces is
    /// queued.
    ///
    /// `Command::Disconnect` needs to wait on the connection, which a
    /// synchronous handler cannot, so it is handed back to the run loop as
    /// [`Flow::Disconnect`].
    fn handle_command(&mut self, cmd: Command) -> Result<Flow> {
        // A (un)subscribe needs a packet identifier. With all 65 535 in use
        // it waits, like a publish, until an acknowledgement frees one.
        if matches!(cmd, Command::Subscribe { .. } | Command::Unsubscribe { .. }) && !self.id_free()
        {
            self.inflight_deferred.push_back(Deferred::Command(cmd));
            return Ok(Flow::Continue);
        }

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
                // QoS 1/2 publishes are deferred when the in-flight window is
                // full or no packet identifier is free. QoS 0 is never gated
                // (no ACK, no packet ID).
                if qos != QoS::QoS0 && !(self.inflight_slot_free() && self.id_free()) {
                    self.inflight_deferred
                        .push_back(Deferred::Command(Command::Publish {
                            topic,
                            payload,
                            qos,
                            retain,
                            props,
                            tx,
                        }));
                    return Ok(Flow::Continue);
                }

                // The server states the highest alias it will accept (0, the
                // default, means none at all); sending a larger one earns a
                // DISCONNECT, so fail this publish instead (§3.2.2.3.8).
                if let Some(alias) = props.get_u16(PropertyId::TopicAlias)
                    && (alias == 0 || alias > self.limits.server_topic_alias_max)
                {
                    tx.complete(Err(Error::ReasonCode(
                        ReasonCode::TopicAliasInvalid,
                        Properties::new(),
                    )));
                    return Ok(Flow::Continue);
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

                // A publish that cannot be encoded, or that exceeds the
                // server's maximum packet size, is this publish's failure,
                // not the connection's: fail its token and carry on.
                let end = match self.queue(&packet) {
                    Ok(end) => end,
                    Err(e) => {
                        tx.complete(Err(e));
                        return Ok(Flow::Continue);
                    }
                };

                // Persist a QoS 1/2 PUBLISH, exactly as queued, so it can be
                // re-sent after a crash.
                if let Some(pid) = id {
                    self.persist_last_frame(b's', pid);
                }

                let Packet::Publish(pub_pkt) = packet else {
                    unreachable!()
                };
                match id {
                    // QoS 0 is complete once its bytes are written and flushed.
                    None => self.out.complete_when_flushed(end, tx),
                    // QoS 1/2 is held, so it can be re-sent with DUP if the
                    // connection drops before it is acknowledged. It counts
                    // as sent: if the session ends before the bytes leave,
                    // re-sending it is what a resumed session does anyway.
                    Some(id) => {
                        self.state.insert(
                            id,
                            tx,
                            PublishPhase::AwaitingAck(Box::new(pub_pkt)),
                            true,
                        );
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
                self.queue(&Packet::Subscribe(sub))?;
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
                self.queue(&Packet::Unsubscribe(unsub))?;
                self.pending_unsub.insert(id, (tx, single));
            }

            // The streams belong to the client and outlive the connection;
            // the session is only lent them.
            Command::AddSink(sink) => self.sinks.add(sink),
            Command::ClearSinks => self.sinks.clear(),

            Command::Disconnect {
                timeout,
                reason_code,
                props,
                tx,
                connected_flag,
            } => {
                return Ok(Flow::Disconnect(DisconnectReq {
                    timeout,
                    reason_code,
                    props,
                    tx,
                    connected_flag,
                }));
            }
        }
        Ok(Flow::Continue)
    }
}

/////////////////////////////////////////////////////////////////////////////
// Writing

/// What one [`writer_step`] did.
enum Step {
    /// The transport accepted this many bytes.
    Wrote(usize),
    /// Everything written so far has been flushed.
    Flushed,
}

/// Makes one write of the outbox's unwritten bytes or, once they are all
/// written, one flush; pends forever when there is nothing to do.
///
/// Never both: if it flushed after writing and were cancelled during the
/// flush, the count of bytes just written would be lost. On TCP a flush is a
/// no-op that completes at once, so it costs one more pass of the loop, with
/// no syscall and no wakeup.
///
/// Takes the writer and the outbox as separate borrows rather than the whole
/// session, so that it can be polled in the same `select!` as a read.
async fn writer_step<B: Backend>(writer: &mut B::Writer, out: &Outbox) -> io::Result<Step> {
    if !out.is_empty() {
        let mut iov = [IoSlice::new(&[]); MAX_IOV];
        let n = out.io_slices(&mut iov);
        B::write_vectored(writer, &iov[..n]).await.map(Step::Wrote)
    } else if out.needs_flush() {
        B::flush(writer).await.map(|()| Step::Flushed)
    } else {
        pending().await
    }
}

/// Returns `Pending` once, having asked to be polled again, so the executor
/// can run other tasks first.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

/// Reads from the transport, or pends forever when `enabled` is false.
async fn read_when<B: Backend>(
    enabled: bool,
    reader: &mut B::Reader,
    buf: &mut BytesMut,
) -> io::Result<usize> {
    if enabled {
        B::read_buf(reader, buf).await
    } else {
        pending().await
    }
}

/// Completes once the dispatcher has room for another message, or pends
/// forever when `enabled` is false or the dispatcher wants no messages at
/// all. Holds nothing, so it is cancel-safe.
async fn room_when(enabled: bool, dispatch: &Dispatch) {
    if enabled && dispatch.wanted() {
        std::future::poll_fn(|cx| dispatch.gate.poll_room(cx)).await
    } else {
        pending().await
    }
}

/// How long the application may leave messages undelivered before the stall
/// is worth a warning when it ends. A consumer that pauses briefly is
/// ordinary; one that stops the client for seconds is what someone reading
/// the log is trying to find.
const STALL_WARN: Duration = Duration::from_secs(1);

/// How long a delivery made outside a session — the messages held when it
/// ended, and the connection events around it — waits for a full stream
/// before it is given up.
///
/// Inside a session a full stream simply stops the reads, and waits as long
/// as it likes. Between sessions there is nothing to stop, and waiting
/// indefinitely on a consumer that has stopped reading would wedge the loop
/// where it can no longer answer the API at all, so here the wait is
/// bounded and what will not fit is dropped with a warning.
const OFFLINE_DELIVER_WAIT: Duration = Duration::from_secs(5);

/// Awaits a parked fan-out outside a session, for at most
/// [`OFFLINE_DELIVER_WAIT`].
async fn deliver_offline<B: Backend>(parked: Option<Parked>) {
    let Some(parked) = parked else {
        return;
    };
    let mut timeout = B::Sleep::new(OFFLINE_DELIVER_WAIT);
    select! {
        _ = parked.fuse() => {}
        _ = timeout => warn!(
            "An application stream is still full after {} s; giving up on delivering to it",
            OFFLINE_DELIVER_WAIT.as_secs()
        ),
    }
}

/// Completes once a parked fan-out has finished, or pends forever when
/// there is none.
///
/// The future itself lives in the session, so a `select!` that drops this
/// branch mid-poll loses no progress and no message.
async fn deliver_when(parked: &mut Option<Parked>) {
    match parked {
        Some(fut) => fut.await,
        None => pending().await,
    }
}

/// Receives the next command, or pends forever when `enabled` is false.
async fn recv_when(
    enabled: bool,
    rx: &async_channel::Receiver<Command>,
) -> std::result::Result<Command, async_channel::RecvError> {
    if enabled {
        rx.recv().await
    } else {
        pending().await
    }
}

/// What the run loop should do after a command.
enum Flow {
    Continue,
    /// `disconnect()` was called; the run loop performs it, since it has to
    /// wait on the connection.
    Disconnect(DisconnectReq),
}

/// The parts of a [`Command::Disconnect`].
struct DisconnectReq {
    timeout: Duration,
    reason_code: ReasonCode,
    props: Properties,
    tx: Completer<()>,
    connected_flag: Arc<AtomicBool>,
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
    assigned_id: Arc<RwLock<Option<String>>>,
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
}

/// A transport with the MQTT CONNECT exchange completed.
struct Connected<B: Backend> {
    reader: B::Reader,
    writer: B::Writer,
    connack: ConnAck,
    /// Bytes that came in with the CONNACK and have not been parsed yet.
    ///
    /// A broker is free to pipeline packets behind the CONNACK — the queued
    /// messages of a session it has just resumed, say — and they can arrive
    /// in the same read. So the buffer travels on into the session instead
    /// of being dropped here with the connect: whatever is in it is handled
    /// before the session waits on another read.
    read_buf: BytesMut,
}

/// Opens persistence and the transport, sends CONNECT, waits for CONNACK.
async fn do_connect<B: Backend>(job: &ConnectJob) -> Result<Connected<B>> {
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
    let mut read_buf = BytesMut::with_capacity(READ_BUF_CAPACITY);
    let connack = recv_connack::<B>(&mut reader, &mut writer, &mut read_buf, p).await?;

    Ok(Connected {
        reader,
        writer,
        connack,
        read_buf,
    })
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
    sinks: &mut Sinks,
) -> Offline {
    match cmd {
        // A stream may be started before the first connect, and must still
        // be there for it.
        Command::AddSink(sink) => {
            sinks.add(sink);
            Offline::Handled
        }
        Command::ClearSinks => {
            sinks.clear();
            Offline::Handled
        }
        Command::Connect {
            params,
            tx,
            connected_flag,
            assigned_id,
            persist,
        } => Offline::Connect(
            Box::new(ConnectJob {
                params,
                server_idx: 0,
                connected_flag,
                assigned_id,
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
    Connected(Connected<B>),
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
    sinks: &mut Sinks,
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
                    Ok(conn) => Attempt::Connected(conn),
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
            Ok(cmd) => match handle_offline_command(cmd, buf, buf_cfg, should_buf, sinks) {
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
    sinks: &mut Sinks,
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
                match handle_offline_command(
                    cmd,
                    buf,
                    buf_cfg,
                    buf_cfg.send_while_disconnected,
                    sinks,
                ) {
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
    dispatch: Dispatch,
    buf_cfg: BufConfig,
) {
    // Buffer for Publish commands received while disconnected.
    let mut buf: VecDeque<Command> = VecDeque::new();
    // True once the first successful CONNACK has been received.
    let mut ever_connected = false;
    // The application's streams belong to the client, not to a session or a
    // connection: they are registered here, lent to each session in turn,
    // and taken back when it ends.
    let mut sinks = Sinks::default();

    'outer: loop {
        // Message state belongs to the session, not the connection, so it is
        // built here and handed from one connection to the next.
        let mut state = SessionState::default();

        // ---- Wait for a Connect command ----
        let (mut job, mut tx_opt) = loop {
            let cmd = match cmd_rx.recv().await {
                Ok(cmd) => cmd,
                Err(_) => return,
            };
            let should_buf =
                buf_cfg.allow_send_any_time || (ever_connected && buf_cfg.send_while_disconnected);
            match handle_offline_command(cmd, &mut buf, &buf_cfg, should_buf, &mut sinks) {
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
                    match attempt_connect::<B>(
                        &job, &cmd_rx, &mut buf, &buf_cfg, should_buf, &mut sinks,
                    )
                    .await
                    {
                        Attempt::Connected(c) if c.connack.reason_code.is_err() => {
                            last_err = Some(Error::ReasonCode(
                                c.connack.reason_code,
                                c.connack.properties,
                            ));
                        }
                        Attempt::Connected(c) => {
                            result = Some(c);
                            break;
                        }
                        Attempt::Failed(e) => last_err = Some(e),
                        Attempt::Aborted => continue 'outer,
                    }
                }
                match result {
                    Some(c) => Attempt::Connected(c),
                    None => {
                        Attempt::Failed(last_err.unwrap_or(Error::General("no servers configured")))
                    }
                }
            } else {
                match reconnect_wait::<B>(retry_delay, &cmd_rx, &mut buf, &buf_cfg, &mut sinks)
                    .await
                {
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
                match attempt_connect::<B>(
                    &job, &cmd_rx, &mut buf, &buf_cfg, should_buf, &mut sinks,
                )
                .await
                {
                    Attempt::Connected(c) if c.connack.reason_code.is_err() => Attempt::Failed(
                        Error::ReasonCode(c.connack.reason_code, c.connack.properties),
                    ),
                    other => other,
                }
            };

            let Connected {
                mut reader,
                writer,
                connack,
                read_buf,
            } = match attempt {
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
                Attempt::Connected(c) => c,
            };

            // ---- Connected ----
            let limits = SessionLimits::new(&job.params, &connack);
            let keep_alive = negotiated_keep_alive(&job.params, &connack);
            let resumed = session_resumed(&job.params, &connack);

            // A clean start throws away whatever the previous session left
            // behind: the server has already discarded its half, so the
            // store holds nothing that can still be completed.
            if job.params.clean_start
                && let Some(p) = &job.persist
            {
                let _ = p.lock().unwrap().clear();
                state = SessionState::default();
            }

            // Read a store written by an earlier run of the program into the
            // session state. Only the first connection needs to: from then
            // on the in-memory state is the authority and the store mirrors
            // it.
            if buf_cfg.restore_msgs && !state.loaded {
                load_persisted(&mut state, job.persist.as_ref(), job.params.ver);
            }
            state.loaded = true;

            // An assigned identifier replaces the empty one we sent, so that
            // `client_id()` reports it and a later reconnect resumes the same
            // session rather than asking for another new one.
            if let Some(id) = assigned_client_id(&connack) {
                *job.assigned_id.write().unwrap() = Some(id.clone());
                job.params.client_id = id;
            }

            let response = build_connect_response(&job, connack);
            // Set the flag before resolving the token so `is_connected()` is
            // true as soon as `wait()` / `.await` returns in the caller.
            job.connected_flag.store(true, Ordering::SeqCst);
            ever_connected = true;
            let _ = event_tx.send(Event::Connected(response.clone())).await;
            // Event streams see the connection come up before any of its
            // messages, because the session that delivers those is not
            // built until below.
            deliver_offline::<B>(sinks.deliver_event(crate::Event::Connected(response.clone())))
                .await;
            if let Some(tx) = tx_opt.take() {
                tx.complete(Ok(response));
            }

            let flush_needed = job.params.servers[job.server_idx].0.buffers_writes();
            let mut session = Session::<B>::new(
                writer,
                flush_needed,
                read_buf,
                Negotiated {
                    ver: job.params.ver,
                    keep_alive,
                    limits,
                },
                Delivery {
                    event_tx: event_tx.clone(),
                    dispatch: dispatch.clone(),
                    sinks,
                },
                job.persist.clone(),
                state,
            );
            let result = session.serve(&mut reader, &cmd_rx, &mut buf, resumed).await;
            // Whatever was received and held back is delivered, ahead of the
            // ConnectionLost below.
            let flushed = session.flush_held();
            if let Some(p) = &session.persist {
                let _ = p.lock().unwrap().close();
            }
            // Carry what is still in flight into the next connection of this
            // session; a clean disconnect below drops it.
            state = session.state;
            // Take the streams back for the next connection.
            sinks = session.sinks;
            deliver_offline::<B>(flushed).await;

            match result {
                // Clean disconnect: back to waiting for a Connect.
                Ok(()) => continue 'outer,
                Err(e) => {
                    job.connected_flag.store(false, Ordering::SeqCst);
                    let _ = event_tx.send(Event::ConnectionLost(e)).await;
                    // A message stream ends its connection with `None`, as
                    // the message callback is given; an event stream gets
                    // `ConnectionLost`. After the messages above, never
                    // before them.
                    deliver_offline::<B>(sinks.deliver_connection_lost()).await;
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

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Instant,
    };

    use bytes::Bytes;

    use super::{super::ServerAddr, super::sinks::Sink, *};
    use crate::Token;

    /// A timer on the wall clock. `never()` reports itself terminated, so
    /// `select!` skips its branch. The tests poll by hand with a no-op
    /// waker (see [`poll_until`]), so the timer need not wake anyone: it is
    /// simply found ready on a later poll.
    struct MockSleep {
        deadline: Option<Instant>,
    }

    impl Future for MockSleep {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            match self.deadline {
                Some(d) if Instant::now() >= d => {
                    self.deadline = None;
                    Poll::Ready(())
                }
                _ => Poll::Pending,
            }
        }
    }

    impl futures::future::FusedFuture for MockSleep {
        fn is_terminated(&self) -> bool {
            self.deadline.is_none()
        }
    }

    impl Sleep for MockSleep {
        fn new(dur: Duration) -> Self {
            Self {
                deadline: Some(Instant::now() + dur),
            }
        }
        fn never() -> Self {
            Self { deadline: None }
        }
        fn reset(&mut self, dur: Duration) {
            self.deadline = Some(Instant::now() + dur);
        }
    }

    /// Inbound bytes for a [`MockBackend`] session: each chunk is one read.
    /// Once they are gone, reads report EOF, or pend forever if `hold_open`.
    #[derive(Default)]
    struct MockReader {
        chunks: VecDeque<Vec<u8>>,
        hold_open: bool,
    }

    impl MockReader {
        fn eof() -> Self {
            Self::default()
        }

        fn open_with(chunks: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                chunks: chunks.into_iter().collect(),
                hold_open: true,
            }
        }
    }

    /// The transport a [`MockBackend`] session writes to, and how it
    /// behaves. Shared, so a test can change it while the session runs.
    #[derive(Default)]
    struct MockWire {
        /// Everything written, in order.
        bytes: Vec<u8>,
        /// Start address of every slice offered to a write.
        slice_ptrs: Vec<usize>,
        /// Most bytes accepted per write; 0 for no limit.
        max_per_write: usize,
        /// Every write pends this often (every n-th call); 0 for never.
        pending_every: usize,
        /// Writes pend forever, as on a socket whose peer reads nothing.
        stalled: bool,
        /// Flushes pend.
        hold_flush: bool,
        /// Writes fail once this many bytes have been accepted.
        fail_after: Option<usize>,
        write_calls: usize,
        flushes: usize,
    }

    #[derive(Clone, Default)]
    struct MockWriter(Arc<Mutex<MockWire>>);

    impl MockWriter {
        fn wire(&self) -> std::sync::MutexGuard<'_, MockWire> {
            self.0.lock().unwrap()
        }
    }

    /// Pends once, then completes.
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                Poll::Pending
            }
        }
    }

    /// A backend over in-memory transport halves; see [`MockReader`] and
    /// [`MockWire`].
    struct MockBackend;

    impl Backend for MockBackend {
        type Reader = MockReader;
        type Writer = MockWriter;
        type Sleep = MockSleep;

        async fn open(_: &ServerAddr, _: &ConnectParams) -> Result<(Self::Reader, Self::Writer)> {
            Err(Error::General("MockBackend opens no transports"))
        }

        async fn read_buf(reader: &mut Self::Reader, buf: &mut BytesMut) -> io::Result<usize> {
            match reader.chunks.pop_front() {
                Some(chunk) => {
                    buf.extend_from_slice(&chunk);
                    Ok(chunk.len())
                }
                None if reader.hold_open => pending().await,
                None => Ok(0),
            }
        }

        async fn write_all(writer: &mut Self::Writer, data: &[u8]) -> io::Result<()> {
            writer.wire().bytes.extend_from_slice(data);
            Ok(())
        }

        async fn write_vectored(
            writer: &mut Self::Writer,
            bufs: &[IoSlice<'_>],
        ) -> io::Result<usize> {
            let (stalled, yield_now) = {
                let mut w = writer.wire();
                w.write_calls += 1;
                (
                    w.stalled,
                    w.pending_every > 0 && w.write_calls % w.pending_every == 0,
                )
            };
            if stalled {
                return pending().await;
            }
            if yield_now {
                YieldOnce(false).await;
            }

            let mut w = writer.wire();
            let mut room = match w.max_per_write {
                0 => usize::MAX,
                n => n,
            };
            if let Some(limit) = w.fail_after {
                if w.bytes.len() >= limit {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                room = room.min(limit - w.bytes.len());
            }
            let mut n = 0;
            for b in bufs {
                w.slice_ptrs.push(b.as_ptr() as usize);
                let k = b.len().min(room - n);
                w.bytes.extend_from_slice(&b[..k]);
                n += k;
                if n == room {
                    break;
                }
            }
            Ok(n)
        }

        /// Checks `hold_flush` on every poll, as a real transport's
        /// `poll_flush` would find its buffer drained.
        async fn flush(writer: &mut Self::Writer) -> io::Result<()> {
            std::future::poll_fn(|_| {
                let mut w = writer.wire();
                if w.hold_flush {
                    return Poll::Pending;
                }
                w.flushes += 1;
                Poll::Ready(Ok(()))
            })
            .await
        }
    }

    /// Polls `fut` with a no-op waker until it completes or `limit` passes,
    /// yielding the thread between polls so wall-clock timers can expire.
    /// Every mock future is either ready or, until a test changes something,
    /// pending for good, so the polling loses no wakeups.
    fn poll_until<F: Future>(mut fut: Pin<&mut F>, limit: Duration) -> Option<F::Output> {
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        let end = Instant::now() + limit;
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return Some(v);
            }
            if Instant::now() >= end {
                return None;
            }
            std::thread::yield_now();
        }
    }

    /// Long enough for every ready step of a mock session to run.
    const SETTLE: Duration = Duration::from_millis(50);

    fn test_limits() -> SessionLimits {
        SessionLimits {
            max_inflight: MAX_PACKET_IDS,
            max_packet_size: paho_mqtt_codec::MAX_PACKET_SIZE,
            server_max_packet_size: None,
            server_topic_alias_max: 0,
            topic_alias_max: 0,
        }
    }

    /// A session over [`MockBackend`], with the far ends of its channels.
    struct Harness {
        session: Session<MockBackend>,
        wire: MockWriter,
        event_rx: async_channel::Receiver<Event>,
        gate: Arc<InboundGate>,
        dispatch: Dispatch,
        cmd_tx: async_channel::Sender<Command>,
        cmd_rx: async_channel::Receiver<Command>,
    }

    /// A session over [`MockBackend`]. The mock transport is treated as one
    /// that buffers until flushed, like TLS, unless `flush_needed` is false.
    /// `msg_cap` is how many messages may wait for the (simulated)
    /// dispatcher; see [`consume`].
    fn harness_with(
        read_buf: BytesMut,
        keep_alive: Duration,
        state: SessionState,
        flush_needed: bool,
        msg_cap: usize,
    ) -> Harness {
        let (event_tx, event_rx) = async_channel::unbounded();
        let (cmd_tx, cmd_rx) = async_channel::unbounded();
        let wire = MockWriter::default();
        let dispatch = Dispatch {
            gate: InboundGate::new(msg_cap),
            // A dispatcher that wants messages, as one with a callback or a
            // synchronous consumer does; the sink tests clear this.
            wants_messages: Arc::new(AtomicBool::new(true)),
        };
        let gate = dispatch.gate.clone();
        let session = Session::<MockBackend>::new(
            wire.clone(),
            flush_needed,
            read_buf,
            Negotiated {
                ver: MqttVersion::V5,
                keep_alive,
                limits: test_limits(),
            },
            Delivery {
                event_tx,
                dispatch: dispatch.clone(),
                sinks: Sinks::default(),
            },
            None,
            state,
        );
        Harness {
            session,
            wire,
            event_rx,
            gate,
            dispatch,
            cmd_tx,
            cmd_rx,
        }
    }

    fn harness(read_buf: BytesMut, keep_alive: Duration, state: SessionState) -> Harness {
        harness_with(read_buf, keep_alive, state, true, INBOUND_MSG_CAP)
    }

    /// Builds a session over [`MockBackend`] whose inbound buffer already
    /// holds `read_buf`, as one carried over from the CONNACK read does.
    fn session_with_buffered(read_buf: BytesMut) -> Harness {
        harness(read_buf, Duration::ZERO, SessionState::default())
    }

    fn publish_cmd(topic: &str, qos: QoS, payload: Bytes) -> (Token<PublishResponse>, Command) {
        let (tok, tx) = Token::new();
        let cmd = Command::Publish {
            topic: topic.into(),
            payload,
            qos,
            retain: false,
            props: Properties::new(),
            tx,
        };
        (tok, cmd)
    }

    fn subscribe_cmd(filter: &str) -> (Token<SubscribeResponse>, Command) {
        let (tok, tx) = Token::new();
        let cmd = Command::Subscribe {
            single: true,
            filters: vec![(filter.into(), QoS::QoS1, Default::default())],
            props: Properties::new(),
            tx,
        };
        (tok, cmd)
    }

    fn disconnect_cmd() -> (Token<()>, Command) {
        let (tok, tx) = Token::new();
        let cmd = Command::Disconnect {
            timeout: Duration::ZERO,
            reason_code: ReasonCode::Success,
            props: Properties::new(),
            tx,
            connected_flag: Arc::new(AtomicBool::new(true)),
        };
        (tok, cmd)
    }

    fn wire_publish(topic: &str, qos: QoS, id: Option<u16>, payload: &Bytes) -> Vec<u8> {
        let mut buf = BytesMut::new();
        Packet::Publish(paho_mqtt_codec::Publish {
            dup: false,
            qos,
            retain: false,
            topic: topic.into(),
            packet_id: id,
            payload: payload.clone(),
            properties: Properties::new(),
        })
        .encode(&mut buf, MqttVersion::V5)
        .unwrap();
        buf.to_vec()
    }

    fn wire_packet(pkt: Packet) -> Vec<u8> {
        let mut buf = BytesMut::new();
        pkt.encode(&mut buf, MqttVersion::V5).unwrap();
        buf.to_vec()
    }

    fn encoded_publish(topic: &str, qos: QoS, id: Option<u16>) -> BytesMut {
        let mut buf = BytesMut::new();
        let pkt = Packet::Publish(paho_mqtt_codec::Publish {
            dup: false,
            qos,
            retain: false,
            topic: topic.into(),
            packet_id: id,
            payload: Bytes::from_static(b"body"),
            properties: Properties::new(),
        });
        pkt.encode(&mut buf, MqttVersion::V5).unwrap();
        buf
    }

    /// A broker may pipeline packets behind the CONNACK, and they can arrive
    /// in the same read. Those bytes are carried into the session and taken
    /// before it waits on a read that may never come: here the transport is
    /// at EOF, so a message left in the buffer would be lost outright.
    #[test]
    fn test_packets_pipelined_with_the_connack_are_delivered() {
        let Harness {
            mut session,
            event_rx,
            cmd_rx,
            ..
        } = session_with_buffered(encoded_publish("pipelined/qos0", QoS::QoS0, None));

        let res = futures::executor::block_on(session.run(&mut MockReader::eof(), &cmd_rx));
        assert!(
            matches!(res, Err(Error::Disconnected)),
            "the mock transport is at EOF: {res:?}"
        );

        match event_rx.try_recv() {
            Ok(Event::MessageArrived(msg)) => assert_eq!(msg.topic, "pipelined/qos0"),
            other => panic!("pipelined PUBLISH was dropped: {:?}", other.is_ok()),
        }
    }

    /// Every packet in the carried-over buffer is handled, not just the
    /// first, and acknowledgements for them go out on the new connection.
    #[test]
    fn test_every_pipelined_packet_is_handled() {
        let mut buffered = encoded_publish("pipelined/a", QoS::QoS0, None);
        buffered.extend_from_slice(&encoded_publish("pipelined/b", QoS::QoS1, Some(7)));

        let Harness {
            mut session,
            wire,
            event_rx,
            cmd_rx,
            ..
        } = session_with_buffered(buffered);
        let _ = futures::executor::block_on(session.run(&mut MockReader::eof(), &cmd_rx));

        let topics: Vec<String> = std::iter::from_fn(|| event_rx.try_recv().ok())
            .filter_map(|ev| match ev {
                Event::MessageArrived(msg) => Some(msg.topic),
                _ => None,
            })
            .collect();
        assert_eq!(topics, vec!["pipelined/a", "pipelined/b"]);

        // The QoS 1 message was acknowledged: PUBACK for packet ID 7.
        assert_eq!(&wire.wire().bytes[..], &[0x40, 0x02, 0x00, 0x07]);
    }
    fn params(ver: MqttVersion, clean_start: bool) -> ConnectParams {
        ConnectParams {
            servers: Vec::new(),
            client_id: String::new(),
            clean_start,
            keep_alive_secs: 0,
            user_name: None,
            password: None,
            will: None,
            ver,
            props: Properties::new(),
            auth: None,
            #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
            tls: None,
            #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
            http_headers: Default::default(),
            #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
            http_proxy: String::new(),
            #[cfg(any(
                all(feature = "websocket", any(feature = "tls", feature = "openssl")),
                all(feature = "smol-websocket", feature = "smol-tls")
            ))]
            https_proxy: String::new(),
            auto_reconnect: false,
            min_retry_interval: Duration::ZERO,
            max_retry_interval: Duration::ZERO,
            max_inflight: 0,
            connect_timeout: Duration::ZERO,
        }
    }

    /// v3.1.1 and v5 are told by the CONNACK; v3.1 has no such flag, so the
    /// client's own request is all there is to go on.
    #[test]
    fn test_session_resumed() {
        let ack = |present| ConnAck::new(present, ReasonCode::Success);

        for ver in [MqttVersion::V3_1_1, MqttVersion::V5] {
            assert!(session_resumed(&params(ver, false), &ack(true)), "{ver}");
            assert!(!session_resumed(&params(ver, false), &ack(false)), "{ver}");
        }

        assert!(session_resumed(
            &params(MqttVersion::V3_1, false),
            &ack(false)
        ));
        assert!(!session_resumed(
            &params(MqttVersion::V3_1, true),
            &ack(false)
        ));
    }

    /// Re-transmission follows the order the messages were published, not
    /// the arbitrary order of the hash map that holds them.
    #[test]
    fn test_inflight_ids_in_publish_order() {
        let mut state = SessionState::default();
        for id in [9_u16, 3, 7] {
            let (_tok, comp) = Token::<PublishResponse>::new();
            state.insert(id, comp, PublishPhase::AwaitingPubComp, true);
        }
        assert_eq!(state.ids_in_order(), vec![9, 3, 7]);
    }

    /// Keys are built without allocating, and must match the format stores
    /// written by earlier versions hold.
    #[test]
    fn test_persist_key_format() {
        for id in [1u16, 9, 10, 42, 999, 1000, 65535] {
            assert_eq!(PersistKey::new(b's', id).as_str(), format!("s{id}"));
            assert_eq!(PersistKey::new(b'r', id).as_str(), format!("r{id}"));
        }
    }

    /// The in-flight window's occupancy is kept as a count, not recounted
    /// on every publish and ACK; it must stay in step through every change.
    #[test]
    fn test_sent_count_follows_every_change() {
        let mut state = SessionState::default();
        let phase = || PublishPhase::AwaitingPubComp;

        state.insert(1, Completer::orphan(), phase(), true);
        state.insert(2, Completer::orphan(), phase(), false);
        assert_eq!(state.sent_count(), 1);

        state.mark_sent(2);
        state.mark_sent(2); // already sent: no change
        assert_eq!(state.sent_count(), 2);

        state.mark_unsent(1);
        state.mark_unsent(1);
        assert_eq!(state.sent_count(), 1);

        // Replacing a record accounts for the one it replaces.
        state.insert(2, Completer::orphan(), phase(), false);
        assert_eq!(state.sent_count(), 0);

        state.mark_sent(1);
        assert!(state.remove(1).is_some());
        assert!(state.remove(1).is_none());
        assert!(state.remove(2).is_some());
        assert_eq!(state.sent_count(), 0);
    }

    /// Every outstanding exchange holds a 16-bit packet identifier, and there
    /// are only 65 535. With all of them in use, a QoS 1 publish and a
    /// subscribe wait for one to be freed, rather than searching forever, and
    /// go out in order as acknowledgements free them. QoS 0 needs none.
    #[test]
    fn test_operations_wait_for_a_free_packet_id() {
        // Identifiers 1..=65534 in flight, and 65535 held by a SUBSCRIBE.
        // The window is not full (65 534 < 65 535); the identifiers are.
        let mut state = SessionState::default();
        for id in 1..u16::MAX {
            state.insert(id, Completer::orphan(), PublishPhase::AwaitingPubComp, true);
        }
        let mut h = harness(BytesMut::new(), Duration::ZERO, state);
        let s = &mut h.session;
        let (_sub_tok, sub_tx) = Token::<SubscribeResponse>::new();
        s.pending_sub.insert(u16::MAX, (sub_tx, true));
        assert!(s.inflight_slot_free());
        assert!(!s.id_free());

        let (mut t1, c1) = publish_cmd("q1", QoS::QoS1, Bytes::from_static(b"1"));
        assert!(matches!(s.handle_command(c1), Ok(Flow::Continue)));
        let (_t2, c2) = subscribe_cmd("s/#");
        assert!(matches!(s.handle_command(c2), Ok(Flow::Continue)));
        assert_eq!(s.inflight_deferred.len(), 2);
        assert!(s.out.is_empty(), "nothing went out");

        let (_t0, c0) = publish_cmd("q0", QoS::QoS0, Bytes::from_static(b"0"));
        assert!(matches!(s.handle_command(c0), Ok(Flow::Continue)));
        assert!(!s.out.is_empty(), "QoS 0 needs no identifier");

        // The SUBACK frees 65535, which the publish at the front takes.
        s.handle_packet(Packet::SubAck(paho_mqtt_codec::SubAck::new(
            u16::MAX,
            vec![ReasonCode::Success],
        )))
        .unwrap();
        assert_eq!(s.inflight_deferred.len(), 1);
        assert!(s.state.inflight.contains_key(&u16::MAX));
        assert!(t1.try_wait().is_none(), "sent, awaiting its PUBACK");

        // A PUBCOMP frees 7, which the subscribe takes.
        s.handle_packet(Packet::PubComp(PubAck::new(7))).unwrap();
        assert!(s.inflight_deferred.is_empty());
        assert!(s.pending_sub.contains_key(&7));
    }

    // ---- The write pipeline ----

    /// Runs a session on `chunks` of inbound bytes, with `cmds` waiting in
    /// the command channel, until it ends (the last command is normally a
    /// disconnect). Returns what went out on the wire.
    fn run_to_end(h: &mut Harness, chunks: Vec<Vec<u8>>, cmds: Vec<Command>) -> Vec<u8> {
        for cmd in cmds {
            h.cmd_tx.try_send(cmd).unwrap();
        }
        let mut reader = MockReader::open_with(chunks);
        let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
        let res = poll_until(run, Duration::from_secs(5)).expect("session did not finish");
        assert!(res.is_ok(), "{res:?}");
        std::mem::take(&mut h.wire.wire().bytes)
    }

    /// Packets go out in the order they were queued, whether they came from
    /// commands or from handling inbound packets. Reads are taken before
    /// commands, so the PUBACK owed for the inbound message goes first.
    fn check_queue_order(configure: impl FnOnce(&mut MockWire)) {
        let mut h = session_with_buffered(BytesMut::new());
        configure(&mut h.wire.wire());

        let body = Bytes::from_static(b"payload");
        let inbound = wire_publish("in", QoS::QoS1, Some(7), &body);
        let (mut t0, c0) = publish_cmd("a", QoS::QoS0, body.clone());
        let (_t1, c1) = subscribe_cmd("s/#");
        let (_t2, c2) = publish_cmd("b", QoS::QoS1, body.clone());
        let (mut td, cd) = disconnect_cmd();

        let wire = run_to_end(&mut h, vec![inbound], vec![c0, c1, c2, cd]);

        let mut expect = wire_packet(Packet::PubAck(PubAck::new(7)));
        expect.extend(wire_publish("a", QoS::QoS0, None, &body));
        expect.extend(wire_packet(Packet::Subscribe(Subscribe {
            packet_id: 1,
            subscriptions: vec![Subscription {
                topic_filter: "s/#".into(),
                qos: QoS::QoS1,
                no_local: false,
                retain_as_published: false,
                retain_handling: CodecRetainHandling::SendOnSubscribe,
            }],
            properties: Properties::new(),
        })));
        expect.extend(wire_publish("b", QoS::QoS1, Some(2), &body));
        expect.extend(wire_packet(Packet::Disconnect(
            Disconnect::with_properties(ReasonCode::Success, Properties::new()),
        )));
        assert_eq!(wire, expect);

        assert!(matches!(t0.try_wait(), Some(Ok(_))));
        assert!(matches!(td.try_wait(), Some(Ok(()))));
    }

    #[test]
    fn test_packets_reach_the_wire_in_queue_order() {
        check_queue_order(|_| {});
    }

    /// The same, through a transport that takes 7 bytes per write and pends
    /// on every third: every frame is reassembled exactly.
    #[test]
    fn test_partial_and_pending_writes_reassemble() {
        check_queue_order(|w| {
            w.max_per_write = 7;
            w.pending_every = 3;
        });
    }

    /// A QoS 0 publish completes once its bytes are written *and* flushed:
    /// TLS and WebSocket transports hold written bytes until a flush.
    #[test]
    fn test_qos0_completes_only_once_flushed() {
        let mut h = session_with_buffered(BytesMut::new());
        h.wire.wire().hold_flush = true;

        let (mut tok, cmd) = publish_cmd("q0", QoS::QoS0, Bytes::from_static(b"x"));
        h.cmd_tx.try_send(cmd).unwrap();

        let mut reader = MockReader::open_with([]);
        {
            let mut run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run.as_mut(), SETTLE).is_none());
            assert!(!h.wire.wire().bytes.is_empty(), "the bytes were written");
            assert!(tok.try_wait().is_none(), "but not flushed");

            h.wire.wire().hold_flush = false;
            assert!(poll_until(run.as_mut(), SETTLE).is_none());
        }
        assert!(matches!(tok.try_wait(), Some(Ok(_))));
        assert!(h.wire.wire().flushes >= 1);
    }

    /// On a transport whose flush does nothing (plain TCP, Unix), a QoS 0
    /// publish completes as soon as it is written, and the writer makes no
    /// flush at all: that pass would only add latency.
    #[test]
    fn test_qos0_completes_on_write_without_flush() {
        let mut h = harness_with(
            BytesMut::new(),
            Duration::ZERO,
            SessionState::default(),
            false,
            INBOUND_MSG_CAP,
        );
        h.wire.wire().hold_flush = true; // would block any flush

        let (mut tok, cmd) = publish_cmd("q0", QoS::QoS0, Bytes::from_static(b"x"));
        h.cmd_tx.try_send(cmd).unwrap();

        let mut reader = MockReader::open_with([]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        assert!(matches!(tok.try_wait(), Some(Ok(_))));
        assert_eq!(h.wire.wire().flushes, 0);
    }

    /// A write error ends the session. QoS 0 messages still queued fail with
    /// `Disconnected`; a QoS 1 message stays in flight and is re-sent, with
    /// DUP set, when the session resumes.
    #[test]
    fn test_write_error_fails_qos0_and_keeps_qos1() {
        let body = Bytes::from_static(b"body");
        let first = wire_publish("a", QoS::QoS0, None, &body);

        let mut h = session_with_buffered(BytesMut::new());
        // The first message gets out; the write after it fails.
        h.wire.wire().fail_after = Some(first.len());
        h.wire.wire().max_per_write = first.len();

        let (mut t0, c0) = publish_cmd("a", QoS::QoS0, body.clone());
        let (mut t1, c1) = publish_cmd("b", QoS::QoS1, body.clone());
        let (mut t2, c2) = publish_cmd("c", QoS::QoS0, body.clone());
        for c in [c0, c1, c2] {
            h.cmd_tx.try_send(c).unwrap();
        }

        let mut reader = MockReader::open_with([]);
        let res = {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            poll_until(run, Duration::from_secs(5)).expect("session did not end")
        };
        assert!(matches!(res, Err(Error::Io(_))), "{res:?}");

        // The session is torn down; its state carries on.
        let Harness { mut session, .. } = h;
        let state = std::mem::take(&mut session.state);
        drop(session);
        // "a" was written but never flushed, so it is not known to be out.
        assert!(matches!(t0.try_wait(), Some(Err(Error::Disconnected))));
        assert!(matches!(t2.try_wait(), Some(Err(Error::Disconnected))));
        assert!(t1.try_wait().is_none(), "QoS 1 is still in flight");

        let mut h = harness(BytesMut::new(), Duration::ZERO, state);
        h.session.resume(true).unwrap();
        let (_td, cd) = disconnect_cmd();
        let wire = run_to_end(&mut h, vec![], vec![cd]);

        let mut dup = wire_publish("b", QoS::QoS1, Some(1), &body);
        dup[0] |= 0x08;
        assert!(wire.starts_with(&dup), "re-sent with DUP: {wire:02x?}");
    }

    /// A write that cannot complete does not stop reads.
    #[test]
    fn test_reads_continue_while_writer_blocked() {
        let mut h = session_with_buffered(BytesMut::new());
        h.wire.wire().stalled = true;

        let (_tok, cmd) = publish_cmd("out", QoS::QoS0, Bytes::from_static(b"x"));
        h.cmd_tx.try_send(cmd).unwrap();
        let inbound = wire_publish("in", QoS::QoS0, None, &Bytes::from_static(b"y"));

        let mut reader = MockReader::open_with([inbound]);
        let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
        assert!(poll_until(run, SETTLE).is_none());

        match h.event_rx.try_recv() {
            Ok(Event::MessageArrived(msg)) => assert_eq!(msg.topic, "in"),
            _ => panic!("the inbound message was not delivered"),
        }
    }

    /// With a stalled writer, the loop stops taking commands once
    /// `HIGH_WATER` bytes are queued; the rest wait in the channel.
    #[test]
    fn test_high_water_leaves_commands_in_channel() {
        let mut h = session_with_buffered(BytesMut::new());
        h.wire.wire().stalled = true;

        let body = Bytes::from(vec![0u8; 16 * 1024]);
        let n = 2 * HIGH_WATER / body.len();
        let mut toks = Vec::new();
        for i in 0..n {
            let (tok, cmd) = publish_cmd(&format!("t/{i}"), QoS::QoS0, body.clone());
            toks.push(tok);
            h.cmd_tx.try_send(cmd).unwrap();
        }

        let mut reader = MockReader::open_with([]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        let queued = h.session.out.len();
        assert!(queued >= HIGH_WATER, "{queued}");
        assert!(
            queued < HIGH_WATER + BATCH_BYTES + 2 * body.len(),
            "{queued}"
        );
        assert!(!h.cmd_rx.is_empty(), "commands were left in the channel");
    }

    /// A disconnect writes everything queued before it, then the DISCONNECT,
    /// before its token resolves.
    #[test]
    fn test_disconnect_drains_outbox_first() {
        let mut h = session_with_buffered(BytesMut::new());
        h.wire.wire().max_per_write = 5;

        let body = Bytes::from(vec![1u8; 100]);
        let (_t0, c0) = publish_cmd("a", QoS::QoS0, body.clone());
        let (mut td, cd) = disconnect_cmd();
        let wire = run_to_end(&mut h, vec![], vec![c0, cd]);

        let mut expect = wire_publish("a", QoS::QoS0, None, &body);
        expect.extend(wire_packet(Packet::Disconnect(Disconnect::new())));
        assert_eq!(wire, expect);
        assert!(matches!(td.try_wait(), Some(Ok(()))));
    }

    /// A transport that never takes the DISCONNECT does not hang the
    /// disconnect: it gives up after `CLOSE_CAP`.
    #[test]
    fn test_disconnect_bounded_when_writer_stalls() {
        let mut h = session_with_buffered(BytesMut::new());
        h.wire.wire().stalled = true;

        let (mut td, cd) = disconnect_cmd();
        h.cmd_tx.try_send(cd).unwrap();

        let start = Instant::now();
        let mut reader = MockReader::open_with([]);
        let res = {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            poll_until(run, CLOSE_CAP * 3).expect("disconnect hung")
        };
        assert!(res.is_ok());
        assert!(start.elapsed() >= CLOSE_CAP);
        assert!(matches!(td.try_wait(), Some(Ok(()))));
    }

    /// A large payload is written from the caller's buffer, not copied.
    #[test]
    fn test_large_payload_written_by_reference() {
        let mut h = session_with_buffered(BytesMut::new());
        let body = Bytes::from(vec![5u8; crate::runtime::outbox::INLINE_PAYLOAD_MAX + 1]);
        let (_t0, c0) = publish_cmd("big", QoS::QoS0, body.clone());
        let (_td, cd) = disconnect_cmd();
        run_to_end(&mut h, vec![], vec![c0, cd]);
        assert!(h.wire.wire().slice_ptrs.contains(&(body.as_ptr() as usize)));
    }

    /// On an idle connection a PINGREQ goes out after the keep-alive
    /// interval, and a missing PINGRESP ends the session.
    #[test]
    fn test_keep_alive_ping_and_timeout() {
        let ka = Duration::from_millis(30);
        let mut h = harness(BytesMut::new(), ka, SessionState::default());
        let mut reader = MockReader::open_with([]);

        let start = Instant::now();
        let res = {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            poll_until(run, Duration::from_secs(5)).expect("no keep-alive timeout")
        };
        assert!(matches!(res, Err(Error::Timeout)), "{res:?}");
        assert!(start.elapsed() >= 2 * ka);
        assert_eq!(h.wire.wire().bytes, wire_packet(Packet::PingReq));
    }

    // ---- Inbound backpressure ----

    /// Plays the event dispatcher: takes every message waiting for it,
    /// reporting each consumed, and returns their topics.
    fn consume(event_rx: &async_channel::Receiver<Event>, gate: &InboundGate) -> Vec<String> {
        let mut topics = Vec::new();
        while let Ok(ev) = event_rx.try_recv() {
            if let Event::MessageArrived(msg) = ev {
                gate.consumed();
                topics.push(msg.topic);
            }
        }
        topics
    }

    /// The PUBACK packet identifiers on the wire so far, in order.
    fn pubacks(wire: &MockWriter) -> Vec<u16> {
        let bytes = wire.wire().bytes.clone();
        let mut buf = BytesMut::from(&bytes[..]);
        let mut ids = Vec::new();
        while let Ok(Some(raw)) = RawPacket::decode(&mut buf) {
            if let Ok(Packet::PubAck(ack)) = Packet::from_raw(raw, MqttVersion::V5) {
                ids.push(ack.packet_id);
            }
        }
        ids
    }

    fn qos1_msgs(ids: impl IntoIterator<Item = u16>) -> Vec<u8> {
        let mut bytes = Vec::new();
        for id in ids {
            bytes.extend(wire_publish(
                &format!("m{id}"),
                QoS::QoS1,
                Some(id),
                &Bytes::from_static(b"x"),
            ));
        }
        bytes
    }

    fn gated(keep_alive: Duration, state: SessionState, msg_cap: usize) -> Harness {
        harness_with(BytesMut::new(), keep_alive, state, true, msg_cap)
    }

    /// Gives the session an async message stream, as a `get_stream` whose
    /// `AddSink` command the loop has already taken would have.
    fn add_stream(h: &mut Harness, cap: usize) -> async_channel::Receiver<Option<Message>> {
        let (tx, rx) = async_channel::bounded(cap);
        h.session.sinks.add(Sink::Messages(tx));
        rx
    }

    /// `get_stream` reaches the event loop as a command, which registers the
    /// stream for this connection and the ones after it.
    #[test]
    fn test_add_sink_command_registers_a_stream() {
        let mut h = gated(Duration::ZERO, SessionState::default(), 1024);
        let (tx, _rx) = async_channel::bounded::<Option<Message>>(4);
        h.cmd_tx
            .try_send(Command::AddSink(Sink::Messages(tx)))
            .unwrap();
        let mut reader = MockReader::open_with([]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        assert!(!h.session.sinks.is_empty(), "the stream was not registered");
    }

    /// The topics a stream has received so far.
    fn stream_topics(rx: &async_channel::Receiver<Option<Message>>) -> Vec<String> {
        let mut topics = Vec::new();
        while let Ok(Some(msg)) = rx.try_recv() {
            topics.push(msg.topic);
        }
        topics
    }

    /// With nothing registered on the dispatcher side, messages go straight
    /// into the application's stream and the event channel is not touched
    /// at all — the thread hop the sinks exist to remove.
    #[test]
    fn test_stream_is_fed_without_the_dispatcher() {
        let mut h = gated(Duration::ZERO, SessionState::default(), 1024);
        h.dispatch.wants_messages.store(false, Ordering::SeqCst);
        let rx = add_stream(&mut h, 16);
        let mut reader = MockReader::open_with([qos1_msgs(1..=3)]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        assert_eq!(stream_topics(&rx), ["m1", "m2", "m3"]);
        assert_eq!(h.event_rx.len(), 0, "nothing went to the dispatcher");
        assert_eq!(pubacks(&h.wire), vec![1, 2, 3]);
    }

    /// A stream and the dispatcher both receive every message: neither
    /// displaces the other, and each sees them in order.
    #[test]
    fn test_stream_and_dispatcher_both_receive() {
        let mut h = gated(Duration::ZERO, SessionState::default(), 1024);
        let rx = add_stream(&mut h, 16);
        let mut reader = MockReader::open_with([qos1_msgs(1..=2)]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        assert_eq!(stream_topics(&rx), ["m1", "m2"]);
        assert_eq!(consume(&h.event_rx, &h.gate), ["m1", "m2"]);
    }

    /// A full stream is not dropped into. The session parks the delivery,
    /// stops reading, and withholds the PUBACK until the application has
    /// taken the message; then everything goes through, in order.
    #[test]
    fn test_full_stream_parks_and_stops_reads() {
        let mut h = gated(Duration::ZERO, SessionState::default(), 1024);
        h.dispatch.wants_messages.store(false, Ordering::SeqCst);
        let rx = add_stream(&mut h, 1);
        let mut reader = MockReader::open_with([qos1_msgs(1..=3), qos1_msgs(4..=4)]);
        let mut topics = Vec::new();
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        // One message fits; the second is parked on the full stream and is
        // not acknowledged, and the last read was not made.
        assert_eq!(pubacks(&h.wire), vec![1], "acknowledged what was taken");
        assert_eq!(reader.chunks.len(), 1, "the next read was not made");
        assert!(h.session.pending.is_some(), "the delivery is parked");

        {
            let mut run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            // The application drains, a message at a time.
            for _ in 0..10 {
                topics.extend(stream_topics(&rx));
                assert!(poll_until(run.as_mut(), SETTLE).is_none());
            }
        }
        topics.extend(stream_topics(&rx));
        assert_eq!(topics, ["m1", "m2", "m3", "m4"]);
        assert_eq!(pubacks(&h.wire), vec![1, 2, 3, 4]);
        assert!(reader.chunks.is_empty(), "reading resumed");
    }

    /// A stream whose receiver has been dropped is forgotten, and the rest
    /// of the session carries on as though it had never been there.
    #[test]
    fn test_dropped_stream_stops_holding_the_session() {
        let mut h = gated(Duration::ZERO, SessionState::default(), 1024);
        h.dispatch.wants_messages.store(false, Ordering::SeqCst);
        let rx = add_stream(&mut h, 1);
        drop(rx);
        let mut reader = MockReader::open_with([qos1_msgs(1..=3)]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        assert_eq!(pubacks(&h.wire), vec![1, 2, 3], "nothing held anything up");
        assert!(h.session.sinks.is_empty(), "the dead stream was forgotten");
    }

    /// A consumer slower than the broker: once the dispatcher is full the
    /// session holds the next message back and stops reading, so TCP pushes
    /// back on the broker rather than memory growing. Each QoS 1 message is
    /// acknowledged only once handed over, and none is lost or reordered.
    #[test]
    fn test_slow_consumer_pauses_reads() {
        let mut h = gated(Duration::ZERO, SessionState::default(), 2);
        let mut reader = MockReader::open_with([qos1_msgs(1..=4), qos1_msgs(5..=5)]);
        let mut topics = Vec::new();
        {
            let mut run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run.as_mut(), SETTLE).is_none());

            // Two handed over and acknowledged; the third held, unacknowledged.
            assert_eq!(h.event_rx.len(), 2);
            assert_eq!(pubacks(&h.wire), vec![1, 2]);

            // The consumer catches up, a little at a time.
            for _ in 0..10 {
                topics.extend(consume(&h.event_rx, &h.gate));
                assert!(poll_until(run.as_mut(), SETTLE).is_none());
            }
        }
        assert_eq!(topics, ["m1", "m2", "m3", "m4", "m5"]);
        assert_eq!(pubacks(&h.wire), vec![1, 2, 3, 4, 5]);
        assert!(reader.chunks.is_empty());
    }

    /// Reading stops while messages are held back...
    #[test]
    fn test_reads_stop_while_held() {
        let mut h = gated(Duration::ZERO, SessionState::default(), 1);
        let mut reader = MockReader::open_with([qos1_msgs(1..=2), qos1_msgs(3..=3)]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        assert_eq!(h.session.held.len(), 1);
        assert_eq!(reader.chunks.len(), 1, "the next read was not made");
    }

    /// ...except while one of our own operations awaits an acknowledgement,
    /// which may be behind the backlog: a consumer waiting on it would never
    /// make room. Reading goes on, the acknowledgement is processed, and the
    /// held QoS 1 messages stay unacknowledged until handed over.
    #[test]
    fn test_reads_continue_while_own_ack_awaited() {
        let mut state = SessionState::default();
        let (mut tok, tx) = Token::<PublishResponse>::new();
        state.insert(9, tx, PublishPhase::AwaitingPubComp, true);
        let mut h = gated(Duration::ZERO, state, 1);

        let mut chunk = qos1_msgs(1..=3);
        chunk.extend(wire_packet(Packet::PubComp(PubAck::new(9))));
        let mut reader = MockReader::open_with([chunk]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        assert!(matches!(tok.try_wait(), Some(Ok(_))), "no deadlock");
        assert_eq!(h.session.held.len(), 2);
        assert_eq!(
            pubacks(&h.wire),
            vec![1],
            "held messages are not acknowledged"
        );
    }

    /// While reading is paused a PINGRESP cannot be seen, so its absence is
    /// not taken for a dead connection; PINGREQs keep going out, so the
    /// broker does not drop us either.
    #[test]
    fn test_keep_alive_continues_while_reads_paused() {
        let ka = Duration::from_millis(30);
        let mut h = gated(ka, SessionState::default(), 1);
        let mut reader = MockReader::open_with([qos1_msgs(1..=2)]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, ka * 5).is_none(), "the session ended");
        }
        let pings = h
            .wire
            .wire()
            .bytes
            .windows(2)
            .filter(|w| w == &[0xC0, 0x00])
            .count();
        assert!(pings >= 2, "{pings} PINGREQs");
    }

    /// Held messages are not lost when the session ends: they are handed to
    /// the dispatcher, in order, ahead of the ConnectionLost that follows.
    #[test]
    fn test_held_messages_flushed_at_session_end() {
        let mut h = gated(Duration::ZERO, SessionState::default(), 1);
        let mut reader = MockReader::open_with([qos1_msgs(1..=3)]);
        {
            let run = pin!(h.session.run(&mut reader, &h.cmd_rx));
            assert!(poll_until(run, SETTLE).is_none());
        }
        assert_eq!(h.session.held.len(), 1);
        h.session.flush_held();
        let topics: Vec<_> = std::iter::from_fn(|| h.event_rx.try_recv().ok())
            .filter_map(|ev| match ev {
                Event::MessageArrived(m) => Some(m.topic),
                _ => None,
            })
            .collect();
        assert_eq!(topics, ["m1", "m2"]);
    }
}
