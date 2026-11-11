// paho-mqtt/src/runtime/commands.rs
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

//! Commands and events for the internal async runtime.
//!
//! [`Command`] values flow from the public API to the event loop.
//! [`Event`] values flow back from the event loop to the public layer.

use std::{
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use paho_mqtt_codec::{
    ConnAck, MAX_PACKET_SIZE, MqttVersion, Properties as CodecProperties, PropertyId, QoS,
    ReasonCode, Will,
};

use super::sinks::Sink;
use crate::{
    AuthHandler, ClientPersistence, ConnectResponse, ConnectionState, CreateOptions, Error,
    Message, PublishResponse, SubscribeOptions, SubscribeResponse, UnsubscribeResponse,
    token::Completer,
};

#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
use std::collections::HashMap;

/////////////////////////////////////////////////////////////////////////////

/// The client's [`ConnectionState`], as the event loop sets it.
///
/// Created with the event loop (`RuntimeHandle::new`) and held only by it and
/// the session it is running, so the loop is the one writer. That is the
/// point: the `connected` flag this replaced was also cleared by the
/// dispatcher thread when it got round to a `ConnectionLost`, by which time
/// an automatic reconnect could already have set it again — leaving a
/// connected client reporting itself disconnected. The API reads the state
/// through a [`StateView`], which cannot write.
///
/// Every change is made before the token or event that reveals it.
///
/// The cell also holds the URI of the server the client is connected to, for
/// a client given a list of servers. It is written under its lock together
/// with the move to [`Connected`](ConnectionState::Connected), so a reader
/// holding the lock that sees `Connected` sees that connection's URI.
#[derive(Clone)]
pub(crate) struct StateCell(Arc<Shared>);

/// What a [`StateCell`] and its [`StateView`]s share.
struct Shared {
    state: AtomicU8,
    server_uri: RwLock<Option<String>>,
}

impl StateCell {
    /// A new cell, [`Disconnected`](ConnectionState::Disconnected).
    pub(crate) fn new() -> Self {
        Self(Arc::new(Shared {
            state: AtomicU8::new(ConnectionState::Disconnected.as_u8()),
            server_uri: RwLock::new(None),
        }))
    }

    /// Records the state the event loop has moved to.
    ///
    /// A connection is recorded with [`set_connected`](Self::set_connected),
    /// which also says to which server.
    pub(crate) fn set(&self, state: ConnectionState) {
        self.0.state.store(state.as_u8(), Ordering::SeqCst);
    }

    /// Records a connection to the server at `server_uri`.
    pub(crate) fn set_connected(&self, server_uri: &str) {
        let mut uri = self.0.server_uri.write().unwrap();
        *uri = Some(server_uri.to_string());
        self.set(ConnectionState::Connected);
    }

    /// A read-only handle on the same state, for the API.
    pub(crate) fn view(&self) -> StateView {
        StateView(Arc::clone(&self.0))
    }
}

/// A read-only handle on the client's [`ConnectionState`]; see [`StateCell`].
pub(crate) struct StateView(Arc<Shared>);

impl StateView {
    /// The current state.
    pub(crate) fn get(&self) -> ConnectionState {
        ConnectionState::from_u8(self.0.state.load(Ordering::SeqCst))
    }

    /// The URI of the server the client is connected to, or `None` when it
    /// is not [`Connected`](ConnectionState::Connected).
    ///
    /// The URI is left in place when the connection ends; it is the state,
    /// read under the lock that [`StateCell::set_connected`] writes both
    /// under, that says whether it is still current.
    pub(crate) fn server_uri(&self) -> Option<String> {
        let uri = self.0.server_uri.read().unwrap();
        (self.get() == ConnectionState::Connected)
            .then(|| uri.clone())
            .flatten()
    }
}

/////////////////////////////////////////////////////////////////////////////

/// The [`CreateOptions`] that govern the client while it has no session:
/// the send-while-disconnected buffer, and how the persistence store is
/// read back and waited on.
///
/// Passed to the event loop at spawn time; fixed for the life of the client.
///
/// [`CreateOptions`]: crate::CreateOptions
#[derive(Clone, Copy, Default)]
pub(crate) struct OfflineConfig {
    /// Buffer publish commands when the client is disconnected after a prior
    /// successful connection.
    pub send_while_disconnected: bool,
    /// Maximum number of commands that may be held in the buffer at once.
    pub max_buffered_msgs: usize,
    /// See [`CreateOptions::max_held_qos0_msgs`](crate::CreateOptions).
    pub max_held_qos0_msgs: usize,
    /// See [`CreateOptions::max_held_qos0_bytes`](crate::CreateOptions).
    pub max_held_qos0_bytes: usize,
    /// Buffer publishes even before the first successful connection has been
    /// made.
    pub allow_send_any_time: bool,
    /// When the buffer is full, evict the oldest entry rather than the newest.
    pub delete_oldest: bool,
    /// On the first connect, load the session state a previous run left in
    /// the store: outbound QoS 1/2 messages to re-send with DUP=true, and
    /// inbound QoS 2 state.
    pub restore_msgs: bool,
    /// Buffer QoS 0 publishes in the send-while-disconnected buffer.
    ///
    /// When `false`, QoS 0 publishes received while disconnected are completed
    /// immediately with `Ok` (silently dropped) rather than queued.
    pub persist_qos0: bool,
    /// How long a session waits for a blocking persistence store before
    /// ending the connection; zero means forever. See
    /// [`CreateOptions::persist_timeout`](crate::CreateOptions::persist_timeout).
    pub persist_timeout: Duration,
}

impl OfflineConfig {
    /// Whether a publish made while there is no session is buffered, given
    /// whether this client has ever been connected.
    pub fn buffers(&self, ever_connected: bool) -> bool {
        self.allow_send_any_time || (ever_connected && self.send_while_disconnected)
    }
}

impl From<&CreateOptions> for OfflineConfig {
    fn from(opts: &CreateOptions) -> Self {
        Self {
            send_while_disconnected: opts.send_while_disconnected,
            max_buffered_msgs: opts.max_buffered_msgs,
            max_held_qos0_msgs: opts.max_held_qos0_msgs,
            max_held_qos0_bytes: opts.max_held_qos0_bytes,
            allow_send_any_time: opts.allow_send_any_time,
            delete_oldest: opts.delete_oldest,
            restore_msgs: opts.restore_msgs,
            persist_qos0: opts.persist_qos0,
            persist_timeout: opts.persist_timeout,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Splits the authority of a URI — `host`, `host:port`, `[v6]` or
/// `[v6]:port` — into its host and port, using `default_port` when it
/// gives none.
///
/// An IPv6 address must be in brackets (RFC 3986 §3.2.2), since its own
/// colons would otherwise be taken for the port's; the host is returned
/// without them, which is the form a socket address or a TLS server name
/// wants. `host_port` puts them back to write the two out together.
pub(crate) fn split_host_port(authority: &str, default_port: u16) -> crate::Result<(String, u16)> {
    let bad = |what: &str| Error::InvalidArgument(format!("{what} in address: {authority}"));
    let port = |p: &str| p.parse::<u16>().map_err(|_| bad("invalid port"));

    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest.split_once(']').ok_or_else(|| bad("unclosed '['"))?;
        let port = match after {
            "" => default_port,
            _ => port(after.strip_prefix(':').ok_or_else(|| bad("invalid port"))?)?,
        };
        (host, port)
    } else {
        match authority.split_once(':') {
            None => (authority, default_port),
            Some((_, rest)) if rest.contains(':') => {
                return Err(bad("an IPv6 address must be in brackets"));
            }
            Some((host, p)) => (host, port(p)?),
        }
    };
    if host.is_empty() {
        return Err(bad("no host"));
    }
    Ok((host.to_string(), port))
}

/// Writes `host` and `port` as the authority of a URL or an HTTP request,
/// putting an IPv6 address in brackets: the inverse of [`split_host_port`].
/// Only WebSocket and proxy connections write one.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
pub(crate) fn host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// The resolved transport address for a connection attempt.
///
/// Hosts are held without the brackets an IPv6 address has in a URI; see
/// [`split_host_port`].
pub(crate) enum ServerAddr {
    /// Plain (unencrypted) TCP connection.
    Tcp {
        /// Broker hostname or IP address.
        host: String,
        /// Broker TCP port.
        port: u16,
    },
    /// TLS connection (requires the `tls`, `openssl`, or `smol-tls` feature).
    #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
    Tls {
        /// Broker hostname or IP address.
        host: String,
        /// Broker TCP port.
        port: u16,
    },
    /// Unix-domain socket connection (Unix/Linux/macOS only).
    #[cfg(unix)]
    Unix {
        /// Filesystem path to the socket.
        path: String,
    },
    /// Plain (unencrypted) WebSocket connection.
    ///
    /// Requires the `websocket` or `smol-websocket` feature.
    #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
    Ws {
        /// Broker hostname or IP address.
        host: String,
        /// Broker TCP port.
        port: u16,
        /// WebSocket path (e.g. `/mqtt`).
        path: String,
    },
    /// WebSocket over TLS connection.
    ///
    /// Requires `websocket` + (`tls` or `openssl`), or `smol-websocket` + `smol-tls`.
    #[cfg(any(
        all(feature = "websocket", any(feature = "tls", feature = "openssl")),
        all(feature = "smol-websocket", feature = "smol-tls")
    ))]
    Wss {
        /// Broker hostname or IP address.
        host: String,
        /// Broker TCP port.
        port: u16,
        /// WebSocket path (e.g. `/mqtt`).
        path: String,
    },
}

impl ServerAddr {
    /// Whether the transport holds written bytes until it is flushed.
    ///
    /// TLS gathers them into records and WebSocket into frames, so for those
    /// a write is not delivery until a flush completes. A plain TCP or Unix
    /// socket hands them to the kernel on the write itself, and its flush
    /// does nothing.
    pub fn buffers_writes(&self) -> bool {
        !matches!(self, Self::Tcp { .. }) && !self.is_unix()
    }

    #[cfg(unix)]
    fn is_unix(&self) -> bool {
        matches!(self, Self::Unix { .. })
    }

    #[cfg(not(unix))]
    fn is_unix(&self) -> bool {
        false
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Connection parameters grouped for reuse across reconnect attempts.
///
/// This is verified version of the user's [`ConnectOptions`] with some
/// additional and converted fields for internal use.
///
/// [`ConnectOptions`]: crate::ConnectOptions
pub(crate) struct ConnectParams {
    /// Ordered list of `(transport address, URI string)` pairs.
    ///
    /// The event loop tries them left-to-right on the initial connect and
    /// cycles round-robin on each auto-reconnect attempt.
    pub servers: Vec<(ServerAddr, String)>,
    /// The server URI the persistence store is opened with: the one the
    /// client was created with, whichever of `servers` a connection is
    /// made to. The session — and so its stored messages — goes with the
    /// client from one server to the next, so the store must too.
    pub persist_uri: String,
    /// Client identifier.
    pub client_id: String,
    /// Whether to request a clean session / discard existing state.
    pub clean_start: bool,
    /// Keep-alive interval in seconds (0 = disabled).
    pub keep_alive_secs: u16,
    /// Optional username for authentication.
    pub user_name: Option<String>,
    /// Optional password for authentication.
    pub password: Option<Bytes>,
    /// Optional Last Will and Testament message.
    pub will: Option<Will>,
    /// MQTT protocol version to use.
    pub ver: MqttVersion,
    /// MQTT v5 CONNECT properties; empty for v3.1.1.
    pub props: CodecProperties,
    /// Handler for the v5 enhanced-authentication exchange, when the connect
    /// properties name an `AuthenticationMethod`.
    pub auth: Option<AuthHandler>,
    /// TLS options; `Some` only when using a TLS transport.
    #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
    pub tls: Option<crate::TlsOptions>,
    /// HTTP headers included in the WebSocket upgrade request.
    #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
    pub http_headers: HashMap<String, String>,
    /// Proxy to tunnel a `ws://` connection through; empty for a direct one.
    #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
    pub http_proxy: String,
    /// Proxy to tunnel a `wss://` connection through; empty for a direct one.
    #[cfg(any(
        all(feature = "websocket", any(feature = "tls", feature = "openssl")),
        all(feature = "smol-websocket", feature = "smol-tls")
    ))]
    pub https_proxy: String,
    /// Whether the event loop should automatically reconnect on session loss.
    pub auto_reconnect: bool,
    /// Minimum backoff interval between reconnect attempts.
    pub min_retry_interval: Duration,
    /// Maximum backoff interval between reconnect attempts.
    pub max_retry_interval: Duration,
    /// Maximum concurrent outbound QoS 1/2 publishes; 0 means as many as the
    /// protocol allows (see [`MAX_PACKET_IDS`]).
    pub max_inflight: usize,
    /// Deadline for one connection attempt — transport connect, TLS/WebSocket
    /// handshake, CONNECT and CONNACK together. Zero disables the deadline.
    pub connect_timeout: Duration,
}

impl ConnectParams {
    /// Converts `keep_alive_secs` to a [`Duration`].
    ///
    /// Returns [`Duration::ZERO`] when keep-alive is disabled (`keep_alive_secs == 0`).
    pub fn keep_alive_duration(&self) -> Duration {
        Duration::from_secs(self.keep_alive_secs as u64)
    }

    /// Largest inbound packet the client will accept, in bytes.
    ///
    /// This is the v5 `MaximumPacketSize` CONNECT property when the caller
    /// set one; otherwise the protocol maximum. The broker is required not to
    /// exceed an advertised limit, so anything larger is rejected at the
    /// framing layer before its body is buffered.
    pub fn max_packet_size(&self) -> u32 {
        self.props
            .get_u32(PropertyId::MaximumPacketSize)
            .filter(|n| *n > 0)
            .unwrap_or(MAX_PACKET_SIZE)
    }
}

/// How many packet identifiers there are: 16 bits, with 0 reserved (§2.2.1
/// in v3.1.1 and v5). Every QoS 1/2 publish, SUBSCRIBE and UNSUBSCRIBE holds
/// one until it completes, so this bounds what one session can have
/// outstanding.
pub(crate) const MAX_PACKET_IDS: usize = u16::MAX as usize;

/// Per-session limits, fixed once the CONNACK has arrived.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SessionLimits {
    /// Effective cap on concurrent outbound QoS 1/2 publishes: the lesser of
    /// the configured `max_inflight` and the server's Receive Maximum. Always
    /// in `1..=MAX_PACKET_IDS`.
    pub max_inflight: usize,
    /// Largest inbound packet accepted by the framing layer, in bytes.
    pub max_packet_size: u32,
    /// Largest packet the server said it will accept (CONNACK
    /// `MaximumPacketSize`), if any. Outbound packets above it are refused.
    pub server_max_packet_size: Option<u32>,
    /// Highest topic alias the *server* accepts (CONNACK `TopicAliasMaximum`,
    /// 0 when absent — aliases not permitted). Outbound publishes above it
    /// are refused before they reach the wire.
    pub server_topic_alias_max: u16,
    /// Highest topic alias *we* accept, as advertised in our own CONNECT
    /// properties (0 when we did not offer any). An inbound alias above it
    /// is a protocol error by the server.
    pub topic_alias_max: u16,
    /// Highest QoS the server accepts on a PUBLISH (CONNACK `MaximumQoS`,
    /// QoS 2 when absent, as always in v3) [MQTT-3.2.2-11].
    pub server_max_qos: QoS,
    /// Whether the server accepts retained messages (CONNACK
    /// `RetainAvailable`, `true` when absent) [MQTT-3.2.2-14].
    pub retain_available: bool,
}

impl SessionLimits {
    /// Limits for a session on `params` whose CONNACK was `connack`.
    pub fn new(params: &ConnectParams, connack: &ConnAck) -> Self {
        Self {
            max_inflight: effective_max_inflight(params, connack),
            max_packet_size: params.max_packet_size(),
            server_max_packet_size: server_max_packet_size(connack),
            server_topic_alias_max: u16_prop(&connack.properties, PropertyId::TopicAliasMaximum)
                .unwrap_or(0),
            topic_alias_max: u16_prop(&params.props, PropertyId::TopicAliasMaximum).unwrap_or(0),
            server_max_qos: connack
                .properties
                .get_byte(PropertyId::MaximumQoS)
                .and_then(|b| QoS::try_from(b).ok())
                .unwrap_or(QoS::QoS2),
            retain_available: connack
                .properties
                .get_byte(PropertyId::RetainAvailable)
                .is_none_or(|b| b != 0),
        }
    }

    /// Why the server would refuse a PUBLISH at `qos`, with `retain`, if it
    /// would: a QoS above its Maximum QoS, or RETAIN when it said it has no
    /// retained messages. Either earns a DISCONNECT (0x9B, 0x9A), so the
    /// client must not send it (v5 §3.2.2.3.4, §3.2.2.3.5).
    pub fn publish_refusal(&self, qos: QoS, retain: bool) -> Option<ReasonCode> {
        if qos > self.server_max_qos {
            Some(ReasonCode::QosNotSupported)
        } else if retain && !self.retain_available {
            Some(ReasonCode::RetainNotSupported)
        } else {
            None
        }
    }
}

/// Reads a two-byte property, if present with the right type.
fn u16_prop(props: &CodecProperties, id: PropertyId) -> Option<u16> {
    props.get_u16(id)
}

/// The number of QoS 1/2 publishes that may be in flight at once.
///
/// MQTT v5 §3.2.2.3.3: the server's Receive Maximum is how many unacknowledged
/// QoS 1/2 PUBLISH packets it will accept, defaulting to 65 535 when the
/// property is absent; exceeding it earns a DISCONNECT with
/// `ReceiveMaximumExceeded`. The client's own `max_inflight` narrows that
/// further. v3 has no such property, so the configured value stands alone.
///
/// Either way the result is in `1..=MAX_PACKET_IDS`: each exchange in flight
/// holds a packet identifier, so no configuration can allow more than there
/// are identifiers, and 0 ("as many as possible") means exactly that many.
fn effective_max_inflight(params: &ConnectParams, connack: &ConnAck) -> usize {
    let configured = match params.max_inflight {
        0 => MAX_PACKET_IDS,
        n => n.min(MAX_PACKET_IDS),
    };
    if !params.ver.is_v5() {
        return configured;
    }
    let server = u16_prop(&connack.properties, PropertyId::ReceiveMaximum)
        .filter(|&n| n > 0)
        .unwrap_or(u16::MAX) as usize;
    configured.min(server)
}

/// The keep-alive the session must use.
///
/// MQTT v5 §3.2.2.3.2: a Server Keep Alive in the CONNACK replaces whatever
/// the client asked for.
pub(crate) fn negotiated_keep_alive(params: &ConnectParams, connack: &ConnAck) -> Duration {
    match u16_prop(&connack.properties, PropertyId::ServerKeepAlive) {
        Some(secs) if params.ver.is_v5() => Duration::from_secs(secs as u64),
        _ => params.keep_alive_duration(),
    }
}

/// How long the session outlives its network connection, which decides
/// whether the client keeps its session state when a connection ends.
///
/// MQTT v3.x has no interval: a session started with Clean Session = 1
/// *"lasts as long as the Network Connection"* and its state *"MUST NOT be
/// reused in any subsequent Session"*; any other is kept (v3.1.1
/// §3.1.2.4). MQTT v5 replaced the flag's second half with the Session
/// Expiry Interval: 0, the default, means *"the Session ends when the
/// Network Connection is closed"* (§3.1.2.11.2). The interval the client
/// asks for in its CONNECT can be replaced by the server's in the CONNACK
/// (§3.2.2.3.2), and by the client's own in its DISCONNECT (§3.14.2.2.2).
/// A DISCONNECT may not raise it from a CONNECT's 0, which `disconnect()`
/// refuses, so any it carries is taken as it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SessionExpiry {
    /// The interval in force, in seconds; `u32::MAX` never expires.
    secs: u32,
}

impl SessionExpiry {
    /// The interval agreed by the CONNECT and its CONNACK.
    pub(crate) fn negotiated(params: &ConnectParams, connack: &ConnAck) -> Self {
        if !params.ver.is_v5() {
            let secs = if params.clean_start { 0 } else { u32::MAX };
            return Self { secs };
        }
        let asked = params
            .props
            .get_u32(PropertyId::SessionExpiryInterval)
            .unwrap_or(0);
        let secs = connack
            .properties
            .get_u32(PropertyId::SessionExpiryInterval)
            .unwrap_or(asked);
        Self { secs }
    }

    /// Takes in the properties of a DISCONNECT the client has sent.
    pub(crate) fn on_disconnect(&mut self, props: &CodecProperties) {
        if let Some(secs) = props.get_u32(PropertyId::SessionExpiryInterval) {
            self.secs = secs;
        }
    }

    /// Whether the session ends with the connection, its state with it.
    pub(crate) fn ends_with_connection(&self) -> bool {
        self.secs == 0
    }
}

/// The client identifier the server assigned, when the client sent an empty
/// one (MQTT v5 §3.2.2.3.7).
pub(crate) fn assigned_client_id(connack: &ConnAck) -> Option<String> {
    connack
        .properties
        .get_str(PropertyId::AssignedClientIdentifier)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// The largest packet the server will accept, from its CONNACK's v5
/// `MaximumPacketSize` property; `None` if it did not advertise one (or the
/// connection is v3), meaning the protocol maximum.
///
/// A client must not send a packet larger than this (MQTT-3.2.2-15); a
/// server that receives one disconnects with `PacketTooLarge`, so the
/// session refuses such a packet locally instead.
pub(crate) fn server_max_packet_size(connack: &ConnAck) -> Option<u32> {
    connack
        .properties
        .get_u32(PropertyId::MaximumPacketSize)
        .filter(|n| *n > 0)
}

/////////////////////////////////////////////////////////////////////////////

/// Commands sent from the public API to the internal event loop.
pub(crate) enum Command {
    /// Establish an MQTT connection.
    Connect {
        /// Session, authentication, and server-list parameters.
        params: Box<ConnectParams>,
        /// Completer resolved with the CONNACK result.
        tx: Completer<ConnectResponse>,
        /// Receives the identifier the server assigned when the client
        /// connected with an empty one, written before `tx` resolves so
        /// `client_id()` is correct as soon as `connect()` returns.
        assigned_id: Arc<RwLock<Option<String>>>,
        /// Persistence store, opened for this connection (if any).
        persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
    },

    /// Publish an application message.
    Publish {
        /// Topic name.
        topic: String,
        /// Application payload.
        payload: Bytes,
        /// Quality of service.
        qos: QoS,
        /// Whether the broker should retain this message.
        retain: bool,
        /// MQTT v5 properties; empty for v3.1.1.
        props: CodecProperties,
        /// Completer resolved with the PUBACK / PUBCOMP result.
        tx: Completer<PublishResponse>,
    },

    /// Subscribe to one or more topic filters.
    Subscribe {
        /// `true` for `subscribe()` (one filter, whose failure fails the
        /// token); `false` for `subscribe_many()` (per-filter codes are
        /// returned in the response even when some fail).
        single: bool,
        /// `(topic_filter, max_qos, subscribe_options)` triples.
        filters: Vec<(String, QoS, SubscribeOptions)>,
        /// MQTT v5 properties; empty for v3.1.1.
        props: CodecProperties,
        /// Completer resolved with the SUBACK result.
        tx: Completer<SubscribeResponse>,
    },

    /// Unsubscribe from one or more topic filters.
    Unsubscribe {
        /// As for [`Command::Subscribe`].
        single: bool,
        /// Topic filters to remove.
        filters: Vec<String>,
        /// MQTT v5 properties; empty for v3.1.1.
        props: CodecProperties,
        /// Completer resolved with the UNSUBACK result.
        tx: Completer<UnsubscribeResponse>,
    },

    /// Register an application delivery sink: a message or event stream
    /// the event loop feeds directly, rather than through a callback.
    ///
    /// The sink lasts until its receiver is dropped or `stop_stream` clears
    /// it; it survives reconnects, as the streams always have.
    AddSink(Sink),

    /// Close every application sink, from `stop_stream`.
    ClearSinks,

    /// Send a DISCONNECT packet and close the connection.
    Disconnect {
        /// How long to wait for in-flight operations to complete before
        /// disconnecting. `Duration::ZERO` means disconnect immediately.
        timeout: Duration,
        /// Reason for disconnecting (v5 only; ignored on v3.1.1).
        reason_code: ReasonCode,
        /// MQTT v5 properties; empty for v3.1.1.
        props: CodecProperties,
        /// Completer signalled when the disconnect is complete.
        tx: Completer<()>,
    },
}

/////////////////////////////////////////////////////////////////////////////

/// Events emitted by the internal event loop to the public layer.
pub(crate) enum Event {
    /// A connection to the broker was successfully established (CONNACK received).
    Connected(ConnectResponse),
    /// The network connection was lost unexpectedly.
    ConnectionLost(Error),
    /// An application message arrived from the broker.
    MessageArrived(Message),
    /// The broker sent a DISCONNECT packet (v5) or cleanly closed the connection.
    Disconnected {
        /// Disconnect reason code (always `NormalDisconnection` for v3.1.1).
        reason_code: ReasonCode,
        /// MQTT v5 properties; empty for v3.1.1.
        props: CodecProperties,
    },
    /// A TLS error occurred during connection setup (only when the `tls`
    /// feature is enabled and TLS is configured).
    TlsError(crate::TlsError),
    /// A `disconnect()` has completed, one event for each call. The message
    /// callback is given `None` for it. It comes from the event loop, after
    /// every message that connection delivered, so that the callback sees
    /// the end of the stream after the stream, and is only ever called from
    /// the dispatcher thread.
    DisconnectDone,
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use paho_mqtt_codec::Property;

    /// Host names, IPv4 and bracketed IPv6 addresses, with and without a
    /// port; the IPv6 host comes back without its brackets.
    #[test]
    fn test_split_host_port() {
        let ok = |a: &str| split_host_port(a, 1883).unwrap();
        assert_eq!(ok("broker"), ("broker".into(), 1883));
        assert_eq!(ok("broker:8883"), ("broker".into(), 8883));
        assert_eq!(ok("10.0.0.1:1"), ("10.0.0.1".into(), 1));
        assert_eq!(ok("[::1]"), ("::1".into(), 1883));
        assert_eq!(ok("[::1]:8883"), ("::1".into(), 8883));
        assert_eq!(ok("[fe80::1%eth0]:1"), ("fe80::1%eth0".into(), 1));

        for bad in [
            "",
            ":1883",
            "[]",
            "[]:1",
            "broker:",
            "broker:x",
            "broker:65536",
            "[::1",
            "[::1]x",
            "[::1]:",
            "[::1]:x",
            "::1",
            "::1:1883",
            "fe80::1",
        ] {
            assert!(
                matches!(split_host_port(bad, 1883), Err(Error::InvalidArgument(_))),
                "{bad:?}"
            );
        }
    }

    #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
    #[test]
    fn test_host_port() {
        assert_eq!(host_port("broker", 80), "broker:80");
        assert_eq!(host_port("10.0.0.1", 1), "10.0.0.1:1");
        assert_eq!(host_port("::1", 9001), "[::1]:9001");
        for a in ["broker:80", "[::1]:9001"] {
            let (h, p) = split_host_port(a, 0).unwrap();
            assert_eq!(host_port(&h, p), a);
        }
    }

    fn params_with(props: CodecProperties) -> ConnectParams {
        ConnectParams {
            servers: Vec::new(),
            persist_uri: String::new(),
            client_id: String::new(),
            clean_start: true,
            keep_alive_secs: 0,
            user_name: None,
            password: None,
            will: None,
            ver: MqttVersion::V5,
            props,
            auth: None,
            #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
            tls: None,
            #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
            http_headers: HashMap::new(),
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

    #[test]
    fn test_max_packet_size_defaults_to_protocol_max() {
        assert_eq!(
            params_with(CodecProperties::new()).max_packet_size(),
            MAX_PACKET_SIZE
        );
    }

    #[test]
    fn test_max_packet_size_from_connect_property() {
        let mut props = CodecProperties::new();
        props.push(Property::MaximumPacketSize(4096));
        assert_eq!(params_with(props).max_packet_size(), 4096);
    }

    #[test]
    fn test_server_max_packet_size_from_connack() {
        use paho_mqtt_codec::ReasonCode;
        let mut ack = ConnAck::new(false, ReasonCode::Success);
        assert_eq!(server_max_packet_size(&ack), None);
        ack.properties.push(Property::MaximumPacketSize(512));
        assert_eq!(server_max_packet_size(&ack), Some(512));
    }

    #[test]
    fn test_max_packet_size_zero_property_falls_back() {
        // 0 is a protocol error on the wire; treat it as "not set".
        let mut props = CodecProperties::new();
        props.push(Property::MaximumPacketSize(0));
        assert_eq!(params_with(props).max_packet_size(), MAX_PACKET_SIZE);
    }

    /// v3.x: a clean session ends with its connection, any other is kept.
    /// v5: the CONNECT's interval, 0 by default, replaced by the CONNACK's,
    /// then by the client's DISCONNECT. (One that would lengthen a CONNECT's
    /// 0, a Protocol Error, is refused by `disconnect()` before it gets
    /// here; see `AsyncClient::check_disconnect_props`.)
    #[test]
    fn test_session_expiry() {
        let ack = connack_with(vec![]);
        for ver in [MqttVersion::V3_1, MqttVersion::V3_1_1] {
            for clean in [true, false] {
                let mut p = params_with(CodecProperties::new());
                p.ver = ver;
                p.clean_start = clean;
                let e = SessionExpiry::negotiated(&p, &ack);
                assert_eq!(e.ends_with_connection(), clean, "{ver} clean={clean}");
            }
        }

        let v5 = |connect: Option<u32>, connack: Option<u32>| {
            let mut props = CodecProperties::new();
            if let Some(secs) = connect {
                props.push(Property::SessionExpiryInterval(secs));
            }
            let mut p = params_with(props);
            p.ver = MqttVersion::V5;
            // Clean Start does not decide it in v5.
            p.clean_start = true;
            let ack = connack_with(
                connack
                    .map(Property::SessionExpiryInterval)
                    .into_iter()
                    .collect(),
            );
            SessionExpiry::negotiated(&p, &ack)
        };
        let disconnect = |mut e: SessionExpiry, secs: u32| {
            let mut props = CodecProperties::new();
            props.push(Property::SessionExpiryInterval(secs));
            e.on_disconnect(&props);
            e.ends_with_connection()
        };

        assert!(v5(None, None).ends_with_connection(), "0 by default");
        assert!(v5(Some(0), None).ends_with_connection());
        assert!(!v5(Some(60), None).ends_with_connection());
        assert!(
            !v5(None, Some(60)).ends_with_connection(),
            "the server's wins"
        );
        assert!(
            v5(Some(60), Some(0)).ends_with_connection(),
            "the server's wins"
        );

        assert!(disconnect(v5(Some(60), None), 0), "ended by the DISCONNECT");
        assert!(!disconnect(v5(Some(60), None), 30));
        assert!(disconnect(v5(None, Some(60)), 0), "the server's, shortened");
        assert!(!disconnect(v5(Some(60), Some(0)), 30), "taken as it is");
        let mut e = v5(Some(60), None);
        e.on_disconnect(&CodecProperties::new());
        assert!(!e.ends_with_connection(), "no interval, no change");
    }

    /// The server's Maximum QoS and Retain Available, from the CONNACK, with
    /// their defaults when it does not say (and always in v3); and what they
    /// refuse.
    #[test]
    fn test_publish_limits_from_the_connack() {
        let p = params_with(CodecProperties::new());
        let limits = |props| SessionLimits::new(&p, &connack_with(props));

        let open = limits(vec![]);
        assert_eq!(
            (open.server_max_qos, open.retain_available),
            (QoS::QoS2, true)
        );
        assert_eq!(open.publish_refusal(QoS::QoS2, true), None);

        let tight = limits(vec![Property::MaximumQoS(0), Property::RetainAvailable(0)]);
        assert_eq!(tight.publish_refusal(QoS::QoS0, false), None);
        assert_eq!(
            tight.publish_refusal(QoS::QoS1, false),
            Some(ReasonCode::QosNotSupported)
        );
        assert_eq!(
            tight.publish_refusal(QoS::QoS0, true),
            Some(ReasonCode::RetainNotSupported)
        );
        let qos1 = limits(vec![Property::MaximumQoS(1)]);
        assert_eq!(qos1.publish_refusal(QoS::QoS1, true), None);
        assert_eq!(
            qos1.publish_refusal(QoS::QoS2, false),
            Some(ReasonCode::QosNotSupported)
        );
    }

    fn connack_with(props: Vec<Property>) -> ConnAck {
        let mut ack = ConnAck::new(false, ReasonCode::Success);
        for p in props {
            ack.properties.push(p);
        }
        ack
    }

    fn params_v(ver: MqttVersion, max_inflight: usize) -> ConnectParams {
        ConnectParams {
            ver,
            max_inflight,
            keep_alive_secs: 60,
            ..params_with(CodecProperties::new())
        }
    }

    // ---- Receive Maximum ----

    #[test]
    fn test_receive_maximum_caps_configured_inflight() {
        let ack = connack_with(vec![Property::ReceiveMaximum(5)]);
        // Unlimited locally: the server's window is the cap.
        assert_eq!(
            SessionLimits::new(&params_v(MqttVersion::V5, 0), &ack).max_inflight,
            5
        );
        // The smaller of the two wins, either way round.
        assert_eq!(
            SessionLimits::new(&params_v(MqttVersion::V5, 2), &ack).max_inflight,
            2
        );
        assert_eq!(
            SessionLimits::new(&params_v(MqttVersion::V5, 50), &ack).max_inflight,
            5
        );
    }

    #[test]
    fn test_receive_maximum_defaults_and_v3() {
        let bare = connack_with(vec![]);
        // v5 with no property: the specification's default of 65 535.
        assert_eq!(
            SessionLimits::new(&params_v(MqttVersion::V5, 0), &bare).max_inflight,
            u16::MAX as usize
        );
        // v3 has no Receive Maximum, so the configured value stands alone,
        // and 0 means as many as there are packet identifiers.
        assert_eq!(
            SessionLimits::new(&params_v(MqttVersion::V3_1_1, 0), &bare).max_inflight,
            MAX_PACKET_IDS
        );
        assert_eq!(
            SessionLimits::new(&params_v(MqttVersion::V3_1_1, 7), &bare).max_inflight,
            7
        );
    }

    // ---- Server Keep Alive ----

    #[test]
    fn test_server_keep_alive_overrides_request() {
        let ack = connack_with(vec![Property::ServerKeepAlive(15)]);
        let p = params_v(MqttVersion::V5, 0);
        assert_eq!(negotiated_keep_alive(&p, &ack), Duration::from_secs(15));

        // Absent: our own interval stands.
        assert_eq!(
            negotiated_keep_alive(&p, &connack_with(vec![])),
            Duration::from_secs(60)
        );
        // v3 has no such property, so ours stands even if one is present.
        let p3 = params_v(MqttVersion::V3_1_1, 0);
        assert_eq!(negotiated_keep_alive(&p3, &ack), Duration::from_secs(60));
    }

    // ---- Assigned Client Identifier ----

    #[test]
    fn test_assigned_client_id() {
        let ack = connack_with(vec![Property::AssignedClientIdentifier("auto-42".into())]);
        assert_eq!(assigned_client_id(&ack), Some(String::from("auto-42")));
        assert_eq!(assigned_client_id(&connack_with(vec![])), None);
    }

    // ---- Topic alias maxima ----

    #[test]
    fn test_topic_alias_maxima_come_from_both_sides() {
        let ack = connack_with(vec![Property::TopicAliasMaximum(8)]);
        let mut ours = CodecProperties::new();
        ours.push(Property::TopicAliasMaximum(3));
        let params = ConnectParams {
            ver: MqttVersion::V5,
            props: ours,
            ..params_with(CodecProperties::new())
        };
        let limits = SessionLimits::new(&params, &ack);
        assert_eq!(limits.server_topic_alias_max, 8, "what the server accepts");
        assert_eq!(limits.topic_alias_max, 3, "what we accept");

        // Absent on both sides means aliases are not in use.
        let limits = SessionLimits::new(&params_v(MqttVersion::V5, 0), &connack_with(vec![]));
        assert_eq!(limits.server_topic_alias_max, 0);
        assert_eq!(limits.topic_alias_max, 0);
    }

    #[test]
    fn test_server_uri_follows_the_state() {
        let cell = StateCell::new();
        let view = cell.view();
        assert_eq!(view.server_uri(), None);

        cell.set(ConnectionState::Connecting);
        assert_eq!(view.server_uri(), None);

        cell.set_connected("tcp://b:1883");
        assert_eq!(view.get(), ConnectionState::Connected);
        assert_eq!(view.server_uri().as_deref(), Some("tcp://b:1883"));

        // Kept in the cell, but no longer current.
        cell.set(ConnectionState::Retrying);
        assert_eq!(view.server_uri(), None);

        cell.set_connected("tcp://a:1883");
        assert_eq!(view.server_uri().as_deref(), Some("tcp://a:1883"));
    }
}
