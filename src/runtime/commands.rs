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

#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
use std::collections::HashMap;
use std::{
    sync::{Arc, Mutex, RwLock, atomic::AtomicBool},
    time::Duration,
};

use bytes::Bytes;
use paho_mqtt_codec::{
    ConnAck, MAX_PACKET_SIZE, MqttVersion, Properties as CodecProperties, PropertyId, QoS,
    ReasonCode, Will,
};

use crate::{
    ClientPersistence, ConnectResponse, Error, Message, Property, PublishResponse,
    SubscribeOptions, SubscribeResponse, UnsubscribeResponse, token::Completer,
};

/////////////////////////////////////////////////////////////////////////////

/// Configuration for the send-while-disconnected message buffer.
///
/// Passed to the event loop at spawn time; derived from [`CreateOptions`].
///
/// [`CreateOptions`]: crate::CreateOptions
pub(crate) struct BufConfig {
    /// Buffer publish commands when the client is disconnected after a prior
    /// successful connection.
    pub send_while_disconnected: bool,
    /// Maximum number of commands that may be held in the buffer at once.
    pub max_buffered_msgs: usize,
    /// Buffer publishes even before the first successful connection has been
    /// made.
    pub allow_send_any_time: bool,
    /// When the buffer is full, evict the oldest entry rather than the newest.
    pub delete_oldest: bool,
    /// After a successful connect, re-transmit persisted in-flight QoS 1/2
    /// messages with DUP=true and reload inbound QoS 2 state from persistence.
    pub restore_msgs: bool,
    /// Buffer QoS 0 publishes in the send-while-disconnected buffer.
    ///
    /// When `false`, QoS 0 publishes received while disconnected are completed
    /// immediately with `Ok` (silently dropped) rather than queued.
    pub persist_qos0: bool,
}

/////////////////////////////////////////////////////////////////////////////

/// The resolved transport address for a connection attempt.
pub(crate) enum ServerAddr {
    /// Plain TCP connection.
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
    /// Plain WebSocket connection (requires the `websocket` or `smol-websocket` feature).
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

/////////////////////////////////////////////////////////////////////////////

/// Connection parameters grouped for reuse across reconnect attempts.
pub(crate) struct ConnectParams {
    /// Ordered list of `(transport address, URI string)` pairs.
    ///
    /// The event loop tries them left-to-right on the initial connect and
    /// cycles round-robin on each auto-reconnect attempt.
    pub servers: Vec<(ServerAddr, String)>,
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
    pub auth: Option<crate::AuthHandler>,
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
    /// Maximum concurrent outbound QoS 1/2 publishes; 0 means unlimited.
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
        match self.props.get(PropertyId::MaximumPacketSize) {
            Some(Property::MaximumPacketSize(n)) if *n > 0 => *n,
            _ => MAX_PACKET_SIZE,
        }
    }
}

/// Per-session limits, fixed once the CONNACK has arrived.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SessionLimits {
    /// Effective cap on concurrent outbound QoS 1/2 publishes: the lesser of
    /// the configured `max_inflight` and the server's Receive Maximum.
    /// 0 means unlimited (only reachable on v3, which has no Receive Maximum).
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
/// further. v3 has no such property, so the configured value stands alone
/// (0 = unlimited).
fn effective_max_inflight(params: &ConnectParams, connack: &ConnAck) -> usize {
    if !params.ver.is_v5() {
        return params.max_inflight;
    }
    let server = u16_prop(&connack.properties, PropertyId::ReceiveMaximum)
        .filter(|&n| n > 0)
        .unwrap_or(u16::MAX) as usize;
    if params.max_inflight == 0 {
        server
    } else {
        params.max_inflight.min(server)
    }
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

/// The client identifier the server assigned, when the client sent an empty
/// one (MQTT v5 §3.2.2.3.7).
pub(crate) fn assigned_client_id(connack: &ConnAck) -> Option<String> {
    match connack.properties.get(PropertyId::AssignedClientIdentifier) {
        Some(Property::AssignedClientIdentifier(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// The largest packet the server will accept, from its CONNACK's v5
/// `MaximumPacketSize` property; `None` if it did not advertise one (or the
/// connection is v3), meaning the protocol maximum.
///
/// A client must not send a packet larger than this (MQTT-3.2.2-15); a
/// server that receives one disconnects with `PacketTooLarge`, so the
/// session refuses such a packet locally instead.
pub(crate) fn server_max_packet_size(connack: &ConnAck) -> Option<u32> {
    match connack.properties.get(PropertyId::MaximumPacketSize) {
        Some(Property::MaximumPacketSize(n)) if *n > 0 => Some(*n),
        _ => None,
    }
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
        /// Set to `true` before resolving `tx` on success; cleared on failure.
        connected_flag: Arc<AtomicBool>,
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
        /// Cleared to `false` before resolving `tx`.
        connected_flag: Arc<AtomicBool>,
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
    TlsError(String),
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use paho_mqtt_codec::Property;

    fn params_with(props: CodecProperties) -> ConnectParams {
        ConnectParams {
            servers: Vec::new(),
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
        // v3 has no Receive Maximum, so the configured value stands alone.
        assert_eq!(
            SessionLimits::new(&params_v(MqttVersion::V3_1_1, 0), &bare).max_inflight,
            0
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
}
