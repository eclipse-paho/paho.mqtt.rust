// paho-mqtt/src/runtime/commands.rs
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

//! Commands and events for the internal async runtime.
//!
//! [`Command`] values flow from the public API to the event loop.
//! [`Event`] values flow back from the event loop to the public layer.

#[cfg(feature = "websocket")]
use std::collections::HashMap;
use std::{
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};

use bytes::Bytes;
use paho_mqtt_codec::{MqttVersion, Properties as CodecProperties, QoS, ReasonCode, Will};

use crate::{
    Error,
    persistence::ClientPersistence,
    responses::{ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse},
    subscribe_options::SubscribeOptions,
    token::Completer,
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
    /// TLS connection (requires the `tls` or `openssl` feature).
    #[cfg(any(feature = "tls", feature = "openssl"))]
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
    /// Plain WebSocket connection (requires the `websocket` feature).
    #[cfg(feature = "websocket")]
    Ws {
        /// Broker hostname or IP address.
        host: String,
        /// Broker TCP port.
        port: u16,
        /// WebSocket path (e.g. `/mqtt`).
        path: String,
    },
    /// WebSocket over TLS connection (requires `websocket` and `tls` or `openssl`).
    #[cfg(all(feature = "websocket", any(feature = "tls", feature = "openssl")))]
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
    /// TLS options; `Some` only when using a TLS transport.
    pub tls: Option<crate::TlsOptions>,
    /// HTTP headers included in the WebSocket upgrade request.
    #[cfg(feature = "websocket")]
    pub http_headers: HashMap<String, String>,
    /// Whether the event loop should automatically reconnect on session loss.
    pub auto_reconnect: bool,
    /// Minimum backoff interval between reconnect attempts.
    pub min_retry_interval: Duration,
    /// Maximum backoff interval between reconnect attempts.
    pub max_retry_interval: Duration,
    /// Maximum concurrent outbound QoS 1/2 publishes; 0 means unlimited.
    pub max_inflight: usize,
}

impl ConnectParams {
    /// Converts `keep_alive_secs` to a [`Duration`].
    ///
    /// Returns [`Duration::ZERO`] when keep-alive is disabled (`keep_alive_secs == 0`).
    pub fn keep_alive_duration(&self) -> Duration {
        Duration::from_secs(self.keep_alive_secs as u64)
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
        /// `(topic_filter, max_qos, subscribe_options)` triples.
        filters: Vec<(String, QoS, SubscribeOptions)>,
        /// MQTT v5 properties; empty for v3.1.1.
        props: CodecProperties,
        /// Completer resolved with the SUBACK result.
        tx: Completer<SubscribeResponse>,
    },

    /// Unsubscribe from one or more topic filters.
    Unsubscribe {
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
    Connected(crate::responses::ConnectResponse),
    /// The network connection was lost unexpectedly.
    ConnectionLost(Error),
    /// An application message arrived from the broker.
    MessageArrived(crate::Message),
    /// A TLS error occurred during connection setup (only when the `tls`
    /// feature is enabled and TLS is configured).
    TlsError(String),
}
