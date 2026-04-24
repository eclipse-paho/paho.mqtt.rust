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

/// The resolved transport address for a connection attempt.
pub(crate) enum ServerAddr {
    /// Plain TCP connection.
    Tcp {
        /// Broker hostname or IP address.
        host: String,
        /// Broker TCP port.
        port: u16,
    },
    /// TLS connection (requires the `tls` feature).
    #[cfg(feature = "tls")]
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
}

/////////////////////////////////////////////////////////////////////////////

/// Connection parameters grouped for reuse across reconnect attempts.
pub(crate) struct ConnectParams {
    /// Original server URI string, carried into `ConnectResponse`.
    pub server_uri: String,
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
    /// TLS/SSL options; `Some` only when using a TLS transport.
    pub ssl: Option<crate::SslOptions>,
    /// Whether the event loop should automatically reconnect on session loss.
    pub auto_reconnect: bool,
    /// Minimum backoff interval between reconnect attempts.
    pub min_retry_interval: Duration,
    /// Maximum backoff interval between reconnect attempts.
    pub max_retry_interval: Duration,
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
        /// Resolved transport address.
        addr: ServerAddr,
        /// Session and authentication parameters.
        params: ConnectParams,
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
}
