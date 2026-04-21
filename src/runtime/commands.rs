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

use std::sync::{Arc, atomic::AtomicBool};

use bytes::Bytes;
use paho_mqtt_codec::{MqttVersion, Properties as CodecProperties, QoS, ReasonCode, Will};

use crate::{
    responses::{ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse},
    token::Completer,
};

/////////////////////////////////////////////////////////////////////////////

/// An inbound MQTT message using codec-native types.
///
/// Delivered to the public layer via [`Event::MessageArrived`].
pub(crate) struct ReceivedMessage {
    /// Topic on which the message was published.
    pub topic: String,
    /// Application payload.
    pub payload: Bytes,
    /// Quality of service level.
    pub qos: QoS,
    /// Whether the broker stored and forwarded this message.
    pub retain: bool,
    /// Whether this is a re-delivery of an unacknowledged packet.
    pub dup: bool,
    /// Packet identifier; present for QoS 1 and 2, absent for QoS 0.
    pub packet_id: Option<u16>,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub props: CodecProperties,
}

/////////////////////////////////////////////////////////////////////////////

/// Commands sent from the public API to the internal event loop.
pub(crate) enum Command {
    /// Establish an MQTT connection.
    Connect {
        /// Broker hostname or IP address.
        host: String,
        /// Broker TCP port.
        port: u16,
        /// Client identifier.
        client_id: String,
        /// Whether to request a clean session / discard existing state.
        clean_start: bool,
        /// Keep-alive interval in seconds (0 = disabled).
        keep_alive_secs: u16,
        /// Optional username for authentication.
        user_name: Option<String>,
        /// Optional password for authentication.
        password: Option<Vec<u8>>,
        /// Optional Last Will and Testament message.
        will: Option<Will>,
        /// MQTT protocol version to use.
        ver: MqttVersion,
        /// MQTT v5 CONNECT properties; empty for v3.1.1.
        props: CodecProperties,
        /// Completer resolved with the CONNACK result.
        tx: Completer<ConnectResponse>,
        /// Set to `true` before resolving `tx` on success; cleared on failure.
        connected_flag: Arc<AtomicBool>,
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
        /// `(topic_filter, max_qos)` pairs.
        filters: Vec<(String, QoS)>,
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
    ConnectionLost(crate::Error),
    /// An application message arrived from the broker.
    MessageArrived(ReceivedMessage),
}
