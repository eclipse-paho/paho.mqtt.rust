// message.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

use crate::{QoS, properties::Properties};
use bytes::Bytes;
use paho_mqtt_codec::Publish as PublishPacket;
use std::{borrow::Cow, fmt};

/// An MQTT Message.
///
/// This represents the information passed in an MQTT PUBLISH packet.
/// It is the primary data transfer mechanism for the protocol, used for
/// both outbound publications and inbound deliveries.
#[derive(Debug, Default, Clone)]
pub struct Message {
    /// The message topic
    pub topic: String,
    /// The message payload
    pub payload: Bytes,
    /// The message quality of service
    pub qos: QoS,
    /// Whether this is a retain message
    pub retain: bool,
    /// The message properties
    pub props: Properties,
    /// Whether this is a duplicate
    pub(crate) dup: bool,
    /// The message identifier.
    pub(crate) msgid: Option<u16>,
}

impl Message {
    /// Creates a new message.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic on which the message is published.
    /// * `payload` The binary payload of the message
    /// * `qos` The quality of service for message delivery (0, 1, or 2)
    pub fn new<S, V, Q>(topic: S, payload: V, qos: Q) -> Self
    where
        S: Into<String>,
        V: Into<Bytes>,
        Q: Into<QoS>,
    {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            qos: qos.into(),
            ..Self::default()
        }
    }

    /// Creates a new message that will be retained by the broker.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic on which the message is published.
    /// * `payload` The binary payload of the message
    /// * `qos` The quality of service for message delivery (0, 1, or 2)
    pub fn new_retained<S, V, Q>(topic: S, payload: V, qos: Q) -> Self
    where
        S: Into<String>,
        V: Into<Bytes>,
        Q: Into<QoS>,
    {
        Self {
            retain: true,
            ..Self::new(topic, payload, qos)
        }
    }

    /// Gets the message topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Gets the message payload
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Gets the payload of the message as a string.
    ///
    /// This utilizes the "lossy" style of conversion from the std library.
    /// If the contents of the CStr are valid UTF-8 data, this function will
    /// return a `Cow::Borrowed(&str)` with the the corresponding `&str` slice.
    /// Otherwise, it will replace any invalid UTF-8 sequences with U+FFFD
    /// REPLACEMENT CHARACTER and return a `Cow::Owned(String)` with the result.
    pub fn payload_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.payload())
    }

    /// Gets the QoS of the message.
    pub fn qos(&self) -> QoS {
        self.qos
    }

    /// Gets the retain flag of the message
    pub fn retain(&self) -> bool {
        self.retain
    }

    // TODO: Mark as deprecated?
    /// Gets the retain flag of the message
    #[inline]
    pub fn retained(&self) -> bool {
        self.retain()
    }

    /// Gets the properties in the message
    pub fn properties(&self) -> &Properties {
        &self.props
    }

    /// Gets the message identifier.
    pub fn msgid(&self) -> Option<u16> {
        self.msgid
    }

    /// Gets the value of the DUP flag.
    pub fn dup(&self) -> bool {
        self.dup
    }
}

impl From<PublishPacket> for Message {
    fn from(pub_pkt: PublishPacket) -> Self {
        let id = match pub_pkt.qos {
            QoS::QoS0 => None,
            _ => Some(pub_pkt.packet_id.unwrap_or(0)),
        };
        Self {
            topic: pub_pkt.topic,
            payload: pub_pkt.payload,
            qos: pub_pkt.qos,
            retain: pub_pkt.retain,
            dup: pub_pkt.dup,
            msgid: id,
            props: pub_pkt.properties,
        }
    }
}

impl fmt::Display for Message {
    /// Formats the message for display
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.topic(), self.payload_str())
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Builder to create a new Message
#[derive(Debug, Default, Clone)]
pub struct MessageBuilder(Message);

impl MessageBuilder {
    /// Create a new message builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the topic for the message
    ///
    /// # Arguments
    ///
    /// `topic` The topic on which the message should be published.
    pub fn topic<T: Into<String>>(mut self, topic: T) -> Self {
        self.0.topic = topic.into();
        self
    }

    /// Sets the payload for the message
    ///
    /// # Arguments
    ///
    /// `payload` The binary payload of the message
    pub fn payload<V: Into<Bytes>>(mut self, payload: V) -> Self {
        self.0.payload = payload.into();
        self
    }

    /// Sets the Quality of Service for the message.
    ///
    /// # Arguments
    ///
    /// `qos` The quality of service for the message.
    pub fn qos<Q: Into<QoS>>(mut self, qos: Q) -> Self {
        self.0.qos = qos.into();
        self
    }

    /// Sets whether or not the published message should be retained by the
    /// broker.
    ///
    /// # Arguments
    ///
    /// `retained` Set true if the message should be retained by the broker,
    ///            false if not.
    pub fn retain(mut self, retain: bool) -> Self {
        self.0.retain = retain;
        self
    }

    // TODO: Mark deprecated?
    /// Obsolete method for `retain`
    pub fn retained(self, retain: bool) -> Self {
        self.retain(retain)
    }

    /// Sets the collection of properties for the message.
    ///
    /// # Arguments
    ///
    /// `props` The collection of properties to include with the message.
    pub fn properties(mut self, props: Properties) -> Self {
        self.0.props = props;
        self
    }

    /// Finalize the builder to create the message.
    pub fn finalize(self) -> Message {
        self.0
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    const TOPIC: &str = "test";
    const PAYLOAD: &[u8] = b"Hello world";
    const QOS: QoS = QoS::ExactlyOnce;

    // Determine that a message can be sent across threads.
    #[test]
    fn test_message_send() {
        let msg = Message::new(TOPIC, PAYLOAD, QOS);
        let thr = thread::spawn(move || {
            assert_eq!(TOPIC, msg.topic());
            assert_eq!(PAYLOAD, msg.payload());
            assert_eq!(QOS, msg.qos());
        });
        thr.join().unwrap();
    }
}
