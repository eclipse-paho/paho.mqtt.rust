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
use bytes::{Bytes, BytesMut};
use paho_mqtt_codec::Publish as PublishPacket;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, fmt};

/////////////////////////////////////////////////////////////////////////////

/// A type that can be used as the payload of a [`Message`].
///
/// This exists so that a payload can be given as borrowed data. A message
/// payload is a [`Bytes`], and a `Bytes` can only be built from a borrowed
/// slice or string when that borrow is `'static`, so a bound of
/// `Into<Bytes>` would reject the common cases of a byte-string literal
/// (`b"data"`), a slice of a local buffer (`&buf[..]`), or a `&str` taken
/// from an owned `String`.
///
/// The impls for the owning types — [`Bytes`], `BytesMut`, `Vec<u8>`,
/// `String`, `Box<[u8]>` and an owned [`Cow`] — move the buffer and do not
/// copy it. The impls for borrowed data necessarily copy, including for a
/// `&'static` slice, which `Bytes::from_static` could have aliased. Pass an
/// owned value where that copy would matter.
///
/// # Examples
///
/// ```
/// use paho_mqtt::{Message, QOS_1};
///
/// let buf = vec![0u8, 1, 2, 3];
/// let text = String::from("hello");
///
/// // All of these are accepted.
/// let _ = Message::new("data", b"literal", QOS_1);
/// let _ = Message::new("data", &buf[..], QOS_1);
/// let _ = Message::new("text", text.as_str(), QOS_1);
/// let _ = Message::new("text", text, QOS_1);
/// ```
pub trait IntoPayload {
    /// Converts the value into a message payload.
    fn into_payload(self) -> Bytes;
}

impl IntoPayload for Bytes {
    fn into_payload(self) -> Bytes {
        self
    }
}

impl IntoPayload for BytesMut {
    fn into_payload(self) -> Bytes {
        self.freeze()
    }
}

impl IntoPayload for Vec<u8> {
    fn into_payload(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoPayload for String {
    fn into_payload(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoPayload for Box<[u8]> {
    fn into_payload(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoPayload for &[u8] {
    fn into_payload(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl IntoPayload for &mut [u8] {
    fn into_payload(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl<const N: usize> IntoPayload for [u8; N] {
    fn into_payload(self) -> Bytes {
        Bytes::copy_from_slice(&self)
    }
}

impl<const N: usize> IntoPayload for &[u8; N] {
    fn into_payload(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl IntoPayload for &str {
    fn into_payload(self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl IntoPayload for Cow<'_, [u8]> {
    fn into_payload(self) -> Bytes {
        match self {
            Cow::Borrowed(v) => Bytes::copy_from_slice(v),
            Cow::Owned(v) => Bytes::from(v),
        }
    }
}

impl IntoPayload for Cow<'_, str> {
    fn into_payload(self) -> Bytes {
        match self {
            Cow::Borrowed(s) => Bytes::copy_from_slice(s.as_bytes()),
            Cow::Owned(s) => Bytes::from(s),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

/// An MQTT Message.
///
/// This represents the information passed in an MQTT PUBLISH packet.
/// It is the primary data transfer mechanism for the protocol, used for
/// both outbound publications and inbound deliveries.
///
/// With the `serde` feature the whole message serializes, including the
/// duplicate flag and packet identifier that [`dup`](Self::dup) and
/// [`msgid`](Self::msgid) report — both written by the protocol rather than
/// by the application. Every field is optional on the way in, taking a
/// default message's value when absent, so a message named in a
/// configuration file need only give the parts that matter.
#[derive(Debug, Default, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
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
        V: IntoPayload,
        Q: Into<QoS>,
    {
        Self {
            topic: topic.into(),
            payload: payload.into_payload(),
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
        V: IntoPayload,
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

    /// Gets the retain flag of the message.
    ///
    /// An alias of [`retain`](Self::retain), the name the specification uses
    /// for the flag. `retained` is kept from v1.x and matches the other Paho
    /// clients (C, C++, Java, Go).
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
    ///
    /// The message starts with an empty topic and payload, no properties,
    /// the retain flag clear, and **QoS 0**. Note for anyone coming from
    /// v1.x of this library, where the default was QoS 1: a chain that does
    /// not call [`qos`](Self::qos) now publishes at most once, so the
    /// message is not acknowledged, persisted or re-sent.
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
    pub fn payload<V: IntoPayload>(mut self, payload: V) -> Self {
        self.0.payload = payload.into_payload();
        self
    }

    /// Sets the Quality of Service for the message.
    ///
    /// The default is QoS 0 — see [`new`](Self::new).
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

    /// Sets whether the message should be retained by the broker.
    ///
    /// An alias of [`retain`](Self::retain), the name the specification uses
    /// for the flag. `retained` is kept from v1.x and matches the other Paho
    /// clients (C, C++, Java, Go).
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

    /// Everything the accessors report survives a serde round trip,
    /// including the two protocol fields the application does not set.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_round_trip() {
        let mut props = Properties::new();
        props.push_user("k", "v");
        let msg = MessageBuilder::new()
            .topic(TOPIC)
            .payload(PAYLOAD)
            .qos(QOS)
            .retain(true)
            .properties(props)
            .finalize();

        let json = serde_json::to_string(&msg).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();

        assert_eq!(back.topic(), msg.topic());
        assert_eq!(back.payload(), msg.payload());
        assert_eq!(back.qos(), msg.qos());
        assert_eq!(back.retain(), msg.retain());
        assert_eq!(back.properties().get_user("k"), Some("v"));
        assert_eq!(back.dup(), msg.dup());
        assert_eq!(back.msgid(), msg.msgid());
    }

    /// The QoS is written in the codec's own spelling, and read back from
    /// either that or the v0.14 word form.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_qos_spelling() {
        let msg = Message::new(TOPIC, PAYLOAD, QoS::AtLeastOnce);
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""qos":"QoS1""#), "{json}");

        let back: Message = serde_json::from_str(
            r#"{"topic":"t","payload":[],"qos":"AtLeastOnce","retain":false,"props":[]}"#,
        )
        .unwrap();
        assert_eq!(back.qos(), QoS::QoS1);
    }

    /// `dup` and `msgid` belong to the protocol, not the application, so a
    /// representation without them is still a message — as is one that names
    /// only the parts a configuration file would.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_without_the_protocol_fields() {
        let json = r#"{"topic":"t","payload":[104,105],"qos":"QoS0","retain":false,"props":[]}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert_eq!(msg.topic(), "t");
        assert_eq!(msg.payload(), b"hi");
        assert!(!msg.dup());
        assert_eq!(msg.msgid(), None);

        let msg: Message = serde_json::from_str(r#"{"topic":"lwt"}"#).unwrap();
        assert_eq!(msg.topic(), "lwt");
        assert!(msg.payload().is_empty());
        assert_eq!(msg.qos(), QoS::default());
        assert!(!msg.retain());
        assert!(msg.properties().is_empty());
    }

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

    // Every shape of payload v0.14 accepted through `Into<Vec<u8>>` must be
    // accepted here, including the borrowed ones that `Into<Bytes>` refused:
    // a byte-string literal, a slice of a local buffer, and a `&str` that
    // isn't `'static`. This test is mostly a compile-time proof.
    #[test]
    fn test_payload_conversions() {
        // Builds the message as an application would, and hands back the
        // payload so the conversion can be checked.
        fn payload_of<V: IntoPayload>(payload: V) -> Bytes {
            Message::new("t", payload, QOS).payload
        }

        let owned = vec![1u8, 2, 3];
        let text = String::from("abc");
        let mut scratch = [1u8, 2, 3];
        let abc = &b"abc"[..];
        let num = &[1u8, 2, 3][..];

        // Borrowed, and none of it `'static`: these are what `Into<Bytes>`
        // refused.
        assert_eq!(payload_of(b"abc"), abc);
        assert_eq!(payload_of(*b"abc"), abc);
        assert_eq!(payload_of(&owned[..]), num);
        assert_eq!(payload_of(&mut scratch[..]), num);
        assert_eq!(payload_of(text.as_str()), abc);
        assert_eq!(payload_of("abc"), abc);
        assert_eq!(payload_of(Cow::Borrowed("abc")), abc);
        assert_eq!(payload_of(Cow::Borrowed(num)), num);

        // Owning.
        assert_eq!(payload_of(Cow::<[u8]>::Owned(owned.clone())), num);
        assert_eq!(payload_of(Cow::<str>::Owned(text.clone())), abc);
        assert_eq!(payload_of(owned.clone().into_boxed_slice()), num);
        assert_eq!(payload_of(text.clone()), abc);
        assert_eq!(payload_of(Bytes::from_static(b"abc")), abc);
        assert_eq!(payload_of(BytesMut::from(abc)), abc);
        assert_eq!(payload_of(owned), num);

        // The builder and the retained constructor take the same set.
        assert_eq!(
            MessageBuilder::new()
                .topic("t")
                .payload(&text[..])
                .finalize()
                .payload,
            abc
        );
        assert_eq!(Message::new_retained("t", b"abc", QOS).payload, abc);
    }

    // An owned buffer must be moved into the payload, not copied. The `Bytes`
    // impl is the identity, so the payload aliases the same allocation.
    #[test]
    fn test_owned_payload_is_not_copied() {
        let src = Bytes::from(vec![9u8; 64]);
        let addr = src.as_ptr();
        let msg = Message::new("t", src, QOS);
        assert_eq!(msg.payload().as_ptr(), addr);
    }
}
