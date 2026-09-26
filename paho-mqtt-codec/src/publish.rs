// paho-mqtt-codec/src/publish.rs
//
// This file is part of the Eclipse Paho MQTT Rust Codec library.

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

//! MQTT PUBLISH packet and its acknowledgements.
//!
//! PUBLISH carries the DUP, QoS, and RETAIN flags in the fixed-header byte
//! rather than the payload, so [`Publish::decode`] requires those flags as
//! a separate parameter extracted from [`crate::packet::RawPacket::header`].

use crate::{
    Error, MqttVersion, PacketType, Properties, PropertyId, QoS, ReasonCode, Result, String, topic,
    wire::{check_packet_id, expect_end, get_utf8, put_utf8, utf8_wire_size},
};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// A PUBLISH packet, unified across MQTT v3.1, v3.1.1, and v5.
///
/// The `properties` field is always empty on v3 connections and is not
/// encoded on the wire for that version.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Publish {
    /// Duplicate delivery flag — set when re-sending an unacknowledged packet.
    pub dup: bool,
    /// Quality of Service level.
    pub qos: QoS,
    /// Retain flag — broker should store this message for new subscribers.
    pub retain: bool,
    /// Topic name.
    pub topic: String,
    /// Packet identifier. Required for QoS 1 and 2, absent for QoS 0.
    pub packet_id: Option<u16>,
    /// Application message payload.
    pub payload: Bytes,
    /// MQTT v5 properties. Empty on v3 connections.
    pub properties: Properties,
}

impl Publish {
    /// Creates a QoS 0 PUBLISH with no DUP, no retain, and no properties.
    ///
    /// A PUBLISH has too many fields for a constructor per combination, so
    /// anything beyond a plain QoS 0 publication is built with
    /// [`builder`](Self::builder) or by setting the fields directly.
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            dup: false,
            qos: QoS::default(),
            retain: false,
            topic: topic.into(),
            packet_id: None,
            payload: payload.into(),
            properties: Properties::new(),
        }
    }

    /// Starts building a PUBLISH a field at a time.
    ///
    /// Shorthand for [`PublishBuilder::new`].
    pub fn builder() -> PublishBuilder {
        PublishBuilder::new()
    }

    /// Returns the number of variable-header + payload bytes for version, `ver`.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        let mut size = utf8_wire_size(&self.topic);
        if self.qos != QoS::QoS0 {
            size += 2; // packet ID
        }
        size += self.properties.encoded_size(ver);
        size += self.payload.len();
        size
    }

    /// Encodes the PUBLISH variable header and payload into `dst`.
    ///
    /// This encodes the rest of the packet after the fixed header, but does so
    /// by _copying_ the payload into `dst`. An application can consider a more
    /// efficient solution by using [`Self::encode_header`] and then writing the
    /// payload directly to the output stream.
    ///
    /// Returns [`Error::ProtocolError`] if `qos > 0` but `packet_id` is `None`
    /// or `Some(0)`, or if `qos == 0` and `packet_id` is set.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        self.encode_header(dst, ver)?;
        dst.put_slice(&self.payload);
        Ok(())
    }

    /// Encodes only the PUBLISH variable header (topic, packet ID, and
    /// properties) into `dst`, leaving the application payload to be written
    /// separately by the caller.
    ///
    /// Returns [`Error::ProtocolError`] if `qos > 0` but `packet_id` is `None`
    /// or `Some(0)`, or if `qos == 0` and `packet_id` is set.
    pub fn encode_header<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        // Same rules as decode: no wildcards; empty only with a v5 alias.
        if topic::has_wildcards(&self.topic)
            || (self.topic.is_empty()
                && (ver != MqttVersion::V5
                    || self.properties.get(PropertyId::TopicAlias).is_none()))
        {
            return Err(Error::TopicNameInvalid);
        }
        put_utf8(&self.topic, dst)?;
        match (self.qos, self.packet_id) {
            // A QoS 0 PUBLISH has no packet identifier (MQTT-2.2.1-2); one
            // that is set would be dropped silently, so treat it as a
            // caller error.
            (QoS::QoS0, Some(_)) => return Err(Error::ProtocolError),
            (QoS::QoS0, None) => {}
            (_, id) => dst.put_u16(check_packet_id(id.ok_or(Error::ProtocolError)?)?),
        }
        self.properties.check_context(PacketType::Publish)?;
        self.properties.encode(dst, ver)
    }

    /// Decodes a PUBLISH from the fixed-header `flags` (lower 4 bits of the
    /// first header byte) and the raw `payload` bytes for the given version, `ver`.
    pub fn decode(flags: u8, mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        let dup = flags & 0x08 != 0;
        let qos = QoS::try_from((flags >> 1) & 0x03)?;
        let retain = flags & 0x01 != 0;

        // DUP is meaningless without an acknowledgement (MQTT-3.3.1-2).
        if dup && qos == QoS::QoS0 {
            return Err(Error::ProtocolError);
        }

        // Topic: wildcards are never allowed in a topic *name* (MQTT-3.3.2-2).
        // Emptiness is checked below, once the properties are known.
        let topic = get_utf8(&mut payload)?;
        if topic::has_wildcards(&topic) {
            return Err(Error::TopicNameInvalid);
        }

        // Packet ID (QoS 1 and 2 only). Zero is reserved (MQTT-2.2.1-2).
        let packet_id = if qos != QoS::QoS0 {
            if payload.remaining() < 2 {
                return Err(Error::PacketTooShort);
            }
            Some(check_packet_id(payload.get_u16())?)
        } else {
            None
        };

        // v5 properties
        let properties = Properties::decode_in(&mut payload, ver, PacketType::Publish)?;

        // An empty topic name is only allowed in v5, and only when a topic
        // alias supplies the topic instead (MQTT-3.3.2-1, §3.3.2.3.4).
        if topic.is_empty()
            && (ver != MqttVersion::V5 || properties.get(PropertyId::TopicAlias).is_none())
        {
            return Err(Error::TopicNameInvalid);
        }

        // What's left in `payload` is the application payload

        Ok(Self {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            payload,
            properties,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Builder for a [`Publish`] packet.
///
/// A PUBLISH has seven fields, most of which a given caller leaves alone, so
/// setting them one at a time reads better than a constructor with seven
/// parameters. Each setter consumes the builder and returns it, and
/// [`finalize`](Self::finalize) yields the packet. [`Publish::builder`] is a
/// shorthand for [`new`](Self::new).
///
/// The builder does not validate; as everywhere else in this crate, the
/// packet is checked when it is encoded. Two couplings are worth knowing
/// while building one: QoS 1 and 2 require a packet identifier and QoS 0
/// forbids one (MQTT-2.2.1-2), and an empty topic name is legal only on v5
/// with a `TopicAlias` property.
#[derive(Debug, Default, Clone)]
pub struct PublishBuilder(Publish);

impl PublishBuilder {
    /// Creates a builder for a QoS 0 PUBLISH with an empty topic and payload.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the topic on which the message is published.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.0.topic = topic.into();
        self
    }

    /// Sets the application message payload.
    pub fn payload(mut self, payload: impl Into<Bytes>) -> Self {
        self.0.payload = payload.into();
        self
    }

    /// Sets the quality of service for the publication.
    ///
    /// QoS 1 and 2 also need a [`packet_id`](Self::packet_id); QoS 0 must not
    /// have one.
    pub fn qos(mut self, qos: QoS) -> Self {
        self.0.qos = qos;
        self
    }

    /// Sets the packet identifier, or clears it with `None`.
    ///
    /// Required for QoS 1 and 2, and forbidden for QoS 0.
    pub fn packet_id(mut self, packet_id: impl Into<Option<u16>>) -> Self {
        self.0.packet_id = packet_id.into();
        self
    }

    /// Sets the QoS and the packet identifier together, which is what QoS 1
    /// and 2 need.
    ///
    /// Equivalent to [`qos`](Self::qos) followed by
    /// [`packet_id`](Self::packet_id), spelled as one call so the two cannot
    /// drift apart.
    pub fn qos_with_id(self, qos: QoS, packet_id: u16) -> Self {
        self.qos(qos).packet_id(packet_id)
    }

    /// Sets whether the broker should retain the message as the last known
    /// good value for its topic.
    pub fn retain(mut self, retain: bool) -> Self {
        self.0.retain = retain;
        self
    }

    /// Sets the duplicate-delivery flag.
    ///
    /// A sender sets this on a re-delivery of a QoS 1 or 2 message; it is
    /// always 0 for QoS 0 (MQTT-3.3.1-2).
    pub fn dup(mut self, dup: bool) -> Self {
        self.0.dup = dup;
        self
    }

    /// Sets the MQTT v5 properties.
    ///
    /// Only the PUBLISH properties are permitted (§3.3.2.3); encoding rejects
    /// anything else, and rejects a non-empty set for any version but v5.
    pub fn properties(mut self, properties: Properties) -> Self {
        self.0.properties = properties;
        self
    }

    /// Finalizes the builder into the packet.
    pub fn finalize(self) -> Publish {
        self.0
    }
}

/////////////////////////////////////////////////////////////////////////////

/// PUBACK, PUBREC, PUBREL, or PUBCOMP packet.
///
/// All four publish-acknowledgement packets have identical structure:
/// a packet identifier, an optional reason code, and optional properties.
/// They are distinguished only by their packet-type nibble in the fixed
/// header, which is handled by the [`Packet`](crate::packet::Packet) enum.
///
/// **v5 shorthand encodings**: when `reason_code` is [`ReasonCode::Success`]
/// and `properties` is empty the remaining length is 2 (packet ID only). When
/// only `properties` is empty the remaining length is 3 (packet ID + reason
/// code), omitting the property-length byte. All three forms are accepted on
/// decode.
///
/// Type aliases [`PubRec`], [`PubRel`], and [`PubComp`] are provided for
/// readability at call sites.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PubAck {
    /// Packet identifier matching the original PUBLISH.
    pub packet_id: u16,
    /// Outcome of the operation; defaults to [`ReasonCode::Success`].
    pub reason_code: ReasonCode,
    /// MQTT v5 properties; empty on v3 connections.
    pub properties: Properties,
}

/// PUBREC acknowledgement — same structure as [`PubAck`].
pub type PubRec = PubAck;
/// PUBREL acknowledgement — same structure as [`PubAck`].
pub type PubRel = PubAck;
/// PUBCOMP acknowledgement — same structure as [`PubAck`].
pub type PubComp = PubAck;

impl PubAck {
    /// Creates a plain success acknowledgement, valid on any version.
    ///
    /// This is everything a v3 acknowledgement can carry, and it is what v5
    /// encodes in its two-byte shorthand form.
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            reason_code: ReasonCode::Success,
            properties: Properties::new(),
        }
    }

    /// Creates an acknowledgement with a reason code and MQTT v5 properties.
    ///
    /// v5 only.
    ///
    /// Both extras are v5-only: on a v3 connection the reason code cannot be
    /// expressed and is dropped, and non-empty properties are refused by
    /// [`encode`](Self::encode) — use [`new`](Self::new) for an
    /// acknowledgement that must work on either version.
    ///
    /// `properties` accepts a [`Properties`] or `None`. An empty set is a
    /// distinct wire form here rather than a different constructor: `None`
    /// gives the three-byte packet ID + reason code encoding, which is what a
    /// broker reporting a non-success outcome usually sends.
    pub fn with_properties<P>(packet_id: u16, reason_code: ReasonCode, properties: P) -> Self
    where
        P: Into<Option<Properties>>,
    {
        Self {
            packet_id,
            reason_code,
            properties: properties.into().unwrap_or_default(),
        }
    }

    /// Returns the remaining-length byte count for version, `ver`.
    ///
    /// Uses the shortest v5 form permitted: 2 bytes when the reason code is
    /// `Success` and there are no properties, 3 bytes when there are no
    /// properties, otherwise the full form.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        if ver != MqttVersion::V5 {
            return 2;
        }
        match (self.reason_code, self.properties.is_empty()) {
            (ReasonCode::Success, true) => 2,
            (_, true) => 3,
            _ => 3 + self.properties.encoded_size(ver),
        }
    }

    /// Encodes the variable header into `dst`.
    ///
    /// Returns [`Error::ProtocolError`] if `packet_id` is 0, or if `ver` is
    /// v3 and `properties` is not empty (v3 acknowledgements carry only the
    /// packet identifier; a non-success `reason_code` cannot be expressed
    /// either and is dropped — see [`PubAck`]).
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        dst.put_u16(check_packet_id(self.packet_id)?);
        if ver != MqttVersion::V5 {
            // Nothing more on the wire; a non-empty list is an error.
            return self.properties.encode(dst, ver);
        }
        match (self.reason_code, self.properties.is_empty()) {
            (ReasonCode::Success, true) => {}
            (rc, true) => dst.put_u8(rc as u8),
            (rc, false) => {
                dst.put_u8(rc as u8);
                self.properties.check_context(PacketType::PubAck)?;
                self.properties.encode(dst, ver)?;
            }
        }
        Ok(())
    }

    /// Decodes from the raw `payload` bytes for the given version, `ver`.
    pub fn decode(mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }
        let packet_id = check_packet_id(payload.get_u16())?;

        let (reason_code, properties) = match ver {
            MqttVersion::V5 => {
                if payload.is_empty() {
                    // Shorthand: no reason code byte → implicit Success
                    (ReasonCode::Success, Properties::new())
                } else {
                    let rc = ReasonCode::try_from(payload.get_u8())?;
                    // Property length is omitted when the remaining length < 4.
                    let props = if payload.is_empty() {
                        Properties::new()
                    } else {
                        Properties::decode_in(&mut payload, ver, PacketType::PubAck)?
                    };
                    (rc, props)
                }
            }
            _ => (ReasonCode::Success, Properties::new()),
        };
        expect_end(&payload)?;

        Ok(Self::with_properties(packet_id, reason_code, properties))
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        properties::{Property, PropertyId, PropertyValue},
        types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5},
    };
    use bytes::BytesMut;

    fn v311() -> MqttVersion {
        MQTT_VERSION_3_1_1
    }

    fn v5() -> MqttVersion {
        MQTT_VERSION_5
    }

    // Encode a Publish to bytes, extracting the flags byte separately (as
    // packet.rs would when building the full fixed header).
    fn encode_with_flags(pub_: &Publish, ver: MqttVersion) -> (u8, BytesMut) {
        let mut flags: u8 = 0x30; // PUBLISH type nibble
        if pub_.dup {
            flags |= 0x08;
        }
        flags |= (pub_.qos as u8) << 1;
        if pub_.retain {
            flags |= 0x01;
        }
        let mut dst = BytesMut::new();
        pub_.encode(&mut dst, ver).unwrap();
        (flags, dst)
    }

    fn round_trip(pub_: Publish, ver: MqttVersion) {
        let (flags_byte, encoded) = encode_with_flags(&pub_, ver);
        let decoded = Publish::decode(flags_byte & 0x0F, encoded.freeze(), ver).unwrap();
        assert_eq!(decoded, pub_);
    }

    // ---- QoS 0 ----

    #[test]
    fn test_publish_qos0_encode() {
        let pub_ = Publish::new("sensors/temp", Bytes::from_static(b"42.5"));
        let (_, encoded) = encode_with_flags(&pub_, v311());
        // topic len=12, topic, payload "42.5"
        assert_eq!(&encoded[..2], &[0x00, 0x0C]);
        assert_eq!(&encoded[2..14], b"sensors/temp");
        assert_eq!(&encoded[14..], b"42.5");
    }

    #[test]
    fn test_publish_qos0_round_trip_v311() {
        round_trip(Publish::new("a/b", Bytes::from_static(b"hello")), v311());
    }

    #[test]
    fn test_publish_qos0_round_trip_v5() {
        round_trip(Publish::new("a/b", Bytes::from_static(b"hello")), v5());
    }

    #[test]
    fn test_publish_qos0_empty_payload() {
        round_trip(Publish::new("t", Bytes::new()), v311());
    }

    // ---- QoS 1 ----

    #[test]
    fn test_publish_qos1_round_trip_v311() {
        let pub_ = Publish {
            qos: QoS::AtLeastOnce,
            packet_id: Some(42),
            topic: "t".into(),
            payload: Bytes::from_static(b"msg"),
            ..Default::default()
        };
        round_trip(pub_, v311());
    }

    #[test]
    fn test_publish_qos1_round_trip_v5() {
        let pub_ = Publish {
            qos: QoS::AtLeastOnce,
            packet_id: Some(1),
            topic: "t".into(),
            payload: Bytes::from_static(b"data"),
            ..Default::default()
        };
        round_trip(pub_, v5());
    }

    // ---- QoS 2 ----

    #[test]
    fn test_publish_qos2_round_trip_v311() {
        let pub_ = Publish {
            qos: QoS::ExactlyOnce,
            packet_id: Some(100),
            topic: "t/u/v".into(),
            payload: Bytes::from_static(b"exact"),
            ..Default::default()
        };
        round_trip(pub_, v311());
    }

    // ---- flags ----

    #[test]
    fn test_publish_retain_flag_round_trip() {
        let pub_ = Publish {
            retain: true,
            topic: "retained".into(),
            payload: Bytes::from_static(b"val"),
            ..Default::default()
        };
        round_trip(pub_.clone(), v311());
        round_trip(pub_, v5());
    }

    #[test]
    fn test_publish_dup_flag_round_trip() {
        let pub_ = Publish {
            dup: true,
            qos: QoS::AtLeastOnce,
            packet_id: Some(7),
            topic: "t".into(),
            payload: Bytes::new(),
            ..Default::default()
        };
        round_trip(pub_, v311());
    }

    // ---- full kitchen-sink ----

    #[test]
    fn test_publish_all_flags_v311() {
        let pub_ = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: "a/b/c".into(),
            packet_id: Some(0xFFFF),
            payload: Bytes::from_static(b"payload data"),
            properties: Properties::new(),
        };
        round_trip(pub_, v311());
    }

    #[test]
    fn test_publish_all_flags_v5() {
        let pub_ = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: "test/topic".into(),
            packet_id: Some(999),
            payload: Bytes::from_static(b"v5 data"),
            properties: Properties::new(),
        };
        round_trip(pub_, v5());
    }

    // ---- encoded_size ----

    #[test]
    fn test_encoded_size_qos0_v311() {
        let pub_ = Publish::new("abc", Bytes::from_static(b"xyz"));
        // 2+3 (topic) + 3 (payload) = 8
        assert_eq!(pub_.encoded_size(v311()), 8);
    }

    #[test]
    fn test_encoded_size_qos1_v311() {
        let pub_ = Publish {
            qos: QoS::AtLeastOnce,
            packet_id: Some(1),
            topic: "abc".into(),
            payload: Bytes::from_static(b"xyz"),
            ..Default::default()
        };
        // 2+3 (topic) + 2 (packet_id) + 3 (payload) = 10
        assert_eq!(pub_.encoded_size(v311()), 10);
    }

    #[test]
    fn test_encoded_size_qos0_v5() {
        let pub_ = Publish::new("abc", Bytes::from_static(b"xyz"));
        // 2+3 + 1 (empty props VBI) + 3 = 9
        assert_eq!(pub_.encoded_size(v5()), 9);
    }

    #[test]
    fn test_encoded_size_matches_encode() {
        let pub_ = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: "sensors/temperature".into(),
            packet_id: Some(5),
            payload: Bytes::from_static(b"23.4"),
            properties: Properties::new(),
        };
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            pub_.encode(&mut dst, ver).unwrap();
            assert_eq!(dst.len(), pub_.encoded_size(ver), "mismatch for {ver:?}");
        }
    }

    // ---- error cases ----

    #[test]
    fn test_publish_decode_packet_id_zero_errors() {
        // flags: QoS 1; body: topic "t", packet id 0
        let payload = Bytes::from_static(&[0x00, 0x01, b't', 0x00, 0x00]);
        assert_eq!(
            Publish::decode(0x02, payload, v311()),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_publish_decode_dup_with_qos0_errors() {
        let payload = Bytes::from_static(&[0x00, 0x01, b't']);
        assert_eq!(
            Publish::decode(0x08, payload, v311()),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_publish_decode_wildcard_in_topic_errors() {
        for topic in [&b"a/#"[..], b"a/+/b", b"+", b"#"] {
            let mut payload = BytesMut::new();
            payload.put_u16(topic.len() as u16);
            payload.put_slice(topic);
            assert_eq!(
                Publish::decode(0x00, payload.freeze(), v311()),
                Err(Error::TopicNameInvalid),
                "{}",
                core::str::from_utf8(topic).unwrap()
            );
        }
    }

    #[test]
    fn test_publish_decode_empty_topic() {
        // Never valid in v3.1.1.
        assert_eq!(
            Publish::decode(0x00, Bytes::from_static(&[0x00, 0x00]), v311()),
            Err(Error::TopicNameInvalid)
        );
        // In v5 only with a topic alias: empty topic, empty properties -> error
        assert_eq!(
            Publish::decode(0x00, Bytes::from_static(&[0x00, 0x00, 0x00]), v5()),
            Err(Error::TopicNameInvalid)
        );
        // ... but with TopicAlias(7) present it is accepted.
        let payload = Bytes::from_static(&[0x00, 0x00, 0x03, 0x23, 0x00, 0x07, b'x']);
        let p = Publish::decode(0x00, payload, v5()).unwrap();
        assert!(p.topic.is_empty());
        assert_eq!(p.payload, Bytes::from_static(b"x"));
    }

    #[test]
    fn test_publish_topic_with_nul_rejected_both_directions() {
        let p = Publish::new("a\0b", Bytes::new());
        let mut dst = BytesMut::new();
        assert_eq!(p.encode(&mut dst, v311()), Err(Error::MalformedPacket));
        assert_eq!(
            Publish::decode(
                0x00,
                Bytes::from_static(&[0x00, 0x03, b'a', 0x00, b'b']),
                v311()
            ),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_publish_qos1_missing_packet_id_errors() {
        let pub_ = Publish {
            qos: QoS::AtLeastOnce,
            packet_id: None, // missing!
            topic: "t".into(),
            ..Default::default()
        };
        let mut dst = BytesMut::new();
        assert_eq!(pub_.encode(&mut dst, v311()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_publish_encode_topic_too_long() {
        let topic: String = core::iter::repeat_n('x', 65_536).collect();
        let p = Publish::new(topic, Bytes::new());
        let mut dst = BytesMut::new();
        assert_eq!(p.encode(&mut dst, v5()), Err(Error::StringTooLong));
    }

    #[test]
    fn test_publish_encode_topic_at_limit_ok() {
        let topic: String = core::iter::repeat_n('x', 65_535).collect();
        let p = Publish::new(topic, Bytes::new());
        let mut dst = BytesMut::new();
        assert!(p.encode(&mut dst, v5()).is_ok());
        assert_eq!(&dst[..2], &[0xFF, 0xFF]);
    }

    #[test]
    fn test_publish_decode_too_short_errors() {
        // Only 1 byte — too short for even a 1-char topic
        let payload = Bytes::from_static(&[0x00]);
        assert_eq!(
            Publish::decode(0x00, payload, v311()),
            Err(Error::PacketTooShort)
        );
    }

    #[test]
    fn test_publish_decode_invalid_qos_errors() {
        // QoS bits = 3 (0x06 in flags = bits 2-1 = 11)
        let payload = Bytes::from_static(&[0x00, 0x01, b't']);
        assert!(Publish::decode(0x06, payload, v311()).is_err());
    }

    // ======================================================================
    // PubAck / PubRec / PubRel / PubComp tests
    // ======================================================================

    // ---- PublishBuilder ----

    #[test]
    fn test_builder_default_matches_new_with_empty_fields() {
        assert_eq!(PublishBuilder::new().finalize(), Publish::new("", ""));
    }

    #[test]
    fn test_builder_sets_every_field() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ContentType,
            PropertyValue::String("text/plain".into()),
        ));
        let p = PublishBuilder::new()
            .topic("a/b")
            .payload("hi")
            .qos(QoS::QoS2)
            .packet_id(77)
            .retain(true)
            .dup(true)
            .properties(props)
            .finalize();

        assert_eq!(p.topic, "a/b");
        assert_eq!(&p.payload[..], b"hi");
        assert_eq!(p.qos, QoS::QoS2);
        assert_eq!(p.packet_id, Some(77));
        assert!(p.retain);
        assert!(p.dup);
        assert_eq!(
            p.properties.get(PropertyId::ContentType),
            Some(&PropertyValue::String("text/plain".into()))
        );
    }

    #[test]
    fn test_publish_builder_shortcut_matches_the_builder() {
        let a = Publish::builder().topic("a").payload("x").finalize();
        let b = PublishBuilder::new().topic("a").payload("x").finalize();
        assert_eq!(a, b);
        assert_eq!(a, Publish::new("a", "x"));
    }

    #[test]
    fn test_builder_matches_the_constructor() {
        assert_eq!(
            PublishBuilder::new().topic("a").payload("x").finalize(),
            Publish::new("a", "x")
        );
    }

    /// Later calls win, so a builder can be reconfigured before finalizing.
    #[test]
    fn test_builder_setters_overwrite() {
        let p = PublishBuilder::new()
            .topic("first")
            .topic("second")
            .qos(QoS::QoS2)
            .qos(QoS::QoS1)
            .finalize();
        assert_eq!(p.topic, "second");
        assert_eq!(p.qos, QoS::QoS1);
    }

    #[test]
    fn test_builder_packet_id_takes_a_value_or_none() {
        assert_eq!(
            PublishBuilder::new().packet_id(5).finalize().packet_id,
            Some(5)
        );
        assert_eq!(
            PublishBuilder::new()
                .packet_id(Some(5))
                .finalize()
                .packet_id,
            Some(5)
        );
        // `None` clears an identifier set earlier, as QoS 0 requires.
        assert_eq!(
            PublishBuilder::new()
                .packet_id(5)
                .packet_id(None)
                .finalize()
                .packet_id,
            None
        );
    }

    #[test]
    fn test_builder_qos_with_id_sets_both() {
        let p = PublishBuilder::new()
            .topic("a")
            .qos_with_id(QoS::QoS1, 9)
            .finalize();
        assert_eq!(p.qos, QoS::QoS1);
        assert_eq!(p.packet_id, Some(9));
        assert_eq!(
            p,
            PublishBuilder::new()
                .topic("a")
                .qos(QoS::QoS1)
                .packet_id(9)
                .finalize()
        );
    }

    #[test]
    fn test_builder_qos1_round_trips_v5() {
        let p = PublishBuilder::new()
            .topic("a/b")
            .payload("hi")
            .qos_with_id(QoS::QoS1, 42)
            .retain(true)
            .properties(publish_props())
            .finalize();

        let mut dst = BytesMut::new();
        p.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), p.encoded_size(v5()));
        // QoS 1 (bit 1) plus retain (bit 0).
        assert_eq!(Publish::decode(0x03, dst.freeze(), v5()).unwrap(), p);
    }

    /// The builder does not validate; a QoS 0 packet with an identifier is
    /// caught where every other bad packet is, on encode.
    #[test]
    fn test_builder_defers_validation_to_encode() {
        let p = PublishBuilder::new().topic("a").packet_id(1).finalize();
        let mut dst = BytesMut::new();
        assert_eq!(p.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }

    // ---- Publish with properties ----

    fn publish_props() -> Properties {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ContentType,
            PropertyValue::String("application/json".into()),
        ));
        props.push(Property::new(
            PropertyId::MessageExpiryInterval,
            PropertyValue::U32(60),
        ));
        props
    }

    /// Adding properties to a `new` packet leaves every other field alone.
    #[test]
    fn test_publish_properties_do_not_disturb_the_other_fields() {
        let mut p = Publish::new("a/b", "hi");
        p.properties = publish_props();
        assert_eq!(p.topic, "a/b");
        assert_eq!(&p.payload[..], b"hi");
        assert_eq!(
            p.properties.get(PropertyId::ContentType),
            Some(&PropertyValue::String("application/json".into()))
        );
        assert_eq!(p.qos, QoS::QoS0);
        assert!(!p.dup);
        assert!(!p.retain);
        assert!(p.packet_id.is_none());
    }

    #[test]
    fn test_publish_with_properties_round_trip_v5() {
        let p = PublishBuilder::new()
            .topic("a/b")
            .payload("hi")
            .properties(publish_props())
            .finalize();
        let mut dst = BytesMut::new();
        p.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), p.encoded_size(v5()));
        assert_eq!(Publish::decode(0, dst.freeze(), v5()).unwrap(), p);
    }

    /// The QoS and packet ID are set together, and must agree.
    #[test]
    fn test_publish_with_properties_promoted_to_qos1_round_trips() {
        let p = PublishBuilder::new()
            .topic("a/b")
            .payload("hi")
            .properties(publish_props())
            .qos_with_id(QoS::QoS1, 9)
            .finalize();
        let mut dst = BytesMut::new();
        p.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), p.encoded_size(v5()));
        // flags carry QoS 1 (bit 1).
        assert_eq!(Publish::decode(0x02, dst.freeze(), v5()).unwrap(), p);
    }

    #[test]
    fn test_publish_with_properties_on_v3_is_an_error() {
        let p = PublishBuilder::new()
            .topic("a")
            .payload("x")
            .properties(publish_props())
            .finalize();
        let mut dst = BytesMut::new();
        assert_eq!(p.encode(&mut dst, v311()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_publish_with_properties_rejects_a_property_not_allowed_here() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::WillDelayInterval,
            PropertyValue::U32(5),
        ));
        let p = PublishBuilder::new()
            .topic("a")
            .payload("x")
            .properties(props)
            .finalize();
        let mut dst = BytesMut::new();
        assert_eq!(p.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }

    fn round_trip_puback(ack: PubAck, ver: MqttVersion) {
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, ver).unwrap();
        let decoded = PubAck::decode(dst.freeze(), ver).unwrap();
        assert_eq!(decoded, ack);
    }

    // ---- v3.1.1: always 2 bytes ----

    #[test]
    fn test_puback_encode_v311() {
        let ack = PubAck::new(42);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[..], &[0x00, 0x2A]); // packet_id = 42
    }

    #[test]
    fn test_puback_round_trip_v311() {
        round_trip_puback(PubAck::new(1), v311());
        round_trip_puback(PubAck::new(0xFFFF), v311());
    }

    #[test]
    fn test_puback_v311_ignores_reason_code() {
        // Even with a non-Success reason code, v311 wire format is still 2 bytes.
        let ack = PubAck::with_properties(5, ReasonCode::NotAuthorized, None);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v311()).unwrap();
        assert_eq!(dst.len(), 2);
        // Decode recovers Success (v311 ignores reason code)
        let decoded = PubAck::decode(dst.freeze(), v311()).unwrap();
        assert_eq!(decoded.reason_code, ReasonCode::Success);
    }

    // ---- v5 shorthand (Success + empty properties = 2 bytes) ----

    #[test]
    fn test_puback_v5_shorthand_encode() {
        let ack = PubAck::new(7);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v5()).unwrap();
        assert_eq!(&dst[..], &[0x00, 0x07]); // packet_id only
    }

    #[test]
    fn test_puback_v5_shorthand_encoded_size() {
        assert_eq!(PubAck::new(1).encoded_size(v5()), 2);
    }

    #[test]
    fn test_puback_v5_shorthand_round_trip() {
        round_trip_puback(PubAck::new(100), v5());
    }

    // ---- v5 full form (non-Success reason code) ----

    #[test]
    fn test_puback_v5_full_encode() {
        let ack = PubAck::with_properties(3, ReasonCode::NotAuthorized, None);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v5()).unwrap();
        // packet_id(2) + reason(1); the property length is omitted when empty.
        assert_eq!(&dst[..], &[0x00, 0x03, 0x87]);
    }

    #[test]
    fn test_puback_v5_full_round_trip() {
        let ack = PubAck::with_properties(200, ReasonCode::PacketIdentifierInUse, None);
        round_trip_puback(ack, v5());
    }

    #[test]
    fn test_puback_v5_full_form_with_properties_encode() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String("no".into()),
        ));
        let ack = PubAck::with_properties(3, ReasonCode::NotAuthorized, props);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v5()).unwrap();
        // id(2) + reason(1) + props_len(1=5) + [0x1F, 0x00, 0x02, 'n', 'o']
        assert_eq!(
            &dst[..],
            &[0x00, 0x03, 0x87, 0x05, 0x1F, 0x00, 0x02, b'n', b'o']
        );
        assert_eq!(ack.encoded_size(v5()), dst.len());
    }

    // ---- v5: shorthand decode (payload == 2 bytes) ----

    #[test]
    fn test_puback_v5_decode_shorthand_payload() {
        // A broker may send just the packet ID with no reason byte.
        let payload = Bytes::from_static(&[0x00, 0x05]);
        let ack = PubAck::decode(payload, v5()).unwrap();
        assert_eq!(ack.packet_id, 5);
        assert_eq!(ack.reason_code, ReasonCode::Success);
        assert!(ack.properties.is_empty());
    }

    #[test]
    fn test_puback_v5_decode_reason_code_no_props() {
        // Remaining length 3: packet ID + reason code, no property length.
        let payload = Bytes::from_static(&[0x00, 0x05, 0x10]);
        let ack = PubAck::decode(payload, v5()).unwrap();
        assert_eq!(ack.packet_id, 5);
        assert_eq!(ack.reason_code, ReasonCode::NoMatchingSubscribers);
        assert!(ack.properties.is_empty());
    }

    // ---- encoded_size ----

    #[test]
    fn test_puback_encoded_size_v311() {
        assert_eq!(PubAck::new(1).encoded_size(v311()), 2);
    }

    #[test]
    fn test_puback_encoded_size_v5_full() {
        let ack = PubAck::with_properties(1, ReasonCode::UnspecifiedError, None);
        // 2 (id) + 1 (reason); the property length is omitted when empty.
        assert_eq!(ack.encoded_size(v5()), 3);
    }

    #[test]
    fn test_puback_encoded_size_matches_encode() {
        let cases = [
            PubAck::new(1),
            PubAck::with_properties(2, ReasonCode::UnspecifiedError, None),
        ];
        for ack in &cases {
            for ver in [v311(), v5()] {
                let mut dst = BytesMut::new();
                ack.encode(&mut dst, ver).unwrap();
                assert_eq!(dst.len(), ack.encoded_size(ver));
            }
        }
    }

    // ---- error cases ----

    #[test]
    fn test_puback_decode_too_short() {
        assert_eq!(
            PubAck::decode(Bytes::from_static(&[0x00]), v311()),
            Err(Error::PacketTooShort)
        );
    }

    #[test]
    fn test_puback_v5_decode_unknown_reason_code_errors() {
        // 0x05 is undefined; must not be reinterpreted as an error code.
        assert_eq!(
            PubAck::decode(Bytes::from_static(&[0x00, 0x01, 0x05]), v5()),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            PubAck::decode(Bytes::from_static(&[0x00, 0x01, 0xFF, 0x00]), v5()),
            Err(Error::ProtocolError)
        );
    }

    // ---- v3 acknowledgements cannot carry properties ----

    #[test]
    fn test_puback_encode_props_on_v3_is_protocol_error() {
        let mut ack = PubAck::new(1);
        ack.properties.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String("x".into()),
        ));
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v311()), Err(Error::ProtocolError));
        // Same packet is fine on v5.
        let mut dst = BytesMut::new();
        assert!(ack.encode(&mut dst, v5()).is_ok());
    }

    // ---- constructors ----

    fn reason_props(text: &str) -> Properties {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String(text.into()),
        ));
        props
    }

    #[test]
    fn test_puback_with_properties_keeps_all_arguments() {
        let ack = PubAck::with_properties(
            99,
            ReasonCode::NoMatchingSubscribers,
            reason_props("nobody listening"),
        );
        assert_eq!(ack.packet_id, 99);
        assert_eq!(ack.reason_code, ReasonCode::NoMatchingSubscribers);
        assert_eq!(
            ack.properties.get(PropertyId::ReasonString),
            Some(&PropertyValue::String("nobody listening".into()))
        );
    }

    /// The three spellings of "a plain success ack" agree: `new`, and
    /// `with_properties` with `None` or an empty `Properties`.
    #[test]
    fn test_puback_with_properties_success_and_empty_matches_new() {
        let expected = PubAck::new(4);
        assert_eq!(
            PubAck::with_properties(4, ReasonCode::Success, None),
            expected
        );
        assert_eq!(
            PubAck::with_properties(4, ReasonCode::Success, Properties::new()),
            expected
        );
    }

    /// `properties` takes a `Properties`, a `Some(Properties)`, or `None`.
    #[test]
    fn test_puback_with_properties_accepts_properties_by_value_or_option() {
        let props = reason_props("denied");
        let bare = PubAck::with_properties(1, ReasonCode::NotAuthorized, props.clone());
        assert_eq!(
            PubAck::with_properties(1, ReasonCode::NotAuthorized, Some(props)),
            bare
        );
        assert!(
            PubAck::with_properties(1, ReasonCode::NotAuthorized, None)
                .properties
                .is_empty()
        );
    }

    #[test]
    fn test_puback_new_is_the_default_but_for_the_id() {
        let ack = PubAck::new(4);
        assert_eq!(ack.reason_code, ReasonCode::Success);
        assert!(ack.properties.is_empty());
        assert_eq!(
            ack,
            PubAck {
                packet_id: 4,
                ..Default::default()
            }
        );
    }

    /// `with_properties` reaches all three v5 wire forms; which one is used
    /// is decided by the arguments, not by the constructor.
    #[test]
    fn test_puback_with_properties_round_trip_v5_all_forms() {
        for ack in [
            PubAck::with_properties(1, ReasonCode::Success, None),
            PubAck::with_properties(2, ReasonCode::NotAuthorized, None),
            PubAck::with_properties(3, ReasonCode::NotAuthorized, reason_props("denied")),
        ] {
            let mut dst = BytesMut::new();
            ack.encode(&mut dst, v5()).unwrap();
            assert_eq!(dst.len(), ack.encoded_size(v5()));
            round_trip_puback(ack, v5());
        }
    }

    #[test]
    fn test_puback_with_properties_on_v3_is_an_error() {
        let ack = PubAck::with_properties(1, ReasonCode::Success, reason_props("why"));
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v311()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_puback_with_properties_rejects_a_property_not_allowed_here() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(1),
        ));
        let ack = PubAck::with_properties(1, ReasonCode::Success, props);
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }

    /// The reason code is still checked against the packet type: `new` takes
    /// any `ReasonCode`, and an inappropriate one fails at the `Packet` level.
    #[test]
    fn test_puback_with_properties_with_a_reason_code_not_allowed_here() {
        let ack = PubAck::with_properties(1, ReasonCode::GrantedQos2, None);
        let mut dst = BytesMut::new();
        assert_eq!(
            crate::Packet::PubAck(ack).encode(&mut dst, v5()),
            Err(Error::ProtocolError)
        );
    }

    // ---- A QoS 0 PUBLISH must not carry a packet identifier ----

    #[test]
    fn test_publish_qos0_with_packet_id_is_protocol_error_on_encode() {
        let mut p = Publish::new("a", "x");
        p.packet_id = Some(7);
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            assert_eq!(p.encode(&mut dst, ver), Err(Error::ProtocolError), "{ver}");
            let mut dst = BytesMut::new();
            assert_eq!(
                p.encode_header(&mut dst, ver),
                Err(Error::ProtocolError),
                "{ver}"
            );
        }
        p.packet_id = None;
        let mut dst = BytesMut::new();
        assert!(p.encode(&mut dst, v5()).is_ok());
    }
}
