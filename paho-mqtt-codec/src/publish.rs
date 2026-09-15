// paho-mqtt-codec/src/publish.rs
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

//! MQTT PUBLISH packet and its acknowledgements.
//!
//! PUBLISH carries the DUP, QoS, and RETAIN flags in the fixed-header byte
//! rather than the payload, so [`Publish::decode`] requires those flags as
//! a separate parameter extracted from [`crate::packet::RawPacket::header`].

use crate::{
    Error, MqttVersion, Properties, QoS, ReasonCode, Result, String,
    wire::{get_utf8, put_utf8, utf8_wire_size},
};
use bytes::{Buf, BufMut, Bytes};

/////////////////////////////////////////////////////////////////////////////

/// A PUBLISH packet, unified across MQTT v3.1.1 and v5.
///
/// The `properties` field is always empty on v3.1.1 connections and is not
/// encoded on the wire for that version.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Publish {
    /// Duplicate delivery flag — set when re-sending an unacknowledged packet.
    pub dup: bool,
    /// Quality of Service level.
    pub qos: QoS,
    /// Retain flag — broker should store this message for new subscribers.
    pub retain: bool,
    /// Topic name.
    pub topic: String,
    /// Packet identifier; required for QoS 1 and 2, absent for QoS 0.
    pub packet_id: Option<u16>,
    /// Application message payload.
    pub payload: Bytes,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl Publish {
    /// Creates a QoS 0 PUBLISH with no DUP, no retain, and no properties.
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Publish {
            dup: false,
            qos: QoS::QoS0,
            retain: false,
            topic: topic.into(),
            packet_id: None,
            payload: payload.into(),
            properties: Properties::new(),
        }
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
    /// Returns [`Error::MalformedPacket`] if `qos > 0` but `packet_id` is `None`.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        // Topic
        put_utf8(&self.topic, dst)?;
        // Packet ID (QoS 1 and 2 only)
        if self.qos != QoS::QoS0 {
            let id = self.packet_id.ok_or(Error::MalformedPacket)?;
            dst.put_u16(id);
        }
        // v5 properties
        self.properties.encode(dst, ver)?;
        // Payload
        dst.put_slice(&self.payload);
        Ok(())
    }

    /// Decodes a PUBLISH from the fixed-header `flags` (lower 4 bits of the
    /// first header byte) and the raw `payload` bytes for the given version, `ver`.
    pub fn decode(flags: u8, mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        let dup = flags & 0x08 != 0;
        let qos = QoS::try_from((flags >> 1) & 0x03)?;
        let retain = flags & 0x01 != 0;

        // Topic
        let topic = get_utf8(&mut payload)?;

        // Packet ID (QoS 1 and 2 only)
        let packet_id = if qos != QoS::QoS0 {
            if payload.remaining() < 2 {
                return Err(Error::PacketTooShort);
            }
            Some(payload.get_u16())
        }
        else {
            None
        };

        // v5 properties
        let properties = Properties::decode(&mut payload, ver)?;

        // Remainder is the application payload
        let msg_payload = payload;

        Ok(Publish {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            payload: msg_payload,
            properties,
        })
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
pub struct PubAck {
    /// Packet identifier matching the original PUBLISH.
    pub packet_id: u16,
    /// Outcome of the operation; defaults to [`ReasonCode::Success`].
    pub reason_code: ReasonCode,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

/// PUBREC acknowledgement — same structure as [`PubAck`].
pub type PubRec = PubAck;
/// PUBREL acknowledgement — same structure as [`PubAck`].
pub type PubRel = PubAck;
/// PUBCOMP acknowledgement — same structure as [`PubAck`].
pub type PubComp = PubAck;

impl PubAck {
    /// Creates a success acknowledgement with empty properties.
    pub fn new(packet_id: u16) -> Self {
        PubAck {
            packet_id,
            reason_code: ReasonCode::Success,
            properties: Properties::new(),
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
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        dst.put_u16(self.packet_id);
        if ver != MqttVersion::V5 {
            return Ok(());
        }
        match (self.reason_code, self.properties.is_empty()) {
            (ReasonCode::Success, true) => {}
            (rc, true) => dst.put_u8(rc as u8),
            (rc, false) => {
                dst.put_u8(rc as u8);
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
        let packet_id = payload.get_u16();

        let (reason_code, properties) = match ver {
            MqttVersion::V5 => {
                if payload.is_empty() {
                    // Shorthand: no reason code byte → implicit Success
                    (ReasonCode::Success, Properties::new())
                }
                else {
                    let rc = ReasonCode::try_from(payload.get_u8())?;
                    // Property length is omitted when the remaining length < 4.
                    let props = if payload.is_empty() {
                        Properties::new()
                    }
                    else {
                        Properties::decode(&mut payload, ver)?
                    };
                    (rc, props)
                }
            }
            _ => (ReasonCode::Success, Properties::new()),
        };

        Ok(PubAck {
            packet_id,
            reason_code,
            properties,
        })
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
        MqttVersion::from(MQTT_VERSION_3_1_1)
    }

    fn v5() -> MqttVersion {
        MqttVersion::from(MQTT_VERSION_5)
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
    fn test_publish_qos1_missing_packet_id_errors() {
        let pub_ = Publish {
            qos: QoS::AtLeastOnce,
            packet_id: None, // missing!
            topic: "t".into(),
            ..Default::default()
        };
        let mut dst = BytesMut::new();
        assert_eq!(pub_.encode(&mut dst, v311()), Err(Error::MalformedPacket));
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
        let ack = PubAck {
            packet_id: 5,
            reason_code: ReasonCode::NotAuthorized,
            properties: Properties::new(),
        };
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
        let ack = PubAck {
            packet_id: 3,
            reason_code: ReasonCode::NotAuthorized,
            properties: Properties::new(),
        };
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v5()).unwrap();
        // packet_id(2) + reason(1); the property length is omitted when empty.
        assert_eq!(&dst[..], &[0x00, 0x03, 0x87]);
    }

    #[test]
    fn test_puback_v5_full_round_trip() {
        let ack = PubAck {
            packet_id: 200,
            reason_code: ReasonCode::PacketIdentifierInUse,
            properties: Properties::new(),
        };
        round_trip_puback(ack, v5());
    }

    #[test]
    fn test_puback_v5_full_form_with_properties_encode() {
        let mut ack = PubAck::new(3);
        ack.reason_code = ReasonCode::NotAuthorized;
        ack.properties.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String("no".into()),
        ));
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
        let ack = PubAck {
            packet_id: 1,
            reason_code: ReasonCode::UnspecifiedError,
            properties: Properties::new(),
        };
        // 2 (id) + 1 (reason); the property length is omitted when empty.
        assert_eq!(ack.encoded_size(v5()), 3);
    }

    #[test]
    fn test_puback_encoded_size_matches_encode() {
        let cases = [
            PubAck::new(1),
            PubAck {
                packet_id: 2,
                reason_code: ReasonCode::UnspecifiedError,
                properties: Properties::new(),
            },
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
            Err(Error::MalformedPacket)
        );
        assert_eq!(
            PubAck::decode(Bytes::from_static(&[0x00, 0x01, 0xFF, 0x00]), v5()),
            Err(Error::MalformedPacket)
        );
    }
}
