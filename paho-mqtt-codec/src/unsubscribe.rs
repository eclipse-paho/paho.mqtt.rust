// paho-mqtt-codec/src/unsubscribe.rs
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

//! MQTT UNSUBSCRIBE and UNSUBACK packets.
//!
//! UNSUBSCRIBE is a list of topic filters with no per-filter options.
//!
//! UNSUBACK has an asymmetry between versions: v3 carries only the
//! packet identifier (no reason codes), while v5 adds properties and one
//! reason code per topic filter. The unified [`UnsubAck`] struct represents
//! both — `reason_codes` is an empty `Vec` on v3 connections.

use crate::{
    Error, MqttVersion, PacketType, Properties, ReasonCode, Result, String, Vec, topic,
    wire::{check_packet_id, expect_end, get_utf8, put_utf8, utf8_wire_size},
};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// An UNSUBSCRIBE packet, unified across MQTT v3.1, v3.1.1, and v5.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Unsubscribe {
    /// Packet identifier.
    pub packet_id: u16,
    /// Topic filters to remove. Must not be empty.
    pub topic_filters: Vec<String>,
    /// MQTT v5 properties; empty on v3 connections.
    pub properties: Properties,
}

impl Unsubscribe {
    /// Creates an UNSUBSCRIBE with no properties.
    pub fn new(packet_id: u16, topic_filters: Vec<String>) -> Self {
        Self {
            packet_id,
            topic_filters,
            properties: Properties::new(),
        }
    }

    /// Creates an UNSUBSCRIBE carrying MQTT v5 properties.
    ///
    /// v5 only.
    ///
    /// Only `UserProperty` is permitted here (§3.10.2.1);
    /// [`encode`](Self::encode) rejects anything else, including the
    /// `SubscriptionIdentifier` a SUBSCRIBE may carry. A v3 connection cannot
    /// carry properties at all.
    pub fn with_properties(
        packet_id: u16,
        topic_filters: Vec<String>,
        properties: Properties,
    ) -> Self {
        Self {
            packet_id,
            topic_filters,
            properties,
        }
    }

    /// Returns the remaining-length byte count for version, `ver`.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        let mut size = 2 + self.properties.encoded_size(ver);
        for filter in &self.topic_filters {
            size += utf8_wire_size(filter);
        }
        size
    }

    /// Encodes the variable header and payload into `dst`.
    ///
    /// Returns [`Error::ProtocolError`] if `packet_id` is 0 or
    /// `topic_filters` is empty (MQTT-3.10.3-2).
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        if self.topic_filters.is_empty() {
            return Err(Error::ProtocolError);
        }
        dst.put_u16(check_packet_id(self.packet_id)?);
        self.properties.check_context(PacketType::Unsubscribe)?;
        self.properties.encode(dst, ver)?;
        for filter in &self.topic_filters {
            topic::validate_filter(filter)?;
            put_utf8(filter, dst)?;
        }
        Ok(())
    }

    /// Decodes from the raw `payload` bytes for the given version, `ver`.
    pub fn decode(mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }
        let packet_id = check_packet_id(payload.get_u16())?;
        let properties = Properties::decode_in(&mut payload, ver, PacketType::Unsubscribe)?;
        let mut topic_filters = Vec::new();
        while payload.has_remaining() {
            let filter = get_utf8(&mut payload)?;
            topic::validate_filter(&filter)?;
            topic_filters.push(filter);
        }

        if topic_filters.is_empty() {
            return Err(Error::ProtocolError); // UNSUBSCRIBE must have ≥1 filter (MQTT-3.10.3-2)
        }

        Ok(Self {
            packet_id,
            topic_filters,
            properties,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////

/// An UNSUBACK packet, unified across MQTT v3.1, v3.1.1, and v5.
///
/// In v3, UNSUBACK carries only the packet identifier — `reason_codes`
/// and `properties` must be empty; encoding a v3 UNSUBACK with either set
/// is an error.
/// In v5, one reason code is present per topic filter from the UNSUBSCRIBE.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct UnsubAck {
    /// Packet identifier matching the original UNSUBSCRIBE.
    pub packet_id: u16,
    /// One reason code per unsubscribed topic filter (v5 only; empty on v3).
    pub reason_codes: Vec<ReasonCode>,
    /// MQTT v5 properties; empty on v3 connections.
    pub properties: Properties,
}

impl UnsubAck {
    /// Creates an UNSUBACK with no properties.
    pub fn new(packet_id: u16, reason_codes: Vec<ReasonCode>) -> Self {
        Self {
            packet_id,
            reason_codes,
            properties: Properties::new(),
        }
    }

    /// Creates an UNSUBACK carrying MQTT v5 properties.
    ///
    /// v5 only.
    ///
    /// Only `ReasonString` and `UserProperty` are permitted here (§3.11.2.1);
    /// [`encode`](Self::encode) rejects anything else. On v3 an UNSUBACK is
    /// the packet identifier and nothing more — neither properties nor reason
    /// codes can be sent — so encoding either for v3 is an error.
    pub fn with_properties(
        packet_id: u16,
        reason_codes: Vec<ReasonCode>,
        properties: Properties,
    ) -> Self {
        Self {
            packet_id,
            reason_codes,
            properties,
        }
    }

    /// Returns the remaining-length byte count for version, `ver`.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        let rc_len = if ver == MqttVersion::V5 {
            self.reason_codes.len()
        } else {
            0
        };
        2 + self.properties.encoded_size(ver) + rc_len
    }

    /// Encodes the variable header and payload into `dst`.
    ///
    /// Returns [`Error::ProtocolError`] if `packet_id` is 0, if `ver` is v5
    /// and `reason_codes` is empty (§3.11.3), or if `ver` is v3 and
    /// `reason_codes` is *not* empty — a v3 UNSUBACK cannot carry them, and
    /// dropping them silently would hide a caller's mistake.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        if (ver == MqttVersion::V5) == self.reason_codes.is_empty() {
            return Err(Error::ProtocolError);
        }
        dst.put_u16(check_packet_id(self.packet_id)?);
        self.properties.check_context(PacketType::UnsubAck)?;
        self.properties.encode(dst, ver)?;
        if ver == MqttVersion::V5 {
            for &rc in &self.reason_codes {
                dst.put_u8(rc as u8);
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
        let properties = Properties::decode_in(&mut payload, ver, PacketType::UnsubAck)?;
        let reason_codes = if ver == MqttVersion::V5 {
            let codes = payload
                .iter()
                .copied()
                .map(ReasonCode::try_from)
                .collect::<Result<Vec<_>>>()?;
            if codes.is_empty() {
                return Err(Error::ProtocolError); // one code per filter (§3.11.3)
            }
            codes
        } else {
            // v3 UNSUBACK is exactly the packet identifier.
            expect_end(&payload)?;
            Vec::new()
        };

        Ok(UnsubAck {
            packet_id,
            reason_codes,
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
        Property, PropertyId, PropertyValue,
        types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5},
    };
    use alloc::{string::ToString, vec};
    use bytes::BytesMut;

    fn v311() -> MqttVersion {
        MQTT_VERSION_3_1_1
    }

    fn v5() -> MqttVersion {
        MQTT_VERSION_5
    }

    fn filters(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    // ======================================================================
    // Unsubscribe
    // ======================================================================

    fn round_trip_unsub(u: Unsubscribe, ver: MqttVersion) {
        let mut dst = BytesMut::new();
        u.encode(&mut dst, ver).unwrap();
        let decoded = Unsubscribe::decode(dst.freeze(), ver).unwrap();
        assert_eq!(decoded, u);
    }

    #[test]
    fn test_unsubscribe_encode_single_v311() {
        let u = Unsubscribe::new(1, filters(&["a/b"]));
        let mut dst = BytesMut::new();
        u.encode(&mut dst, v311()).unwrap();
        // packet_id=1, topic len=3, "a/b"
        assert_eq!(&dst[..], &[0x00, 0x01, 0x00, 0x03, b'a', b'/', b'b']);
    }

    #[test]
    fn test_unsubscribe_v5_has_properties_byte() {
        let u = Unsubscribe::new(1, filters(&["t"]));
        let mut dst = BytesMut::new();
        u.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst[2], 0x00); // empty properties VBI
    }

    #[test]
    fn test_unsubscribe_round_trip_single_v311() {
        round_trip_unsub(Unsubscribe::new(5, filters(&["sensors/#"])), v311());
    }

    #[test]
    fn test_unsubscribe_round_trip_multi_v311() {
        round_trip_unsub(
            Unsubscribe::new(10, filters(&["a/b", "c/+/d", "#"])),
            v311(),
        );
    }

    #[test]
    fn test_unsubscribe_round_trip_single_v5() {
        round_trip_unsub(Unsubscribe::new(3, filters(&["t/u"])), v5());
    }

    #[test]
    fn test_unsubscribe_round_trip_multi_v5() {
        round_trip_unsub(Unsubscribe::new(99, filters(&["a/b", "c/d"])), v5());
    }

    #[test]
    fn test_unsubscribe_encoded_size_v311() {
        let u = Unsubscribe::new(1, filters(&["abc"]));
        // 2 (id) + (2+3) = 7
        assert_eq!(u.encoded_size(v311()), 7);
    }

    #[test]
    fn test_unsubscribe_encoded_size_v5() {
        let u = Unsubscribe::new(1, filters(&["abc"]));
        // 2 (id) + 1 (props VBI) + (2+3) = 8
        assert_eq!(u.encoded_size(v5()), 8);
    }

    #[test]
    fn test_unsubscribe_encoded_size_matches_encode() {
        let u = Unsubscribe::new(7, filters(&["a/b", "c/#"]));
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            u.encode(&mut dst, ver).unwrap();
            assert_eq!(dst.len(), u.encoded_size(ver));
        }
    }

    #[test]
    fn test_unsubscribe_rejects_invalid_filter_both_directions() {
        let u = Unsubscribe::new(1, vec!["a/+b".to_string()]);
        let mut dst = BytesMut::new();
        assert_eq!(u.encode(&mut dst, v5()), Err(Error::TopicFilterInvalid));
        let mut src = BytesMut::new();
        src.put_u16(1);
        src.put_u8(0);
        put_utf8("a/+b", &mut src).unwrap();
        assert_eq!(
            Unsubscribe::decode(src.freeze(), v5()),
            Err(Error::TopicFilterInvalid)
        );
    }

    #[test]
    fn test_unsubscribe_empty_filters_errors() {
        let payload = Bytes::from_static(&[0x00, 0x01]); // id only, no filters
        assert_eq!(
            Unsubscribe::decode(payload, v311()),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_unsubscribe_too_short_errors() {
        assert_eq!(
            Unsubscribe::decode(Bytes::from_static(&[0x00]), v311()),
            Err(Error::PacketTooShort)
        );
    }

    // ======================================================================
    // UnsubAck
    // ======================================================================

    fn round_trip_unsuback(ua: UnsubAck, ver: MqttVersion) {
        let mut dst = BytesMut::new();
        ua.encode(&mut dst, ver).unwrap();
        let decoded = UnsubAck::decode(dst.freeze(), ver).unwrap();
        assert_eq!(decoded, ua);
    }

    // ---- v3.1.1: always 2 bytes, no reason codes ----

    #[test]
    fn test_unsuback_encode_v311() {
        let ua = UnsubAck::new(42, vec![]);
        let mut dst = BytesMut::new();
        ua.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[..], &[0x00, 0x2A]);
    }

    #[test]
    fn test_unsuback_v311_rejects_reason_codes() {
        // A v3 UNSUBACK cannot carry reason codes; refusing to encode is
        // better than dropping them silently.
        let ua = UnsubAck::new(1, vec![ReasonCode::Success, ReasonCode::NotAuthorized]);
        let mut dst = BytesMut::new();
        assert_eq!(ua.encode(&mut dst, v311()), Err(Error::ProtocolError));
        assert!(dst.is_empty());
    }

    #[test]
    fn test_unsuback_round_trip_v311() {
        // Round-trip always produces empty reason_codes on v3.1.1.
        let ua = UnsubAck::new(7, vec![]);
        round_trip_unsuback(ua, v311());
    }

    #[test]
    fn test_unsuback_decode_v311_rejects_extra_bytes() {
        // A v3 UNSUBACK is exactly the packet identifier; a v5 encoding of
        // the same packet carries more and must not decode as v3.
        let mut dst = BytesMut::new();
        let ua_v5 = UnsubAck::new(3, vec![ReasonCode::Success]);
        ua_v5.encode(&mut dst, v5()).unwrap();
        assert_eq!(
            UnsubAck::decode(dst.freeze(), v311()),
            Err(Error::MalformedPacket)
        );
    }

    // ---- v5: properties + reason codes ----

    #[test]
    fn test_unsuback_encode_v5() {
        let ua = UnsubAck::new(1, vec![ReasonCode::Success, ReasonCode::NotAuthorized]);
        let mut dst = BytesMut::new();
        ua.encode(&mut dst, v5()).unwrap();
        // id(2) + props_len(1=0x00) + rc0(0x00) + rc1(0x87)
        assert_eq!(&dst[..], &[0x00, 0x01, 0x00, 0x00, 0x87]);
    }

    #[test]
    fn test_unsuback_round_trip_v5_single() {
        round_trip_unsuback(UnsubAck::new(5, vec![ReasonCode::Success]), v5());
    }

    #[test]
    fn test_unsuback_round_trip_v5_multi() {
        round_trip_unsuback(
            UnsubAck::new(
                9,
                vec![
                    ReasonCode::Success,
                    ReasonCode::NoSubscriptionFound,
                    ReasonCode::NotAuthorized,
                ],
            ),
            v5(),
        );
    }

    #[test]
    fn test_unsuback_v5_decode_empty_reason_codes_errors() {
        // UNSUBSCRIBE carries at least one filter, so UNSUBACK carries at least
        // one reason code (§3.11.3): packet_id + empty properties only.
        assert_eq!(
            UnsubAck::decode(Bytes::from_static(&[0x00, 0x01, 0x00]), v5()),
            Err(Error::ProtocolError)
        );
        // v3.1.1 UNSUBACK has no reason codes at all, so this stays valid.
        round_trip_unsuback(UnsubAck::new(1, vec![]), v311());
    }

    // ---- encoded_size ----

    #[test]
    fn test_unsuback_encoded_size_v311() {
        assert_eq!(UnsubAck::new(1, vec![]).encoded_size(v311()), 2);
    }

    #[test]
    fn test_unsuback_encoded_size_v5() {
        let ua = UnsubAck::new(1, vec![ReasonCode::Success, ReasonCode::Success]);
        // 2 (id) + 1 (props VBI) + 2 (reason codes) = 5
        assert_eq!(ua.encoded_size(v5()), 5);
    }

    #[test]
    fn test_unsuback_encoded_size_matches_encode() {
        let ua = UnsubAck::new(3, vec![ReasonCode::Success, ReasonCode::NotAuthorized]);
        let mut dst = BytesMut::new();
        ua.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), ua.encoded_size(v5()));

        let ua = UnsubAck::new(3, vec![]);
        let mut dst = BytesMut::new();
        ua.encode(&mut dst, v311()).unwrap();
        assert_eq!(dst.len(), ua.encoded_size(v311()));
    }

    #[test]
    fn test_unsuback_too_short_errors() {
        assert_eq!(
            UnsubAck::decode(Bytes::from_static(&[0x00]), v311()),
            Err(Error::PacketTooShort)
        );
    }

    #[test]
    fn test_unsuback_v5_decode_unknown_reason_code_errors() {
        assert_eq!(
            UnsubAck::decode(Bytes::from_static(&[0x00, 0x01, 0x00, 0x00, 0x12]), v5()),
            Err(Error::ProtocolError)
        );
    }

    // ---- Empty lists are rejected on encode as they are on decode ----

    #[test]
    fn test_unsubscribe_encode_empty_list_is_protocol_error() {
        let unsub = Unsubscribe::new(1, vec![]);
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            assert_eq!(unsub.encode(&mut dst, ver), Err(Error::ProtocolError));
            assert!(dst.is_empty());
        }
    }

    #[test]
    fn test_unsuback_encode_empty_codes_is_protocol_error_on_v5_only() {
        let ack = UnsubAck::new(1, vec![]);
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v5()), Err(Error::ProtocolError));
        assert!(dst.is_empty());
        // v3 has no codes on the wire, so an empty list is the normal case.
        let mut dst = BytesMut::new();
        assert!(ack.encode(&mut dst, v311()).is_ok());
        assert_eq!(&dst[..], &[0x00, 0x01]);
    }

    // ---- with_properties ----

    fn user_props(k: &str, v: &str) -> Properties {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair(k.into(), v.into()),
        ));
        props
    }

    #[test]
    fn test_unsubscribe_with_properties_keeps_all_arguments() {
        let filters = vec!["a/b".to_string()];
        let unsub = Unsubscribe::with_properties(4, filters.clone(), user_props("why", "done"));
        assert_eq!(unsub.packet_id, 4);
        assert_eq!(unsub.topic_filters, filters);
        assert_eq!(
            unsub.properties.get(PropertyId::UserProperty),
            Some(&PropertyValue::StringPair("why".into(), "done".into()))
        );
    }

    #[test]
    fn test_unsubscribe_with_properties_empty_matches_new() {
        let filters = vec!["a".to_string()];
        assert_eq!(
            Unsubscribe::with_properties(1, filters.clone(), Properties::new()),
            Unsubscribe::new(1, filters)
        );
    }

    #[test]
    fn test_unsubscribe_with_properties_round_trip_v5() {
        let mut props = user_props("a", "1");
        props.push(Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair("b".into(), "2".into()),
        ));
        let unsub =
            Unsubscribe::with_properties(11, vec!["a/+".to_string(), "b/#".to_string()], props);

        let mut dst = BytesMut::new();
        unsub.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), unsub.encoded_size(v5()));
        assert_eq!(Unsubscribe::decode(dst.freeze(), v5()).unwrap(), unsub);
    }

    #[test]
    fn test_unsubscribe_with_properties_on_v3_is_an_error() {
        let unsub = Unsubscribe::with_properties(1, vec!["a".to_string()], user_props("k", "v"));
        let mut dst = BytesMut::new();
        assert_eq!(unsub.encode(&mut dst, v311()), Err(Error::ProtocolError));
    }

    /// UNSUBSCRIBE takes only UserProperty — not even the ReasonString a
    /// SUBACK may carry (§3.10.2.1).
    #[test]
    fn test_unsubscribe_with_properties_rejects_a_property_not_allowed_here() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String("why".into()),
        ));
        let unsub = Unsubscribe::with_properties(1, vec!["a".to_string()], props);
        let mut dst = BytesMut::new();
        assert_eq!(unsub.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_unsuback_with_properties_keeps_all_arguments() {
        let codes = vec![ReasonCode::Success, ReasonCode::NoSubscriptionFound];
        let ack = UnsubAck::with_properties(6, codes.clone(), user_props("k", "v"));
        assert_eq!(ack.packet_id, 6);
        assert_eq!(ack.reason_codes, codes);
        assert_eq!(ack.properties.len(), 1);
    }

    #[test]
    fn test_unsuback_with_properties_empty_matches_new() {
        let codes = vec![ReasonCode::Success];
        assert_eq!(
            UnsubAck::with_properties(1, codes.clone(), Properties::new()),
            UnsubAck::new(1, codes)
        );
    }

    #[test]
    fn test_unsuback_with_properties_round_trip_v5() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String("one was not subscribed".into()),
        ));
        props.push(Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair("a".into(), "1".into()),
        ));
        let ack = UnsubAck::with_properties(
            12,
            vec![ReasonCode::Success, ReasonCode::NoSubscriptionFound],
            props,
        );

        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), ack.encoded_size(v5()));
        assert_eq!(UnsubAck::decode(dst.freeze(), v5()).unwrap(), ack);
    }

    /// On v3 an UNSUBACK is the packet identifier and nothing else, so
    /// properties are refused — as reason codes already are.
    #[test]
    fn test_unsuback_with_properties_on_v3_is_an_error() {
        let ack = UnsubAck::with_properties(1, vec![], user_props("k", "v"));
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v311()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_unsuback_with_properties_rejects_a_property_not_allowed_here() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(1),
        ));
        let ack = UnsubAck::with_properties(1, vec![ReasonCode::Success], props);
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }
}
