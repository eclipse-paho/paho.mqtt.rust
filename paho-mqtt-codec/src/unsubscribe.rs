// paho-mqtt-codec/src/unsubscribe.rs
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

//! MQTT UNSUBSCRIBE and UNSUBACK packets.
//!
//! UNSUBSCRIBE is a list of topic filters with no per-filter options.
//!
//! UNSUBACK has an asymmetry between versions: v3.1.1 carries only the
//! packet identifier (no reason codes), while v5 adds properties and one
//! reason code per topic filter. The unified [`UnsubAck`] struct represents
//! both — `reason_codes` is an empty `Vec` on v3.1.1 connections.

use crate::{
    Error, MqttVersion, Properties, ReasonCode, Result, String, Vec,
    wire::{get_utf8, put_utf8, utf8_wire_size},
};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// An UNSUBSCRIBE packet, unified across MQTT v3.1.1 and v5.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Unsubscribe {
    /// Packet identifier.
    pub packet_id: u16,
    /// Topic filters to remove. Must not be empty.
    pub topic_filters: Vec<String>,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl Unsubscribe {
    /// Creates an UNSUBSCRIBE with no properties.
    pub fn new(packet_id: u16, topic_filters: Vec<String>) -> Self {
        Unsubscribe {
            packet_id,
            topic_filters,
            properties: Properties::new(),
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
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        dst.put_u16(self.packet_id);
        self.properties.encode(dst, ver)?;
        for filter in &self.topic_filters {
            put_utf8(filter, dst)?;
        }
        Ok(())
    }

    /// Decodes from the raw `payload` bytes for the given version, `ver`.
    pub fn decode(mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }
        let packet_id = payload.get_u16();
        let properties = Properties::decode(&mut payload, ver)?;
        let mut topic_filters = Vec::new();
        while payload.has_remaining() {
            topic_filters.push(get_utf8(&mut payload)?);
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

/// An UNSUBACK packet, unified across MQTT v3.1.1 and v5.
///
/// In v3.1.1, UNSUBACK carries only the packet identifier — `reason_codes`
/// and `properties` are always empty and are not encoded on the wire.
/// In v5, one reason code is present per topic filter from the UNSUBSCRIBE.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct UnsubAck {
    /// Packet identifier matching the original UNSUBSCRIBE.
    pub packet_id: u16,
    /// One reason code per unsubscribed topic filter (v5 only; empty on v3.1.1).
    pub reason_codes: Vec<ReasonCode>,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl UnsubAck {
    /// Creates an UNSUBACK with no properties.
    pub fn new(packet_id: u16, reason_codes: Vec<ReasonCode>) -> Self {
        UnsubAck {
            packet_id,
            reason_codes,
            properties: Properties::new(),
        }
    }

    /// Returns the remaining-length byte count for version, `ver`.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        let rc_len = if ver == MqttVersion::V5 {
            self.reason_codes.len()
        }
        else {
            0
        };
        2 + self.properties.encoded_size(ver) + rc_len
    }

    /// Encodes the variable header and payload into `dst`.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        dst.put_u16(self.packet_id);
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
        let packet_id = payload.get_u16();
        let properties = Properties::decode(&mut payload, ver)?;
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
        }
        else {
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
    use crate::types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5};
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
    fn test_unsuback_v311_ignores_reason_codes() {
        // Reason codes are present in the struct but must not appear on the wire.
        let ua = UnsubAck::new(1, vec![ReasonCode::Success, ReasonCode::NotAuthorized]);
        let mut dst = BytesMut::new();
        ua.encode(&mut dst, v311()).unwrap();
        assert_eq!(dst.len(), 2);
    }

    #[test]
    fn test_unsuback_round_trip_v311() {
        // Round-trip always produces empty reason_codes on v3.1.1.
        let ua = UnsubAck::new(7, vec![]);
        round_trip_unsuback(ua, v311());
    }

    #[test]
    fn test_unsuback_decode_v311_ignores_extra_bytes() {
        // v3.1.1 decode reads only 2 bytes; reason_codes stays empty.
        let mut dst = BytesMut::new();
        let ua_v5 = UnsubAck::new(3, vec![ReasonCode::Success]);
        ua_v5.encode(&mut dst, v5()).unwrap();
        // Decode the same bytes as v3.1.1 — reason codes are ignored.
        let decoded = UnsubAck::decode(dst.freeze(), v311()).unwrap();
        assert_eq!(decoded.packet_id, 3);
        assert!(decoded.reason_codes.is_empty());
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
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            ua.encode(&mut dst, ver).unwrap();
            assert_eq!(dst.len(), ua.encoded_size(ver));
        }
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
}
