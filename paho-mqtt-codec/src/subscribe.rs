// paho-mqtt-codec/src/subscribe.rs
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

//! MQTT SUBSCRIBE and SUBACK packets.
//!
//! SUBSCRIBE carries per-subscription options in a single byte. In v3
//! that byte is just the requested QoS; in v5 it also carries `no_local`,
//! `retain_as_published`, and `retain_handling` flags. The unified
//! [`Subscription`] struct carries all fields; v3 encode writes only
//! the QoS bits and v3 decode leaves the v5 fields at their defaults.

use crate::{
    Error, MqttVersion, PacketType, Properties, QoS, ReasonCode, Result, String, Vec, topic,
    wire::{check_packet_id, get_utf8, put_utf8, utf8_wire_size},
};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// How the broker should send retained messages when a subscription is created.
///
/// Carried in the [`Subscription`] options byte (v5 only, bits 5–4).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum RetainHandling {
    /// Send retained messages at subscribe time (v3 behaviour).
    #[default]
    SendOnSubscribe = 0,
    /// Send retained messages only if the subscription does not already exist.
    SendIfNew = 1,
    /// Do not send retained messages at subscribe time.
    DoNotSend = 2,
}

/////////////////////////////////////////////////////////////////////////////

/// A single topic-filter entry inside a SUBSCRIBE packet.
///
/// On v3 connections the v5-only fields (`no_local`,
/// `retain_as_published`, `retain_handling`) are ignored during encode and
/// left at their defaults during decode.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Subscription {
    /// Topic filter, may contain `+` and `#` wildcards.
    pub topic_filter: String,
    /// Maximum QoS the client is willing to accept for this subscription.
    pub qos: QoS,
    /// v5: do not deliver messages published by this client to itself.
    pub no_local: bool,
    /// v5: forward the RETAIN flag as received rather than clearing it.
    pub retain_as_published: bool,
    /// v5: how retained messages are sent on subscribe.
    pub retain_handling: RetainHandling,
}

impl Subscription {
    /// Creates a subscription with v5 fields set to their defaults.
    pub fn new(topic_filter: impl Into<String>, qos: QoS) -> Self {
        Self {
            topic_filter: topic_filter.into(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::default(),
        }
    }

    /// Returns the encoded options byte for the given version, `ver`.
    fn options_byte(&self, ver: MqttVersion) -> u8 {
        let mut byte = self.qos as u8;
        if ver == MqttVersion::V5 {
            if self.no_local {
                byte |= 0x04;
            }
            if self.retain_as_published {
                byte |= 0x08;
            }
            byte |= (self.retain_handling as u8) << 4;
        }
        byte
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A SUBSCRIBE packet, unified across MQTT v3.1, v3.1.1, and v5.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Subscribe {
    /// Packet identifier.
    pub packet_id: u16,
    /// One or more topic-filter / options pairs. Must not be empty.
    pub subscriptions: Vec<Subscription>,
    /// MQTT v5 properties; empty on v3 connections.
    pub properties: Properties,
}

impl Subscribe {
    /// Creates a SUBSCRIBE with no properties, valid on any version.
    pub fn new(packet_id: u16, subscriptions: Vec<Subscription>) -> Self {
        Self {
            packet_id,
            subscriptions,
            properties: Properties::new(),
        }
    }

    /// Creates a SUBSCRIBE carrying MQTT v5 properties.
    ///
    /// v5 only.
    ///
    /// Only `SubscriptionIdentifier` and `UserProperty` are permitted here
    /// (§3.8.2.1), and the identifier at most once — unlike in a PUBLISH,
    /// where the server may send several. [`encode`](Self::encode) rejects
    /// anything else. A v3 connection cannot carry properties at all.
    pub fn with_properties(
        packet_id: u16,
        subscriptions: Vec<Subscription>,
        properties: Properties,
    ) -> Self {
        Self {
            packet_id,
            subscriptions,
            properties,
        }
    }

    /// Returns the remaining-length byte count for version, `ver`.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        let mut size = 2; // packet_id
        size += self.properties.encoded_size(ver);
        for sub in &self.subscriptions {
            size += utf8_wire_size(&sub.topic_filter) + 1; // filter + options byte
        }
        size
    }

    /// Encodes the variable header and payload into `dst`.
    ///
    /// Returns [`Error::ProtocolError`] if `packet_id` is 0 or
    /// `subscriptions` is empty (MQTT-3.8.3-2) — the same rules decode
    /// applies.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        if self.subscriptions.is_empty() {
            return Err(Error::ProtocolError);
        }
        dst.put_u16(check_packet_id(self.packet_id)?);
        self.properties.check_context(PacketType::Subscribe)?;
        self.properties.encode(dst, ver)?;
        for sub in &self.subscriptions {
            topic::validate_filter(&sub.topic_filter)?;
            put_utf8(&sub.topic_filter, dst)?;
            dst.put_u8(sub.options_byte(ver));
        }
        Ok(())
    }

    /// Decodes from the raw `payload` bytes for the given version, `ver`.
    pub fn decode(mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }
        let packet_id = check_packet_id(payload.get_u16())?;
        let properties = Properties::decode_in(&mut payload, ver, PacketType::Subscribe)?;
        let mut subscriptions = Vec::new();
        while payload.has_remaining() {
            let topic_filter = get_utf8(&mut payload)?;
            topic::validate_filter(&topic_filter)?;
            if !payload.has_remaining() {
                return Err(Error::PacketTooShort);
            }
            let options = payload.get_u8();
            let qos = QoS::try_from(options & 0x03)?;

            let (no_local, retain_as_published, retain_handling) = match ver {
                MqttVersion::V5 => {
                    if options & 0xC0 != 0 {
                        return Err(Error::MalformedPacket); // reserved bits must be 0
                    }
                    let rh = match (options >> 4) & 0x03 {
                        0 => RetainHandling::SendOnSubscribe,
                        1 => RetainHandling::SendIfNew,
                        2 => RetainHandling::DoNotSend,
                        _ => return Err(Error::ProtocolError), // retain handling 3 (§3.8.3.1)
                    };
                    (options & 0x04 != 0, options & 0x08 != 0, rh)
                }
                _ => {
                    if options & 0xFC != 0 {
                        return Err(Error::MalformedPacket); // bits 7-2 reserved in v3
                    }
                    (false, false, RetainHandling::SendOnSubscribe)
                }
            };

            subscriptions.push(Subscription {
                topic_filter,
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            });
        }

        if subscriptions.is_empty() {
            return Err(Error::ProtocolError); // SUBSCRIBE must have ≥1 filter (MQTT-3.8.3-2)
        }

        Ok(Self {
            packet_id,
            subscriptions,
            properties,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A SUBACK packet, unified across MQTT v3.1, v3.1.1, and v5.
///
/// In v3 the per-subscription return codes:
/// - `0x00` - `0x02` = Max QoS Granted
/// - `0x80` = Failure
///
/// These share byte values with
/// [`ReasonCode::GrantedQos0`] / `GrantedQos1` / `GrantedQos2` /
/// [`ReasonCode::UnspecifiedError`], so no translation is needed. Any other
/// code is rejected on a v3 connection in both directions.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SubAck {
    /// Packet identifier matching the original SUBSCRIBE.
    pub packet_id: u16,
    /// One reason code per topic filter in the SUBSCRIBE, in order.
    pub reason_codes: Vec<ReasonCode>,
    /// MQTT v5 properties; empty on v3 connections.
    pub properties: Properties,
}

impl SubAck {
    /// Creates a SUBACK with no properties, valid on any version.
    ///
    /// For v3, the [`ReasonCode`] values are the same as the return codes,
    /// where `0-2` is the maximum QoS granted for each subscription request,
    /// or `0x80` indicates a failure.
    pub fn new(packet_id: u16, reason_codes: Vec<ReasonCode>) -> Self {
        Self {
            packet_id,
            reason_codes,
            properties: Properties::new(),
        }
    }

    /// Creates a SUBACK carrying MQTT v5 properties.
    ///
    /// v5 only.
    ///
    /// Only `ReasonString` and `UserProperty` are permitted here (§3.9.2.1);
    /// [`encode`](Self::encode) rejects anything else. A v3 connection
    /// cannot carry properties at all, and its reason codes are limited to
    /// the four v3 return codes.
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
        2 + self.properties.encoded_size(ver) + self.reason_codes.len()
    }

    /// Encodes the variable header and payload into `dst`.
    ///
    /// Returns [`Error::ProtocolError`] if `packet_id` is 0 or
    /// `reason_codes` is empty (MQTT-3.9.3-1), and
    /// [`Error::MalformedPacket`] if `ver` is v3 and a code is not one of the
    /// four v3 return codes (see [`is_v3_return_code`](Self::is_v3_return_code)).
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        if self.reason_codes.is_empty() {
            return Err(Error::ProtocolError);
        }
        dst.put_u16(check_packet_id(self.packet_id)?);
        self.properties.check_context(PacketType::SubAck)?;
        self.properties.encode(dst, ver)?;
        for &rc in &self.reason_codes {
            if !ver.is_v5() && !Self::is_v3_return_code(rc) {
                return Err(Error::MalformedPacket);
            }
            dst.put_u8(rc as u8);
        }
        Ok(())
    }

    /// Returns `true` for the four return codes a v3 SUBACK may carry:
    /// `GrantedQos0`/`1`/`2` (0x00–0x02) and `UnspecifiedError` (0x80, the
    /// v3 "Failure"). Every other value is reserved in v3 (§3.9.3).
    pub fn is_v3_return_code(rc: ReasonCode) -> bool {
        matches!(
            rc,
            ReasonCode::GrantedQos0
                | ReasonCode::GrantedQos1
                | ReasonCode::GrantedQos2
                | ReasonCode::UnspecifiedError
        )
    }

    /// Decodes from the raw `payload` bytes for the given version, `ver`.
    pub fn decode(mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }
        let packet_id = check_packet_id(payload.get_u16())?;
        let properties = Properties::decode_in(&mut payload, ver, PacketType::SubAck)?;
        let reason_codes = payload
            .iter()
            .copied()
            .map(|b| {
                let rc = ReasonCode::try_from(b)?;
                // v3 defines only 0x00–0x02 and 0x80; the rest are reserved.
                if !ver.is_v5() && !Self::is_v3_return_code(rc) {
                    return Err(Error::MalformedPacket);
                }
                Ok(rc)
            })
            .collect::<Result<Vec<_>>>()?;
        if reason_codes.is_empty() {
            return Err(Error::ProtocolError); // one code per filter (MQTT-3.9.3-1)
        }
        Ok(Self {
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
    use alloc::vec;
    use bytes::BytesMut;

    fn v311() -> MqttVersion {
        MQTT_VERSION_3_1_1
    }

    fn v5() -> MqttVersion {
        MQTT_VERSION_5
    }

    fn sub(filter: &str, qos: QoS) -> Subscription {
        Subscription::new(filter, qos)
    }

    // ======================================================================
    // Subscribe
    // ======================================================================

    fn round_trip_subscribe(s: Subscribe, ver: MqttVersion) {
        let mut dst = BytesMut::new();
        s.encode(&mut dst, ver).unwrap();
        let decoded = Subscribe::decode(dst.freeze(), ver).unwrap();
        assert_eq!(decoded, s);
    }

    // ---- basic encode ----

    #[test]
    fn test_subscribe_encode_single_v311() {
        let s = Subscribe::new(1, vec![sub("a/b", QoS::AtLeastOnce)]);
        let mut dst = BytesMut::new();
        s.encode(&mut dst, v311()).unwrap();
        // packet_id=1, topic len=3, "a/b", qos=1
        assert_eq!(&dst[..], &[0x00, 0x01, 0x00, 0x03, b'a', b'/', b'b', 0x01]);
    }

    #[test]
    fn test_subscribe_encode_v5_has_properties_byte() {
        let s = Subscribe::new(1, vec![sub("t", QoS::AtMostOnce)]);
        let mut dst = BytesMut::new();
        s.encode(&mut dst, v5()).unwrap();
        // After packet_id, byte 2 should be the properties VBI (0x00)
        assert_eq!(dst[2], 0x00);
    }

    // ---- round-trip ----

    #[test]
    fn test_subscribe_round_trip_single_v311() {
        round_trip_subscribe(
            Subscribe::new(5, vec![sub("sensors/#", QoS::AtMostOnce)]),
            v311(),
        );
    }

    #[test]
    fn test_subscribe_round_trip_multi_v311() {
        round_trip_subscribe(
            Subscribe::new(
                10,
                vec![
                    sub("a/b", QoS::AtMostOnce),
                    sub("c/+/d", QoS::AtLeastOnce),
                    sub("#", QoS::ExactlyOnce),
                ],
            ),
            v311(),
        );
    }

    #[test]
    fn test_subscribe_round_trip_single_v5() {
        round_trip_subscribe(Subscribe::new(1, vec![sub("t/u", QoS::AtLeastOnce)]), v5());
    }

    // ---- v5 subscription options ----

    #[test]
    fn test_subscribe_v5_options_round_trip() {
        let sub_full = Subscription {
            topic_filter: "t".into(),
            qos: QoS::AtLeastOnce,
            no_local: true,
            retain_as_published: true,
            retain_handling: RetainHandling::SendIfNew,
        };
        round_trip_subscribe(Subscribe::new(3, vec![sub_full]), v5());
    }

    #[test]
    fn test_subscribe_v5_no_local_encoded() {
        let sub_nl = Subscription {
            topic_filter: "t".into(),
            qos: QoS::AtMostOnce,
            no_local: true,
            retain_as_published: false,
            retain_handling: RetainHandling::SendOnSubscribe,
        };
        // options byte = QoS(0) | no_local(0x04) = 0x04
        assert_eq!(sub_nl.options_byte(v5()), 0x04);
    }

    #[test]
    fn test_subscribe_v5_retain_handling_encoded() {
        let sub_rh = Subscription {
            topic_filter: "t".into(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::DoNotSend,
        };
        // retain_handling=2 → bits 5-4 = 10 → 0x20
        assert_eq!(sub_rh.options_byte(v5()), 0x20);
    }

    #[test]
    fn test_subscribe_v5_options_not_written_for_v311() {
        // v3.1.1 encode must write only the QoS byte, even if v5 fields are set.
        let sub_full = Subscription {
            topic_filter: "t".into(),
            qos: QoS::AtLeastOnce,
            no_local: true,
            retain_as_published: true,
            retain_handling: RetainHandling::DoNotSend,
        };
        assert_eq!(sub_full.options_byte(v311()), 0x01); // only QoS=1
    }

    // ---- encoded_size ----

    #[test]
    fn test_subscribe_encoded_size_v311() {
        let s = Subscribe::new(1, vec![sub("abc", QoS::AtMostOnce)]);
        // 2 (id) + (2+3) (filter) + 1 (qos) = 8
        assert_eq!(s.encoded_size(v311()), 8);
    }

    #[test]
    fn test_subscribe_encoded_size_v5() {
        let s = Subscribe::new(1, vec![sub("abc", QoS::AtMostOnce)]);
        // 2 (id) + 1 (props VBI) + (2+3) + 1 = 9
        assert_eq!(s.encoded_size(v5()), 9);
    }

    #[test]
    fn test_subscribe_encoded_size_matches_encode() {
        let s = Subscribe::new(
            7,
            vec![sub("a/b", QoS::AtMostOnce), sub("c/#", QoS::ExactlyOnce)],
        );
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            s.encode(&mut dst, ver).unwrap();
            assert_eq!(dst.len(), s.encoded_size(ver));
        }
    }

    // ---- error cases ----

    #[test]
    fn test_subscribe_decode_empty_subscriptions_errors() {
        // Only packet_id, no subscriptions
        let payload = Bytes::from_static(&[0x00, 0x01]);
        assert_eq!(
            Subscribe::decode(payload, v311()),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_subscribe_decode_too_short_errors() {
        assert_eq!(
            Subscribe::decode(Bytes::from_static(&[0x00]), v311()),
            Err(Error::PacketTooShort)
        );
    }

    #[test]
    fn test_subscribe_rejects_invalid_filter_both_directions() {
        for bad in ["", "a/#/b", "sport+", "$share//x"] {
            let s = Subscribe::new(1, vec![sub(bad, QoS::AtMostOnce)]);
            let mut dst = BytesMut::new();
            assert_eq!(
                s.encode(&mut dst, v5()),
                Err(Error::TopicFilterInvalid),
                "{bad:?}"
            );
        }
        // On the wire: packet_id=1, no props, filter "a/#/b", options 0
        let mut src = BytesMut::new();
        src.put_u16(1);
        src.put_u8(0);
        put_utf8("a/#/b", &mut src).unwrap();
        src.put_u8(0);
        assert_eq!(
            Subscribe::decode(src.freeze(), v5()),
            Err(Error::TopicFilterInvalid)
        );
    }

    #[test]
    fn test_subscribe_shared_filter_round_trips() {
        let s = Subscribe::new(1, vec![sub("$share/group1/sport/#", QoS::AtLeastOnce)]);
        round_trip_subscribe(s, v5());
    }

    #[test]
    fn test_subscribe_v311_reserved_options_bits_error() {
        // v3.1.1 options byte is QoS only; bits 7-2 are reserved (MQTT-3-8.3-4).
        let s = Subscribe::new(1, vec![sub("t", QoS::AtMostOnce)]);
        let mut dst = BytesMut::new();
        s.encode(&mut dst, v311()).unwrap();
        let last = dst.len() - 1;
        dst[last] |= 0x04; // what v5 would call no_local
        assert_eq!(
            Subscribe::decode(dst.freeze(), v311()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_suback_decode_empty_reason_codes_errors() {
        // packet_id only (v3.1.1) / packet_id + empty properties (v5).
        assert_eq!(
            SubAck::decode(Bytes::from_static(&[0x00, 0x01]), v311()),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            SubAck::decode(Bytes::from_static(&[0x00, 0x01, 0x00]), v5()),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_subscribe_v5_reserved_options_bits_error() {
        // Build a valid v5 subscribe then corrupt the options byte reserved bits
        let s = Subscribe::new(1, vec![sub("t", QoS::AtMostOnce)]);
        let mut dst = BytesMut::new();
        s.encode(&mut dst, v5()).unwrap();
        // options byte is the last byte; set reserved bit 7
        let last = dst.len() - 1;
        dst[last] |= 0x80;
        assert_eq!(
            Subscribe::decode(dst.freeze(), v5()),
            Err(Error::MalformedPacket)
        );
    }

    // ======================================================================
    // SubAck
    // ======================================================================

    fn round_trip_suback(sa: SubAck, ver: MqttVersion) {
        let mut dst = BytesMut::new();
        sa.encode(&mut dst, ver).unwrap();
        let decoded = SubAck::decode(dst.freeze(), ver).unwrap();
        assert_eq!(decoded, sa);
    }

    #[test]
    fn test_suback_encode_v311() {
        let sa = SubAck::new(
            3,
            vec![
                ReasonCode::Success, // GrantedQos0
                ReasonCode::GrantedQos1,
                ReasonCode::GrantedQos2,
                ReasonCode::UnspecifiedError, // failure
            ],
        );
        let mut dst = BytesMut::new();
        sa.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[..], &[0x00, 0x03, 0x00, 0x01, 0x02, 0x80]);
    }

    #[test]
    fn test_suback_round_trip_v311() {
        round_trip_suback(
            SubAck::new(1, vec![ReasonCode::Success, ReasonCode::GrantedQos2]),
            v311(),
        );
    }

    #[test]
    fn test_suback_round_trip_v5() {
        round_trip_suback(
            SubAck::new(2, vec![ReasonCode::GrantedQos1, ReasonCode::NotAuthorized]),
            v5(),
        );
    }

    #[test]
    fn test_suback_encoded_size_v311() {
        let sa = SubAck::new(1, vec![ReasonCode::Success, ReasonCode::GrantedQos1]);
        // 2 (id) + 2 (reason codes)
        assert_eq!(sa.encoded_size(v311()), 4);
    }

    #[test]
    fn test_suback_encoded_size_v5() {
        let sa = SubAck::new(1, vec![ReasonCode::Success]);
        // 2 (id) + 1 (props VBI) + 1 (reason code)
        assert_eq!(sa.encoded_size(v5()), 4);
    }

    #[test]
    fn test_suback_encoded_size_matches_encode() {
        let sa = SubAck::new(
            9,
            vec![ReasonCode::GrantedQos2, ReasonCode::UnspecifiedError],
        );
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            sa.encode(&mut dst, ver).unwrap();
            assert_eq!(dst.len(), sa.encoded_size(ver));
        }
    }

    #[test]
    fn test_suback_decode_too_short_errors() {
        assert_eq!(
            SubAck::decode(Bytes::from_static(&[0x00]), v311()),
            Err(Error::PacketTooShort)
        );
    }

    #[test]
    fn test_suback_decode_unknown_reason_code_errors() {
        // Second code (0x03) is undefined in both versions.
        assert_eq!(
            SubAck::decode(Bytes::from_static(&[0x00, 0x01, 0x00, 0x03]), v311()),
            Err(Error::ProtocolError)
        );
        assert_eq!(
            SubAck::decode(Bytes::from_static(&[0x00, 0x01, 0x00, 0x00, 0x03]), v5()),
            Err(Error::ProtocolError)
        );
    }

    #[test]
    fn test_suback_v311_byte_values_match_reason_codes() {
        // v3.1.1 granted-QoS byte values match ReasonCode discriminants directly.
        let sa = SubAck::new(
            1,
            vec![
                ReasonCode::Success,          // 0x00
                ReasonCode::GrantedQos1,      // 0x01
                ReasonCode::GrantedQos2,      // 0x02
                ReasonCode::UnspecifiedError, // 0x80
            ],
        );
        let mut dst = BytesMut::new();
        sa.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[2..], &[0x00, 0x01, 0x02, 0x80]);
    }

    // ---- Empty lists are rejected on encode as they are on decode ----

    #[test]
    fn test_subscribe_encode_empty_list_is_protocol_error() {
        let sub = Subscribe::new(1, vec![]);
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            assert_eq!(sub.encode(&mut dst, ver), Err(Error::ProtocolError));
            assert!(dst.is_empty());
        }
    }

    #[test]
    fn test_suback_encode_empty_codes_is_protocol_error() {
        let ack = SubAck::new(1, vec![]);
        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            assert_eq!(ack.encode(&mut dst, ver), Err(Error::ProtocolError));
            assert!(dst.is_empty());
        }
    }

    #[test]
    fn test_subscribe_encode_rejects_second_subscription_identifier() {
        // A PUBLISH may carry several; a SUBSCRIBE only one (§3.8.2.1.2).
        let mut sub = Subscribe::new(1, vec![Subscription::new("a", QoS::QoS0)]);
        sub.properties.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(1),
        ));
        let mut dst = BytesMut::new();
        assert!(sub.encode(&mut dst, v5()).is_ok(), "one is fine");
        sub.properties.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(2),
        ));
        let mut dst = BytesMut::new();
        assert_eq!(sub.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_subscribe_decode_rejects_second_subscription_identifier() {
        // packet id 1; props len 4: (0x0B, 1), (0x0B, 2); filter "a" qos 0
        let bytes: &[u8] = &[
            0x00, 0x01, 0x04, 0x0B, 0x01, 0x0B, 0x02, 0x00, 0x01, b'a', 0x00,
        ];
        assert_eq!(
            Subscribe::decode(Bytes::from_static(bytes), v5()),
            Err(Error::ProtocolError)
        );
        // With a single identifier the same layout decodes.
        let bytes: &[u8] = &[0x00, 0x01, 0x02, 0x0B, 0x01, 0x00, 0x01, b'a', 0x00];
        assert!(Subscribe::decode(Bytes::from_static(bytes), v5()).is_ok());
    }

    // ---- v3 SUBACK carries only the four v3 return codes (§3.9.3) ----

    #[test]
    fn test_is_v3_return_code() {
        assert!(SubAck::is_v3_return_code(ReasonCode::GrantedQos0));
        assert!(SubAck::is_v3_return_code(ReasonCode::GrantedQos1));
        assert!(SubAck::is_v3_return_code(ReasonCode::GrantedQos2));
        assert!(SubAck::is_v3_return_code(ReasonCode::UnspecifiedError));
        assert!(!SubAck::is_v3_return_code(ReasonCode::NotAuthorized));
        assert!(!SubAck::is_v3_return_code(ReasonCode::TopicFilterInvalid));
    }

    #[test]
    fn test_suback_v3_decode_rejects_v5_only_code() {
        // id 1, code 0x87 (NotAuthorized): valid in a v5 SUBACK, reserved in v3.
        let bytes: &[u8] = &[0x00, 0x01, 0x87];
        assert_eq!(
            SubAck::decode(Bytes::from_static(bytes), v311()),
            Err(Error::MalformedPacket)
        );
        assert_eq!(
            SubAck::decode(Bytes::from_static(bytes), MqttVersion::V3_1),
            Err(Error::MalformedPacket)
        );
        // 0x80 (Failure) is the v3 error code and stays accepted.
        let ok = SubAck::decode(Bytes::from_static(&[0x00, 0x01, 0x80]), v311()).unwrap();
        assert_eq!(ok.reason_codes, vec![ReasonCode::UnspecifiedError]);
    }

    #[test]
    fn test_suback_v3_encode_rejects_v5_only_code() {
        let ack = SubAck::new(1, vec![ReasonCode::GrantedQos1, ReasonCode::NotAuthorized]);
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v311()), Err(Error::MalformedPacket));
        let mut dst = BytesMut::new();
        assert!(ack.encode(&mut dst, v5()).is_ok());
    }

    // ---- with_properties ----

    fn user_prop(k: &str, v: &str) -> Property {
        Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair(k.into(), v.into()),
        )
    }

    fn sub_id_props(id: u32) -> Properties {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(id),
        ));
        props
    }

    #[test]
    fn test_subscribe_with_properties_keeps_all_arguments() {
        let subs = vec![Subscription::new("a/b", QoS::AtLeastOnce)];
        let sub = Subscribe::with_properties(7, subs.clone(), sub_id_props(5));
        assert_eq!(sub.packet_id, 7);
        assert_eq!(sub.subscriptions, subs);
        assert_eq!(
            sub.properties.get(PropertyId::SubscriptionIdentifier),
            Some(&PropertyValue::VarInt(5))
        );
    }

    #[test]
    fn test_subscribe_with_properties_empty_matches_new() {
        let subs = vec![Subscription::new("a", QoS::AtMostOnce)];
        assert_eq!(
            Subscribe::with_properties(1, subs.clone(), Properties::new()),
            Subscribe::new(1, subs)
        );
    }

    #[test]
    fn test_subscribe_with_properties_round_trip_v5() {
        let mut props = sub_id_props(9);
        props.push(user_prop("a", "1"));
        props.push(user_prop("b", "2"));
        let sub = Subscribe::with_properties(
            42,
            vec![
                Subscription::new("a/+", QoS::AtLeastOnce),
                Subscription::new("b/#", QoS::ExactlyOnce),
            ],
            props,
        );

        let mut dst = BytesMut::new();
        sub.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), sub.encoded_size(v5()));
        assert_eq!(Subscribe::decode(dst.freeze(), v5()).unwrap(), sub);
    }

    #[test]
    fn test_subscribe_with_properties_on_v3_is_an_error() {
        let sub = Subscribe::with_properties(
            1,
            vec![Subscription::new("a", QoS::AtMostOnce)],
            sub_id_props(1),
        );
        let mut dst = BytesMut::new();
        assert_eq!(sub.encode(&mut dst, v311()), Err(Error::ProtocolError));
    }

    /// A repeated SubscriptionIdentifier is allowed in a PUBLISH but not in a
    /// SUBSCRIBE (§3.8.2.1.2).
    #[test]
    fn test_subscribe_with_properties_rejects_repeated_subscription_identifier() {
        let mut props = sub_id_props(1);
        props.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(2),
        ));
        let sub =
            Subscribe::with_properties(1, vec![Subscription::new("a", QoS::AtMostOnce)], props);
        let mut dst = BytesMut::new();
        assert_eq!(sub.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_subscribe_with_properties_rejects_a_property_not_allowed_here() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String("why".into()),
        ));
        let sub =
            Subscribe::with_properties(1, vec![Subscription::new("a", QoS::AtMostOnce)], props);
        let mut dst = BytesMut::new();
        assert_eq!(sub.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }

    fn reason_props(text: &str) -> Properties {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String(text.into()),
        ));
        props
    }

    #[test]
    fn test_suback_with_properties_keeps_all_arguments() {
        let codes = vec![ReasonCode::GrantedQos1, ReasonCode::NotAuthorized];
        let ack = SubAck::with_properties(3, codes.clone(), reason_props("partly denied"));
        assert_eq!(ack.packet_id, 3);
        assert_eq!(ack.reason_codes, codes);
        assert_eq!(
            ack.properties.get(PropertyId::ReasonString),
            Some(&PropertyValue::String("partly denied".into()))
        );
    }

    #[test]
    fn test_suback_with_properties_empty_matches_new() {
        let codes = vec![ReasonCode::GrantedQos0];
        assert_eq!(
            SubAck::with_properties(1, codes.clone(), Properties::new()),
            SubAck::new(1, codes)
        );
    }

    #[test]
    fn test_suback_with_properties_round_trip_v5() {
        let mut props = reason_props("one filter refused");
        props.push(user_prop("k", "v"));
        let ack = SubAck::with_properties(
            8,
            vec![ReasonCode::GrantedQos2, ReasonCode::NotAuthorized],
            props,
        );

        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), ack.encoded_size(v5()));
        assert_eq!(SubAck::decode(dst.freeze(), v5()).unwrap(), ack);
    }

    #[test]
    fn test_suback_with_properties_on_v3_is_an_error() {
        let ack = SubAck::with_properties(1, vec![ReasonCode::GrantedQos0], reason_props("no"));
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v311()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_suback_with_properties_rejects_a_property_not_allowed_here() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::SubscriptionIdentifier,
            PropertyValue::VarInt(1),
        ));
        let ack = SubAck::with_properties(1, vec![ReasonCode::GrantedQos0], props);
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }
}
