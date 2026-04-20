// paho-mqtt-codec/src/subscribe.rs
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

//! MQTT SUBSCRIBE and SUBACK packets.
//!
//! SUBSCRIBE carries per-subscription options in a single byte. In v3.1.1
//! that byte is just the requested QoS; in v5 it also carries `no_local`,
//! `retain_as_published`, and `retain_handling` flags. The unified
//! [`Subscription`] struct carries all fields; v3.1.1 encode writes only
//! the QoS bits and v3.1.1 decode leaves the v5 fields at their defaults.

use crate::{
    Error, MqttVersion, Properties, QoS, ReasonCode, Result,
    wire::{get_utf8, put_utf8, utf8_wire_size},
};
use bytes::{Buf, BufMut, Bytes};

/////////////////////////////////////////////////////////////////////////////

/// How the broker should send retained messages when a subscription is created.
///
/// Carried in the [`Subscription`] options byte (v5 only, bits 5–4).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetainHandling {
    /// Send retained messages at subscribe time (v3.1.1 behaviour).
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
/// On v3.1.1 connections the v5-only fields (`no_local`,
/// `retain_as_published`, `retain_handling`) are ignored during encode and
/// left at their defaults during decode.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        Subscription {
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

/// A SUBSCRIBE packet, unified across MQTT v3.1.1 and v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// Packet identifier.
    pub packet_id: u16,
    /// One or more topic-filter / options pairs. Must not be empty.
    pub subscriptions: Vec<Subscription>,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl Subscribe {
    /// Creates a SUBSCRIBE with no properties.
    pub fn new(packet_id: u16, subscriptions: Vec<Subscription>) -> Self {
        Subscribe {
            packet_id,
            subscriptions,
            properties: Properties::new(),
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
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        dst.put_u16(self.packet_id);
        self.properties.encode(dst, ver)?;
        for sub in &self.subscriptions {
            put_utf8(&sub.topic_filter, dst);
            dst.put_u8(sub.options_byte(ver));
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
        let mut subscriptions = Vec::new();
        while payload.has_remaining() {
            let topic_filter = get_utf8(&mut payload)?;
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
                        _ => return Err(Error::MalformedPacket),
                    };
                    (options & 0x04 != 0, options & 0x08 != 0, rh)
                }
                _ => (false, false, RetainHandling::SendOnSubscribe),
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
            return Err(Error::MalformedPacket); // SUBSCRIBE must have ≥1 filter
        }

        Ok(Subscribe {
            packet_id,
            subscriptions,
            properties,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A SUBACK packet, unified across MQTT v3.1.1 and v5.
///
/// In v3.1.1 the per-subscription return codes (`0x00`=QoS0, `0x01`=QoS1,
/// `0x02`=QoS2, `0x80`=Failure) share byte values with
/// [`ReasonCode::GrantedQos0`] / `GrantedQos1` / `GrantedQos2` /
/// [`ReasonCode::UnspecifiedError`], so no translation is needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// Packet identifier matching the original SUBSCRIBE.
    pub packet_id: u16,
    /// One reason code per topic filter in the SUBSCRIBE, in order.
    pub reason_codes: Vec<ReasonCode>,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl SubAck {
    /// Creates a SUBACK with no properties.
    pub fn new(packet_id: u16, reason_codes: Vec<ReasonCode>) -> Self {
        SubAck {
            packet_id,
            reason_codes,
            properties: Properties::new(),
        }
    }

    /// Returns the remaining-length byte count for version, `ver`.
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        2 + self.properties.encoded_size(ver) + self.reason_codes.len()
    }

    /// Encodes the variable header and payload into `dst`.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        dst.put_u16(self.packet_id);
        self.properties.encode(dst, ver)?;
        for &rc in &self.reason_codes {
            dst.put_u8(rc as u8);
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
        let reason_codes = payload.iter().copied().map(ReasonCode::from).collect();

        Ok(SubAck {
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
    use bytes::BytesMut;

    fn v311() -> MqttVersion {
        MqttVersion::from(MQTT_VERSION_3_1_1)
    }

    fn v5() -> MqttVersion {
        MqttVersion::from(MQTT_VERSION_5)
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
            Err(Error::MalformedPacket)
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
}
