// paho-mqtt-codec/src/packet.rs
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

//! MQTT packet framing and typed packet representation.
//!
//! Decoding is split into two phases:
//!
//! 1. **Framing** ([`RawPacket::decode`]): version-independent. Reads the
//!    fixed header byte and gathers exactly `remaining_length` bytes into a
//!    [`Bytes`] payload, returning `Ok(None)` until a complete frame is
//!    available.
//!
//! 2. **Parsing** ([`Packet::from_raw`]): version-aware. Interprets the raw
//!    payload given the negotiated [`MqttVersion`].

use crate::{
    Auth, ConnAck, Connect, Disconnect, Error, MqttVersion, PubAck, Publish, Result,
    SubAck, Subscribe, UnsubAck, Unsubscribe, varlen,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// ---- packet type nibbles (upper 4 bits of fixed header) ----

const PKT_CONNECT: u8 = 1;
const PKT_CONNACK: u8 = 2;
const PKT_PUBLISH: u8 = 3;
const PKT_PUBACK: u8 = 4;
const PKT_PUBREC: u8 = 5;
const PKT_PUBREL: u8 = 6;
const PKT_PUBCOMP: u8 = 7;
const PKT_SUBSCRIBE: u8 = 8;
const PKT_SUBACK: u8 = 9;
const PKT_UNSUBSCRIBE: u8 = 10;
const PKT_UNSUBACK: u8 = 11;
const PKT_PINGREQ: u8 = 12;
const PKT_PINGRESP: u8 = 13;
const PKT_DISCONNECT: u8 = 14;
const PKT_AUTH: u8 = 15;

/// Largest total packet size (fixed header + remaining length) representable
/// on the wire: a 4-byte remaining-length field can encode [`varlen::MAX_VARLEN`]
/// bytes, plus 1 byte for the packet-type header and 4 for the length itself.
pub const MAX_PACKET_SIZE: u32 = varlen::MAX_VARLEN + 5;

// ---- fixed header bytes ----

const HDR_CONNECT: u8 = PKT_CONNECT << 4; // 0x10
const HDR_CONNACK: u8 = PKT_CONNACK << 4; // 0x20
const HDR_PUBACK: u8 = PKT_PUBACK << 4; // 0x40
const HDR_PUBREC: u8 = PKT_PUBREC << 4; // 0x50
const HDR_PUBREL: u8 = (PKT_PUBREL << 4) | 0x02; // 0x62  (reserved flags = 0b0010)
const HDR_PUBCOMP: u8 = PKT_PUBCOMP << 4; // 0x70
const HDR_SUBSCRIBE: u8 = (PKT_SUBSCRIBE << 4) | 0x02; // 0x82 (reserved flags = 0b0010)
const HDR_SUBACK: u8 = PKT_SUBACK << 4; // 0x90
const HDR_UNSUBSCRIBE: u8 = (PKT_UNSUBSCRIBE << 4) | 0x02; // 0xA2 (reserved flags = 0b0010)
const HDR_UNSUBACK: u8 = PKT_UNSUBACK << 4; // 0xB0
const HDR_PINGREQ: u8 = PKT_PINGREQ << 4;
const HDR_PINGRESP: u8 = PKT_PINGRESP << 4;
const HDR_DISCONNECT: u8 = PKT_DISCONNECT << 4; // 0xE0
const HDR_AUTH: u8 = PKT_AUTH << 4; // 0xF0

/// A raw, uninterpreted MQTT packet produced by the framing layer.
///
/// The `header` byte carries the packet type (upper 4 bits) and flags
/// (lower 4 bits). The `payload` contains exactly `remaining_length` bytes
/// as decoded from the fixed header — no more, no less.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPacket {
    /// First fixed-header byte: packet type (bits 7–4) and flags (bits 3–0).
    pub header: u8,
    /// Packet payload: exactly `remaining_length` bytes.
    pub payload: Bytes,
}

impl RawPacket {
    /// Attempts to decode one complete MQTT packet from `src`.
    ///
    /// Returns `Ok(None)` if `src` does not yet contain a complete packet.
    /// On success the consumed bytes are removed from `src`.
    ///
    /// No size limit is applied beyond the protocol maximum; see
    /// [`decode_max`](Self::decode_max) to enforce a negotiated limit.
    pub fn decode(src: &mut BytesMut) -> Result<Option<Self>> {
        Self::decode_max(src, MAX_PACKET_SIZE)
    }

    /// Attempts to decode one complete MQTT packet from `src`, rejecting any
    /// packet whose total size (fixed header included) exceeds
    /// `max_packet_size` bytes.
    ///
    /// The check is made as soon as the fixed header has been read, so an
    /// oversize packet is rejected with [`Error::PacketTooLarge`] before its
    /// body is buffered. This is the value a client advertises in the v5
    /// CONNECT `MaximumPacketSize` property, and the peer is required not to
    /// exceed it.
    ///
    /// Returns `Ok(None)` if `src` does not yet contain a complete packet.
    /// On success the consumed bytes are removed from `src`.
    pub fn decode_max(src: &mut BytesMut, max_packet_size: u32) -> Result<Option<Self>> {
        if src.is_empty() {
            return Ok(None);
        }

        // Peek at the variable-length remaining_length without advancing src.
        let mut peek: &[u8] = &src[1..];
        let remaining_len = match varlen::decode(&mut peek)? {
            Some(n) => n as usize,
            None => return Ok(None),
        };

        // Number of bytes consumed by the remaining_length field.
        let varlen_size = src.len() - 1 - peek.len();
        let total = 1 + varlen_size + remaining_len;

        if total > max_packet_size as usize {
            return Err(Error::PacketTooLarge);
        }

        if src.len() < total {
            return Ok(None);
        }

        let header = src[0];
        src.advance(1 + varlen_size);
        let payload = src.copy_to_bytes(remaining_len);
        Ok(Some(RawPacket { header, payload }))
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A typed MQTT packet.
///
/// This enum covers all packet types for both MQTT v3.1.1 and v5. v5-only
/// packets (e.g. `Auth`) are present but will not appear on v3.1.1
/// connections. v5 fields (reason codes, properties) are present on all
/// applicable structs and default to empty/success on v3.1.1 connections.
#[derive(Debug, PartialEq)]
pub enum Packet {
    /// `CONNECT` — client connection request.
    Connect(Connect),
    /// `CONNACK` — connection acknowledgement from broker to client.
    ConnAck(ConnAck),
    /// `PUBLISH` — application message from client or broker.
    Publish(Publish),
    /// `PUBACK` — QoS 1 publish acknowledgement.
    PubAck(PubAck),
    /// `PUBREC` — QoS 2 publish received (first ack).
    PubRec(PubAck),
    /// `PUBREL` — QoS 2 publish release (second ack).
    PubRel(PubAck),
    /// `PUBCOMP` — QoS 2 publish complete (third ack).
    PubComp(PubAck),
    /// `SUBSCRIBE` — subscription request from client to broker.
    Subscribe(Subscribe),
    /// `SUBACK` — subscription acknowledgement.
    SubAck(SubAck),
    /// `UNSUBSCRIBE` — unsubscription request.
    Unsubscribe(Unsubscribe),
    /// `UNSUBACK` — unsubscription acknowledgement.
    UnsubAck(UnsubAck),
    /// `PINGREQ` — keep-alive ping.
    PingReq,
    /// `PINGRESP` — keep-alive ping response.
    PingResp,
    /// `DISCONNECT` — connection shutdown.
    Disconnect(Disconnect),
    /// `AUTH` — extended authentication exchange (v5 only).
    Auth(Auth),
}

impl Packet {
    /// Parses a [`RawPacket`] into a typed [`Packet`] given `ver`.
    pub fn from_raw(raw: RawPacket, ver: MqttVersion) -> Result<Self> {
        match raw.header >> 4 {
            PKT_CONNECT => Ok(Packet::Connect(Connect::decode(raw.payload, ver)?)),
            PKT_CONNACK => Ok(Packet::ConnAck(ConnAck::decode(raw.payload, ver)?)),
            PKT_PUBLISH => Ok(Packet::Publish(Publish::decode(
                raw.header & 0x0F,
                raw.payload,
                ver,
            )?)),
            PKT_PUBACK => Ok(Packet::PubAck(PubAck::decode(raw.payload, ver)?)),
            PKT_PUBREC => Ok(Packet::PubRec(PubAck::decode(raw.payload, ver)?)),
            PKT_PUBREL => {
                if raw.header & 0x0F != 0x02 {
                    return Err(Error::MalformedPacket);
                }
                Ok(Packet::PubRel(PubAck::decode(raw.payload, ver)?))
            }
            PKT_PUBCOMP => Ok(Packet::PubComp(PubAck::decode(raw.payload, ver)?)),
            PKT_SUBSCRIBE => {
                if raw.header & 0x0F != 0x02 {
                    return Err(Error::MalformedPacket);
                }
                Ok(Packet::Subscribe(Subscribe::decode(raw.payload, ver)?))
            }
            PKT_SUBACK => Ok(Packet::SubAck(SubAck::decode(raw.payload, ver)?)),
            PKT_UNSUBSCRIBE => {
                if raw.header & 0x0F != 0x02 {
                    return Err(Error::MalformedPacket);
                }
                Ok(Packet::Unsubscribe(Unsubscribe::decode(raw.payload, ver)?))
            }
            PKT_UNSUBACK => Ok(Packet::UnsubAck(UnsubAck::decode(raw.payload, ver)?)),
            PKT_PINGREQ => Ok(Packet::PingReq),
            PKT_PINGRESP => Ok(Packet::PingResp),
            PKT_DISCONNECT => Ok(Packet::Disconnect(Disconnect::decode(raw.payload, ver)?)),
            PKT_AUTH => Ok(Packet::Auth(Auth::decode(raw.payload)?)),
            t => Err(Error::UnknownPacketType(t)),
        }
    }

    /// Encodes this packet into `dst`.
    ///
    /// On error nothing is appended to `dst`: any bytes written before the
    /// failure was detected are removed.
    pub fn encode(&self, dst: &mut BytesMut, ver: MqttVersion) -> Result<()> {
        let start = dst.len();
        let res = self.encode_inner(dst, ver);
        if res.is_err() {
            dst.truncate(start);
        }
        res
    }

    fn encode_inner(&self, dst: &mut BytesMut, ver: MqttVersion) -> Result<()> {
        use Packet::*;
        match self {
            Connect(conn) => {
                let remaining = conn.encoded_size(ver);
                dst.put_u8(HDR_CONNECT);
                varlen::encode(remaining as u32, dst)?;
                conn.encode(dst, ver)?;
            }
            ConnAck(ack) => {
                let remaining = ack.encoded_size(ver);
                dst.put_u8(HDR_CONNACK);
                varlen::encode(remaining as u32, dst)?;
                ack.encode(dst, ver)?;
            }
            Publish(pub_) => {
                let remaining = pub_.encoded_size(ver);
                let mut hdr = PKT_PUBLISH << 4;
                if pub_.dup {
                    hdr |= 0x08;
                }
                hdr |= (pub_.qos as u8) << 1;
                if pub_.retain {
                    hdr |= 0x01;
                }
                dst.put_u8(hdr);
                varlen::encode(remaining as u32, dst)?;
                pub_.encode(dst, ver)?;
            }
            PubAck(ack) => {
                dst.put_u8(HDR_PUBACK);
                varlen::encode(ack.encoded_size(ver) as u32, dst)?;
                ack.encode(dst, ver)?;
            }
            PubRec(ack) => {
                dst.put_u8(HDR_PUBREC);
                varlen::encode(ack.encoded_size(ver) as u32, dst)?;
                ack.encode(dst, ver)?;
            }
            PubRel(ack) => {
                dst.put_u8(HDR_PUBREL);
                varlen::encode(ack.encoded_size(ver) as u32, dst)?;
                ack.encode(dst, ver)?;
            }
            PubComp(ack) => {
                dst.put_u8(HDR_PUBCOMP);
                varlen::encode(ack.encoded_size(ver) as u32, dst)?;
                ack.encode(dst, ver)?;
            }
            Subscribe(s) => {
                dst.put_u8(HDR_SUBSCRIBE);
                varlen::encode(s.encoded_size(ver) as u32, dst)?;
                s.encode(dst, ver)?;
            }
            SubAck(sa) => {
                dst.put_u8(HDR_SUBACK);
                varlen::encode(sa.encoded_size(ver) as u32, dst)?;
                sa.encode(dst, ver)?;
            }
            Unsubscribe(u) => {
                dst.put_u8(HDR_UNSUBSCRIBE);
                varlen::encode(u.encoded_size(ver) as u32, dst)?;
                u.encode(dst, ver)?;
            }
            UnsubAck(ua) => {
                dst.put_u8(HDR_UNSUBACK);
                varlen::encode(ua.encoded_size(ver) as u32, dst)?;
                ua.encode(dst, ver)?;
            }
            PingReq => {
                dst.put_u8(HDR_PINGREQ);
                varlen::encode(0, dst)?;
            }
            PingResp => {
                dst.put_u8(HDR_PINGRESP);
                varlen::encode(0, dst)?;
            }
            Disconnect(d) => {
                dst.put_u8(HDR_DISCONNECT);
                varlen::encode(d.encoded_size(ver) as u32, dst)?;
                d.encode(dst, ver)?;
            }
            Auth(a) => {
                dst.put_u8(HDR_AUTH);
                varlen::encode(a.encoded_size() as u32, dst)?;
                a.encode(dst)?;
            }
        }
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        QoS, String,
        types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5},
    };

    fn v311() -> MqttVersion {
        MqttVersion::from(MQTT_VERSION_3_1_1)
    }

    fn v5() -> MqttVersion {
        MqttVersion::from(MQTT_VERSION_5)
    }

    // ---- RawPacket::decode ----

    #[test]
    fn test_raw_decode_empty_returns_none() {
        let mut src = BytesMut::new();
        assert!(RawPacket::decode(&mut src).unwrap().is_none());
    }

    #[test]
    fn test_raw_decode_partial_header_returns_none() {
        // Only the fixed header byte, no remaining_length byte yet.
        let mut src = BytesMut::from(&[HDR_PINGREQ][..]);
        assert!(RawPacket::decode(&mut src).unwrap().is_none());
        assert_eq!(src.len(), 1); // not consumed
    }

    #[test]
    fn test_raw_decode_pingreq() {
        let mut src = BytesMut::from(&[HDR_PINGREQ, 0x00][..]);
        let raw = RawPacket::decode(&mut src).unwrap().unwrap();
        assert_eq!(raw.header, HDR_PINGREQ);
        assert!(raw.payload.is_empty());
        assert!(src.is_empty());
    }

    #[test]
    fn test_raw_decode_pingresp() {
        let mut src = BytesMut::from(&[HDR_PINGRESP, 0x00][..]);
        let raw = RawPacket::decode(&mut src).unwrap().unwrap();
        assert_eq!(raw.header, HDR_PINGRESP);
        assert!(raw.payload.is_empty());
        assert!(src.is_empty());
    }

    #[test]
    fn test_raw_decode_leaves_trailing_bytes() {
        // Two back-to-back PINGREQ frames.
        let mut src = BytesMut::from(&[HDR_PINGREQ, 0x00, HDR_PINGRESP, 0x00][..]);
        let raw = RawPacket::decode(&mut src).unwrap().unwrap();
        assert_eq!(raw.header, HDR_PINGREQ);
        assert_eq!(src.len(), 2); // second packet still in buffer
    }

    #[test]
    fn test_raw_decode_incomplete_payload_returns_none() {
        // remaining_length = 5, but only 3 payload bytes available.
        let mut src = BytesMut::from(&[0x30u8, 0x05, 0x01, 0x02, 0x03][..]);
        assert!(RawPacket::decode(&mut src).unwrap().is_none());
        assert_eq!(src.len(), 5); // nothing consumed
    }

    // ---- RawPacket::decode_max ----

    #[test]
    fn test_raw_decode_max_rejects_oversize_before_body_arrives() {
        // Header claims remaining_length = 100 (total 102) but only the
        // header is present; the limit must be enforced without waiting.
        let mut src = BytesMut::from(&[0x30u8, 0x64][..]);
        assert_eq!(
            RawPacket::decode_max(&mut src, 101),
            Err(Error::PacketTooLarge)
        );
    }

    #[test]
    fn test_raw_decode_max_limit_is_total_packet_size() {
        // total = 1 (hdr) + 1 (varlen) + 3 (payload) = 5
        let bytes = [0x30u8, 0x03, 0x01, 0x02, 0x03];
        let mut src = BytesMut::from(&bytes[..]);
        assert_eq!(
            RawPacket::decode_max(&mut src, 4),
            Err(Error::PacketTooLarge)
        );
        let mut src = BytesMut::from(&bytes[..]);
        let raw = RawPacket::decode_max(&mut src, 5).unwrap().unwrap();
        assert_eq!(raw.payload.len(), 3);
        assert!(src.is_empty());
    }

    #[test]
    fn test_raw_decode_max_spec_maximum_header() {
        // remaining_length = MAX_VARLEN encoded as 0xFF 0xFF 0xFF 0x7F.
        let hdr = [0x30u8, 0xFF, 0xFF, 0xFF, 0x7F];
        let mut src = BytesMut::from(&hdr[..]);
        // At the protocol maximum this is merely incomplete, not rejected.
        assert!(RawPacket::decode(&mut src).unwrap().is_none());
        // One byte below the protocol maximum rejects it.
        let mut src = BytesMut::from(&hdr[..]);
        assert_eq!(
            RawPacket::decode_max(&mut src, MAX_PACKET_SIZE - 1),
            Err(Error::PacketTooLarge)
        );
    }

    #[test]
    fn test_raw_decode_max_does_not_consume_on_error() {
        let mut src = BytesMut::from(&[0x30u8, 0x64, 0xAA][..]);
        let _ = RawPacket::decode_max(&mut src, 10);
        assert_eq!(&src[..], &[0x30, 0x64, 0xAA]);
    }

    // ---- Packet::from_raw ----

    #[test]
    fn test_from_raw_pingreq() {
        let raw = RawPacket {
            header: HDR_PINGREQ,
            payload: Bytes::new(),
        };
        assert!(matches!(
            Packet::from_raw(raw, v311()).unwrap(),
            Packet::PingReq
        ));
    }

    #[test]
    fn test_from_raw_pingresp() {
        let raw = RawPacket {
            header: HDR_PINGRESP,
            payload: Bytes::new(),
        };
        assert!(matches!(
            Packet::from_raw(raw, v5()).unwrap(),
            Packet::PingResp
        ));
    }

    #[test]
    fn test_from_raw_unknown_type() {
        let raw = RawPacket {
            header: 0x00, // type nibble 0 is reserved/invalid
            payload: Bytes::new(),
        };
        assert_eq!(
            Packet::from_raw(raw, v311()),
            Err(Error::UnknownPacketType(0))
        );
    }

    // ---- Packet::encode ----

    #[test]
    fn test_encode_pingreq() {
        let mut dst = BytesMut::new();
        Packet::PingReq.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[..], &[HDR_PINGREQ, 0x00]);
    }

    #[test]
    fn test_encode_pingresp() {
        let mut dst = BytesMut::new();
        Packet::PingResp.encode(&mut dst, v5()).unwrap();
        assert_eq!(&dst[..], &[HDR_PINGRESP, 0x00]);
    }

    // ---- round-trip ----

    fn round_trip(pkt: Packet, ver: MqttVersion) {
        let mut dst = BytesMut::new();
        pkt.encode(&mut dst, ver).unwrap();
        let raw = RawPacket::decode(&mut dst).unwrap().unwrap();
        // from_raw must not error for these known-good packets
        Packet::from_raw(raw, ver).unwrap();
    }

    #[test]
    fn test_round_trip_pingreq_v311() {
        round_trip(Packet::PingReq, v311());
    }

    #[test]
    fn test_round_trip_pingresp_v5() {
        round_trip(Packet::PingResp, v5());
    }

    // ---- all-or-nothing encode ----

    #[test]
    fn test_encode_error_leaves_dst_untouched_string_too_long() {
        let topic: String = core::iter::repeat_n('x', 70_000).collect();
        let pkt = Packet::Publish(Publish::new(topic, Bytes::new()));
        let mut dst = BytesMut::from(&b"existing"[..]);
        assert_eq!(pkt.encode(&mut dst, v5()), Err(Error::StringTooLong));
        assert_eq!(&dst[..], b"existing");
    }

    #[test]
    fn test_encode_error_leaves_dst_untouched_varlen_overflow() {
        let payload = Bytes::from(alloc::vec![0u8; varlen::MAX_VARLEN as usize + 1]);
        let pkt = Packet::Publish(Publish::new("t", payload));
        let mut dst = BytesMut::from(&b"existing"[..]);
        assert_eq!(pkt.encode(&mut dst, v5()), Err(Error::VarLenOverflow));
        assert_eq!(&dst[..], b"existing");
    }

    #[test]
    fn test_encode_error_leaves_dst_untouched_missing_packet_id() {
        let mut p = Publish::new("t", Bytes::new());
        p.qos = QoS::QoS1; // packet_id is None
        let pkt = Packet::Publish(p);
        let mut dst = BytesMut::from(&b"existing"[..]);
        assert_eq!(pkt.encode(&mut dst, v5()), Err(Error::MalformedPacket));
        assert_eq!(&dst[..], b"existing");
    }
}
