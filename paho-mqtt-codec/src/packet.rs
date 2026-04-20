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
    connect::{ConnAck, Connect},
    error::{Error, Result},
    publish::{PubAck, Publish},
    types::MqttVersion,
    varlen,
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
const PKT_PINGREQ: u8 = 12;
const PKT_PINGRESP: u8 = 13;

// ---- fixed header bytes ----

const HDR_CONNECT: u8 = PKT_CONNECT << 4; // 0x10
const HDR_CONNACK: u8 = PKT_CONNACK << 4; // 0x20
const HDR_PUBACK: u8 = PKT_PUBACK << 4; // 0x40
const HDR_PUBREC: u8 = PKT_PUBREC << 4; // 0x50
const HDR_PUBREL: u8 = (PKT_PUBREL << 4) | 0x02; // 0x62  (reserved flags = 0b0010)
const HDR_PUBCOMP: u8 = PKT_PUBCOMP << 4; // 0x70
const HDR_PINGREQ: u8 = PKT_PINGREQ << 4;
const HDR_PINGRESP: u8 = PKT_PINGRESP << 4;

/// A raw, uninterpreted MQTT packet produced by the framing layer.
///
/// The `header` byte carries the packet type (upper 4 bits) and flags
/// (lower 4 bits). The `payload` contains exactly `remaining_length` bytes
/// as decoded from the fixed header — no more, no less.
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
    pub fn decode(src: &mut BytesMut) -> Result<Option<Self>> {
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

        if src.len() < total {
            return Ok(None);
        }

        let header = src[0];
        src.advance(1 + varlen_size);
        let payload = src.copy_to_bytes(remaining_len);
        Ok(Some(RawPacket { header, payload }))
    }
}

/// A typed MQTT packet.
///
/// This enum covers all packet types for both MQTT v3.1.1 and v5. v5-only
/// packets (e.g. `Auth`) are present but will not appear on v3.1.1
/// connections. v5 fields (reason codes, properties) are present on all
/// applicable structs and default to empty/success on v3.1.1 connections.
#[derive(Debug, PartialEq)]
pub enum Packet {
    /// `CONNECT` — connection request from client to broker.
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
    /// `PINGREQ` — keep-alive ping from client to broker.
    PingReq,
    /// `PINGRESP` — keep-alive response from broker to client.
    PingResp,
}

impl Packet {
    /// Parses a [`RawPacket`] into a typed [`Packet`] given `version`.
    pub fn from_raw(raw: RawPacket, version: MqttVersion) -> Result<Self> {
        match raw.header >> 4 {
            PKT_CONNECT => Ok(Packet::Connect(Connect::decode(raw.payload, version)?)),
            PKT_CONNACK => Ok(Packet::ConnAck(ConnAck::decode(raw.payload, version)?)),
            PKT_PUBLISH => Ok(Packet::Publish(Publish::decode(
                raw.header & 0x0F,
                raw.payload,
                version,
            )?)),
            PKT_PUBACK => Ok(Packet::PubAck(PubAck::decode(raw.payload, version)?)),
            PKT_PUBREC => Ok(Packet::PubRec(PubAck::decode(raw.payload, version)?)),
            PKT_PUBREL => {
                if raw.header & 0x0F != 0x02 {
                    return Err(Error::MalformedPacket);
                }
                Ok(Packet::PubRel(PubAck::decode(raw.payload, version)?))
            }
            PKT_PUBCOMP => Ok(Packet::PubComp(PubAck::decode(raw.payload, version)?)),
            PKT_PINGREQ => Ok(Packet::PingReq),
            PKT_PINGRESP => Ok(Packet::PingResp),
            t => Err(Error::UnknownPacketType(t)),
        }
    }

    /// Encodes this packet into `dst`.
    pub fn encode(&self, dst: &mut BytesMut, version: MqttVersion) -> Result<()> {
        match self {
            Packet::Connect(conn) => {
                let remaining = conn.encoded_size(version);
                dst.put_u8(HDR_CONNECT);
                varlen::encode(remaining as u32, dst)?;
                conn.encode(dst, version)?;
            }
            Packet::ConnAck(ack) => {
                let remaining = ack.encoded_size(version);
                dst.put_u8(HDR_CONNACK);
                varlen::encode(remaining as u32, dst)?;
                ack.encode(dst, version)?;
            }
            Packet::Publish(pub_) => {
                let remaining = pub_.encoded_size(version);
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
                pub_.encode(dst, version)?;
            }
            Packet::PubAck(ack) => {
                dst.put_u8(HDR_PUBACK);
                varlen::encode(ack.encoded_size(version) as u32, dst)?;
                ack.encode(dst, version)?;
            }
            Packet::PubRec(ack) => {
                dst.put_u8(HDR_PUBREC);
                varlen::encode(ack.encoded_size(version) as u32, dst)?;
                ack.encode(dst, version)?;
            }
            Packet::PubRel(ack) => {
                dst.put_u8(HDR_PUBREL);
                varlen::encode(ack.encoded_size(version) as u32, dst)?;
                ack.encode(dst, version)?;
            }
            Packet::PubComp(ack) => {
                dst.put_u8(HDR_PUBCOMP);
                varlen::encode(ack.encoded_size(version) as u32, dst)?;
                ack.encode(dst, version)?;
            }
            Packet::PingReq => {
                dst.put_u8(HDR_PINGREQ);
                varlen::encode(0, dst)?;
            }
            Packet::PingResp => {
                dst.put_u8(HDR_PINGRESP);
                varlen::encode(0, dst)?;
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
    use crate::types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5};

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
        assert_eq!(RawPacket::decode(&mut src).unwrap().is_none(), true);
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

    fn round_trip(pkt: Packet, version: MqttVersion) {
        let mut dst = BytesMut::new();
        pkt.encode(&mut dst, version).unwrap();
        let raw = RawPacket::decode(&mut dst).unwrap().unwrap();
        // from_raw must not error for these known-good packets
        Packet::from_raw(raw, version).unwrap();
    }

    #[test]
    fn test_round_trip_pingreq_v311() {
        round_trip(Packet::PingReq, v311());
    }

    #[test]
    fn test_round_trip_pingresp_v5() {
        round_trip(Packet::PingResp, v5());
    }
}
