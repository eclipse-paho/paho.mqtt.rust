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

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
    /// body is buffered. In MQTT v5 each side advertises the largest packet
    /// it will accept (the `MaximumPacketSize` property in CONNECT for a
    /// client, in CONNACK for a server) and the peer is required not to
    /// exceed it; pass that value here.
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
    ///
    /// CONNECT is self-describing and ignores `ver`: its protocol version is
    /// read from the wire into [`Connect::version`].
    pub fn from_raw(raw: RawPacket, ver: MqttVersion) -> Result<Self> {
        let typ = raw.header >> 4;
        let flags = raw.header & 0x0F;

        // Every type except PUBLISH has fixed flag bits; a receiver must treat
        // any other value as malformed (MQTT-2.2.2-2). PUBLISH flags are
        // interpreted by `Publish::decode`.
        let expected_flags = match typ {
            PKT_PUBLISH => flags,
            PKT_PUBREL | PKT_SUBSCRIBE | PKT_UNSUBSCRIBE => 0x02,
            _ => 0x00,
        };
        if flags != expected_flags {
            return Err(Error::MalformedPacket);
        }

        match typ {
            PKT_CONNECT => Ok(Packet::Connect(Connect::decode(raw.payload)?)),
            PKT_CONNACK => Ok(Packet::ConnAck(ConnAck::decode(raw.payload, ver)?)),
            PKT_PUBLISH => Ok(Packet::Publish(Publish::decode(flags, raw.payload, ver)?)),
            PKT_PUBACK => Ok(Packet::PubAck(PubAck::decode(raw.payload, ver)?)),
            PKT_PUBREC => Ok(Packet::PubRec(PubAck::decode(raw.payload, ver)?)),
            PKT_PUBREL => Ok(Packet::PubRel(PubAck::decode(raw.payload, ver)?)),
            PKT_PUBCOMP => Ok(Packet::PubComp(PubAck::decode(raw.payload, ver)?)),
            PKT_SUBSCRIBE => Ok(Packet::Subscribe(Subscribe::decode(raw.payload, ver)?)),
            PKT_SUBACK => Ok(Packet::SubAck(SubAck::decode(raw.payload, ver)?)),
            PKT_UNSUBSCRIBE => {
                Ok(Packet::Unsubscribe(Unsubscribe::decode(raw.payload, ver)?))
            }
            PKT_UNSUBACK => Ok(Packet::UnsubAck(UnsubAck::decode(raw.payload, ver)?)),
            PKT_PINGREQ => Ok(Packet::PingReq),
            PKT_PINGRESP => Ok(Packet::PingResp),
            PKT_DISCONNECT => Ok(Packet::Disconnect(Disconnect::decode(raw.payload, ver)?)),
            // Type 15 is reserved in v3.1.1; AUTH exists only in v5.
            PKT_AUTH if ver == MqttVersion::V5 => {
                Ok(Packet::Auth(Auth::decode(raw.payload)?))
            }
            t => Err(Error::UnknownPacketType(t)),
        }
    }

    /// Returns the fixed-header byte and the remaining length for this packet.
    fn header_and_remaining(&self, ver: MqttVersion) -> (u8, usize) {
        use Packet::*;
        match self {
            Connect(conn) => (HDR_CONNECT, conn.encoded_size()),
            ConnAck(ack) => (HDR_CONNACK, ack.encoded_size(ver)),
            Publish(pub_) => {
                let mut hdr = PKT_PUBLISH << 4;
                if pub_.dup {
                    hdr |= 0x08;
                }
                hdr |= (pub_.qos as u8) << 1;
                if pub_.retain {
                    hdr |= 0x01;
                }
                (hdr, pub_.encoded_size(ver))
            }
            PubAck(ack) => (HDR_PUBACK, ack.encoded_size(ver)),
            PubRec(ack) => (HDR_PUBREC, ack.encoded_size(ver)),
            PubRel(ack) => (HDR_PUBREL, ack.encoded_size(ver)),
            PubComp(ack) => (HDR_PUBCOMP, ack.encoded_size(ver)),
            Subscribe(s) => (HDR_SUBSCRIBE, s.encoded_size(ver)),
            SubAck(sa) => (HDR_SUBACK, sa.encoded_size(ver)),
            Unsubscribe(u) => (HDR_UNSUBSCRIBE, u.encoded_size(ver)),
            UnsubAck(ua) => (HDR_UNSUBACK, ua.encoded_size(ver)),
            PingReq => (HDR_PINGREQ, 0),
            PingResp => (HDR_PINGRESP, 0),
            Disconnect(d) => (HDR_DISCONNECT, d.encoded_size(ver)),
            Auth(a) => (HDR_AUTH, a.encoded_size()),
        }
    }

    /// Returns the total number of bytes this packet encodes to, including
    /// the fixed header and the remaining-length field.
    ///
    /// Returns [`Error::VarLenOverflow`] if the body exceeds the protocol
    /// maximum ([`varlen::MAX_VARLEN`]).
    pub fn encoded_size(&self, ver: MqttVersion) -> Result<usize> {
        let (_, remaining) = self.header_and_remaining(ver);
        let remaining = Self::checked_remaining(remaining)?;
        Ok(1 + varlen::encoded_size(remaining) + remaining as usize)
    }

    fn checked_remaining(remaining: usize) -> Result<u32> {
        u32::try_from(remaining)
            .ok()
            .filter(|&n| n <= varlen::MAX_VARLEN)
            .ok_or(Error::VarLenOverflow)
    }

    /// Encodes this packet into `dst`.
    ///
    /// The exact output size is computed up front and reserved in `dst`, so
    /// the encode performs at most one allocation.
    ///
    /// CONNECT ignores `ver` and encodes with [`Connect::version`].
    ///
    /// On error nothing is appended to `dst`: any bytes written before the
    /// failure was detected are removed.
    pub fn encode(&self, dst: &mut BytesMut, ver: MqttVersion) -> Result<()> {
        let start = dst.len();
        let res = self.encode_inner(dst, ver, true);
        if res.is_err() {
            dst.truncate(start);
        }
        res.map(|_| ())
    }

    /// Encodes this packet into a freshly allocated, exactly sized [`Bytes`].
    ///
    /// Useful when the same encoding must be handed to more than one consumer
    /// (e.g. persisted and transmitted): the returned `Bytes` can be cloned
    /// and shared without re-encoding or copying.
    pub fn encode_to_bytes(&self, ver: MqttVersion) -> Result<Bytes> {
        let mut dst = BytesMut::new();
        self.encode(&mut dst, ver)?;
        Ok(dst.freeze())
    }

    /// Encodes everything except the application payload into `dst` and
    /// returns the payload, if there is one, to be written after it.
    ///
    /// For a PUBLISH this appends the fixed header and variable header (topic,
    /// packet ID, properties) to `dst` and returns a cheap reference-counted
    /// handle to [`Publish::payload`], which the caller can write directly —
    /// with a vectored write, say — instead of copying it into `dst`. For
    /// every other packet type the whole packet is written to `dst` and
    /// `None` is returned.
    ///
    /// The remaining-length field always accounts for the payload, so the
    /// bytes in `dst` followed by the returned payload form one complete,
    /// valid packet. `None` is also returned for a PUBLISH whose payload is
    /// empty.
    ///
    /// On error nothing is appended to `dst`.
    pub fn encode_split(
        &self,
        dst: &mut BytesMut,
        ver: MqttVersion,
    ) -> Result<Option<Bytes>> {
        let start = dst.len();
        let res = self.encode_inner(dst, ver, false);
        if res.is_err() {
            dst.truncate(start);
        }
        res
    }

    /// Shared body of [`encode`](Self::encode) and
    /// [`encode_split`](Self::encode_split). With `inline_payload` the PUBLISH
    /// payload is copied into `dst`; otherwise it is returned.
    fn encode_inner(
        &self,
        dst: &mut BytesMut,
        ver: MqttVersion,
        inline_payload: bool,
    ) -> Result<Option<Bytes>> {
        use Packet::*;

        let (hdr, remaining) = self.header_and_remaining(ver);
        let remaining = Self::checked_remaining(remaining)?;
        let mut reserve = 1 + varlen::encoded_size(remaining) + remaining as usize;
        if let Publish(pub_) = self
            && !inline_payload
        {
            reserve -= pub_.payload.len();
        }
        dst.reserve(reserve);

        dst.put_u8(hdr);
        varlen::encode(remaining, dst)?;

        match self {
            Connect(conn) => conn.encode(dst)?,
            ConnAck(ack) => ack.encode(dst, ver)?,
            Publish(pub_) => {
                if inline_payload {
                    pub_.encode(dst, ver)?;
                }
                else {
                    pub_.encode_header(dst, ver)?;
                    if !pub_.payload.is_empty() {
                        return Ok(Some(pub_.payload.clone()));
                    }
                }
            }
            PubAck(ack) | PubRec(ack) | PubRel(ack) | PubComp(ack) => {
                ack.encode(dst, ver)?
            }
            Subscribe(s) => s.encode(dst, ver)?,
            SubAck(sa) => sa.encode(dst, ver)?,
            Unsubscribe(u) => u.encode(dst, ver)?,
            UnsubAck(ua) => ua.encode(dst, ver)?,
            PingReq | PingResp => {}
            Disconnect(d) => d.encode(dst, ver)?,
            Auth(a) => a.encode(dst)?,
        }
        Ok(None)
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        QoS, String, Subscription,
        types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5},
    };

    fn v311() -> MqttVersion {
        MQTT_VERSION_3_1_1
    }

    fn v5() -> MqttVersion {
        MQTT_VERSION_5
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

    #[test]
    fn test_from_raw_reserved_flag_bits_rejected() {
        // Every fixed-flag type must reject a non-canonical flag nibble.
        let cases: [(u8, &[u8]); 12] = [
            (PKT_CONNACK, &[0x00, 0x00]),
            (PKT_PUBACK, &[0x00, 0x01]),
            (PKT_PUBREC, &[0x00, 0x01]),
            (PKT_PUBREL, &[0x00, 0x01]),
            (PKT_PUBCOMP, &[0x00, 0x01]),
            (PKT_SUBACK, &[0x00, 0x01, 0x00]),
            (PKT_UNSUBACK, &[0x00, 0x01]),
            (PKT_PINGREQ, &[]),
            (PKT_PINGRESP, &[]),
            (PKT_DISCONNECT, &[]),
            (PKT_AUTH, &[]),
            (PKT_CONNECT, &[]),
        ];
        for (typ, body) in cases {
            let canonical = match typ {
                PKT_PUBREL | PKT_SUBSCRIBE | PKT_UNSUBSCRIBE => 0x02,
                _ => 0x00,
            };
            for bad in (0u8..16).filter(|&f| f != canonical) {
                let raw = RawPacket {
                    header: (typ << 4) | bad,
                    payload: Bytes::copy_from_slice(body),
                };
                assert_eq!(
                    Packet::from_raw(raw, v311()),
                    Err(Error::MalformedPacket),
                    "type {typ} flags {bad:#x}"
                );
            }
        }
    }

    #[test]
    fn test_from_raw_auth_rejected_on_v311() {
        let raw = RawPacket {
            header: HDR_AUTH,
            payload: Bytes::new(),
        };
        assert_eq!(
            Packet::from_raw(raw, v311()),
            Err(Error::UnknownPacketType(PKT_AUTH))
        );
        let raw = RawPacket {
            header: HDR_AUTH,
            payload: Bytes::new(),
        };
        assert!(matches!(Packet::from_raw(raw, v5()), Ok(Packet::Auth(_))));
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
        assert_eq!(pkt.encoded_size(ver).unwrap(), dst.len());
        let raw = RawPacket::decode(&mut dst).unwrap().unwrap();
        // from_raw must not error for these known-good packets
        Packet::from_raw(raw, ver).unwrap();
    }

    #[test]
    fn test_encoded_size_includes_fixed_header() {
        // PUBLISH "t" with a 200-byte payload: remaining = 2+1+1(props)+200 = 204,
        // which needs a 2-byte varlen, so total = 1 + 2 + 204.
        let pkt = Packet::Publish(Publish::new("t", Bytes::from(alloc::vec![0u8; 200])));
        assert_eq!(pkt.encoded_size(v5()).unwrap(), 207);
        let mut dst = BytesMut::new();
        pkt.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), 207);
    }

    #[test]
    fn test_encoded_size_overflow_errors() {
        let payload = Bytes::from(alloc::vec![0u8; varlen::MAX_VARLEN as usize + 1]);
        let pkt = Packet::Publish(Publish::new("t", payload));
        assert_eq!(pkt.encoded_size(v5()), Err(Error::VarLenOverflow));
    }

    // ---- encode_split / encode_to_bytes ----

    fn sample_publish(payload: &'static [u8]) -> Packet {
        let mut p = Publish::new("a/b", Bytes::from_static(payload));
        p.qos = QoS::QoS1;
        p.packet_id = Some(9);
        Packet::Publish(p)
    }

    #[test]
    fn test_encode_split_publish_returns_payload_without_copy() {
        let payload: &'static [u8] = b"hello, world";
        let pkt = sample_publish(payload);
        let mut hdr = BytesMut::new();
        let tail = pkt.encode_split(&mut hdr, v5()).unwrap().unwrap();
        // The returned Bytes is the very same allocation as the packet's payload.
        assert_eq!(tail.as_ptr(), payload.as_ptr());
        // header + payload is byte-for-byte what `encode` produces.
        let mut full = BytesMut::new();
        pkt.encode(&mut full, v5()).unwrap();
        let mut joined = hdr.clone();
        joined.extend_from_slice(&tail);
        assert_eq!(&joined[..], &full[..]);
        // and the header alone is exactly `encoded_size - payload.len()` bytes.
        assert_eq!(hdr.len(), pkt.encoded_size(v5()).unwrap() - payload.len());
    }

    #[test]
    fn test_encode_split_publish_reserves_only_header() {
        let pkt = sample_publish(&[0u8; 4096]);
        let mut hdr = BytesMut::new();
        pkt.encode_split(&mut hdr, v5()).unwrap();
        assert_eq!(hdr.capacity(), hdr.len());
        assert!(hdr.len() < 64);
    }

    #[test]
    fn test_encode_split_empty_payload_returns_none() {
        let pkt = sample_publish(b"");
        let mut hdr = BytesMut::new();
        assert!(pkt.encode_split(&mut hdr, v5()).unwrap().is_none());
        let mut full = BytesMut::new();
        pkt.encode(&mut full, v5()).unwrap();
        assert_eq!(&hdr[..], &full[..]);
    }

    #[test]
    fn test_encode_split_non_publish_writes_whole_packet() {
        for pkt in [
            Packet::PubAck(PubAck::new(1)),
            Packet::Subscribe(Subscribe::new(
                2,
                alloc::vec![Subscription::new("t", QoS::QoS0)],
            )),
            Packet::PingReq,
            Packet::Disconnect(Disconnect::default()),
        ] {
            let mut split = BytesMut::new();
            assert!(pkt.encode_split(&mut split, v5()).unwrap().is_none());
            let mut full = BytesMut::new();
            pkt.encode(&mut full, v5()).unwrap();
            assert_eq!(&split[..], &full[..]);
        }
    }

    #[test]
    fn test_encode_split_error_leaves_dst_untouched() {
        let mut p = Publish::new("t", Bytes::from_static(b"x"));
        p.qos = QoS::QoS2; // packet_id missing
        let pkt = Packet::Publish(p);
        let mut dst = BytesMut::from(&b"keep"[..]);
        assert_eq!(pkt.encode_split(&mut dst, v5()), Err(Error::ProtocolError));
        assert_eq!(&dst[..], b"keep");
    }

    #[test]
    fn test_encode_to_bytes_matches_encode() {
        let pkt = sample_publish(b"payload");
        let bytes = pkt.encode_to_bytes(v5()).unwrap();
        let mut full = BytesMut::new();
        pkt.encode(&mut full, v5()).unwrap();
        assert_eq!(&bytes[..], &full[..]);
        assert_eq!(bytes.len(), pkt.encoded_size(v5()).unwrap());
    }

    #[test]
    fn test_encode_reserves_exact_capacity() {
        let pkt = Packet::Publish(Publish::new("topic", Bytes::from_static(b"payload")));
        let mut dst = BytesMut::new();
        pkt.encode(&mut dst, v5()).unwrap();
        // A fresh buffer is sized in one reservation to exactly what was written.
        assert_eq!(dst.capacity(), dst.len());
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
        assert_eq!(pkt.encode(&mut dst, v5()), Err(Error::ProtocolError));
        assert_eq!(&dst[..], b"existing");
    }
}
