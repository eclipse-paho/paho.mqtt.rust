// paho-mqtt-codec/src/packet.rs
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
    Auth, ConnAck, Connect, Disconnect, Error, MqttVersion, PubAck, Publish, Result, SubAck,
    Subscribe, UnsubAck, Unsubscribe, varlen, wire::expect_end,
};
use alloc::boxed::Box;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// The MQTT control packet types, as carried in the upper four bits of the
/// fixed-header byte.
///
/// Available from a [`RawPacket`] before it is parsed, so a receiver can
/// dispatch on — or reject — a packet without decoding its body.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PacketType {
    /// Client connection request.
    Connect = 1,
    /// Connection acknowledgement.
    ConnAck = 2,
    /// Application message.
    Publish = 3,
    /// QoS 1 publish acknowledgement.
    PubAck = 4,
    /// QoS 2 publish received.
    PubRec = 5,
    /// QoS 2 publish release.
    PubRel = 6,
    /// QoS 2 publish complete.
    PubComp = 7,
    /// Subscription request.
    Subscribe = 8,
    /// Subscription acknowledgement.
    SubAck = 9,
    /// Unsubscription request.
    Unsubscribe = 10,
    /// Unsubscription acknowledgement.
    UnsubAck = 11,
    /// Keep-alive ping.
    PingReq = 12,
    /// Keep-alive ping response.
    PingResp = 13,
    /// Connection shutdown.
    Disconnect = 14,
    /// Extended authentication exchange (v5 only).
    Auth = 15,
}

impl PacketType {
    /// The fixed-header flag bits every packet of this type must carry, or
    /// `None` for PUBLISH, whose flags are data (DUP, QoS, RETAIN).
    #[inline]
    fn fixed_flags(self) -> Option<u8> {
        use PacketType::*;
        match self {
            Publish => None,
            PubRel | Subscribe | Unsubscribe => Some(0x02),
            _ => Some(0x00),
        }
    }

    /// The fixed-header byte for this type with its fixed flags (PUBLISH
    /// gets zero flags; the caller ORs in DUP/QoS/RETAIN).
    #[inline]
    fn header_byte(self) -> u8 {
        ((self as u8) << 4) | self.fixed_flags().unwrap_or(0)
    }
}

impl TryFrom<u8> for PacketType {
    type Error = Error;

    /// Converts a type nibble (1–15). Returns
    /// [`Error::UnknownPacketType`] for 0, which is reserved.
    #[inline]
    fn try_from(nibble: u8) -> Result<Self> {
        use PacketType::*;
        Ok(match nibble {
            1 => Connect,
            2 => ConnAck,
            3 => Publish,
            4 => PubAck,
            5 => PubRec,
            6 => PubRel,
            7 => PubComp,
            8 => Subscribe,
            9 => SubAck,
            10 => Unsubscribe,
            11 => UnsubAck,
            12 => PingReq,
            13 => PingResp,
            14 => Disconnect,
            15 => Auth,
            t => return Err(Error::UnknownPacketType(t)),
        })
    }
}

/// Largest total packet size (fixed header + remaining length) representable
/// on the wire: a 4-byte remaining-length field can encode [`varlen::MAX_VARLEN`]
/// bytes, plus 1 byte for the packet-type header and 4 for the length itself.
pub const MAX_PACKET_SIZE: u32 = varlen::MAX_VARLEN + 5;

/// Options controlling how leniently packets are decoded.
///
/// The decoder is strict by default about everything the specification
/// calls a Malformed Packet or Protocol Error, with a small number of
/// deliberate exceptions where a rule violation by the peer is harmless to
/// the receiver and rejecting the packet would only lose information. Those
/// exceptions can be turned into errors with [`DecodeOptions::strict`], which
/// is what a conformance tester or protocol analyzer wants; a client or
/// server keeping a connection alive wants the default.
///
/// Currently the only lenient case is a CONNACK that refuses the connection
/// but sets Session Present (MQTT-3.2.2-4): by default the flag is cleared and
/// the reason code kept; in strict mode it is [`Error::ProtocolError`].
///
/// The struct is `#[non_exhaustive]`; construct it with
/// [`DecodeOptions::default`], [`DecodeOptions::strict`], or the builder
/// methods.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub struct DecodeOptions {
    /// Report every specification violation, including the cases the
    /// default decoder tolerates.
    pub strict: bool,
}

impl DecodeOptions {
    /// The default: tolerate harmless violations.
    pub const LENIENT: Self = Self { strict: false };
    /// Report every violation.
    pub const STRICT: Self = Self { strict: true };

    /// Options that report every violation.
    pub const fn strict() -> Self {
        Self::STRICT
    }

    /// Sets whether every violation is reported.
    pub const fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }
}

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
    /// The packet type from the fixed header, without parsing the body.
    #[inline]
    pub fn packet_type(&self) -> Result<PacketType> {
        PacketType::try_from(self.header >> 4)
    }

    /// The flag bits (lower nibble) of the fixed header.
    #[inline]
    pub fn flags(&self) -> u8 {
        self.header & 0x0F
    }

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
        Ok(Some(Self { header, payload }))
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A typed MQTT packet.
///
/// This enum covers all packet types for MQTT v3.1, v3.1.1, and v5. The one
/// v5-only packet, `Auth`, is present but will not appear on v3 connections.
/// v5 fields (reason codes, properties) are present on all applicable structs
/// and default to empty/success on v3 connections.
///
/// `Connect` is boxed: it is by far the largest packet struct and occurs once
/// per connection, while every other variant is moved around per message, so
/// keeping the enum small is worth one allocation per CONNECT.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Packet {
    /// `CONNECT` — client connection request.
    Connect(Box<Connect>),
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
    /// The type of this packet.
    #[inline]
    pub fn packet_type(&self) -> PacketType {
        use Packet::*;
        match self {
            Connect(_) => PacketType::Connect,
            ConnAck(_) => PacketType::ConnAck,
            Publish(_) => PacketType::Publish,
            PubAck(_) => PacketType::PubAck,
            PubRec(_) => PacketType::PubRec,
            PubRel(_) => PacketType::PubRel,
            PubComp(_) => PacketType::PubComp,
            Subscribe(_) => PacketType::Subscribe,
            SubAck(_) => PacketType::SubAck,
            Unsubscribe(_) => PacketType::Unsubscribe,
            UnsubAck(_) => PacketType::UnsubAck,
            PingReq => PacketType::PingReq,
            PingResp => PacketType::PingResp,
            Disconnect(_) => PacketType::Disconnect,
            Auth(_) => PacketType::Auth,
        }
    }

    /// Parses a [`RawPacket`] into a typed [`Packet`] given `ver`, with the
    /// default (lenient) [`DecodeOptions`].
    ///
    /// CONNECT is self-describing and ignores `ver`: its protocol version is
    /// read from the wire into [`Connect::version`].
    pub fn from_raw(raw: RawPacket, ver: MqttVersion) -> Result<Self> {
        Self::from_raw_with(raw, ver, DecodeOptions::LENIENT)
    }

    /// Parses a [`RawPacket`] into a typed [`Packet`] given `ver` and
    /// `opts`; see [`DecodeOptions`] for what the options affect.
    pub fn from_raw_with(raw: RawPacket, ver: MqttVersion, opts: DecodeOptions) -> Result<Self> {
        use PacketType as T;

        let ty = raw.packet_type()?;
        let flags = raw.flags();

        // Every type except PUBLISH has fixed flag bits; a receiver must treat
        // any other value as malformed (MQTT-2.2.2-2). PUBLISH flags are
        // interpreted by `Publish::decode`.
        if let Some(expected) = ty.fixed_flags()
            && flags != expected
        {
            return Err(Error::MalformedPacket);
        }

        // Type 15 is reserved in v3.1 and v3.1.1; AUTH exists only in v5.
        if ty == T::Auth && ver != MqttVersion::V5 {
            return Err(Error::UnknownPacketType(ty as u8));
        }

        let pkt = match ty {
            T::Connect => Packet::Connect(Box::new(Connect::decode(raw.payload)?)),
            T::ConnAck => Packet::ConnAck(ConnAck::decode_with(raw.payload, ver, opts)?),
            T::Publish => Packet::Publish(Publish::decode(flags, raw.payload, ver)?),
            T::PubAck => Packet::PubAck(PubAck::decode(raw.payload, ver)?),
            T::PubRec => Packet::PubRec(PubAck::decode(raw.payload, ver)?),
            T::PubRel => Packet::PubRel(PubAck::decode(raw.payload, ver)?),
            T::PubComp => Packet::PubComp(PubAck::decode(raw.payload, ver)?),
            T::Subscribe => Packet::Subscribe(Subscribe::decode(raw.payload, ver)?),
            T::SubAck => Packet::SubAck(SubAck::decode(raw.payload, ver)?),
            T::Unsubscribe => Packet::Unsubscribe(Unsubscribe::decode(raw.payload, ver)?),
            T::UnsubAck => Packet::UnsubAck(UnsubAck::decode(raw.payload, ver)?),
            T::PingReq => {
                expect_end(&raw.payload)?;
                Packet::PingReq
            }
            T::PingResp => {
                expect_end(&raw.payload)?;
                Packet::PingResp
            }
            T::Disconnect => Packet::Disconnect(Disconnect::decode(raw.payload, ver)?),
            T::Auth => Packet::Auth(Auth::decode(raw.payload)?),
        };
        pkt.check_reason_codes(ty)?;
        Ok(pkt)
    }

    /// Checks that every reason code in this packet is one the specification
    /// lists for this packet type (MQTT v5 §2.4); each acknowledgement has
    /// its own allowed set. Returns [`Error::ProtocolError`] otherwise.
    ///
    /// Not version-gated: on v3 the only codes that can be present are
    /// the CONNACK return codes and SUBACK granted-QoS values, which are in
    /// their respective sets.
    #[inline]
    fn check_reason_codes(&self, ty: PacketType) -> Result<()> {
        use Packet::*;
        let ok = match self {
            ConnAck(a) => a.reason_code.is_allowed_in(ty),
            PubAck(a) | PubRec(a) | PubRel(a) | PubComp(a) => a.reason_code.is_allowed_in(ty),
            SubAck(a) => a.reason_codes.iter().all(|rc| rc.is_allowed_in(ty)),
            UnsubAck(a) => a.reason_codes.iter().all(|rc| rc.is_allowed_in(ty)),
            Disconnect(d) => d.reason_code.is_allowed_in(ty),
            Auth(a) => a.reason_code.is_allowed_in(ty),
            Connect(_) | Publish(_) | Subscribe(_) | Unsubscribe(_) | PingReq | PingResp => true,
        };
        if ok {
            Ok(())
        } else {
            Err(Error::ProtocolError)
        }
    }

    /// Returns the fixed-header byte and the remaining length for this packet.
    fn header_and_remaining(&self, ty: PacketType, ver: MqttVersion) -> (u8, usize) {
        use Packet::*;
        let hdr = ty.header_byte();
        match self {
            Connect(conn) => (hdr, conn.encoded_size()),
            ConnAck(ack) => (hdr, ack.encoded_size(ver)),
            Publish(pub_) => {
                let mut hdr = hdr;
                if pub_.dup {
                    hdr |= 0x08;
                }
                hdr |= (pub_.qos as u8) << 1;
                if pub_.retain {
                    hdr |= 0x01;
                }
                (hdr, pub_.encoded_size(ver))
            }
            PubAck(ack) | PubRec(ack) | PubRel(ack) | PubComp(ack) => (hdr, ack.encoded_size(ver)),
            Subscribe(s) => (hdr, s.encoded_size(ver)),
            SubAck(sa) => (hdr, sa.encoded_size(ver)),
            Unsubscribe(u) => (hdr, u.encoded_size(ver)),
            UnsubAck(ua) => (hdr, ua.encoded_size(ver)),
            PingReq | PingResp => (hdr, 0),
            Disconnect(d) => (hdr, d.encoded_size(ver)),
            Auth(a) => (hdr, a.encoded_size()),
        }
    }

    /// Returns the total number of bytes this packet encodes to, including
    /// the fixed header and the remaining-length field.
    ///
    /// Returns [`Error::VarLenOverflow`] if the body exceeds the protocol
    /// maximum ([`varlen::MAX_VARLEN`]).
    pub fn encoded_size(&self, ver: MqttVersion) -> Result<usize> {
        let (_, remaining) = self.header_and_remaining(self.packet_type(), ver);
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
    pub fn encode_split(&self, dst: &mut BytesMut, ver: MqttVersion) -> Result<Option<Bytes>> {
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

        let ty = self.packet_type();
        self.check_reason_codes(ty)?;
        let (hdr, remaining) = self.header_and_remaining(ty, ver);
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
                } else {
                    pub_.encode_header(dst, ver)?;
                    if !pub_.payload.is_empty() {
                        return Ok(Some(pub_.payload.clone()));
                    }
                }
            }
            PubAck(ack) | PubRec(ack) | PubRel(ack) | PubComp(ack) => ack.encode(dst, ver)?,
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
        QoS, ReasonCode, String, Subscription,
        types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5},
    };
    use alloc::vec;

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
        let mut src = BytesMut::from(&[PacketType::PingReq.header_byte()][..]);
        assert!(RawPacket::decode(&mut src).unwrap().is_none());
        assert_eq!(src.len(), 1); // not consumed
    }

    #[test]
    fn test_raw_decode_pingreq() {
        let mut src = BytesMut::from(&[PacketType::PingReq.header_byte(), 0x00][..]);
        let raw = RawPacket::decode(&mut src).unwrap().unwrap();
        assert_eq!(raw.header, PacketType::PingReq.header_byte());
        assert!(raw.payload.is_empty());
        assert!(src.is_empty());
    }

    #[test]
    fn test_raw_decode_pingresp() {
        let mut src = BytesMut::from(&[PacketType::PingResp.header_byte(), 0x00][..]);
        let raw = RawPacket::decode(&mut src).unwrap().unwrap();
        assert_eq!(raw.header, PacketType::PingResp.header_byte());
        assert!(raw.payload.is_empty());
        assert!(src.is_empty());
    }

    #[test]
    fn test_raw_decode_leaves_trailing_bytes() {
        // Two back-to-back PINGREQ frames.
        let mut src = BytesMut::from(
            &[
                PacketType::PingReq.header_byte(),
                0x00,
                PacketType::PingResp.header_byte(),
                0x00,
            ][..],
        );
        let raw = RawPacket::decode(&mut src).unwrap().unwrap();
        assert_eq!(raw.header, PacketType::PingReq.header_byte());
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
            header: PacketType::PingReq.header_byte(),
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
            header: PacketType::PingResp.header_byte(),
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
        use PacketType as T;
        let cases: [(PacketType, &[u8]); 12] = [
            (T::ConnAck, &[0x00, 0x00]),
            (T::PubAck, &[0x00, 0x01]),
            (T::PubRec, &[0x00, 0x01]),
            (T::PubRel, &[0x00, 0x01]),
            (T::PubComp, &[0x00, 0x01]),
            (T::SubAck, &[0x00, 0x01, 0x00]),
            (T::UnsubAck, &[0x00, 0x01]),
            (T::PingReq, &[]),
            (T::PingResp, &[]),
            (T::Disconnect, &[]),
            (T::Auth, &[]),
            (T::Connect, &[]),
        ];
        for (ty, body) in cases {
            let canonical = ty.fixed_flags().unwrap();
            for bad in (0u8..16).filter(|&f| f != canonical) {
                let raw = RawPacket {
                    header: ((ty as u8) << 4) | bad,
                    payload: Bytes::copy_from_slice(body),
                };
                assert_eq!(
                    Packet::from_raw(raw, v311()),
                    Err(Error::MalformedPacket),
                    "type {ty:?} flags {bad:#x}"
                );
            }
        }
    }

    #[test]
    fn test_from_raw_auth_rejected_on_v311() {
        let raw = RawPacket {
            header: PacketType::Auth.header_byte(),
            payload: Bytes::new(),
        };
        assert_eq!(
            Packet::from_raw(raw, v311()),
            Err(Error::UnknownPacketType(PacketType::Auth as u8))
        );
        // The smallest legal AUTH: Success, then a property list naming the
        // authentication method, which §3.15.2.2.2 requires.
        let raw = RawPacket {
            header: PacketType::Auth.header_byte(),
            payload: Bytes::from_static(&[0x00, 0x04, 0x15, 0x00, 0x01, b'x']),
        };
        assert!(matches!(Packet::from_raw(raw, v5()), Ok(Packet::Auth(_))));
    }

    // ---- reason codes per packet type (G3) ----

    #[test]
    fn test_from_raw_rejects_reason_code_from_wrong_packet() {
        // PUBACK carrying SessionTakenOver (0x8E): a DISCONNECT-only code.
        let raw = RawPacket {
            header: PacketType::PubAck.header_byte(),
            payload: Bytes::from_static(&[0x00, 0x01, 0x8E]),
        };
        assert_eq!(Packet::from_raw(raw, v5()), Err(Error::ProtocolError));
        // PUBREL may only say Success or PacketIdentifierNotFound.
        let raw = RawPacket {
            header: PacketType::PubRel.header_byte(),
            payload: Bytes::from_static(&[0x00, 0x01, 0x10]), // NoMatchingSubscribers
        };
        assert_eq!(Packet::from_raw(raw, v5()), Err(Error::ProtocolError));
        // ... but PUBACK may say NoMatchingSubscribers.
        let raw = RawPacket {
            header: PacketType::PubAck.header_byte(),
            payload: Bytes::from_static(&[0x00, 0x01, 0x10]),
        };
        assert!(Packet::from_raw(raw, v5()).is_ok());
        // SUBACK with a CONNACK-only code (BadUserNameOrPassword)
        let raw = RawPacket {
            header: PacketType::SubAck.header_byte(),
            payload: Bytes::from_static(&[0x00, 0x01, 0x00, 0x86]),
        };
        assert_eq!(Packet::from_raw(raw, v5()), Err(Error::ProtocolError));
        // AUTH with anything but Success/Continue/ReAuthenticate
        let raw = RawPacket {
            header: PacketType::Auth.header_byte(),
            payload: Bytes::from_static(&[0x80, 0x04, 0x15, 0x00, 0x01, b'x']),
        };
        assert_eq!(Packet::from_raw(raw, v5()), Err(Error::ProtocolError));
    }

    #[test]
    fn test_encode_rejects_reason_code_from_wrong_packet() {
        let mut ack = PubAck::new(1);
        ack.reason_code = ReasonCode::SessionTakenOver;
        let mut dst = BytesMut::new();
        assert_eq!(
            Packet::PubComp(ack).encode(&mut dst, v5()),
            Err(Error::ProtocolError)
        );
        assert!(dst.is_empty());
    }

    // ---- DecodeOptions ----

    #[test]
    fn test_decode_options_constructors() {
        assert!(!DecodeOptions::default().strict);
        assert_eq!(DecodeOptions::default(), DecodeOptions::LENIENT);
        assert!(DecodeOptions::strict().strict);
        assert_eq!(DecodeOptions::strict(), DecodeOptions::STRICT);
        assert!(!DecodeOptions::STRICT.with_strict(false).strict);
    }

    #[test]
    fn test_from_raw_with_strict_reaches_connack() {
        // CONNACK: session_present=1, NotAuthorized, no properties.
        let raw = RawPacket {
            header: PacketType::ConnAck.header_byte(),
            payload: Bytes::from_static(&[0x01, 0x87, 0x00]),
        };
        assert_eq!(
            Packet::from_raw_with(raw.clone(), v5(), DecodeOptions::STRICT),
            Err(Error::ProtocolError)
        );
        match Packet::from_raw(raw, v5()) {
            Ok(Packet::ConnAck(a)) => {
                assert!(!a.session_present);
                assert_eq!(a.reason_code, ReasonCode::NotAuthorized);
            }
            other => panic!("{other:?}"),
        }
    }

    // ---- PacketType ----

    #[test]
    fn test_packet_type_round_trips_nibble() {
        for n in 1u8..=15 {
            let ty = PacketType::try_from(n).unwrap();
            assert_eq!(ty as u8, n);
        }
        assert_eq!(PacketType::try_from(0), Err(Error::UnknownPacketType(0)));
    }

    #[test]
    fn test_raw_packet_type_and_flags() {
        let raw = RawPacket {
            header: 0x3D, // PUBLISH, dup + qos2 + retain
            payload: Bytes::new(),
        };
        assert_eq!(raw.packet_type(), Ok(PacketType::Publish));
        assert_eq!(raw.flags(), 0x0D);
        let raw = RawPacket {
            header: 0x0F,
            payload: Bytes::new(),
        };
        assert_eq!(raw.packet_type(), Err(Error::UnknownPacketType(0)));
    }

    #[test]
    fn test_packet_packet_type() {
        assert_eq!(Packet::PingReq.packet_type(), PacketType::PingReq);
        assert_eq!(
            Packet::PubRec(PubAck::new(1)).packet_type(),
            PacketType::PubRec
        );
        assert_eq!(
            Packet::Publish(Publish::new("t", Bytes::new())).packet_type(),
            PacketType::Publish
        );
    }

    // ---- Packet::encode ----

    #[test]
    fn test_encode_pingreq() {
        let mut dst = BytesMut::new();
        Packet::PingReq.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[..], &[PacketType::PingReq.header_byte(), 0x00]);
    }

    #[test]
    fn test_encode_pingresp() {
        let mut dst = BytesMut::new();
        Packet::PingResp.encode(&mut dst, v5()).unwrap();
        assert_eq!(&dst[..], &[PacketType::PingResp.header_byte(), 0x00]);
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

    // ---- Packet identifier 0 is rejected everywhere (MQTT-2.2.1-3) ----

    /// Frames `body` under `header` and parses it.
    fn parse(header: u8, body: &[u8], ver: MqttVersion) -> Result<Packet> {
        let mut src = BytesMut::new();
        src.put_u8(header);
        varlen::encode(body.len() as u32, &mut src).unwrap();
        src.put_slice(body);
        Packet::from_raw(RawPacket::decode(&mut src).unwrap().unwrap(), ver)
    }

    #[test]
    fn test_decode_packet_id_zero_is_protocol_error_in_every_packet() {
        use PacketType as T;
        let cases: [(PacketType, &[u8]); 8] = [
            (T::PubAck, &[0x00, 0x00]),
            (T::PubRec, &[0x00, 0x00]),
            (T::PubRel, &[0x00, 0x00]),
            (T::PubComp, &[0x00, 0x00]),
            (T::Subscribe, &[0x00, 0x00, 0x00, 0x01, b'a', 0x00]),
            (T::SubAck, &[0x00, 0x00, 0x00]),
            (T::Unsubscribe, &[0x00, 0x00, 0x00, 0x01, b'a']),
            (T::UnsubAck, &[0x00, 0x00]),
        ];
        for (ty, body) in cases {
            assert_eq!(
                parse(ty.header_byte(), body, v311()),
                Err(Error::ProtocolError),
                "{ty:?} v3"
            );
        }
        // Same bodies are valid once the identifier is non-zero.
        for (ty, body) in cases {
            let mut fixed = body.to_vec();
            fixed[1] = 0x07;
            assert!(parse(ty.header_byte(), &fixed, v311()).is_ok(), "{ty:?}");
        }
        // PUBLISH QoS 1 with ID 0 was already rejected; keep it pinned.
        assert_eq!(
            parse(0x32, &[0x00, 0x01, b'a', 0x00, 0x00], v311()),
            Err(Error::ProtocolError)
        );
    }

    // ---- shorthand acks: the invalid cousins, framed ----

    /// Off the wire the property length is checked against the frame, not
    /// against the rest of the buffer: a remaining length that stops early
    /// while the property length says there is more must be rejected for every
    /// packet that has, or is near, a v5 shorthand form. Whether the packet
    /// has a shorthand only decides *where* the body may legally end.
    #[test]
    fn test_from_raw_rejects_property_length_past_the_frame() {
        use PacketType as T;
        // Each body's last byte is a property length of 5 with nothing after.
        let cases: [(PacketType, &[u8]); 7] = [
            (T::PubAck, &[0x00, 0x01, 0x00, 0x05]),
            (T::PubRec, &[0x00, 0x01, 0x00, 0x05]),
            (T::PubRel, &[0x00, 0x01, 0x00, 0x05]),
            (T::PubComp, &[0x00, 0x01, 0x00, 0x05]),
            (T::Disconnect, &[0x00, 0x05]),
            (T::SubAck, &[0x00, 0x01, 0x05]),
            (T::UnsubAck, &[0x00, 0x01, 0x05]),
        ];
        for (ty, body) in cases {
            assert_eq!(
                parse(ty.header_byte(), body, v5()),
                Err(Error::PacketTooShort),
                "{ty:?}"
            );
        }
        // AUTH, which has no shorthand at all, fails the same way.
        assert_eq!(
            parse(T::Auth.header_byte(), &[0x18, 0x05], v5()),
            Err(Error::PacketTooShort)
        );
    }

    /// A frame one byte longer than the properties it declares: the trailing
    /// byte is inside the remaining length, so only the packet parser can
    /// catch it.
    #[test]
    fn test_from_raw_rejects_a_byte_past_the_properties() {
        use PacketType as T;
        let cases: [(PacketType, &[u8]); 3] = [
            (T::PubAck, &[0x00, 0x01, 0x00, 0x00, 0xFF]),
            (T::Disconnect, &[0x00, 0x00, 0xFF]),
            (T::PubRel, &[0x00, 0x01, 0x00, 0x00, 0xFF]),
        ];
        for (ty, body) in cases {
            assert_eq!(
                parse(ty.header_byte(), body, v5()),
                Err(Error::MalformedPacket),
                "{ty:?}"
            );
        }
    }

    #[test]
    fn test_encode_packet_id_zero_is_protocol_error_in_every_packet() {
        let mut publish = Publish::new("a", "x");
        publish.qos = QoS::QoS1;
        publish.packet_id = Some(0);
        let pkts = [
            Packet::Publish(publish),
            Packet::PubAck(PubAck::new(0)),
            Packet::PubRec(PubAck::new(0)),
            Packet::PubRel(PubAck::new(0)),
            Packet::PubComp(PubAck::new(0)),
            Packet::Subscribe(Subscribe::new(0, vec![Subscription::new("a", QoS::QoS0)])),
            Packet::SubAck(SubAck::new(0, vec![ReasonCode::Success])),
            Packet::Unsubscribe(Unsubscribe::new(0, vec![String::from("a")])),
            Packet::UnsubAck(UnsubAck::with_properties(
                0,
                vec![ReasonCode::Success],
                None,
            )),
        ];
        for pkt in pkts {
            for ver in [v311(), v5()] {
                let mut dst = BytesMut::new();
                assert_eq!(
                    pkt.encode(&mut dst, ver),
                    Err(Error::ProtocolError),
                    "{:?} {ver}",
                    pkt.packet_type()
                );
                assert!(dst.is_empty(), "nothing written on failure");
            }
        }
    }

    // ---- Trailing bytes are malformed (§2.1.4) ----

    #[test]
    fn test_pingreq_pingresp_with_body_are_malformed() {
        assert_eq!(
            parse(PacketType::PingReq.header_byte(), &[0x00], v5()),
            Err(Error::MalformedPacket)
        );
        assert_eq!(
            parse(PacketType::PingResp.header_byte(), &[0x00, 0x00], v311()),
            Err(Error::MalformedPacket)
        );
        // Zero remaining length is still fine.
        assert_eq!(
            parse(PacketType::PingReq.header_byte(), &[], v5()),
            Ok(Packet::PingReq)
        );
    }

    #[test]
    fn test_trailing_bytes_after_fixed_size_bodies_are_malformed() {
        use PacketType as T;
        // (type, minimal valid body, version)
        let cases: [(PacketType, &[u8], MqttVersion); 9] = [
            (T::ConnAck, &[0x00, 0x00], v311()),
            (T::ConnAck, &[0x00, 0x00, 0x00], v5()), // with empty properties
            (T::PubAck, &[0x00, 0x01], v311()),
            (T::PubAck, &[0x00, 0x01, 0x00, 0x00], v5()), // rc + empty props
            (T::PubRel, &[0x00, 0x01], v311()),
            (T::UnsubAck, &[0x00, 0x01], v311()),
            (T::Disconnect, &[], v311()),
            (T::Disconnect, &[0x00, 0x00], v5()), // rc + empty props
            (T::Auth, &[0x00, 0x04, 0x15, 0x00, 0x01, b'x'], v5()), // rc + method
        ];
        for (ty, body, ver) in cases {
            assert!(
                parse(ty.header_byte(), body, ver).is_ok(),
                "{ty:?} {ver} baseline"
            );
            let mut extra = body.to_vec();
            extra.push(0xFF);
            assert_eq!(
                parse(ty.header_byte(), &extra, ver),
                Err(Error::MalformedPacket),
                "{ty:?} {ver} with a trailing byte"
            );
        }
    }

    #[test]
    fn test_connect_with_trailing_bytes_is_malformed() {
        let mut conn = Connect::new(v5(), true, "c");
        conn.username = Some(String::from("u"));
        conn.password = Some(Bytes::from_static(b"p"));
        let mut dst = BytesMut::new();
        Packet::Connect(Box::new(conn))
            .encode(&mut dst, v5())
            .unwrap();
        // Append one byte and patch the remaining length (single-byte VBI here).
        dst.put_u8(0xFF);
        dst[1] += 1;
        let raw = RawPacket::decode(&mut dst).unwrap().unwrap();
        assert_eq!(Packet::from_raw(raw, v5()), Err(Error::MalformedPacket));
    }

    #[test]
    fn test_v3_disconnect_with_body_is_malformed() {
        assert_eq!(
            parse(PacketType::Disconnect.header_byte(), &[0x00], v311()),
            Err(Error::MalformedPacket)
        );
        // The same bytes are a valid v5 DISCONNECT (reason code, no props).
        assert!(parse(PacketType::Disconnect.header_byte(), &[0x00], v5()).is_ok());
    }
}
