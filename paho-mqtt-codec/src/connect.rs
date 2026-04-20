// paho-mqtt-codec/src/connect.rs
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

//! MQTT CONNECT and CONNACK packets.

use crate::{
    error::{Error, Result},
    properties::Properties,
    reason_code::ReasonCode,
    types::{MqttVersion, QoS},
    varlen,
    wire::{binary_wire_size, get_binary, get_utf8, put_utf8, utf8_wire_size},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/////////////////////////////////////////////////////////////////////////////

/// The Will message carried in a CONNECT packet.
///
/// On v3.1.1 connections `properties` is always empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    /// Topic on which the Will message is published.
    pub topic: String,
    /// Will message payload.
    pub payload: Bytes,
    /// QoS level for the Will publication.
    pub qos: QoS,
    /// Whether the Will message should be retained by the broker.
    pub retain: bool,
    /// MQTT v5 Will Properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl Will {
    /// Creates a Will with no properties.
    pub fn new(
        topic: impl Into<String>,
        payload: impl Into<Bytes>,
        qos: QoS,
        retain: bool,
    ) -> Self {
        Will {
            topic: topic.into(),
            payload: payload.into(),
            qos,
            retain,
            properties: Properties::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A CONNECT packet, unified across MQTT v3.1.1 and v5.
///
/// The `properties` field and `will.properties` are always empty on v3.1.1
/// connections and are not encoded on the wire for that version.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Connect {
    /// Whether to start a clean session / discard any existing session state.
    pub clean_start: bool,
    /// Keep-alive interval in seconds (0 = disabled).
    pub keep_alive: u16,
    /// Client identifier.
    pub client_id: String,
    /// Optional username for authentication.
    pub username: Option<String>,
    /// Optional password for authentication.
    pub password: Option<Bytes>,
    /// Optional Will message to publish if the connection is lost unexpectedly.
    pub will: Option<Will>,
    /// MQTT v5 connect properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl Connect {
    /// Creates a minimal `Connect` (no will, no credentials, keep-alive 0).
    pub fn new(clean_start: bool, client_id: impl Into<String>) -> Self {
        Connect {
            clean_start,
            keep_alive: 0,
            client_id: client_id.into(),
            username: None,
            password: None,
            will: None,
            properties: Properties::new(),
        }
    }

    /// Returns the number of variable-header + payload bytes for `version`.
    pub fn encoded_size(&self, version: MqttVersion) -> usize {
        // Variable header: 2+4 (proto name) + 1 (level) + 1 (flags) + 2 (keep-alive)
        let mut size = 10;
        if version == MqttVersion::V5 {
            size += self.properties.encoded_size();
        }
        // Payload
        size += utf8_wire_size(&self.client_id);
        if let Some(will) = &self.will {
            if version == MqttVersion::V5 {
                size += will.properties.encoded_size();
            }
            size += utf8_wire_size(&will.topic);
            size += binary_wire_size(&will.payload);
        }
        if let Some(u) = &self.username {
            size += utf8_wire_size(u);
        }
        if let Some(p) = &self.password {
            size += binary_wire_size(p);
        }
        size
    }

    /// Encodes the CONNECT variable header and payload into `dst`.
    pub fn encode<B: BufMut>(&self, dst: &mut B, version: MqttVersion) -> Result<()> {
        // Protocol name
        dst.put_u16(4);
        dst.put_slice(b"MQTT");
        // Protocol level
        dst.put_u8(match version {
            MqttVersion::V5 => 5,
            _ => 4,
        });
        // Connect flags
        let mut flags: u8 = 0;
        if self.clean_start {
            flags |= 0x02;
        }
        if let Some(will) = &self.will {
            flags |= 0x04;
            flags |= (will.qos as u8) << 3;
            if will.retain {
                flags |= 0x20;
            }
        }
        if self.username.is_some() {
            flags |= 0x80;
        }
        if self.password.is_some() {
            flags |= 0x40;
        }
        dst.put_u8(flags);
        dst.put_u16(self.keep_alive);
        // v5 connect properties
        if version == MqttVersion::V5 {
            self.properties.encode(dst)?;
        }
        // Payload: client ID
        put_utf8(&self.client_id, dst);
        // Will
        if let Some(will) = &self.will {
            if version == MqttVersion::V5 {
                will.properties.encode(dst)?;
            }
            put_utf8(&will.topic, dst);
            dst.put_u16(will.payload.len() as u16);
            dst.put_slice(&will.payload);
        }
        // Username / password
        if let Some(u) = &self.username {
            put_utf8(u, dst);
        }
        if let Some(p) = &self.password {
            dst.put_u16(p.len() as u16);
            dst.put_slice(p);
        }
        Ok(())
    }

    /// Decodes a CONNECT from the raw `payload` bytes for the given `version`.
    pub fn decode(mut payload: Bytes, version: MqttVersion) -> Result<Self> {
        // Protocol name
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }
        let name_len = payload.get_u16() as usize;
        if name_len != 4 {
            return Err(Error::MalformedPacket);
        }
        if payload.remaining() < 4 {
            return Err(Error::PacketTooShort);
        }
        let name = payload.copy_to_bytes(4);
        if &name[..] != b"MQTT" {
            return Err(Error::MalformedPacket);
        }
        // Protocol level
        if payload.remaining() < 1 {
            return Err(Error::PacketTooShort);
        }
        let level = payload.get_u8();
        let expected = match version {
            MqttVersion::V5 => 5,
            _ => 4,
        };
        if level != expected {
            return Err(Error::MalformedPacket);
        }
        // Connect flags
        if payload.remaining() < 3 {
            return Err(Error::PacketTooShort);
        }
        let flags = payload.get_u8();
        if flags & 0x01 != 0 {
            return Err(Error::MalformedPacket); // reserved bit must be 0
        }
        let clean_start = flags & 0x02 != 0;
        let will_flag = flags & 0x04 != 0;
        let will_qos_bits = (flags >> 3) & 0x03;
        let will_retain = flags & 0x20 != 0;
        let password_flag = flags & 0x40 != 0;
        let username_flag = flags & 0x80 != 0;
        // If will is absent, will-QoS and will-retain bits must be zero.
        if !will_flag && (flags & 0x38 != 0) {
            return Err(Error::MalformedPacket);
        }
        let will_qos = QoS::try_from(will_qos_bits)?;
        let keep_alive = payload.get_u16();
        // v5 connect properties
        let properties = match version {
            MqttVersion::V5 => Properties::decode(&mut payload)?,
            _ => Properties::new(),
        };
        // Payload: client ID
        let client_id = get_utf8(&mut payload)?;
        // Will
        let will = if will_flag {
            let will_props = match version {
                MqttVersion::V5 => Properties::decode(&mut payload)?,
                _ => Properties::new(),
            };
            let topic = get_utf8(&mut payload)?;
            let will_payload = get_binary(&mut payload)?;
            Some(Will {
                topic,
                payload: will_payload,
                qos: will_qos,
                retain: will_retain,
                properties: will_props,
            })
        }
        else {
            None
        };
        // Username / password
        let username = if username_flag {
            Some(get_utf8(&mut payload)?)
        }
        else {
            None
        };
        let password = if password_flag {
            Some(get_binary(&mut payload)?)
        }
        else {
            None
        };
        Ok(Connect {
            clean_start,
            keep_alive,
            client_id,
            username,
            password,
            will,
            properties,
        })
    }
}

// --------------------------------------------------------------------------

// MQTT v3.1.1 §3.2.2.3: six defined return codes (0–5). On decode we map
// them to the nearest v5 ReasonCode; on encode we reverse the mapping.
// Unknown return codes are treated as malformed.

fn v311_code_to_reason(code: u8) -> Result<ReasonCode> {
    match code {
        0 => Ok(ReasonCode::Success),
        1 => Ok(ReasonCode::UnsupportedProtocolVersion),
        2 => Ok(ReasonCode::ClientIdentifierNotValid),
        3 => Ok(ReasonCode::ServerUnavailable),
        4 => Ok(ReasonCode::BadUserNameOrPassword),
        5 => Ok(ReasonCode::NotAuthorized),
        _ => Err(Error::MalformedPacket),
    }
}

fn reason_to_v311_code(rc: ReasonCode) -> Result<u8> {
    match rc {
        ReasonCode::Success => Ok(0),
        ReasonCode::UnsupportedProtocolVersion => Ok(1),
        ReasonCode::ClientIdentifierNotValid => Ok(2),
        ReasonCode::ServerUnavailable => Ok(3),
        ReasonCode::BadUserNameOrPassword => Ok(4),
        ReasonCode::NotAuthorized => Ok(5),
        _ => Err(Error::MalformedPacket),
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A CONNACK packet, unified across MQTT v3.1.1 and v5.
///
/// On v3.1.1 connections the `properties` field is always empty; on v5 it
/// carries the server's capabilities and assigned values. The v3.1.1
/// connect return code is translated to the nearest [`ReasonCode`] on
/// decode, and the reverse mapping is applied on encode.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ConnAck {
    /// Whether the broker is resuming an existing session for this client.
    pub session_present: bool,
    /// Outcome of the connection attempt.
    pub reason_code: ReasonCode,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl ConnAck {
    /// Creates a `ConnAck` with no properties (suitable for either version).
    pub fn new(session_present: bool, reason_code: ReasonCode) -> Self {
        ConnAck {
            session_present,
            reason_code,
            properties: Properties::new(),
        }
    }

    /// Returns the number of variable-header bytes this packet encodes to
    /// for the given `version` (does **not** include the fixed header or
    /// the remaining-length VBI).
    pub fn encoded_size(&self, version: MqttVersion) -> usize {
        // 1 (ack flags) + 1 (return/reason code)
        let base = 2;
        match version {
            MqttVersion::V5 => base + self.properties.encoded_size(),
            _ => base,
        }
    }

    /// Encodes the CONNACK variable header and payload into `dst`.
    pub fn encode<B: BufMut>(&self, dst: &mut B, version: MqttVersion) -> Result<()> {
        // Byte 1: connect acknowledge flags (bit 0 = session_present)
        dst.put_u8(if self.session_present { 0x01 } else { 0x00 });

        // Byte 2: return/reason code
        match version {
            MqttVersion::V5 => {
                dst.put_u8(self.reason_code as u8);
                self.properties.encode(dst)?;
            }
            _ => {
                dst.put_u8(reason_to_v311_code(self.reason_code)?);
            }
        }
        Ok(())
    }

    /// Decodes a CONNACK from the raw `payload` bytes (i.e. the bytes after
    /// the remaining-length field) for the given `version`.
    pub fn decode(mut payload: Bytes, version: MqttVersion) -> Result<Self> {
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }

        let flags = payload.get_u8();
        // Bits 7–1 are reserved and must be zero.
        if flags & 0xFE != 0 {
            return Err(Error::MalformedPacket);
        }
        let session_present = flags & 0x01 != 0;

        let code_byte = payload.get_u8();
        let (reason_code, properties) = match version {
            MqttVersion::V5 => {
                let rc = ReasonCode::from(code_byte);
                let props = Properties::decode(&mut payload)?;
                (rc, props)
            }
            _ => {
                let rc = v311_code_to_reason(code_byte)?;
                (rc, Properties::new())
            }
        };

        // MQTT §3.2.2.1: session_present MUST be 0 when the connection is refused.
        if session_present && reason_code != ReasonCode::Success {
            return Err(Error::MalformedPacket);
        }

        Ok(ConnAck {
            session_present,
            reason_code,
            properties,
        })
    }
}

/// Encodes a complete CONNACK packet (fixed header + remaining-length + variable header).
pub fn encode_connack(
    ack: &ConnAck,
    dst: &mut BytesMut,
    version: MqttVersion,
) -> Result<()> {
    let remaining = ack.encoded_size(version);
    dst.put_u8(0x20); // fixed header: CONNACK, flags=0
    varlen::encode(remaining as u32, dst)?;
    ack.encode(dst, version)
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

    // ---- v311 encode ----

    #[test]
    fn test_encode_v311_success_no_session() {
        let ack = ConnAck::new(false, ReasonCode::Success);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[..], &[0x00, 0x00]);
    }

    #[test]
    fn test_encode_v311_success_with_session() {
        let ack = ConnAck::new(true, ReasonCode::Success);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[..], &[0x01, 0x00]);
    }

    #[test]
    fn test_encode_v311_refused_bad_credentials() {
        let ack = ConnAck::new(false, ReasonCode::BadUserNameOrPassword);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v311()).unwrap();
        assert_eq!(&dst[..], &[0x00, 0x04]);
    }

    #[test]
    fn test_encode_v311_unmappable_reason_code_errors() {
        let ack = ConnAck::new(false, ReasonCode::Banned); // no v3.1.1 equivalent
        let mut dst = BytesMut::new();
        assert_eq!(ack.encode(&mut dst, v311()), Err(Error::MalformedPacket));
    }

    // ---- v5 encode ----

    #[test]
    fn test_encode_v5_success_no_session() {
        let ack = ConnAck::new(false, ReasonCode::Success);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v5()).unwrap();
        // flags=0x00, reason=0x00, properties length=0x00
        assert_eq!(&dst[..], &[0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_encode_v5_not_authorized() {
        let ack = ConnAck::new(false, ReasonCode::NotAuthorized);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, v5()).unwrap();
        assert_eq!(&dst[..], &[0x00, 0x87, 0x00]);
    }

    // ---- v311 decode ----

    #[test]
    fn test_decode_v311_success_no_session() {
        let payload = Bytes::from_static(&[0x00, 0x00]);
        let ack = ConnAck::decode(payload, v311()).unwrap();
        assert!(!ack.session_present);
        assert_eq!(ack.reason_code, ReasonCode::Success);
        assert!(ack.properties.is_empty());
    }

    #[test]
    fn test_decode_v311_success_with_session() {
        let payload = Bytes::from_static(&[0x01, 0x00]);
        let ack = ConnAck::decode(payload, v311()).unwrap();
        assert!(ack.session_present);
        assert_eq!(ack.reason_code, ReasonCode::Success);
    }

    #[test]
    fn test_decode_v311_all_return_codes() {
        let cases = [
            (0, ReasonCode::Success),
            (1, ReasonCode::UnsupportedProtocolVersion),
            (2, ReasonCode::ClientIdentifierNotValid),
            (3, ReasonCode::ServerUnavailable),
            (4, ReasonCode::BadUserNameOrPassword),
            (5, ReasonCode::NotAuthorized),
        ];
        for (wire, expected) in cases {
            let payload = Bytes::copy_from_slice(&[0x00, wire]);
            let ack = ConnAck::decode(payload, v311()).unwrap();
            assert_eq!(ack.reason_code, expected, "wire code {wire:#x}");
        }
    }

    #[test]
    fn test_decode_v311_invalid_return_code_errors() {
        let payload = Bytes::from_static(&[0x00, 0x06]); // 6 is undefined
        assert_eq!(
            ConnAck::decode(payload, v311()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_decode_v311_reserved_flags_set_errors() {
        let payload = Bytes::from_static(&[0x02, 0x00]); // bit 1 set in flags
        assert_eq!(
            ConnAck::decode(payload, v311()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_decode_v311_too_short_errors() {
        let payload = Bytes::from_static(&[0x00]);
        assert_eq!(ConnAck::decode(payload, v311()), Err(Error::PacketTooShort));
    }

    #[test]
    fn test_decode_session_present_with_nonzero_code_errors() {
        // session_present=1 combined with a refused reason code is malformed.
        let payload = Bytes::from_static(&[0x01, 0x05]); // session_present=1, NotAuthorized
        assert_eq!(
            ConnAck::decode(payload, v311()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_decode_v5_session_present_with_refused_errors() {
        // Same rule applies on v5.
        let payload = Bytes::from_static(&[0x01, 0x87, 0x00]); // session_present=1, NotAuthorized
        assert_eq!(ConnAck::decode(payload, v5()), Err(Error::MalformedPacket));
    }

    // ---- v5 decode ----

    #[test]
    fn test_decode_v5_success_no_session() {
        let payload = Bytes::from_static(&[0x00, 0x00, 0x00]); // flags, reason, props_len=0
        let ack = ConnAck::decode(payload, v5()).unwrap();
        assert!(!ack.session_present);
        assert_eq!(ack.reason_code, ReasonCode::Success);
        assert!(ack.properties.is_empty());
    }

    #[test]
    fn test_decode_v5_reason_code_preserved() {
        // NotAuthorized = 0x87
        let payload = Bytes::from_static(&[0x00, 0x87, 0x00]);
        let ack = ConnAck::decode(payload, v5()).unwrap();
        assert_eq!(ack.reason_code, ReasonCode::NotAuthorized);
    }

    #[test]
    fn test_decode_v5_with_properties() {
        // 5 bytes of fake property data
        let payload = Bytes::from_static(&[0x00, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x0A]);
        let ack = ConnAck::decode(payload, v5()).unwrap();
        assert!(!ack.properties.is_empty());
    }

    // ---- round-trip ----

    fn round_trip(ack: ConnAck, version: MqttVersion) {
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, version).unwrap();
        let decoded = ConnAck::decode(dst.freeze(), version).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn test_round_trip_v311_success() {
        round_trip(ConnAck::new(false, ReasonCode::Success), v311());
        round_trip(ConnAck::new(true, ReasonCode::Success), v311());
    }

    #[test]
    fn test_round_trip_v311_refused() {
        use ReasonCode::*;
        for rc in [
            UnsupportedProtocolVersion,
            ClientIdentifierNotValid,
            ServerUnavailable,
            BadUserNameOrPassword,
            NotAuthorized,
        ] {
            round_trip(ConnAck::new(false, rc), v311());
        }
    }

    #[test]
    fn test_round_trip_v5_success() {
        round_trip(ConnAck::new(false, ReasonCode::Success), v5());
        round_trip(ConnAck::new(true, ReasonCode::Success), v5());
    }

    #[test]
    fn test_round_trip_v5_various_codes() {
        for rc in [
            ReasonCode::NotAuthorized,
            ReasonCode::ServerUnavailable,
            ReasonCode::Banned,
            ReasonCode::UnsupportedProtocolVersion,
        ] {
            round_trip(ConnAck::new(false, rc), v5());
        }
    }

    // ---- encoded_size ----

    #[test]
    fn test_encoded_size_v311() {
        let ack = ConnAck::new(false, ReasonCode::Success);
        assert_eq!(ack.encoded_size(v311()), 2);
    }

    #[test]
    fn test_encoded_size_v5_empty_props() {
        let ack = ConnAck::new(false, ReasonCode::Success);
        // 2 bytes variable header + 1 byte (VBI 0) for empty properties
        assert_eq!(ack.encoded_size(v5()), 3);
    }

    // ---- full packet encode (with fixed header) ----

    #[test]
    fn test_encode_connack_packet_v311() {
        let ack = ConnAck::new(false, ReasonCode::Success);
        let mut dst = BytesMut::new();
        encode_connack(&ack, &mut dst, v311()).unwrap();
        // 0x20=CONNACK, 0x02=remaining_length, 0x00=flags, 0x00=return_code
        assert_eq!(&dst[..], &[0x20, 0x02, 0x00, 0x00]);
    }

    #[test]
    fn test_encode_connack_packet_v5() {
        let ack = ConnAck::new(false, ReasonCode::Success);
        let mut dst = BytesMut::new();
        encode_connack(&ack, &mut dst, v5()).unwrap();
        // 0x20, remaining=3, flags=0, reason=0, props_len=0
        assert_eq!(&dst[..], &[0x20, 0x03, 0x00, 0x00, 0x00]);
    }

    // ======================================================================
    // Connect tests
    // ======================================================================

    fn round_trip_connect(conn: Connect, version: MqttVersion) {
        let mut dst = BytesMut::new();
        conn.encode(&mut dst, version).unwrap();
        let decoded = Connect::decode(dst.freeze(), version).unwrap();
        assert_eq!(decoded, conn);
    }

    // ---- encode / decode basics ----

    #[test]
    fn test_connect_encode_minimal_v311() {
        let conn = Connect::new(true, "test-client");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst, v311()).unwrap();
        let bytes = &dst[..];
        // Protocol name: 0x00 0x04 "MQTT"
        assert_eq!(&bytes[0..6], &[0x00, 0x04, b'M', b'Q', b'T', b'T']);
        // Protocol level
        assert_eq!(bytes[6], 0x04);
        // Connect flags: clean_start=1 → 0x02
        assert_eq!(bytes[7], 0x02);
        // Keep alive: 0
        assert_eq!(&bytes[8..10], &[0x00, 0x00]);
        // Client ID: 0x00 0x0B "test-client"
        assert_eq!(&bytes[10..12], &[0x00, 0x0B]);
        assert_eq!(&bytes[12..], b"test-client");
    }

    #[test]
    fn test_connect_encode_minimal_v5_has_properties_byte() {
        let conn = Connect::new(false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst, v5()).unwrap();
        // Protocol level
        assert_eq!(dst[6], 0x05);
        // Properties length = 0x00 immediately after keep-alive (offset 10)
        assert_eq!(dst[10], 0x00);
    }

    #[test]
    fn test_connect_round_trip_minimal_v311() {
        round_trip_connect(Connect::new(true, "test"), v311());
    }

    #[test]
    fn test_connect_round_trip_minimal_v5() {
        round_trip_connect(Connect::new(false, "test"), v5());
    }

    #[test]
    fn test_connect_round_trip_empty_client_id() {
        round_trip_connect(Connect::new(true, ""), v311());
    }

    // ---- keep-alive ----

    #[test]
    fn test_connect_round_trip_keep_alive() {
        let mut conn = Connect::new(false, "c");
        conn.keep_alive = 60;
        round_trip_connect(conn.clone(), v311());
        round_trip_connect(conn, v5());
    }

    // ---- credentials ----

    #[test]
    fn test_connect_round_trip_with_username() {
        let mut conn = Connect::new(true, "c");
        conn.username = Some("alice".into());
        round_trip_connect(conn.clone(), v311());
        round_trip_connect(conn, v5());
    }

    #[test]
    fn test_connect_round_trip_with_username_and_password() {
        let mut conn = Connect::new(true, "c");
        conn.username = Some("alice".into());
        conn.password = Some(Bytes::from_static(b"secret"));
        round_trip_connect(conn.clone(), v311());
        round_trip_connect(conn, v5());
    }

    // ---- will ----

    #[test]
    fn test_connect_round_trip_with_will_qos0_v311() {
        let mut conn = Connect::new(true, "c");
        conn.will = Some(Will::new(
            "lwt/topic",
            Bytes::from_static(b"gone"),
            QoS::AtMostOnce,
            false,
        ));
        round_trip_connect(conn, v311());
    }

    #[test]
    fn test_connect_round_trip_with_will_qos1_retain_v311() {
        let mut conn = Connect::new(false, "c");
        conn.will = Some(Will::new(
            "lwt/t",
            Bytes::from_static(b"payload"),
            QoS::AtLeastOnce,
            true,
        ));
        round_trip_connect(conn, v311());
    }

    #[test]
    fn test_connect_round_trip_with_will_v5() {
        let mut conn = Connect::new(true, "c");
        conn.will = Some(Will::new(
            "lwt/t",
            Bytes::from_static(b"msg"),
            QoS::ExactlyOnce,
            false,
        ));
        round_trip_connect(conn, v5());
    }

    // ---- full kitchen-sink ----

    #[test]
    fn test_connect_round_trip_full_v311() {
        let mut conn = Connect::new(true, "client-1");
        conn.keep_alive = 30;
        conn.username = Some("bob".into());
        conn.password = Some(Bytes::from_static(b"pass"));
        conn.will = Some(Will::new(
            "lw/t",
            Bytes::from_static(b"bye"),
            QoS::AtLeastOnce,
            true,
        ));
        round_trip_connect(conn, v311());
    }

    #[test]
    fn test_connect_round_trip_full_v5() {
        let mut conn = Connect::new(true, "client-1");
        conn.keep_alive = 30;
        conn.username = Some("bob".into());
        conn.password = Some(Bytes::from_static(b"pass"));
        conn.will = Some(Will::new(
            "lw/t",
            Bytes::from_static(b"bye"),
            QoS::AtLeastOnce,
            true,
        ));
        round_trip_connect(conn, v5());
    }

    // ---- encoded_size ----

    #[test]
    fn test_connect_encoded_size_minimal_v311() {
        let conn = Connect::new(false, "abc");
        // 10 (header) + 2+3 (client_id)
        assert_eq!(conn.encoded_size(v311()), 15);
    }

    #[test]
    fn test_connect_encoded_size_minimal_v5() {
        let conn = Connect::new(false, "abc");
        // 10 + 1 (empty props VBI) + 2+3 (client_id)
        assert_eq!(conn.encoded_size(v5()), 16);
    }

    #[test]
    fn test_encoded_size_matches_encoded_len() {
        let mut conn = Connect::new(true, "myclient");
        conn.keep_alive = 10;
        conn.username = Some("u".into());
        conn.password = Some(Bytes::from_static(b"pw"));
        conn.will = Some(Will::new(
            "t",
            Bytes::from_static(b"p"),
            QoS::AtMostOnce,
            false,
        ));

        for version in [v311(), v5()] {
            let mut dst = BytesMut::new();
            conn.encode(&mut dst, version).unwrap();
            assert_eq!(
                dst.len(),
                conn.encoded_size(version),
                "encoded_size mismatch for {version:?}"
            );
        }
    }

    // ---- error cases ----

    #[test]
    fn test_connect_decode_bad_protocol_name_errors() {
        // Replace "MQTT" with "XQTT"
        let conn = Connect::new(false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst, v311()).unwrap();
        dst[2] = b'X'; // corrupt first byte of protocol name
        assert_eq!(
            Connect::decode(dst.freeze(), v311()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_connect_decode_wrong_protocol_level_errors() {
        let conn = Connect::new(false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst, v311()).unwrap();
        // Decode as v5 — level 4 doesn't match expected 5
        assert_eq!(
            Connect::decode(dst.freeze(), v5()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_connect_decode_reserved_flag_bit_set_errors() {
        let conn = Connect::new(false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst, v311()).unwrap();
        dst[7] |= 0x01; // set reserved bit 0 of connect flags
        assert_eq!(
            Connect::decode(dst.freeze(), v311()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_connect_decode_will_bits_set_without_will_flag_errors() {
        let conn = Connect::new(false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst, v311()).unwrap();
        dst[7] |= 0x08; // set will-QoS bit without will flag
        assert_eq!(
            Connect::decode(dst.freeze(), v311()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_connect_decode_too_short_errors() {
        assert_eq!(
            Connect::decode(Bytes::from_static(&[0x00, 0x04]), v311()),
            Err(Error::PacketTooShort)
        );
    }
}
