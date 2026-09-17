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
    Error, MqttVersion, PacketType, Properties, PropertyContext, QoS, ReasonCode, Result,
    String,
    wire::{binary_wire_size, get_binary, get_utf8, put_binary, put_utf8, utf8_wire_size},
};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// The Will message carried in a CONNECT packet.
///
/// On v3.1.1 connections `properties` is always empty.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
/// Unlike every other packet, CONNECT declares its own protocol version on
/// the wire (the protocol-level byte), so it carries a [`version`](Self::version)
/// field rather than taking one as a parameter. Encoding uses that field;
/// decoding sets it from the wire.
///
/// The `properties` field and `will.properties` are always empty on v3.1.1
/// connections and are not encoded on the wire for that version.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Connect {
    /// Protocol version this CONNECT declares.
    pub version: MqttVersion,
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
    /// Creates a minimal `Connect` for protocol version `ver` (no will, no
    /// credentials, keep-alive 0).
    pub fn new(ver: MqttVersion, clean_start: bool, client_id: impl Into<String>) -> Self {
        Self {
            version: ver,
            clean_start,
            client_id: client_id.into(),
            ..Self::default()
        }
    }

    /// Returns the number of variable-header + payload bytes.
    pub fn encoded_size(&self) -> usize {
        let ver = self.version;
        // Variable header: protocol name + 1 (level) + 1 (flags) + 2 (keep-alive)
        let mut size = utf8_wire_size(ver.protocol_name()) + 4;
        size += self.properties.encoded_size(ver);
        // Payload
        size += utf8_wire_size(&self.client_id);
        if let Some(will) = &self.will {
            size += will.properties.encoded_size(ver);
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

    /// Encodes the CONNECT variable header and payload into `dst`, using
    /// the protocol version in [`version`](Self::version).
    pub fn encode<B: BufMut>(&self, dst: &mut B) -> Result<()> {
        let ver = self.version;
        // Protocol name and level identify the version on the wire.
        put_utf8(ver.protocol_name(), dst)?;
        dst.put_u8(ver as u8);
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
        self.properties.check_context(PacketType::Connect)?;
        self.properties.encode(dst, ver)?;
        // Payload: client ID
        put_utf8(&self.client_id, dst)?;
        // Will
        if let Some(will) = &self.will {
            will.properties.check_context(PropertyContext::Will)?;
            will.properties.encode(dst, ver)?;
            put_utf8(&will.topic, dst)?;
            put_binary(&will.payload, dst)?;
        }
        // Username / password
        if let Some(u) = &self.username {
            put_utf8(u, dst)?;
        }
        if let Some(p) = &self.password {
            put_binary(p, dst)?;
        }
        Ok(())
    }

    /// Decodes a CONNECT from the raw `payload` bytes.
    ///
    /// The protocol version is read from the packet itself and stored in
    /// [`version`](Self::version): `MQIsdp` level 3 is v3.1, `MQTT` level 4
    /// is v3.1.1, `MQTT` level 5 is v5. Any other name/level combination
    /// returns [`Error::UnsupportedProtocolVersion`], so a server can reply
    /// with the appropriate CONNACK reason code; a name that is not a
    /// protocol name at all is [`Error::MalformedPacket`].
    pub fn decode(mut payload: Bytes) -> Result<Self> {
        // Protocol name
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }
        let name_len = payload.get_u16() as usize;
        if payload.remaining() < name_len {
            return Err(Error::PacketTooShort);
        }
        let name = payload.copy_to_bytes(name_len);
        if !matches!(&name[..], b"MQTT" | b"MQIsdp") {
            return Err(Error::MalformedPacket);
        }
        // Protocol level; must agree with the name.
        if payload.remaining() < 1 {
            return Err(Error::PacketTooShort);
        }
        let ver = MqttVersion::try_from(payload.get_u8())?;
        if ver.protocol_name().as_bytes() != &name[..] {
            return Err(Error::UnsupportedProtocolVersion);
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
        // v3.1 and v3.1.1 require a username whenever a password is present
        // (MQTT-3.1.2-22); v5 lifted that restriction.
        if !ver.is_v5() && password_flag && !username_flag {
            return Err(Error::ProtocolError);
        }
        let will_qos = QoS::try_from(will_qos_bits)?;
        let keep_alive = payload.get_u16();
        // v5 connect properties
        let properties = Properties::decode_in(&mut payload, ver, PacketType::Connect)?;
        // Payload: client ID
        let client_id = get_utf8(&mut payload)?;
        // Will
        let will = if will_flag {
            let will_props =
                Properties::decode_in(&mut payload, ver, PropertyContext::Will)?;
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
        Ok(Self {
            version: ver,
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
    use ReasonCode::*;
    match code {
        0 => Ok(Success),
        1 => Ok(UnsupportedProtocolVersion),
        2 => Ok(ClientIdentifierNotValid),
        3 => Ok(ServerUnavailable),
        4 => Ok(BadUserNameOrPassword),
        5 => Ok(NotAuthorized),
        _ => Err(Error::MalformedPacket),
    }
}

fn reason_to_v311_code(rc: ReasonCode) -> Result<u8> {
    use ReasonCode::*;
    match rc {
        Success => Ok(0),
        UnsupportedProtocolVersion => Ok(1),
        ClientIdentifierNotValid => Ok(2),
        ServerUnavailable => Ok(3),
        BadUserNameOrPassword => Ok(4),
        NotAuthorized => Ok(5),
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
        Self {
            session_present,
            reason_code,
            ..Self::default()
        }
    }

    /// Returns the number of variable-header bytes this packet encodes to
    /// for the given version, `ver` (does **not** include the fixed header or
    /// the remaining-length VBI).
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        // 1 (ack flags) + 1 (return/reason code)
        2 + self.properties.encoded_size(ver)
    }

    /// Encodes the CONNACK variable header and payload into `dst`.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        // Byte 1: connect acknowledge flags (bit 0 = session_present).
        // v3.1 has no such flag; the byte is reserved and sent as zero.
        let flags = if self.session_present && ver != MqttVersion::V3_1 {
            0x01
        }
        else {
            0x00
        };
        dst.put_u8(flags);

        // Byte 2: return/reason code
        match ver {
            MqttVersion::V5 => dst.put_u8(self.reason_code as u8),
            _ => dst.put_u8(reason_to_v311_code(self.reason_code)?),
        }
        self.properties.check_context(PacketType::ConnAck)?;
        self.properties.encode(dst, ver)?;
        Ok(())
    }

    /// Decodes a CONNACK from the raw `payload` bytes (i.e. the bytes after
    /// the remaining-length field) for the given version, `ver`.
    pub fn decode(mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        if payload.remaining() < 2 {
            return Err(Error::PacketTooShort);
        }

        let flags = payload.get_u8();
        // Bits 7–1 are reserved and must be zero. In v3.1 the whole byte is
        // reserved (no Session Present flag) and is not validated.
        let session_present = match ver {
            MqttVersion::V3_1 => false,
            _ => {
                if flags & 0xFE != 0 {
                    return Err(Error::MalformedPacket);
                }
                flags & 0x01 != 0
            }
        };

        let code_byte = payload.get_u8();
        let reason_code = match ver {
            MqttVersion::V5 => ReasonCode::try_from(code_byte)?,
            _ => v311_code_to_reason(code_byte)?,
        };
        let properties = Properties::decode_in(&mut payload, ver, PacketType::ConnAck)?;

        // A server must clear Session Present on a refused connection
        // (MQTT-3.2.2-4). If one doesn't, the flag is meaningless anyway;
        // rejecting the packet here would only hide the reason code from
        // the receiver, which is the one thing it needs. Be lenient: clear
        // the flag and keep the code.
        let session_present = session_present && reason_code == ReasonCode::Success;

        Ok(Self {
            session_present,
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
        Property, PropertyId, PropertyValue,
        types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5},
    };
    use bytes::BytesMut;

    fn v311() -> MqttVersion {
        MQTT_VERSION_3_1_1
    }

    fn v5() -> MqttVersion {
        MQTT_VERSION_5
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
    fn test_decode_session_present_with_refused_code_is_cleared() {
        // A non-compliant broker sets session_present on a refused connection.
        // The reason code is what matters to the receiver; keep it and clear the
        // meaningless flag rather than failing the whole packet.
        let payload = Bytes::from_static(&[0x01, 0x05]); // session_present=1, NotAuthorized
        let ack = ConnAck::decode(payload, v311()).unwrap();
        assert!(!ack.session_present);
        assert_eq!(ack.reason_code, ReasonCode::NotAuthorized);
    }

    #[test]
    fn test_decode_v5_session_present_with_refused_code_is_cleared() {
        let payload = Bytes::from_static(&[0x01, 0x87, 0x00]); // session_present=1, NotAuthorized
        let ack = ConnAck::decode(payload, v5()).unwrap();
        assert!(!ack.session_present);
        assert_eq!(ack.reason_code, ReasonCode::NotAuthorized);
        // ... and a compliant success + session_present is untouched.
        let payload = Bytes::from_static(&[0x01, 0x00, 0x00]);
        assert!(ConnAck::decode(payload, v5()).unwrap().session_present);
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

    fn round_trip(ack: ConnAck, ver: MqttVersion) {
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, ver).unwrap();
        let decoded = ConnAck::decode(dst.freeze(), ver).unwrap();
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

    #[test]
    fn test_connack_v5_decode_unknown_reason_code_errors() {
        // flags=0, reason=0x05 (undefined), empty properties
        assert_eq!(
            ConnAck::decode(Bytes::from_static(&[0x00, 0x05, 0x00]), v5()),
            Err(Error::ProtocolError)
        );
    }

    // ======================================================================
    // Connect tests
    // ======================================================================

    fn with_ver(conn: Connect, ver: MqttVersion) -> Connect {
        Connect {
            version: ver,
            ..conn
        }
    }

    fn round_trip_connect(conn: Connect) {
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        let decoded = Connect::decode(dst.freeze()).unwrap();
        assert_eq!(decoded, conn);
    }

    // ---- encode / decode basics ----

    #[test]
    fn test_connect_encode_minimal_v311() {
        let conn = Connect::new(v311(), true, "test-client");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
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
        let conn = Connect::new(v5(), false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        // Protocol level
        assert_eq!(dst[6], 0x05);
        // Properties length = 0x00 immediately after keep-alive (offset 10)
        assert_eq!(dst[10], 0x00);
    }

    #[test]
    fn test_connect_round_trip_minimal_v311() {
        round_trip_connect(Connect::new(v311(), true, "test"));
    }

    #[test]
    fn test_connect_round_trip_minimal_v5() {
        round_trip_connect(Connect::new(v5(), false, "test"));
    }

    #[test]
    fn test_connect_round_trip_empty_client_id() {
        round_trip_connect(Connect::new(v311(), true, ""));
    }

    // ---- keep-alive ----

    #[test]
    fn test_connect_round_trip_keep_alive() {
        let mut conn = Connect::new(v5(), false, "c");
        conn.keep_alive = 60;
        round_trip_connect(with_ver(conn.clone(), v311()));
        round_trip_connect(with_ver(conn, v5()));
    }

    // ---- credentials ----

    #[test]
    fn test_connect_round_trip_with_username() {
        let mut conn = Connect::new(v5(), true, "c");
        conn.username = Some("alice".into());
        round_trip_connect(with_ver(conn.clone(), v311()));
        round_trip_connect(with_ver(conn, v5()));
    }

    #[test]
    fn test_connect_round_trip_with_username_and_password() {
        let mut conn = Connect::new(v5(), true, "c");
        conn.username = Some("alice".into());
        conn.password = Some(Bytes::from_static(b"secret"));
        round_trip_connect(with_ver(conn.clone(), v311()));
        round_trip_connect(with_ver(conn, v5()));
    }

    // ---- will ----

    #[test]
    fn test_connect_round_trip_with_will_qos0_v311() {
        let mut conn = Connect::new(v311(), true, "c");
        conn.will = Some(Will::new(
            "lwt/topic",
            Bytes::from_static(b"gone"),
            QoS::AtMostOnce,
            false,
        ));
        round_trip_connect(conn);
    }

    #[test]
    fn test_connect_round_trip_with_will_qos1_retain_v311() {
        let mut conn = Connect::new(v311(), false, "c");
        conn.will = Some(Will::new(
            "lwt/t",
            Bytes::from_static(b"payload"),
            QoS::AtLeastOnce,
            true,
        ));
        round_trip_connect(conn);
    }

    #[test]
    fn test_connect_round_trip_with_will_v5() {
        let mut conn = Connect::new(v5(), true, "c");
        conn.will = Some(Will::new(
            "lwt/t",
            Bytes::from_static(b"msg"),
            QoS::ExactlyOnce,
            false,
        ));
        round_trip_connect(conn);
    }

    // ---- full kitchen-sink ----

    #[test]
    fn test_connect_round_trip_full_v311() {
        let mut conn = Connect::new(v311(), true, "client-1");
        conn.keep_alive = 30;
        conn.username = Some("bob".into());
        conn.password = Some(Bytes::from_static(b"pass"));
        conn.will = Some(Will::new(
            "lw/t",
            Bytes::from_static(b"bye"),
            QoS::AtLeastOnce,
            true,
        ));
        round_trip_connect(conn);
    }

    #[test]
    fn test_connect_round_trip_full_v5() {
        let mut conn = Connect::new(v5(), true, "client-1");
        conn.keep_alive = 30;
        conn.username = Some("bob".into());
        conn.password = Some(Bytes::from_static(b"pass"));
        conn.will = Some(Will::new(
            "lw/t",
            Bytes::from_static(b"bye"),
            QoS::AtLeastOnce,
            true,
        ));
        round_trip_connect(conn);
    }

    // ---- encoded_size ----

    #[test]
    fn test_connect_encoded_size_minimal_v311() {
        let conn = Connect::new(v311(), false, "abc");
        // 10 (header) + 2+3 (client_id)
        assert_eq!(conn.encoded_size(), 15);
    }

    #[test]
    fn test_connect_encoded_size_minimal_v5() {
        let conn = Connect::new(v5(), false, "abc");
        // 10 + 1 (empty props VBI) + 2+3 (client_id)
        assert_eq!(conn.encoded_size(), 16);
    }

    #[test]
    fn test_encoded_size_matches_encoded_len() {
        let mut conn = Connect::new(v5(), true, "myclient");
        conn.keep_alive = 10;
        conn.username = Some("u".into());
        conn.password = Some(Bytes::from_static(b"pw"));
        conn.will = Some(Will::new(
            "t",
            Bytes::from_static(b"p"),
            QoS::AtMostOnce,
            false,
        ));

        for ver in [v311(), v5()] {
            let mut dst = BytesMut::new();
            with_ver(conn.clone(), ver).encode(&mut dst).unwrap();
            assert_eq!(
                dst.len(),
                with_ver(conn.clone(), ver).encoded_size(),
                "encoded_size mismatch for {ver:?}"
            );
        }
    }

    // ---- error cases ----

    #[test]
    fn test_connect_decode_bad_protocol_name_errors() {
        // Replace "MQTT" with "XQTT"
        let conn = Connect::new(v311(), false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        dst[2] = b'X'; // corrupt first byte of protocol name
        assert_eq!(Connect::decode(dst.freeze()), Err(Error::MalformedPacket));
    }

    #[test]
    fn test_connect_decode_discovers_version_from_level_byte() {
        for ver in [MqttVersion::V3_1, v311(), v5()] {
            let conn = Connect::new(ver, false, "c");
            let mut dst = BytesMut::new();
            conn.encode(&mut dst).unwrap();
            let level_at = 2 + ver.protocol_name().len();
            assert_eq!(dst[level_at], ver as u8);
            let decoded = Connect::decode(dst.freeze()).unwrap();
            assert_eq!(decoded.version, ver);
        }
    }

    #[test]
    fn test_connect_decode_unknown_protocol_level_errors() {
        let conn = Connect::new(v311(), false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        dst[6] = 0x03; // MQTT v3.1 level with the v3.1.1 protocol name
        assert_eq!(
            Connect::decode(dst.freeze()),
            Err(Error::UnsupportedProtocolVersion)
        );
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        dst[6] = 0x06;
        assert_eq!(
            Connect::decode(dst.freeze()),
            Err(Error::UnsupportedProtocolVersion)
        );
    }

    #[test]
    fn test_connect_decode_mqisdp_is_v3_1() {
        // MQTT v3.1: protocol name "MQIsdp", level 3.
        let mut src = BytesMut::new();
        src.put_u16(6);
        src.put_slice(b"MQIsdp");
        src.put_u8(0x03);
        src.put_u8(0x02);
        src.put_u16(0);
        put_utf8("c", &mut src).unwrap();
        let conn = Connect::decode(src.freeze()).unwrap();
        assert_eq!(conn.version, MqttVersion::V3_1);
        assert!(conn.clean_start);
        assert_eq!(conn.client_id, "c");
    }

    #[test]
    fn test_connect_v3_1_wire_format_and_round_trip() {
        let mut conn = Connect::new(MqttVersion::V3_1, true, "client-1");
        conn.keep_alive = 30;
        conn.username = Some("u".into());
        conn.password = Some(Bytes::from_static(b"p"));
        conn.will = Some(Will::new("w", Bytes::from_static(b"bye"), QoS::QoS1, true));
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        // Protocol name "MQIsdp" then level 3.
        assert_eq!(&dst[..8], &[0x00, 0x06, b'M', b'Q', b'I', b's', b'd', b'p']);
        assert_eq!(dst[8], 0x03);
        assert_eq!(dst.len(), conn.encoded_size());
        assert_eq!(Connect::decode(dst.freeze()).unwrap(), conn);
    }

    #[test]
    fn test_connect_decode_name_level_mismatch_is_unsupported() {
        // "MQIsdp" with level 4, and "MQTT" with level 3: neither is a version.
        for (name, level) in [(&b"MQIsdp"[..], 4u8), (b"MQTT", 3), (b"MQIsdp", 5)] {
            let mut src = BytesMut::new();
            src.put_u16(name.len() as u16);
            src.put_slice(name);
            src.put_u8(level);
            src.put_u8(0x02);
            src.put_u16(0);
            put_utf8("c", &mut src).unwrap();
            assert_eq!(
                Connect::decode(src.freeze()),
                Err(Error::UnsupportedProtocolVersion),
                "{name:?} level {level}"
            );
        }
    }

    #[test]
    fn test_connack_v3_1_has_no_session_present() {
        // Encoding never sets the flag on v3.1 ...
        let ack = ConnAck::new(true, ReasonCode::Success);
        let mut dst = BytesMut::new();
        ack.encode(&mut dst, MqttVersion::V3_1).unwrap();
        assert_eq!(&dst[..], &[0x00, 0x00]);
        // ... and decoding ignores the reserved byte entirely.
        let ack =
            ConnAck::decode(Bytes::from_static(&[0xFF, 0x00]), MqttVersion::V3_1).unwrap();
        assert!(!ack.session_present);
        assert_eq!(ack.reason_code, ReasonCode::Success);
        // The same bytes are malformed on v3.1.1.
        assert_eq!(
            ConnAck::decode(Bytes::from_static(&[0xFF, 0x00]), v311()),
            Err(Error::MalformedPacket)
        );
    }

    #[test]
    fn test_connect_decode_reserved_flag_bit_set_errors() {
        let conn = Connect::new(v311(), false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        dst[7] |= 0x01; // set reserved bit 0 of connect flags
        assert_eq!(Connect::decode(dst.freeze()), Err(Error::MalformedPacket));
    }

    #[test]
    fn test_connect_decode_password_without_username() {
        let mut conn = Connect::new(v311(), true, "c");
        conn.password = Some(Bytes::from_static(b"pw"));
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        // Forbidden in v3.1.1 (MQTT-3.1.2-22) ...
        assert_eq!(Connect::decode(dst.freeze()), Err(Error::ProtocolError));
        // ... but allowed in v5.
        let conn = with_ver(conn, v5());
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        assert_eq!(Connect::decode(dst.freeze()).unwrap(), conn);
    }

    #[test]
    fn test_connect_decode_will_bits_set_without_will_flag_errors() {
        let conn = Connect::new(v311(), false, "c");
        let mut dst = BytesMut::new();
        conn.encode(&mut dst).unwrap();
        dst[7] |= 0x08; // set will-QoS bit without will flag
        assert_eq!(Connect::decode(dst.freeze()), Err(Error::MalformedPacket));
    }

    #[test]
    fn test_connect_decode_too_short_errors() {
        assert_eq!(
            Connect::decode(Bytes::from_static(&[0x00, 0x04])),
            Err(Error::PacketTooShort)
        );
    }

    // ---- property context (G2) ----

    #[test]
    fn test_connect_rejects_connack_only_property() {
        let mut c = Connect::new(v5(), true, "c");
        c.properties.push(Property::new(
            PropertyId::MaximumQoS,
            PropertyValue::Byte(1),
        ));
        let mut dst = BytesMut::new();
        assert_eq!(c.encode(&mut dst), Err(Error::ProtocolError));
    }

    #[test]
    fn test_will_rejects_publish_only_property() {
        let mut c = Connect::new(v5(), true, "c");
        let mut will = Will::new("w", Bytes::new(), QoS::QoS0, false);
        // TopicAlias is a PUBLISH property, never a Will property.
        will.properties
            .push(Property::new(PropertyId::TopicAlias, PropertyValue::U16(1)));
        c.will = Some(will);
        let mut dst = BytesMut::new();
        assert_eq!(c.encode(&mut dst), Err(Error::ProtocolError));
        // ... while WillDelayInterval belongs there and round-trips.
        let mut c = Connect::new(v5(), true, "c");
        let mut will = Will::new("w", Bytes::new(), QoS::QoS0, false);
        will.properties.push(Property::new(
            PropertyId::WillDelayInterval,
            PropertyValue::U32(5),
        ));
        c.will = Some(will);
        round_trip_connect(c);
    }

    #[test]
    fn test_connect_decode_rejects_misplaced_property_on_wire() {
        // A valid v5 CONNECT, then splice MaximumQoS (0x24 0x01) into its
        // property block by re-encoding with the context check bypassed.
        let mut c = Connect::new(v5(), true, "c");
        c.properties = alloc::vec![Property::new(
            PropertyId::MaximumQoS,
            PropertyValue::Byte(1)
        )]
        .into();
        let mut dst = BytesMut::new();
        // Bypass Connect::encode's context check by writing the pieces by hand.
        dst.put_u16(4);
        dst.put_slice(b"MQTT");
        dst.put_u8(5);
        dst.put_u8(0x02);
        dst.put_u16(0);
        c.properties.encode(&mut dst, v5()).unwrap(); // Properties::encode has no context
        put_utf8("c", &mut dst).unwrap();
        assert_eq!(Connect::decode(dst.freeze()), Err(Error::ProtocolError));
    }

    // ---- field length limits ----

    #[test]
    fn test_connect_encode_client_id_too_long_errors() {
        let id: String = core::iter::repeat_n('c', 65_536).collect();
        let c = Connect::new(v5(), true, id);
        let mut dst = BytesMut::new();
        assert_eq!(c.encode(&mut dst), Err(Error::StringTooLong));
    }

    #[test]
    fn test_connect_encode_will_payload_too_long_errors() {
        let mut c = Connect::new(v5(), true, "c");
        c.will = Some(Will::new(
            "w",
            Bytes::from(alloc::vec![0u8; 65_536]),
            QoS::QoS0,
            false,
        ));
        let mut dst = BytesMut::new();
        assert_eq!(c.encode(&mut dst), Err(Error::BinaryTooLong));
    }

    #[test]
    fn test_connect_encode_password_too_long_errors() {
        let mut c = Connect::new(v5(), true, "c");
        c.password = Some(Bytes::from(alloc::vec![0u8; 65_536]));
        let mut dst = BytesMut::new();
        assert_eq!(c.encode(&mut dst), Err(Error::BinaryTooLong));
    }

    #[test]
    fn test_connect_encode_username_too_long_errors() {
        let mut c = Connect::new(v5(), true, "c");
        c.username = Some(core::iter::repeat_n('u', 65_536).collect());
        let mut dst = BytesMut::new();
        assert_eq!(c.encode(&mut dst), Err(Error::StringTooLong));
    }
}
