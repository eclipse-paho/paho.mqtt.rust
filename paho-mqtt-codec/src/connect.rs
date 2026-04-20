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
    types::MqttVersion,
    varlen,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// ---- v3.1.1 connect return codes ----------------------------------------
//
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

// ---- ConnAck -------------------------------------------------------------

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
        for rc in [
            ReasonCode::UnsupportedProtocolVersion,
            ReasonCode::ClientIdentifierNotValid,
            ReasonCode::ServerUnavailable,
            ReasonCode::BadUserNameOrPassword,
            ReasonCode::NotAuthorized,
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
}
