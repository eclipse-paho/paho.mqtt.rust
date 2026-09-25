// paho-mqtt-codec/src/auth.rs
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

//! MQTT v5 AUTH packet.
//!
//! AUTH is a v5-only packet used for extended authentication exchanges
//! (e.g. SASL). It flows in both directions during and after the
//! CONNECT handshake.
//!
//! Valid reason codes for AUTH are:
//! - `Success` (0x00) — authentication exchange complete
//! - `ContinueAuthentication` (0x18) — another step follows
//! - `ReAuthenticate` (0x19) — client requests re-authentication
//!
//! Shorthand encoding: when `reason_code` is `Success` (0x00) and
//! `properties` is empty, `remaining_length` is 0 and the payload is
//! omitted entirely.

use crate::{MqttVersion, PacketType, Properties, ReasonCode, Result, wire::expect_end};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// An AUTH packet (MQTT v5 only).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Auth {
    /// Authentication reason code.
    pub reason_code: ReasonCode,
    /// MQTT v5 properties
    /// (typically carries AuthenticationMethod and AuthenticationData).
    pub properties: Properties,
}

impl Auth {
    /// Creates an AUTH packet.
    ///
    /// v5 only.
    ///
    /// The properties are not incidental here as they are elsewhere: the
    /// `AuthenticationMethod` and `AuthenticationData` *are* the exchange,
    /// and §3.15.2.2.2 requires every AUTH to name the method. An AUTH with
    /// no properties is only meaningful as the bare `Success` that ends a
    /// successful exchange, which is `Auth::default()`.
    pub fn new(reason_code: ReasonCode, properties: Properties) -> Self {
        Self {
            reason_code,
            properties,
        }
    }
    /// Returns the remaining-length byte count.
    ///
    /// Uses the shorthand form (0) when `reason_code` is `Success` and
    /// `properties` is empty.
    pub fn encoded_size(&self) -> usize {
        match (self.reason_code, self.properties.is_empty()) {
            (ReasonCode::Success, true) => 0,
            (_, true) => 1,
            _ => 1 + self.properties.encoded_size(MqttVersion::V5),
        }
    }

    /// Encodes the variable header and payload into `dst`.
    ///
    /// Uses the shortest form permitted: nothing for `Success` with no
    /// properties, just the reason code when there are no properties,
    /// otherwise the full form.
    pub fn encode<B: BufMut>(&self, dst: &mut B) -> Result<()> {
        match (self.reason_code, self.properties.is_empty()) {
            (ReasonCode::Success, true) => {}
            (rc, true) => dst.put_u8(rc as u8),
            (rc, false) => {
                dst.put_u8(rc as u8);
                self.properties.check_context(PacketType::Auth)?;
                self.properties.encode(dst, MqttVersion::V5)?;
            }
        }
        Ok(())
    }

    /// Decodes from the raw `payload` bytes.
    pub fn decode(mut payload: Bytes) -> Result<Self> {
        if payload.is_empty() {
            return Ok(Auth::default());
        }
        let reason_code = ReasonCode::try_from(payload.get_u8())?;
        // Property length is omitted when the remaining length < 2.
        let properties = if payload.is_empty() {
            Properties::new()
        } else {
            Properties::decode_in(&mut payload, MqttVersion::V5, PacketType::Auth)?
        };
        expect_end(&payload)?;

        Ok(Self {
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
        Error,
        properties::{Property, PropertyId, PropertyValue},
    };
    use alloc::string::ToString;
    use bytes::BytesMut;

    fn round_trip(a: Auth) {
        let mut dst = BytesMut::new();
        a.encode(&mut dst).unwrap();
        assert_eq!(dst.len(), a.encoded_size());
        let decoded = Auth::decode(dst.freeze()).unwrap();
        assert_eq!(decoded, a);
    }

    #[test]
    fn test_auth_decode_reason_code_no_props() {
        // Remaining length 1: reason code only, no property length.
        let payload = Bytes::from_static(&[0x18]);
        let a = Auth::decode(payload).unwrap();
        assert_eq!(a.reason_code, ReasonCode::ContinueAuthentication);
        assert!(a.properties.is_empty());
    }

    #[test]
    fn test_auth_decode_unknown_reason_code_errors() {
        assert_eq!(
            Auth::decode(Bytes::from_static(&[0x1A])),
            Err(Error::ProtocolError)
        );
    }

    // ---- shorthand (Success + no props) ----

    #[test]
    fn test_auth_success_encodes_empty() {
        let a = Auth::default();
        let mut dst = BytesMut::new();
        a.encode(&mut dst).unwrap();
        assert!(dst.is_empty());
    }

    #[test]
    fn test_auth_success_encoded_size_zero() {
        assert_eq!(Auth::default().encoded_size(), 0);
    }

    #[test]
    fn test_auth_success_round_trip() {
        round_trip(Auth::default());
    }

    // ---- ContinueAuthentication ----

    #[test]
    fn test_auth_continue_encoded_size() {
        let a = Auth::new(ReasonCode::ContinueAuthentication, Properties::new());
        // Reason code only; the property length is omitted when empty.
        assert_eq!(a.encoded_size(), 1);
    }

    #[test]
    fn test_auth_continue_wire_bytes() {
        let a = Auth::new(ReasonCode::ContinueAuthentication, Properties::new());
        let mut dst = BytesMut::new();
        a.encode(&mut dst).unwrap();
        assert_eq!(&dst[..], &[0x18]);
    }

    #[test]
    fn test_auth_continue_round_trip() {
        round_trip(Auth::new(
            ReasonCode::ContinueAuthentication,
            Properties::new(),
        ));
    }

    // ---- ReAuthenticate ----

    #[test]
    fn test_auth_reauthenticate_round_trip() {
        round_trip(Auth::new(ReasonCode::ReAuthenticate, Properties::new()));
    }

    #[test]
    fn test_auth_reauthenticate_wire_bytes() {
        let a = Auth::new(ReasonCode::ReAuthenticate, Properties::new());
        let mut dst = BytesMut::new();
        a.encode(&mut dst).unwrap();
        assert_eq!(&dst[..], &[0x19]);
    }

    // ---- with properties ----

    #[test]
    fn test_auth_with_method_property_round_trip() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::AuthenticationMethod,
            PropertyValue::String("SCRAM-SHA-256".to_string()),
        ));
        round_trip(Auth::new(ReasonCode::ContinueAuthentication, props));
    }

    #[test]
    fn test_auth_with_method_and_data_round_trip() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::AuthenticationMethod,
            PropertyValue::String("GSSAPI".to_string()),
        ));
        props.push(Property::new(
            PropertyId::AuthenticationData,
            PropertyValue::Binary(bytes::Bytes::from_static(b"\x01\x02\x03\x04")),
        ));
        round_trip(Auth::new(ReasonCode::ContinueAuthentication, props));
    }

    // ---- encoded_size matches encode ----

    #[test]
    fn test_encoded_size_matches_encode_continue() {
        let a = Auth::new(ReasonCode::ContinueAuthentication, Properties::new());
        let mut dst = BytesMut::new();
        a.encode(&mut dst).unwrap();
        assert_eq!(dst.len(), a.encoded_size());
    }

    #[test]
    fn test_encoded_size_matches_encode_with_props() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::AuthenticationMethod,
            PropertyValue::String("plain".to_string()),
        ));
        let a = Auth {
            reason_code: ReasonCode::Success,
            properties: props,
        };
        let mut dst = BytesMut::new();
        a.encode(&mut dst).unwrap();
        assert_eq!(dst.len(), a.encoded_size());
    }
}
