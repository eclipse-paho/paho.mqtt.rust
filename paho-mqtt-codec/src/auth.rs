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

use crate::{
    Error, MqttVersion, PacketType, Properties, PropertyId, ReasonCode, Result, wire::expect_end,
};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// An AUTH packet (MQTT v5 only).
///
/// Unlike every other packet with a reason code, AUTH has no shorthand form:
/// §3.15.2 gives no clause letting the Reason Code or the Property Length be
/// omitted, the way §3.4.2.1 does for PUBACK and §3.14.2.1 for DISCONNECT. It
/// always encodes to at least the reason code, a property length, and an
/// `AuthenticationMethod`, which §3.15.2.2.2 requires — receiving an AUTH
/// without one is a Protocol Error \[MQTT-3.15.2-1\].
///
/// [`Auth::default`] therefore names a packet that cannot be sent: it has no
/// method, so [`encode`](Self::encode) rejects it. It exists because every
/// packet struct in the crate has a zero value, not because it is useful.
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
    /// `AuthenticationMethod` and `AuthenticationData` *are* the exchange, and
    /// §3.15.2.2.2 requires every AUTH to name the method, so `properties`
    /// must carry one for the packet to encode. `None` is accepted for
    /// symmetry with the other constructors but yields a packet
    /// [`encode`](Self::encode) will refuse.
    pub fn new<P>(reason_code: ReasonCode, properties: P) -> Self
    where
        P: Into<Option<Properties>>,
    {
        Self {
            reason_code,
            properties: properties.into().unwrap_or_default(),
        }
    }

    /// Returns `true` if the properties name an `AuthenticationMethod`, which
    /// every AUTH must (§3.15.2.2.2).
    fn has_method(&self) -> bool {
        self.properties
            .get(PropertyId::AuthenticationMethod)
            .is_some()
    }

    /// Returns the remaining-length byte count.
    ///
    /// AUTH has no shorthand form — see [`Auth`] — so this is always the
    /// reason code plus the property list.
    pub fn encoded_size(&self) -> usize {
        1 + self.properties.encoded_size(MqttVersion::V5)
    }

    /// Encodes the variable header into `dst`.
    ///
    /// Returns [`Error::ProtocolError`] if the properties do not name an
    /// `AuthenticationMethod` (§3.15.2.2.2), or if any property is not
    /// allowed in an AUTH.
    pub fn encode<B: BufMut>(&self, dst: &mut B) -> Result<()> {
        if !self.has_method() {
            return Err(Error::ProtocolError);
        }
        dst.put_u8(self.reason_code as u8);
        self.properties.check_context(PacketType::Auth)?;
        self.properties.encode(dst, MqttVersion::V5)
    }

    /// Decodes from the raw `payload` bytes.
    ///
    /// Returns [`Error::PacketTooShort`] if the reason code or the property
    /// length is missing — AUTH has no shorthand form that permits either to
    /// be omitted — and [`Error::ProtocolError`] if the properties do not
    /// name an `AuthenticationMethod` \[MQTT-3.15.2-1\].
    pub fn decode(mut payload: Bytes) -> Result<Self> {
        if payload.is_empty() {
            return Err(Error::PacketTooShort);
        }
        let reason_code = ReasonCode::try_from(payload.get_u8())?;
        let properties = Properties::decode_in(&mut payload, MqttVersion::V5, PacketType::Auth)?;
        expect_end(&payload)?;

        let auth = Self::new(reason_code, properties);
        if !auth.has_method() {
            return Err(Error::ProtocolError);
        }
        Ok(auth)
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

    /// Properties naming an authentication method, which every AUTH needs.
    fn method_props(name: &str) -> Properties {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::AuthenticationMethod,
            PropertyValue::String(name.to_string()),
        ));
        props
    }

    fn round_trip(a: Auth) {
        let mut dst = BytesMut::new();
        a.encode(&mut dst).unwrap();
        assert_eq!(dst.len(), a.encoded_size());
        let decoded = Auth::decode(dst.freeze()).unwrap();
        assert_eq!(decoded, a);
    }

    /// AUTH has no clause letting the Property Length be omitted, so a
    /// reason-code-only body is truncated rather than shorthand.
    #[test]
    fn test_auth_decode_reason_code_alone_is_too_short() {
        assert_eq!(
            Auth::decode(Bytes::from_static(&[0x18])),
            Err(Error::PacketTooShort)
        );
    }

    /// Nor may the reason code itself be omitted, as it may in a DISCONNECT
    /// (§3.14.2.1) or a PUBACK (§3.4.2.1).
    #[test]
    fn test_auth_decode_empty_body_is_too_short() {
        assert_eq!(Auth::decode(Bytes::new()), Err(Error::PacketTooShort));
    }

    /// A well-formed body with no Authentication Method is a Protocol Error
    /// [MQTT-3.15.2-1]: reason code 0x18, property length 0.
    #[test]
    fn test_auth_decode_without_method_is_a_protocol_error() {
        assert_eq!(
            Auth::decode(Bytes::from_static(&[0x18, 0x00])),
            Err(Error::ProtocolError)
        );
    }

    /// The property length must agree with what follows it: one claiming more
    /// bytes than the body holds, a truncated or non-minimal length, and a
    /// byte past the properties are each rejected.
    #[test]
    fn test_auth_decode_rejects_bad_property_length() {
        let cases: [(&[u8], Error); 4] = [
            // Property length claims 5 bytes with nothing after it.
            (&[0x18, 0x05], Error::PacketTooShort),
            // The length is a continuation byte with no successor.
            (&[0x18, 0x80], Error::PacketTooShort),
            // Non-minimal encoding of zero.
            (&[0x18, 0x80, 0x00], Error::MalformedVarLen),
            // The method is complete, then a byte that cannot be there.
            (
                &[0x18, 0x04, 0x15, 0x00, 0x01, b'x', 0xFF],
                Error::MalformedPacket,
            ),
        ];
        for (payload, err) in cases {
            assert_eq!(
                Auth::decode(Bytes::copy_from_slice(payload)),
                Err(err),
                "{payload:02X?}"
            );
        }
    }

    #[test]
    fn test_auth_decode_unknown_reason_code_errors() {
        assert_eq!(
            Auth::decode(Bytes::from_static(&[0x1A])),
            Err(Error::ProtocolError)
        );
    }

    // ---- no shorthand: every AUTH names a method (§3.15.2.2.2) ----

    /// `Auth::default()` is a value, not a sendable packet.
    #[test]
    fn test_auth_default_does_not_encode() {
        let mut dst = BytesMut::new();
        assert_eq!(Auth::default().encode(&mut dst), Err(Error::ProtocolError));
    }

    #[test]
    fn test_auth_without_method_does_not_encode() {
        for rc in [
            ReasonCode::Success,
            ReasonCode::ContinueAuthentication,
            ReasonCode::ReAuthenticate,
        ] {
            let mut dst = BytesMut::new();
            assert_eq!(
                Auth::new(rc, None).encode(&mut dst),
                Err(Error::ProtocolError),
                "{rc:?}"
            );
        }
    }

    /// The smallest legal AUTH: reason code, property length, and the method.
    #[test]
    fn test_auth_minimal_wire_bytes() {
        let a = Auth::new(ReasonCode::ContinueAuthentication, method_props("x"));
        let mut dst = BytesMut::new();
        a.encode(&mut dst).unwrap();
        // 0x18 rc | 0x04 props len | 0x15 method id | 0x00 0x01 'x'
        assert_eq!(&dst[..], &[0x18, 0x04, 0x15, 0x00, 0x01, b'x']);
        assert_eq!(a.encoded_size(), dst.len());
    }

    // ---- ContinueAuthentication / ReAuthenticate / Success ----

    #[test]
    fn test_auth_round_trips_every_reason_code() {
        for rc in [
            ReasonCode::Success,
            ReasonCode::ContinueAuthentication,
            ReasonCode::ReAuthenticate,
        ] {
            round_trip(Auth::new(rc, method_props("SCRAM-SHA-256")));
        }
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
        let a = Auth::new(ReasonCode::ContinueAuthentication, method_props("plain"));
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

    // ---- Authentication Data requires an Authentication Method ----

    /// §3.15.2.2.3. Enforced in `Properties`, so this only checks that AUTH
    /// is wired to it; the rule itself is tested in `properties.rs`.
    #[test]
    fn test_auth_rejects_data_without_method() {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::AuthenticationData,
            PropertyValue::Binary(Bytes::from_static(b"tok")),
        ));
        let a = Auth::new(ReasonCode::ContinueAuthentication, props);
        let mut dst = BytesMut::new();
        assert_eq!(a.encode(&mut dst), Err(Error::ProtocolError));
    }

    #[test]
    fn test_auth_accepts_data_with_method() {
        let mut props = method_props("SCRAM-SHA-1");
        props.push(Property::new(
            PropertyId::AuthenticationData,
            PropertyValue::Binary(Bytes::from_static(b"tok")),
        ));
        round_trip(Auth::new(ReasonCode::ContinueAuthentication, props));
    }
}
