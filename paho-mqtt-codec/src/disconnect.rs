// paho-mqtt-codec/src/disconnect.rs
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

//! MQTT DISCONNECT packet.
//!
//! DISCONNECT is sent by either the client or the broker to signal a clean
//! session end.
//!
//! Wire format has an important shorthand shared by all versions: when the
//! reason code is `NormalDisconnection` (0x00) and there are no properties,
//! the remaining_length is 0 (the payload is omitted entirely). This means a
//! v3 DISCONNECT and a v5 normal-disconnect are byte-identical on the
//! wire.

use crate::{MqttVersion, PacketType, Properties, ReasonCode, Result, wire::expect_end};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// A DISCONNECT packet, unified across MQTT v3.1, v3.1.1, and v5.
///
/// In v3, DISCONNECT carries no payload at all — `reason_code` is always
/// `NormalDisconnection` and `properties` is always empty on the wire.
/// In v5, a reason code and optional properties may be present, but if both
/// are absent the wire form is identical to v3.
///
/// The default is a normal disconnection with no properties, which encodes to
/// an empty body on every version.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Disconnect {
    /// Disconnect reason code (v5 only; always `NormalDisconnection` for v3).
    pub reason_code: ReasonCode,
    /// MQTT v5 properties; empty on v3 connections.
    pub properties: Properties,
}

impl Disconnect {
    /// Creates a DISCONNECT with no properties.
    pub fn new(reason_code: ReasonCode) -> Self {
        Self {
            reason_code,
            properties: Properties::new(),
        }
    }

    /// Creates a DISCONNECT carrying MQTT v5 properties.
    ///
    /// v5 only.
    ///
    /// Only `ReasonString`, `ServerReference`, `SessionExpiryInterval`, and
    /// `UserProperty` are permitted here (§3.14.2.2); [`encode`](Self::encode)
    /// rejects anything else. Properties cannot be sent on a v3 connection at
    /// all, so encoding a non-empty list for v3 is an error rather than a
    /// silent drop — use [`new`](Self::new) for a DISCONNECT that must work on
    /// either version.
    pub fn with_properties(reason_code: ReasonCode, properties: Properties) -> Self {
        Self {
            reason_code,
            properties,
        }
    }
    /// Returns the remaining-length byte count for version, `ver`.
    ///
    /// When `reason_code` is `NormalDisconnection` and `properties` is empty,
    /// returns 0 regardless of version (shorthand form).
    pub fn encoded_size(&self, ver: MqttVersion) -> usize {
        if ver != MqttVersion::V5 {
            return 0;
        }
        match (self.reason_code, self.properties.is_empty()) {
            (ReasonCode::NormalDisconnection, true) => 0,
            (_, true) => 1,
            _ => 1 + self.properties.encoded_size(ver),
        }
    }

    /// Encodes the variable header and payload into `dst`.
    ///
    /// Uses the shortest v5 form permitted: nothing for a normal disconnect
    /// with no properties, just the reason code when there are no
    /// properties, otherwise the full form.
    ///
    /// On v3 nothing is written; a non-empty `properties` is
    /// [`Error::ProtocolError`](crate::Error::ProtocolError). A v3 DISCONNECT
    /// has no reason code, so `reason_code` is not checked for that version.
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        if ver != MqttVersion::V5 {
            // A v3 DISCONNECT has no body; non-empty properties are an error.
            return self.properties.encode(dst, ver);
        }
        match (self.reason_code, self.properties.is_empty()) {
            (ReasonCode::NormalDisconnection, true) => {}
            (rc, true) => dst.put_u8(rc as u8),
            (rc, false) => {
                dst.put_u8(rc as u8);
                self.properties.check_context(PacketType::Disconnect)?;
                self.properties.encode(dst, ver)?;
            }
        }
        Ok(())
    }

    /// Decodes from the raw `payload` bytes for the given version, `ver`.
    ///
    /// A v3 DISCONNECT has no body; any payload at all is
    /// [`MalformedPacket`](crate::Error::MalformedPacket).
    pub fn decode(mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        if ver != MqttVersion::V5 {
            expect_end(&payload)?;
            return Ok(Disconnect::default());
        }
        // Empty payload (shorthand or v3) → NormalDisconnection, no props.
        if payload.is_empty() {
            return Ok(Disconnect::default());
        }

        let reason_code = ReasonCode::try_from(payload.get_u8())?;
        // Property length is omitted when the remaining length < 2.
        let properties = if payload.is_empty() {
            Properties::new()
        } else {
            Properties::decode_in(&mut payload, ver, PacketType::Disconnect)?
        };
        expect_end(&payload)?;

        Ok(Disconnect {
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
        Error, Property, PropertyId, PropertyValue,
        types::{MQTT_VERSION_3_1_1, MQTT_VERSION_5},
    };
    use bytes::BytesMut;

    fn v311() -> MqttVersion {
        MQTT_VERSION_3_1_1
    }

    fn v5() -> MqttVersion {
        MQTT_VERSION_5
    }

    fn round_trip(d: Disconnect, ver: MqttVersion) {
        let mut dst = BytesMut::new();
        d.encode(&mut dst, ver).unwrap();
        let decoded = Disconnect::decode(dst.freeze(), ver).unwrap();
        assert_eq!(decoded, d);
    }

    #[test]
    fn test_disconnect_v5_decode_reason_code_no_props() {
        // Remaining length 1: reason code only, no property length.
        let payload = Bytes::from_static(&[0x8E]);
        let d = Disconnect::decode(payload, v5()).unwrap();
        assert_eq!(d.reason_code, ReasonCode::SessionTakenOver);
        assert!(d.properties.is_empty());
    }

    #[test]
    fn test_disconnect_v5_decode_unknown_reason_code_errors() {
        assert_eq!(
            Disconnect::decode(Bytes::from_static(&[0xA3]), v5()),
            Err(Error::ProtocolError)
        );
    }

    // ---- v3.1.1 ----

    #[test]
    fn test_disconnect_v311_encodes_empty() {
        let d = Disconnect::default();
        let mut dst = BytesMut::new();
        d.encode(&mut dst, v311()).unwrap();
        assert!(dst.is_empty());
    }

    #[test]
    fn test_disconnect_v311_encoded_size_zero() {
        assert_eq!(Disconnect::default().encoded_size(v311()), 0);
    }

    #[test]
    fn test_disconnect_v311_round_trip() {
        round_trip(Disconnect::default(), v311());
    }

    #[test]
    fn test_disconnect_v311_ignores_reason_code() {
        // Non-normal reason code is not written on v3.1.1 wire; round-trip
        // decodes as NormalDisconnection.
        let d = Disconnect::new(ReasonCode::NotAuthorized);
        let mut dst = BytesMut::new();
        d.encode(&mut dst, v311()).unwrap();
        assert!(dst.is_empty());
        let decoded = Disconnect::decode(dst.freeze(), v311()).unwrap();
        assert_eq!(decoded.reason_code, ReasonCode::NormalDisconnection);
    }

    // ---- v5 shorthand (reason=Normal, no props) ----

    #[test]
    fn test_disconnect_v5_normal_encodes_empty() {
        let d = Disconnect::default();
        let mut dst = BytesMut::new();
        d.encode(&mut dst, v5()).unwrap();
        assert!(dst.is_empty());
    }

    #[test]
    fn test_disconnect_v5_normal_encoded_size_zero() {
        assert_eq!(Disconnect::default().encoded_size(v5()), 0);
    }

    #[test]
    fn test_disconnect_v5_normal_round_trip() {
        round_trip(Disconnect::default(), v5());
    }

    // ---- v5 full form ----

    #[test]
    fn test_disconnect_v5_reason_code_encoded() {
        let d = Disconnect::new(ReasonCode::SessionTakenOver);
        let mut dst = BytesMut::new();
        d.encode(&mut dst, v5()).unwrap();
        // Reason code only; the property length is omitted when empty.
        assert_eq!(&dst[..], &[ReasonCode::SessionTakenOver as u8]);
    }

    #[test]
    fn test_disconnect_v5_encoded_size_with_reason() {
        let d = Disconnect::new(ReasonCode::SessionTakenOver);
        // Reason code only; the property length is omitted when empty.
        assert_eq!(d.encoded_size(v5()), 1);
    }

    #[test]
    fn test_disconnect_v5_round_trip_reason() {
        round_trip(Disconnect::new(ReasonCode::SessionTakenOver), v5());
    }

    #[test]
    fn test_disconnect_v5_round_trip_not_authorized() {
        round_trip(Disconnect::new(ReasonCode::NotAuthorized), v5());
    }

    #[test]
    fn test_disconnect_v5_encoded_size_matches_encode() {
        let d = Disconnect::new(ReasonCode::ServerBusy);
        let mut dst = BytesMut::new();
        d.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), d.encoded_size(v5()));
    }

    /// The derived default must be a *normal* disconnection: the shorthand
    /// encoding and both `decode` shortcuts depend on it. Deriving gets this
    /// from `ReasonCode`'s own default, so pin it here rather than trust that
    /// it stays put.
    #[test]
    fn test_default_is_normal_disconnection_with_no_properties() {
        let d = Disconnect::default();
        assert_eq!(d.reason_code, ReasonCode::NormalDisconnection);
        assert_eq!(d.reason_code, ReasonCode::Success, "the two are one code");
        assert!(d.properties.is_empty());
        assert_eq!(d, Disconnect::new(ReasonCode::NormalDisconnection));
    }

    // ---- with_properties ----

    /// A ReasonString, as a server explaining itself would send.
    fn reason_props(text: &str) -> Properties {
        let mut props = Properties::new();
        props.push(Property::new(
            PropertyId::ReasonString,
            PropertyValue::String(text.into()),
        ));
        props
    }

    #[test]
    fn test_with_properties_keeps_both_arguments() {
        let d = Disconnect::with_properties(ReasonCode::ServerBusy, reason_props("try later"));
        assert_eq!(d.reason_code, ReasonCode::ServerBusy);
        assert_eq!(
            d.properties.get(PropertyId::ReasonString),
            Some(&PropertyValue::String("try later".into()))
        );
    }

    /// With no properties it is indistinguishable from `new`, shorthand
    /// encoding included.
    #[test]
    fn test_with_properties_empty_matches_new() {
        let a = Disconnect::with_properties(ReasonCode::SessionTakenOver, Properties::new());
        let b = Disconnect::new(ReasonCode::SessionTakenOver);
        assert_eq!(a, b);

        // Reason code only: 1 byte, no property length (§3.14.2.2.1).
        assert_eq!(a.encoded_size(v5()), 1);
        let mut dst = BytesMut::new();
        a.encode(&mut dst, v5()).unwrap();
        assert_eq!(&dst[..], &[ReasonCode::SessionTakenOver as u8]);
    }

    #[test]
    fn test_with_properties_round_trip_v5() {
        let d = Disconnect::with_properties(
            ReasonCode::ServerShuttingDown,
            reason_props("maintenance"),
        );

        let mut dst = BytesMut::new();
        d.encode(&mut dst, v5()).unwrap();
        assert_eq!(
            dst.len(),
            d.encoded_size(v5()),
            "size must match the encoding"
        );

        let decoded = Disconnect::decode(dst.freeze(), v5()).unwrap();
        assert_eq!(decoded, d);
    }

    /// A DISCONNECT with several properties, including a repeated
    /// UserProperty, survives the round trip in order.
    #[test]
    fn test_with_properties_multiple_round_trip_v5() {
        let mut props = reason_props("going away");
        props.push(Property::new(
            PropertyId::SessionExpiryInterval,
            PropertyValue::U32(120),
        ));
        props.push(Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair("a".into(), "1".into()),
        ));
        props.push(Property::new(
            PropertyId::UserProperty,
            PropertyValue::StringPair("b".into(), "2".into()),
        ));
        let d = Disconnect::with_properties(ReasonCode::NormalDisconnection, props);

        let mut dst = BytesMut::new();
        d.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst.len(), d.encoded_size(v5()));

        let decoded = Disconnect::decode(dst.freeze(), v5()).unwrap();
        assert_eq!(decoded, d);
        assert_eq!(decoded.properties.len(), 4);
    }

    /// Even `NormalDisconnection` gets the full form once it has something to
    /// say, rather than the empty shorthand.
    #[test]
    fn test_with_properties_defeats_the_shorthand() {
        let d = Disconnect::with_properties(ReasonCode::NormalDisconnection, reason_props("bye"));
        assert!(d.encoded_size(v5()) > 1);

        let mut dst = BytesMut::new();
        d.encode(&mut dst, v5()).unwrap();
        assert_eq!(dst[0], ReasonCode::NormalDisconnection as u8);
        assert!(!dst[1..].is_empty(), "the properties must be on the wire");
    }

    /// v3 has no properties, so encoding a non-empty list for it fails rather
    /// than dropping them.
    ///
    /// The all-or-nothing guarantee belongs to `Packet::encode`, not to this
    /// method, so the buffer is checked through the packet.
    #[test]
    fn test_with_properties_on_v3_is_an_error() {
        let d = Disconnect::with_properties(ReasonCode::NormalDisconnection, reason_props("bye"));
        let mut dst = BytesMut::new();
        assert_eq!(d.encode(&mut dst, v311()), Err(Error::ProtocolError));

        let mut dst = BytesMut::from(&b"before"[..]);
        assert_eq!(
            crate::Packet::Disconnect(d).encode(&mut dst, v311()),
            Err(Error::ProtocolError)
        );
        assert_eq!(&dst[..], b"before", "nothing partial is left behind");
    }

    /// A property the specification does not allow in a DISCONNECT is
    /// rejected when the packet is encoded (§2.2.2.2).
    #[test]
    fn test_with_properties_rejects_a_property_not_allowed_here() {
        let mut props = Properties::new();
        props.push(Property::new(
            // A PUBLISH property; meaningless in a DISCONNECT.
            PropertyId::ContentType,
            PropertyValue::String("text/plain".into()),
        ));
        let d = Disconnect::with_properties(ReasonCode::NormalDisconnection, props);
        let mut dst = BytesMut::new();
        assert_eq!(d.encode(&mut dst, v5()), Err(Error::ProtocolError));
    }
}
