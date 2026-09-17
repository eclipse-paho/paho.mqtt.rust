// paho-mqtt-codec/src/disconnect.rs
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

//! MQTT DISCONNECT packet.
//!
//! DISCONNECT is sent by either the client or the broker to signal a clean
//! session end.
//!
//! Wire format has an important shorthand shared by both versions: when the
//! reason code is `NormalDisconnection` (0x00) and there are no properties,
//! the remaining_length is 0 (the payload is omitted entirely). This means a
//! v3.1.1 DISCONNECT and a v5 normal-disconnect are byte-identical on the
//! wire.

use crate::{MqttVersion, Properties, ReasonCode, Result};
use bytes::{Buf, BufMut, Bytes};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////

/// A DISCONNECT packet, unified across MQTT v3.1.1 and v5.
///
/// In v3.1.1, DISCONNECT carries no payload at all — `reason_code` is always
/// `NormalDisconnection` and `properties` is always empty on the wire.
/// In v5, a reason code and optional properties may be present, but if both
/// are absent the wire form is identical to v3.1.1.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Disconnect {
    /// Disconnect reason code (v5 only; always `NormalDisconnection` for v3.1.1).
    pub reason_code: ReasonCode,
    /// MQTT v5 properties; empty on v3.1.1 connections.
    pub properties: Properties,
}

impl Default for Disconnect {
    fn default() -> Self {
        Disconnect {
            reason_code: ReasonCode::NormalDisconnection,
            properties: Properties::new(),
        }
    }
}

impl Disconnect {
    /// Creates a DISCONNECT with no properties.
    pub fn new(reason_code: ReasonCode) -> Self {
        Disconnect {
            reason_code,
            properties: Properties::new(),
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
    pub fn encode<B: BufMut>(&self, dst: &mut B, ver: MqttVersion) -> Result<()> {
        if ver != MqttVersion::V5 {
            return Ok(());
        }
        match (self.reason_code, self.properties.is_empty()) {
            (ReasonCode::NormalDisconnection, true) => {}
            (rc, true) => dst.put_u8(rc as u8),
            (rc, false) => {
                dst.put_u8(rc as u8);
                self.properties.encode(dst, ver)?;
            }
        }
        Ok(())
    }

    /// Decodes from the raw `payload` bytes for the given version, `ver`.
    pub fn decode(mut payload: Bytes, ver: MqttVersion) -> Result<Self> {
        // Empty payload (shorthand or v3.1.1) → NormalDisconnection, no props.
        if payload.is_empty() || ver != MqttVersion::V5 {
            return Ok(Disconnect::default());
        }

        let reason_code = ReasonCode::try_from(payload.get_u8())?;
        // Property length is omitted when the remaining length < 2.
        let properties = if payload.is_empty() {
            Properties::new()
        }
        else {
            Properties::decode(&mut payload, ver)?
        };

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
        Error,
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
}
