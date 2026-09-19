// paho-mqtt-codec/src/error.rs
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

//! Error types for the MQTT codec.
//!
//! MQTT v5 distinguishes two kinds of bad input, and a receiver that reports
//! the problem to its peer must say which (§4.13):
//!
//! - a **Malformed Packet** cannot be parsed — bad lengths, invalid UTF-8,
//!   reserved bits set, an unknown packet type or property identifier. The
//!   receiver sends DISCONNECT with reason code `0x81`.
//! - a **Protocol Error** parses fine but breaks a rule — a packet ID of 0,
//!   a property out of range or repeated, a topic name with a wildcard. The
//!   receiver sends DISCONNECT with reason code `0x82`, or a more specific
//!   code where the specification defines one.
//!
//! [`Error::reason_code`] maps every variant to the code to report.

use crate::ReasonCode;
use thiserror::Error;

/// MQTT encoding and protocol errors.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// An invalid QoS byte value was encountered.
    #[error("invalid QoS value")]
    BadQoS,
    /// A variable-byte integer exceeded the maximum allowed value (268,435,455).
    #[error("variable-length integer overflow")]
    VarLenOverflow,
    /// A variable-byte integer had a continuation bit set on the fourth byte,
    /// or used more bytes than its value requires.
    #[error("malformed variable-length integer")]
    MalformedVarLen,
    /// An unrecognized MQTT packet type nibble was encountered.
    #[error("unknown packet type: {0:#x}")]
    UnknownPacketType(u8),
    /// The packet payload was shorter than expected.
    #[error("packet payload too short")]
    PacketTooShort,
    /// The packet could not be parsed: a reserved bit was set, a field held a
    /// value with no defined meaning, or the structure was inconsistent.
    #[error("malformed packet")]
    MalformedPacket,
    /// The packet parsed but violates a protocol rule: a zero packet
    /// identifier, a property out of range or repeated, a required field
    /// missing, an empty subscription list, and so on.
    #[error("protocol error")]
    ProtocolError,
    /// A topic name was empty where one is required, or contained a wildcard.
    #[error("topic name invalid")]
    TopicNameInvalid,
    /// A topic filter was empty, misplaced a wildcard, or had a malformed
    /// shared-subscription prefix.
    #[error("topic filter invalid")]
    TopicFilterInvalid,
    /// A UTF-8 string field exceeded the 65,535-byte wire limit.
    #[error("UTF-8 string exceeds 65,535 bytes")]
    StringTooLong,
    /// A binary data field exceeded the 65,535-byte wire limit.
    #[error("binary data exceeds 65,535 bytes")]
    BinaryTooLong,
    /// An incoming packet's declared size exceeded the configured maximum.
    #[error("packet exceeds maximum packet size")]
    PacketTooLarge,
    /// A CONNECT packet named a protocol level this codec does not support,
    /// or a protocol name and level that do not belong together (the crate
    /// speaks `MQIsdp`/3 for v3.1, `MQTT`/4 for v3.1.1, and `MQTT`/5 for v5).
    /// Also returned by the `TryFrom` conversions on [`MqttVersion`] for an
    /// unknown level byte.
    ///
    /// [`MqttVersion`]: crate::MqttVersion
    #[error("unsupported protocol version")]
    UnsupportedProtocolVersion,
}

impl Error {
    /// The MQTT v5 reason code that describes this error to the peer.
    ///
    /// For a decode failure this is the code to put in the DISCONNECT (or,
    /// for [`UnsupportedProtocolVersion`](Self::UnsupportedProtocolVersion),
    /// the CONNACK) that a receiver sends before closing the connection.
    /// Errors that can only arise while *encoding* a locally built packet
    /// ([`VarLenOverflow`](Self::VarLenOverflow),
    /// [`StringTooLong`](Self::StringTooLong),
    /// [`BinaryTooLong`](Self::BinaryTooLong)) are never reported to a peer;
    /// they map to `MalformedPacket` for completeness.
    pub fn reason_code(&self) -> ReasonCode {
        use Error::*;
        match self {
            BadQoS | VarLenOverflow | MalformedVarLen | UnknownPacketType(_) | PacketTooShort
            | MalformedPacket | StringTooLong | BinaryTooLong => ReasonCode::MalformedPacket,
            ProtocolError => ReasonCode::ProtocolError,
            TopicNameInvalid => ReasonCode::TopicNameInvalid,
            TopicFilterInvalid => ReasonCode::TopicFilterInvalid,
            PacketTooLarge => ReasonCode::PacketTooLarge,
            UnsupportedProtocolVersion => ReasonCode::UnsupportedProtocolVersion,
        }
    }

    /// Returns `true` if the input could not be parsed at all (DISCONNECT
    /// reason `0x81`), as opposed to parsing but breaking a rule.
    pub fn is_malformed(&self) -> bool {
        self.reason_code() == ReasonCode::MalformedPacket
    }
}

/// The result type for codec operations.
pub type Result<T> = core::result::Result<T, Error>;

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reason_code_mapping() {
        use Error::*;
        for e in [
            BadQoS,
            VarLenOverflow,
            MalformedVarLen,
            UnknownPacketType(0),
            PacketTooShort,
            MalformedPacket,
            StringTooLong,
            BinaryTooLong,
        ] {
            assert_eq!(e.reason_code(), ReasonCode::MalformedPacket, "{e:?}");
            assert!(e.is_malformed(), "{e:?}");
        }
        assert_eq!(ProtocolError.reason_code(), ReasonCode::ProtocolError);
        assert_eq!(TopicNameInvalid.reason_code(), ReasonCode::TopicNameInvalid);
        assert_eq!(
            TopicFilterInvalid.reason_code(),
            ReasonCode::TopicFilterInvalid
        );
        assert_eq!(PacketTooLarge.reason_code(), ReasonCode::PacketTooLarge);
        assert_eq!(
            UnsupportedProtocolVersion.reason_code(),
            ReasonCode::UnsupportedProtocolVersion
        );
        for e in [
            ProtocolError,
            TopicNameInvalid,
            TopicFilterInvalid,
            PacketTooLarge,
            UnsupportedProtocolVersion,
        ] {
            assert!(!e.is_malformed(), "{e:?}");
        }
    }

    #[test]
    fn test_every_reported_code_is_an_error_code() {
        use Error::*;
        for e in [
            BadQoS,
            VarLenOverflow,
            MalformedVarLen,
            UnknownPacketType(0),
            PacketTooShort,
            MalformedPacket,
            ProtocolError,
            TopicNameInvalid,
            TopicFilterInvalid,
            StringTooLong,
            BinaryTooLong,
            PacketTooLarge,
            UnsupportedProtocolVersion,
        ] {
            assert!(e.reason_code().is_err(), "{e:?}");
        }
    }
}
