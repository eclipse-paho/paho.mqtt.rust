// paho-mqtt-codec/src/types.rs
//
// This file is part of the Eclipse Paho MQTT Rust Codec library.

/*******************************************************************************
 * Copyright (c) 2019-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! MQTT support types.
//!
//! Each concept is an enum ([`MqttVersion`], [`QoS`]) with a few named
//! constants of the same type (`MQTT_VERSION_5`, `QOS_1`, …) as a second
//! spelling. The constants exist for compatibility with the Paho MQTT Rust
//! client's earlier v1 API, whose values came from the Paho C library.
//! New code should typically prefer the enum variants for safety.

use crate::error::Error;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Supported MQTT protocol versions.
///
/// The discriminant is the protocol-level byte sent in CONNECT. Versions
/// order chronologically, so `ver < MqttVersion::V5` selects both v3 variants.
///
/// MQTT v3.1 differs from v3.1.1 only in the CONNECT packet — protocol name
/// `MQIsdp` and level 3 instead of `MQTT` and level 4 — and in having no
/// Session Present flag in CONNACK. Every other packet is identical on the
/// wire, so the two are handled alike everywhere else.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum MqttVersion {
    /// Version 3.1 (protocol byte 3, protocol name `MQIsdp`).
    V3_1 = 3,
    /// Version 3.1.1 (protocol byte 4).
    V3_1_1 = 4,
    /// Version 5 (protocol byte 5).
    #[default]
    V5 = 5,
}

impl MqttVersion {
    /// The latest v3 revision, v3.1.1.
    /// Clients should typically use this for v3 connections.
    pub const V3: MqttVersion = Self::V3_1_1;
    /// Default: (currently v5)
    #[allow(non_upper_case_globals)]
    pub const Default: MqttVersion = Self::V5;

    /// The protocol name carried in CONNECT for this version.
    pub fn protocol_name(self) -> &'static str {
        match self {
            Self::V3_1 => "MQIsdp",
            Self::V3_1_1 | Self::V5 => "MQTT",
        }
    }

    /// Returns `true` for the MQTT v5 protocol, which adds properties and
    /// reason codes to most packets.
    pub fn is_v5(self) -> bool {
        self == Self::V5
    }
}

/// Connect with the default MQTT version (currently v5).
pub const MQTT_VERSION_DEFAULT: MqttVersion = MqttVersion::Default;

/// Connect with MQTT v3.1.
pub const MQTT_VERSION_3_1: MqttVersion = MqttVersion::V3_1;

/// Connect with MQTT v3.1.1.
pub const MQTT_VERSION_3_1_1: MqttVersion = MqttVersion::V3_1_1;

/// Connect with MQTT v3.1.1, the current v3 revision.
pub const MQTT_VERSION_3: MqttVersion = MqttVersion::V3_1_1;

/// Connect with MQTT v5.
pub const MQTT_VERSION_5: MqttVersion = MqttVersion::V5;

impl TryFrom<u8> for MqttVersion {
    type Error = Error;

    /// Converts a protocol-level byte into a version.
    ///
    /// Returns [`Error::UnsupportedProtocolVersion`] for anything other than
    /// 3 (v3.1), 4 (v3.1.1), or 5 (v5).
    fn try_from(ver: u8) -> Result<Self, Error> {
        match ver {
            3 => Ok(Self::V3_1),
            4 => Ok(Self::V3_1_1),
            5 => Ok(Self::V5),
            _ => Err(Error::UnsupportedProtocolVersion),
        }
    }
}

impl TryFrom<u32> for MqttVersion {
    type Error = Error;

    /// This is a legacy conversion from Paho MQTT v1
    fn try_from(ver: u32) -> Result<Self, Error> {
        u8::try_from(ver)
            .map_err(|_| Error::UnsupportedProtocolVersion)
            .and_then(Self::try_from)
    }
}

impl TryFrom<i32> for MqttVersion {
    type Error = Error;

    /// This is a legacy conversion from Paho MQTT v1
    fn try_from(ver: i32) -> Result<Self, Error> {
        u8::try_from(ver)
            .map_err(|_| Error::UnsupportedProtocolVersion)
            .and_then(Self::try_from)
    }
}

impl fmt::Display for MqttVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::V3_1 => f.write_str("v3.1"),
            Self::V3_1_1 => f.write_str("v3.1.1"),
            Self::V5 => f.write_str("v5"),
        }
    }
}

/// Supported Quality of Service levels.
///
/// The [`AtMostOnce`](Self::AtMostOnce), [`AtLeastOnce`](Self::AtLeastOnce),
/// and [`ExactlyOnce`](Self::ExactlyOnce) associated constants are a second
/// spelling of the three variants, for application that prefer them.
///
/// With the `serde` feature, a value serializes under the variant name —
/// `"QoS0"`, `"QoS1"`, `"QoS2"` — and deserializes from either spelling, so
/// data written by a version of this crate that named the variants
/// `AtMostOnce` and so on still reads.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum QoS {
    /// At most once (fire and forget).
    #[default]
    #[cfg_attr(feature = "serde", serde(alias = "AtMostOnce"))]
    QoS0 = 0,
    /// At least once (acknowledged delivery).
    #[cfg_attr(feature = "serde", serde(alias = "AtLeastOnce"))]
    QoS1 = 1,
    /// Exactly once (assured delivery).
    #[cfg_attr(feature = "serde", serde(alias = "ExactlyOnce"))]
    QoS2 = 2,
}

impl QoS {
    /// At most once (fire and forget). Same as [`QoS::QoS0`].
    #[allow(non_upper_case_globals)]
    pub const AtMostOnce: QoS = QoS::QoS0;
    /// At least once (acknowledged delivery). Same as [`QoS::QoS1`].
    #[allow(non_upper_case_globals)]
    pub const AtLeastOnce: QoS = QoS::QoS1;
    /// Exactly once (assured delivery). Same as [`QoS::QoS2`].
    #[allow(non_upper_case_globals)]
    pub const ExactlyOnce: QoS = QoS::QoS2;
}

/// Quality of Service Zero: At most once.
pub const QOS_0: QoS = QoS::QoS0;

/// Quality of Service One: At least once.
pub const QOS_1: QoS = QoS::QoS1;

/// Quality of Service Two: Exactly once.
pub const QOS_2: QoS = QoS::QoS2;

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

impl TryFrom<u8> for QoS {
    type Error = Error;

    /// Returns [`Error::BadQoS`] for any value other than 0, 1, or 2.
    fn try_from(qos: u8) -> Result<Self, Error> {
        use QoS::*;
        match qos {
            0 => Ok(QoS0),
            1 => Ok(QoS1),
            2 => Ok(QoS2),
            _ => Err(Error::BadQoS),
        }
    }
}

impl TryFrom<u32> for QoS {
    type Error = Error;

    /// This is a legacy conversion from Paho MQTT v1
    fn try_from(qos: u32) -> Result<Self, Error> {
        u8::try_from(qos)
            .map_err(|_| Error::BadQoS)
            .and_then(Self::try_from)
    }
}

impl TryFrom<i32> for QoS {
    type Error = Error;

    /// This is a legacy conversion from Paho MQTT v1
    fn try_from(qos: i32) -> Result<Self, Error> {
        u8::try_from(qos)
            .map_err(|_| Error::BadQoS)
            .and_then(Self::try_from)
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn test_mqtt_version_display() {
        assert_eq!(MqttVersion::V3_1.to_string(), "v3.1");
        assert_eq!(MqttVersion::V3_1_1.to_string(), "v3.1.1");
        assert_eq!(MqttVersion::V3.to_string(), "v3.1.1");
        assert_eq!(MqttVersion::V5.to_string(), "v5");
    }

    #[test]
    fn test_mqtt_version_try_from_u8() {
        assert_eq!(MqttVersion::try_from(3_u8), Ok(MqttVersion::V3_1));
        assert_eq!(MqttVersion::try_from(4_u8), Ok(MqttVersion::V3_1_1));
        assert_eq!(MqttVersion::try_from(5_u8), Ok(MqttVersion::V5));
        for bad in [0_u8, 2, 6, 99, 255] {
            assert_eq!(
                MqttVersion::try_from(bad),
                Err(Error::UnsupportedProtocolVersion),
                "{bad}"
            );
        }
    }

    #[test]
    fn test_mqtt_version_try_from_wider_ints() {
        assert_eq!(MqttVersion::try_from(4_u32), Ok(MqttVersion::V3_1_1));
        assert_eq!(MqttVersion::try_from(5_i32), Ok(MqttVersion::V5));
        // Out-of-range values must not be truncated into a valid byte.
        assert_eq!(
            MqttVersion::try_from(0x105_u32),
            Err(Error::UnsupportedProtocolVersion)
        );
        assert_eq!(
            MqttVersion::try_from(-1_i32),
            Err(Error::UnsupportedProtocolVersion)
        );
    }

    #[test]
    fn test_mqtt_version_constants() {
        assert_eq!(MQTT_VERSION_3_1, MqttVersion::V3_1);
        assert_eq!(MQTT_VERSION_3_1 as u8, 3);
        assert_eq!(MQTT_VERSION_3_1_1, MqttVersion::V3_1_1);
        assert_eq!(MQTT_VERSION_3, MqttVersion::V3_1_1);
        assert_eq!(MQTT_VERSION_5, MqttVersion::V5);
        assert_eq!(MQTT_VERSION_DEFAULT, MqttVersion::default());
        assert_eq!(MQTT_VERSION_3_1_1 as u8, 4);
        assert_eq!(MQTT_VERSION_5 as u8, 5);
    }

    #[test]
    fn test_mqtt_version_ordering_and_helpers() {
        assert!(MqttVersion::V3_1 < MqttVersion::V3_1_1);
        assert!(MqttVersion::V3_1_1 < MqttVersion::V5);
        assert_eq!(MqttVersion::V3_1.protocol_name(), "MQIsdp");
        assert_eq!(MqttVersion::V3_1_1.protocol_name(), "MQTT");
        assert_eq!(MqttVersion::V5.protocol_name(), "MQTT");
        assert!(!MqttVersion::V3_1.is_v5());
        assert!(MqttVersion::V5.is_v5());
    }

    #[test]
    fn test_qos_display() {
        assert_eq!(QoS::AtMostOnce.to_string(), "0");
        assert_eq!(QoS::AtLeastOnce.to_string(), "1");
        assert_eq!(QoS::ExactlyOnce.to_string(), "2");
    }

    #[test]
    fn test_qos_try_from_u8() {
        assert_eq!(QoS::try_from(0_u8), Ok(QoS::AtMostOnce));
        assert_eq!(QoS::try_from(1_u8), Ok(QoS::AtLeastOnce));
        assert_eq!(QoS::try_from(2_u8), Ok(QoS::ExactlyOnce));
        assert_eq!(QoS::try_from(3_u8), Err(Error::BadQoS));
    }

    #[test]
    fn test_qos_try_from_wider_ints() {
        assert_eq!(QoS::try_from(0_i32), Ok(QoS::AtMostOnce));
        assert_eq!(QoS::try_from(1_u32), Ok(QoS::AtLeastOnce));
        assert_eq!(QoS::try_from(2_i32), Ok(QoS::ExactlyOnce));
        assert_eq!(QoS::try_from(3_i32), Err(Error::BadQoS));
        assert_eq!(QoS::try_from(-1_i32), Err(Error::BadQoS));
        assert_eq!(QoS::try_from(0x100_u32), Err(Error::BadQoS));
    }

    #[test]
    fn test_qos_constants() {
        assert_eq!(QOS_0, QoS::AtMostOnce);
        assert_eq!(QOS_1, QoS::AtLeastOnce);
        assert_eq!(QOS_2, QoS::ExactlyOnce);
        assert_eq!(QOS_1, QoS::QoS1);
    }

    // ---- serde representation ----

    /// A value serializes under its variant name, not the word form.
    #[test]
    #[cfg(feature = "serde")]
    fn test_qos_serializes_as_the_variant_name() {
        for (qos, json) in [
            (QoS::QoS0, "\"QoS0\""),
            (QoS::QoS1, "\"QoS1\""),
            (QoS::QoS2, "\"QoS2\""),
        ] {
            assert_eq!(serde_json::to_string(&qos).unwrap(), json);
        }
        // The constants are the same values, so they serialize the same way.
        assert_eq!(
            serde_json::to_string(&QoS::AtLeastOnce).unwrap(),
            "\"QoS1\""
        );
    }

    /// Both spellings deserialize, so data written when the variants were
    /// named `AtMostOnce` and so on still reads.
    #[test]
    #[cfg(feature = "serde")]
    fn test_qos_deserializes_from_either_spelling() {
        for (json, qos) in [
            ("\"QoS0\"", QoS::QoS0),
            ("\"QoS1\"", QoS::QoS1),
            ("\"QoS2\"", QoS::QoS2),
            ("\"AtMostOnce\"", QoS::QoS0),
            ("\"AtLeastOnce\"", QoS::QoS1),
            ("\"ExactlyOnce\"", QoS::QoS2),
        ] {
            assert_eq!(serde_json::from_str::<QoS>(json).unwrap(), qos, "{json}");
        }
        assert!(serde_json::from_str::<QoS>("\"AtSomePoint\"").is_err());
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_qos_round_trips_through_serde() {
        for qos in [QoS::QoS0, QoS::QoS1, QoS::QoS2] {
            let json = serde_json::to_string(&qos).unwrap();
            assert_eq!(serde_json::from_str::<QoS>(&json).unwrap(), qos);
        }
    }
}
