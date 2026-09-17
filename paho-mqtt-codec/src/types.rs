// paho-mqtt-codec/src/types.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

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

//! MQTT support types: protocol version and quality of service.
//!
//! Each concept is an enum ([`MqttVersion`], [`QoS`]) with a few named
//! constants of the same type (`MQTT_VERSION_5`, `QOS_1`, …) as a second
//! spelling. The constants exist for compatibility with the Paho MQTT Rust
//! client's earlier API, whose values came from the Paho C library; new code
//! should prefer the enum variants. The constants may be deprecated in a
//! future release.

use crate::error::Error;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Supported MQTT protocol versions.
///
/// The discriminant is the protocol-level byte sent in CONNECT.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum MqttVersion {
    /// Version 3.1.1 (protocol byte 4).
    V3_1_1 = 4,
    /// Version 5 (protocol byte 5).
    #[default]
    V5 = 5,
}

impl MqttVersion {
    /// The only supported v3 is v3.1.1
    pub const V3: MqttVersion = Self::V3_1_1;
    /// Default: (currently v5)
    #[allow(non_upper_case_globals)]
    pub const Default: MqttVersion = Self::V5;
}

/// Connect with the default MQTT version (currently v5).
pub const MQTT_VERSION_DEFAULT: MqttVersion = MqttVersion::Default;

/// Connect with MQTT v3.1.1.
pub const MQTT_VERSION_3_1_1: MqttVersion = MqttVersion::V3_1_1;

/// Connect with MQTT v3.1.1.
pub const MQTT_VERSION_3: MqttVersion = MqttVersion::V3_1_1;

/// Connect with MQTT v5.
pub const MQTT_VERSION_5: MqttVersion = MqttVersion::V5;

impl TryFrom<u8> for MqttVersion {
    type Error = Error;

    /// Converts a protocol-level byte into a version.
    ///
    /// Returns [`Error::UnsupportedProtocolVersion`] for anything other than
    /// 4 (v3.1.1) or 5 (v5).
    fn try_from(ver: u8) -> Result<Self, Error> {
        match ver {
            4 => Ok(Self::V3_1_1),
            5 => Ok(Self::V5),
            _ => Err(Error::UnsupportedProtocolVersion),
        }
    }
}

impl TryFrom<u32> for MqttVersion {
    type Error = Error;

    fn try_from(ver: u32) -> Result<Self, Error> {
        u8::try_from(ver)
            .map_err(|_| Error::UnsupportedProtocolVersion)
            .and_then(Self::try_from)
    }
}

impl TryFrom<i32> for MqttVersion {
    type Error = Error;

    fn try_from(ver: i32) -> Result<Self, Error> {
        u8::try_from(ver)
            .map_err(|_| Error::UnsupportedProtocolVersion)
            .and_then(Self::try_from)
    }
}

impl fmt::Display for MqttVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

/// Supported Quality of Service levels.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum QoS {
    /// At most once (fire and forget).
    #[default]
    AtMostOnce = 0,
    /// At least once (acknowledged delivery).
    AtLeastOnce = 1,
    /// Exactly once (assured delivery).
    ExactlyOnce = 2,
}

impl QoS {
    #[allow(non_upper_case_globals)]
    /// At most once.
    pub const QoS0: QoS = QoS::AtMostOnce;
    #[allow(non_upper_case_globals)]
    /// At least once.
    pub const QoS1: QoS = QoS::AtLeastOnce;
    #[allow(non_upper_case_globals)]
    /// Exactly once.
    pub const QoS2: QoS = QoS::ExactlyOnce;
}

/// Quality of Service Zero: At most once.
pub const QOS_0: QoS = QoS::AtMostOnce;

/// Quality of Service One: At least once.
pub const QOS_1: QoS = QoS::AtLeastOnce;

/// Quality of Service Two: Exactly once.
pub const QOS_2: QoS = QoS::ExactlyOnce;

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
            0 => Ok(AtMostOnce),
            1 => Ok(AtLeastOnce),
            2 => Ok(ExactlyOnce),
            _ => Err(Error::BadQoS),
        }
    }
}

impl TryFrom<u32> for QoS {
    type Error = Error;

    fn try_from(qos: u32) -> Result<Self, Error> {
        u8::try_from(qos)
            .map_err(|_| Error::BadQoS)
            .and_then(Self::try_from)
    }
}

impl TryFrom<i32> for QoS {
    type Error = Error;

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
        assert_eq!(MqttVersion::V3_1_1.to_string(), "4");
        assert_eq!(MqttVersion::V3.to_string(), "4");
        assert_eq!(MqttVersion::V5.to_string(), "5");
    }

    #[test]
    fn test_mqtt_version_try_from_u8() {
        assert_eq!(MqttVersion::try_from(4_u8), Ok(MqttVersion::V3_1_1));
        assert_eq!(MqttVersion::try_from(5_u8), Ok(MqttVersion::V5));
        for bad in [0_u8, 3, 6, 99, 255] {
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
        assert_eq!(MQTT_VERSION_3_1_1, MqttVersion::V3_1_1);
        assert_eq!(MQTT_VERSION_3, MqttVersion::V3_1_1);
        assert_eq!(MQTT_VERSION_5, MqttVersion::V5);
        assert_eq!(MQTT_VERSION_DEFAULT, MqttVersion::default());
        assert_eq!(MQTT_VERSION_3_1_1 as u8, 4);
        assert_eq!(MQTT_VERSION_5 as u8, 5);
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
}
