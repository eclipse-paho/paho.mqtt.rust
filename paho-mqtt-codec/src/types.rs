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

//! MQTT support types.

use crate::error::Error;
use std::fmt;

/// Connect with the default MQTT version (currently v5).
pub const MQTT_VERSION_DEFAULT: u8 = 5;

/// Connect with MQTT v3.1.1.
pub const MQTT_VERSION_3_1_1: u8 = 4;

/// Connect with MQTT v3.1.1.
pub const MQTT_VERSION_3: u8 = MQTT_VERSION_3_1_1;

/// Connect with MQTT v5.
pub const MQTT_VERSION_5: u8 = 5;

/// Quality of Service Zero: At most once.
pub const QOS_0: i32 = 0;

/// Quality of Service One: At least once.
pub const QOS_1: i32 = 1;

/// Quality of Service Two: Exactly once.
pub const QOS_2: i32 = 2;

/// Supported MQTT protocol versions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum MqttVersion {
    /// Version 3.1.1 (protocol byte 4).
    V3_1_1 = MQTT_VERSION_3_1_1,
    /// Version 5 (protocol byte 5).
    #[default]
    V5 = MQTT_VERSION_5,
}

impl MqttVersion {
    /// The only supported v3 is v3.1.1
    pub const v3: MqttVersion = Self::V3_1_1;
    /// Default: (currently v5)
    pub const Default: MqttVersion = Self::V5;
}

impl From<u8> for MqttVersion {
    fn from(ver: u8) -> Self {
        use MqttVersion::*;
        match ver {
            MQTT_VERSION_3_1_1 => V3_1_1,
            MQTT_VERSION_5 => V5,
            _ => Self::Default,
        }
    }
}

impl From<u32> for MqttVersion {
    fn from(ver: u32) -> Self {
        Self::from(ver as u8)
    }
}

impl From<i32> for MqttVersion {
    fn from(ver: i32) -> Self {
        Self::from(ver as u8)
    }
}

impl fmt::Display for MqttVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

/// Supported Quality of Service levels.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

impl TryFrom<u8> for QoS {
    type Error = Error;

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

impl From<i32> for QoS {
    fn from(qos: i32) -> Self {
        Self::try_from(qos as u8).unwrap_or_default()
    }
}

impl From<u32> for QoS {
    fn from(qos: u32) -> Self {
        Self::try_from(qos as u8).unwrap_or_default()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_version_display() {
        assert_eq!(MqttVersion::V3_1.to_string(), "3");
        assert_eq!(MqttVersion::V3_1_1.to_string(), "4");
        assert_eq!(MqttVersion::V5.to_string(), "5");
    }

    #[test]
    fn test_mqtt_version_from_u8() {
        assert_eq!(MqttVersion::from(3_u8), MqttVersion::V3_1);
        assert_eq!(MqttVersion::from(4_u8), MqttVersion::V3_1_1);
        assert_eq!(MqttVersion::from(5_u8), MqttVersion::V5);
        assert_eq!(MqttVersion::from(0_u8), MqttVersion::Default);
        assert_eq!(MqttVersion::from(99_u8), MqttVersion::Default);
    }

    #[test]
    fn test_mqtt_version_from_u32() {
        assert_eq!(MqttVersion::from(3_u32), MqttVersion::V3_1);
        assert_eq!(MqttVersion::from(4_u32), MqttVersion::V3_1_1);
        assert_eq!(MqttVersion::from(5_u32), MqttVersion::V5);
        assert_eq!(MqttVersion::from(0_u32), MqttVersion::Default);
        assert_eq!(MqttVersion::from(99_u32), MqttVersion::Default);
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
    fn test_qos_from_i32() {
        assert_eq!(QoS::from(0_i32), QoS::AtMostOnce);
        assert_eq!(QoS::from(1_i32), QoS::AtLeastOnce);
        assert_eq!(QoS::from(2_i32), QoS::ExactlyOnce);
        assert_eq!(QoS::from(99_i32), QoS::AtMostOnce); // invalid → default
    }
}
