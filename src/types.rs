// types.rs
//
// Miscellaneous small types for the MQTT library.
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2019-2023 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! MQTT support types

use crate::{Error, Result};
use std::{fmt, os::raw::c_int};

/// The default version to connect with.
/// First try v3.1.1, and if that fails, try v3.1
pub const MQTT_VERSION_DEFAULT: u32 = ffi::MQTTVERSION_DEFAULT;

/// Connect with MQTT v3.1
pub const MQTT_VERSION_3_1: u32 = ffi::MQTTVERSION_3_1;

/// Connect with MQTT v3.1.1
pub const MQTT_VERSION_3_1_1: u32 = ffi::MQTTVERSION_3_1_1;

/// Connect with MQTT v5
pub const MQTT_VERSION_5: u32 = ffi::MQTTVERSION_5;

/// Quality of Service Zero: At most once
pub const QOS_0: i32 = 0;

/// Quality of Service One: At least once
pub const QOS_1: i32 = 1;

/// Quality of Service Two: Exactly Once
pub const QOS_2: i32 = 2;

/// Supported MQTT protocol versions
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u32)]
pub enum MqttVersion {
    /// The default v3.1.1 or v3.1
    #[default]
    Default = MQTT_VERSION_DEFAULT,
    /// Version 3.1 (byte 3)
    V3_1 = MQTT_VERSION_3_1,
    /// Version 3.1.1 (byte 4)
    V3_1_1 = MQTT_VERSION_3_1_1,
    /// Version 5
    V5 = MQTT_VERSION_5,
}

impl From<u32> for MqttVersion {
    fn from(ver: u32) -> Self {
        use MqttVersion::*;
        match ver {
            MQTT_VERSION_3_1 => V3_1,
            MQTT_VERSION_3_1_1 => V3_1_1,
            MQTT_VERSION_5 => V5,
            _ => Default,
        }
    }
}

impl From<c_int> for MqttVersion {
    fn from(ver: c_int) -> Self {
        Self::from(ver as u32)
    }
}

impl fmt::Display for MqttVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

/// Supported Quality of Service levels
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QoS {
    /// At most once
    AtMostOnce = 0,
    /// At least once
    #[default]
    AtLeastOnce = 1,
    /// Exactly Once
    ExactlyOnce = 2,
}

impl QoS {
    /// At most once
    pub const QoS0: QoS = QoS::AtMostOnce;
    /// At least once
    pub const QoS1: QoS = QoS::AtLeastOnce;
    /// Exactly Once
    pub const QoS2: QoS = QoS::ExactlyOnce;
}

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

impl TryFrom<u8> for QoS {
    type Error = Error;

    fn try_from(qos: u8) -> Result<Self> {
        use QoS::*;
        match qos {
            0 => Ok(AtMostOnce),
            1 => Ok(AtLeastOnce),
            2 => Ok(ExactlyOnce),
            _ => Err(Error::BadQos),
        }
    }
}

// This is included for backward compatibility, but should eventually be
// changed to TryFrom, possibly if/when we change the client API to return
// Result<Token> from most calls.
impl From<i32> for QoS {
    fn from(qos: i32) -> Self {
        Self::try_from(qos as u8).unwrap_or_default()
    }
}

// This is included for backward compatibility, but should eventually be
// changed to TryFrom, possibly if/when we change the client API to return
// Result<Token> from most calls.
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
    fn test_qos_display() {
        assert_eq!(QoS::AtMostOnce.to_string(), "0");
        assert_eq!(QoS::AtLeastOnce.to_string(), "1");
        assert_eq!(QoS::ExactlyOnce.to_string(), "2");
    }
}
