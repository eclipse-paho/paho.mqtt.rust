// paho-mqtt/src/properties.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2018-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! MQTT v5 properties.
//!
//! Properties are key/value pairs carried in MQTT v5 packets. The
//! [`Properties`] collection holds any number of [`Property`] values, each
//! carrying the value its [`PropertyId`] calls for.
//!
//! On v3.1.1 connections properties are always empty.

pub use paho_mqtt_codec::{Properties, PropertiesBuilder, Property, PropertyId, PropertyType};

/// Backward-compatible alias for [`PropertyId`].
pub type PropertyCode = PropertyId;

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(all(test, feature = "serde"))]
mod tests {
    use super::*;
    use crate::{QoS, ReasonCode};

    /// The crate's `serde` feature must reach the codec, where these types
    /// are defined. It once enabled only the `serde` dependency, so the
    /// re-exported types had no derives and nothing here compiled.
    #[test]
    fn test_property_types_round_trip() {
        let mut props = Properties::new();
        props.push(Property::SessionExpiryInterval(3600));
        props.push_user("k", "v");

        let json = serde_json::to_string(&props).unwrap();
        let back: Properties = serde_json::from_str(&json).unwrap();
        assert_eq!(back.get_u32(PropertyId::SessionExpiryInterval), Some(3600));
        assert_eq!(back.get_user("k"), Some("v"));
    }

    /// A single property, its identifier and its value serialize too.
    #[test]
    fn test_property_round_trip() {
        let prop = Property::ContentType("t/p".into());
        let back: Property = serde_json::from_str(&serde_json::to_string(&prop).unwrap()).unwrap();
        assert_eq!(back, prop);

        let id: PropertyId =
            serde_json::from_str(&serde_json::to_string(&PropertyId::TopicAlias).unwrap()).unwrap();
        assert_eq!(id, PropertyId::TopicAlias);
    }

    /// The other re-exported codec types the API hands back.
    #[test]
    fn test_reason_code_and_qos_round_trip() {
        let rc: ReasonCode =
            serde_json::from_str(&serde_json::to_string(&ReasonCode::NotAuthorized).unwrap())
                .unwrap();
        assert_eq!(rc, ReasonCode::NotAuthorized);

        let qos: QoS = serde_json::from_str(&serde_json::to_string(&QoS::QoS2).unwrap()).unwrap();
        assert_eq!(qos, QoS::QoS2);
    }
}
