// paho-mqtt-codec/src/lib.rs
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

//! MQTT packet codec for the Eclipse Paho Rust client library.
//!
//! This crate provides pure-Rust encoding and decoding of MQTT packets
//! for protocol versions 3.1.1 and 5.0. It has no I/O dependency — all
//! types are pure data transformations.

#![no_std]
#![deny(missing_docs)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

/// The Vec type for the 'std' build.
#[cfg(feature = "std")]
pub type Vec<T> = std::vec::Vec<T>;

/// The Vec type for the 'no_std' build.
#[cfg(not(feature = "std"))]
pub type Vec<T> = alloc::vec::Vec<T>;

/// The String type for the 'std' build.
#[cfg(feature = "std")]
pub type String = std::string::String;

/// The String type for the 'no_std' build.
#[cfg(not(feature = "std"))]
pub type String = alloc::string::String;

pub mod error;

pub mod varlen;

pub(crate) mod wire;

pub mod properties;

pub mod packet;

pub mod types;

pub mod reason_code;

pub mod connect;

pub mod publish;

pub mod subscribe;

pub mod unsubscribe;

pub mod disconnect;

pub mod auth;

pub use auth::Auth;
pub use connect::{ConnAck, Connect, Will};
pub use disconnect::Disconnect;
pub use error::{Error, Result};
pub use properties::{Properties, Property, PropertyId, PropertyType, PropertyValue};
pub use publish::{PubAck, PubComp, PubRec, PubRel, Publish};
pub use reason_code::ReasonCode;
pub use subscribe::{RetainHandling, SubAck, Subscribe, Subscription};
pub use types::{
    MQTT_VERSION_3, MQTT_VERSION_3_1_1, MQTT_VERSION_5, MQTT_VERSION_DEFAULT, MqttVersion,
    QOS_0, QOS_1, QOS_2, QoS,
};
pub use unsubscribe::{UnsubAck, Unsubscribe};
