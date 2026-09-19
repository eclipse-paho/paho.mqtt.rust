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

//! Pure-Rust encoding and decoding of MQTT control packets for protocol
//! versions 3.1, 3.1.1, and 5.0.
//!
//! The crate is a wire-format layer only: it converts between typed packet
//! structs and bytes, and has no dependency on I/O, an async runtime, or any
//! particular role in the protocol. It is used by the Eclipse Paho MQTT Rust
//! client, and is equally suited to a broker, a proxy, a protocol analyzer,
//! or an embedded device (`no_std` with `alloc`).
//!
//! Every packet type can be both encoded and decoded, so a program can sit
//! on either end of a connection. Decoding is strict: input that violates
//! the specification is rejected with an [`Error`] rather than reinterpreted.

#![no_std]
#![deny(missing_docs)]

extern crate alloc;

// `alloc`'s `String` and `Vec` are the same types `std` re-exports, so one
// import serves both configurations.
pub(crate) use alloc::{string::String, vec::Vec};

pub(crate) mod wire;

pub mod auth;
pub mod connect;
pub mod disconnect;
pub mod error;
pub mod packet;
pub mod properties;
pub mod publish;
pub mod reason_code;
pub mod subscribe;
pub mod topic;
pub mod types;
pub mod unsubscribe;
pub mod varlen;

pub use auth::Auth;
pub use connect::{ConnAck, Connect, Will};
pub use disconnect::Disconnect;
pub use error::{Error, Result};
pub use packet::{DecodeOptions, MAX_PACKET_SIZE, Packet, PacketType, RawPacket};
pub use properties::{
    Properties, Property, PropertyContext, PropertyId, PropertyType, PropertyValue,
};
pub use publish::{PubAck, PubComp, PubRec, PubRel, Publish};
pub use reason_code::ReasonCode;
pub use subscribe::{RetainHandling, SubAck, Subscribe, Subscription};
pub use types::{
    MQTT_VERSION_3, MQTT_VERSION_3_1, MQTT_VERSION_3_1_1, MQTT_VERSION_5, MQTT_VERSION_DEFAULT,
    MqttVersion, QOS_0, QOS_1, QOS_2, QoS,
};
pub use unsubscribe::{UnsubAck, Unsubscribe};
