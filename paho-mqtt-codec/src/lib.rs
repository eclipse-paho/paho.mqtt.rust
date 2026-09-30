// paho-mqtt-codec/src/lib.rs
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

//! Pure-Rust encoding and decoding of MQTT control packets for protocol
//! versions 3.1, 3.1.1, and 5.0.
//!
//! The crate is a wire-format layer only. It converts between typed packet
//! structs and bytes, and has no dependency on I/O or any particular runtime.
//! It is used by the Eclipse Paho MQTT Rust client, and is equally suited to a
//! broker, a proxy, a protocol analyzer, or an embedded device.
//!
//! It builds for `std` or `no_std` (though `no_std` requires `alloc`).
//!
//! Every packet type can be both encoded and decoded, so a program can sit
//! on either end of a connection, client or server. Decoding is strict; input
//! that violates the specification is rejected with an [`Error`].
//!
//! # Constructors
//!
//! MQTT v5 properties are the one field every packet struct carries and most
//! callers never set, so the constructors follow a single rule:
//!
//! - A packet that exists in v3 as well as v5 has a `new` that takes no
//!   properties, so a v3-only caller is never asked for a type it cannot use,
//!   and a `with_properties` that takes them. Where a *reason code* is v5-only
//!   too, `new` leaves that out as well: [`Disconnect::new`],
//!   [`UnsubAck::new`], and [`PubAck::new`] build the bare packets v3 can
//!   express, and the reason code moves to `with_properties`.
//! - [`Auth`], the one packet v5 introduced, takes properties directly in
//!   `new` — there is no v3 caller to spare, and an AUTH without an
//!   Authentication Method is meaningless. Its argument accepts a
//!   [`Properties`] or `None`.
//!
//! Encoding a non-empty property set for a v3 connection is an error rather
//! than a silent drop, so reaching for the wrong constructor is caught rather
//! than quietly changing what goes on the wire.
//!
//! `with_properties` accepts `None` as well as a [`Properties`] wherever an
//! empty set is an ordinary packet rather than a reason to reach for `new`:
//! [`PubAck`] (with its `PubRec`/`PubRel`/`PubComp` aliases) and
//! [`Disconnect`], where a reason code with no properties is its own short
//! wire form, and [`UnsubAck`], whose reason codes are the point of the
//! packet. So is [`Auth::new`], the one v5-only packet. There are never more
//! than two constructors: `new` for the version-neutral minimum, and
//! `with_properties` for everything v5 adds.
//!
//! [`Publish`] and [`Connect`] are the exceptions, with seven and eight
//! fields: properties are rarely the only one a caller wants to change, so
//! each has `new` and a builder — [`Publish::builder`] and
//! [`Connect::builder`] — and no `with_properties`.

//! # Version mismatches
//!
//! Every packet struct carries the v5 fields whether or not the connection can
//! use them, so encoding for v3 has to say what happens to a field that
//! version has no room for. There are two cases, and they are treated
//! differently on purpose:
//!
//! - **The field does not exist in v3.** Properties anywhere, the reason codes
//!   of DISCONNECT and the PUBACK family, the reason-code list of UNSUBACK, and
//!   `ConnAck::session_present` on v3.1. Encoding one of these for v3 is an
//!   [`Error::ProtocolError`]: the caller built something the connection cannot
//!   express, and writing the packet without it would change what goes on the
//!   wire without saying so. A v3 PUBACK carrying `NotAuthorized` would arrive
//!   as `Success`.
//! - **The field exists in v3 but the value is reserved there.** A CONNACK
//!   return code outside 0x00–0x05, or a SUBACK return code outside
//!   0x00–0x02 and 0x80. These are [`Error::MalformedPacket`], matching what
//!   the decoder must report for the same byte arriving on the wire: the
//!   packet's format is right and one field holds a value with no meaning.
//!
//! Neither case is ever silently dropped.
//!
//! # Panics
//!
//! The `encode` methods write through [`bytes::BufMut`], which panics rather
//! than returning an error when the sink is out of room. That cannot happen
//! for a growable sink such as a [`BytesMut`](bytes::BytesMut) — and
//! [`Packet::encode`] reserves its exact output up front — but encoding
//! directly into a fixed buffer, say a `&mut [u8]` on an embedded target, will
//! panic if the buffer is too small. Size it with
//! [`Packet::encoded_size`] first. Nothing else in the crate panics; decoding
//! reports every failure as an [`Error`].

#![no_std]
#![deny(missing_docs)]

extern crate alloc;

// `alloc`'s `String` and `Vec` are the same types `std` re-exports.
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
pub use connect::{ConnAck, Connect, ConnectBuilder, Will};
pub use disconnect::Disconnect;
pub use error::{Error, Result};
pub use packet::{DecodeOptions, MAX_PACKET_SIZE, Packet, PacketType, RawPacket};
pub use properties::{
    Properties, PropertiesBuilder, Property, PropertyContext, PropertyId, PropertyType,
    PropertyValue,
};
pub use publish::{PubAck, PubComp, PubRec, PubRel, Publish, PublishBuilder};
pub use reason_code::ReasonCode;
pub use subscribe::{RetainHandling, SubAck, Subscribe, Subscription};
pub use types::{
    MQTT_VERSION_3, MQTT_VERSION_3_1, MQTT_VERSION_3_1_1, MQTT_VERSION_5, MQTT_VERSION_DEFAULT,
    MqttVersion, QOS_0, QOS_1, QOS_2, QoS,
};
pub use unsubscribe::{UnsubAck, Unsubscribe};
