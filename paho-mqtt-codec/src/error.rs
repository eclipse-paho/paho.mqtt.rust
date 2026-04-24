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
    /// A variable-byte integer had a continuation bit set on the fourth byte.
    #[error("malformed variable-length integer")]
    MalformedVarLen,
    /// An unrecognized MQTT packet type nibble was encountered.
    #[error("unknown packet type: {0:#x}")]
    UnknownPacketType(u8),
    /// The packet payload was shorter than expected.
    #[error("packet payload too short")]
    PacketTooShort,
    /// A packet field contained an invalid or reserved value.
    #[error("malformed packet")]
    MalformedPacket,
}

/// The result type for codec operations.
pub type Result<T> = core::result::Result<T, Error>;
