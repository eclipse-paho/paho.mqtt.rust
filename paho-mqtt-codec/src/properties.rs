// paho-mqtt-codec/src/properties.rs
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

//! MQTT v5 properties.
//!
//! This module currently holds a stub implementation that stores property
//! bytes verbatim. Full `(PropertyId, PropertyValue)` parsing is deferred
//! to a later implementation step.

use crate::{
    error::{Error, Result},
    varlen,
};
use bytes::{Buf, BufMut, Bytes};

/// MQTT v5 properties.
///
/// Properties are a length-prefixed list of `(id, value)` pairs carried by
/// MQTT v5 packets. This implementation stores the raw wire bytes so that
/// nothing is lost during decode; full property-by-property parsing will be
/// added in a later step.
///
/// On v3.1.1 connections properties are always empty and are not encoded
/// on the wire.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Properties(Bytes);

impl Properties {
    /// Creates an empty `Properties` set.
    pub fn new() -> Self {
        Properties(Bytes::new())
    }

    /// Returns `true` if there are no properties.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of bytes this encodes to on the wire, including
    /// the variable-byte-integer length prefix.
    pub fn encoded_size(&self) -> usize {
        let data_len = self.0.len();
        varlen::encoded_size(data_len as u32) + data_len
    }

    /// Encodes the properties into `dst`: VBI length prefix followed by
    /// the raw property bytes.
    pub fn encode<B: BufMut>(&self, dst: &mut B) -> Result<()> {
        varlen::encode(self.0.len() as u32, dst)?;
        dst.put_slice(&self.0);
        Ok(())
    }

    /// Decodes properties from `src`: reads the VBI length prefix, then
    /// captures exactly that many bytes as the raw property payload.
    pub fn decode<B: Buf>(src: &mut B) -> Result<Self> {
        let len = match varlen::decode(src)? {
            Some(n) => n as usize,
            None => return Err(Error::PacketTooShort),
        };
        if src.remaining() < len {
            return Err(Error::PacketTooShort);
        }
        Ok(Properties(src.copy_to_bytes(len)))
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};

    #[test]
    fn test_empty_properties() {
        let props = Properties::new();
        assert!(props.is_empty());
        assert_eq!(props.encoded_size(), 1); // just the 0x00 VBI
    }

    #[test]
    fn test_encode_empty() {
        let mut dst = BytesMut::new();
        Properties::new().encode(&mut dst).unwrap();
        assert_eq!(&dst[..], &[0x00]);
    }

    #[test]
    fn test_decode_empty() {
        let mut src = Bytes::from_static(&[0x00]);
        let props = Properties::decode(&mut src).unwrap();
        assert!(props.is_empty());
        assert!(src.is_empty());
    }

    #[test]
    fn test_round_trip_non_empty() {
        // Pretend property bytes: id=0x11 (session_expiry_interval), value=0x00 0x00 0x00 0x0A
        let raw: &[u8] = &[0x11, 0x00, 0x00, 0x00, 0x0A];
        let props = Properties(Bytes::copy_from_slice(raw));
        assert!(!props.is_empty());
        assert_eq!(props.encoded_size(), 1 + raw.len()); // 1-byte VBI prefix

        let mut dst = BytesMut::new();
        props.encode(&mut dst).unwrap();
        assert_eq!(dst[0], raw.len() as u8);
        assert_eq!(&dst[1..], raw);

        let decoded = Properties::decode(&mut dst.freeze()).unwrap();
        assert_eq!(decoded, props);
    }

    #[test]
    fn test_decode_too_short_returns_err() {
        // length prefix says 5 bytes but only 2 follow
        let mut src = Bytes::from_static(&[0x05, 0x01, 0x02]);
        assert_eq!(Properties::decode(&mut src), Err(Error::PacketTooShort));
    }
}
