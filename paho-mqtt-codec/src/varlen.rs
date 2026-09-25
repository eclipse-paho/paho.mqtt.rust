// paho-mqtt-codec/src/varlen.rs
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

//! MQTT variable-byte integer (VBI) encoding and decoding.
//!
//! The MQTT protocol encodes the remaining length of each packet, and the
//! total length of the Properties section in v5 packets, as a variable-byte
//! integer. Each byte contributes 7 bits of value; the high bit is a
//! continuation flag indicating that more bytes follow, with a maximum of
//! four encoded bytes, giving a range of 2^28 - 1 = 268,435,455.
//!
//! | Value range             |Bits| Encoded bytes |
//! |-------------------------|----|---------------|
//! | 0 – 127                 |  7 |      1        |
//! | 128 – 16,383            | 14 |      2        |
//! | 16,384 – 2,097,151      | 21 |      3        |
//! | 2,097,152 – 268,435,455 | 28 |      4        |
//!
//! Values above 268,435,455 are not valid in MQTT.

use crate::error::{Error, Result};
use bytes::{Buf, BufMut};

/// Maximum value that can be encoded as a variable-byte integer.
pub const MAX_VARLEN: u32 = 268_435_455;

/// Returns the number of bytes required to encode `value` as a
/// variable-byte integer.
///
/// Panics in debug builds if `value` exceeds [`MAX_VARLEN`].
pub fn encoded_size(value: u32) -> usize {
    debug_assert!(value <= MAX_VARLEN, "varlen value out of range");
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

/// Encodes `value` as a variable-byte integer into `dst`.
///
/// Returns an error if `value` exceeds [`MAX_VARLEN`].
pub fn encode<B: BufMut>(value: u32, dst: &mut B) -> Result<()> {
    if value > MAX_VARLEN {
        return Err(Error::VarLenOverflow);
    }
    let mut val = value;
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val > 0 {
            byte |= 0x80;
        }
        dst.put_u8(byte);
        if val == 0 {
            break;
        }
    }
    Ok(())
}

/// Decodes a variable-byte integer from `src`.
///
/// Returns `Ok(None)` if `src` contains a partial encoding (continuation
/// bit set on the last available byte). Note that the bytes examined are
/// consumed from `src` even in that case; callers that need to retry later
/// should decode from a copy or a peek.
///
/// Returns [`Error::MalformedVarLen`] if a fourth byte has its continuation
/// bit set, or if the encoding is not the shortest possible one — MQTT
/// requires the minimum number of bytes (MQTT-1.5.5-1), so a trailing
/// zero byte such as `0x80 0x00` is rejected rather than read as `0`.
pub fn decode<B: Buf>(src: &mut B) -> Result<Option<u32>> {
    let mut value: u32 = 0;
    let mut shift: u32 = 0;

    loop {
        if !src.has_remaining() {
            return Ok(None);
        }
        let byte = src.get_u8();
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            // A final byte of zero after a continuation is a non-minimal
            // encoding (e.g. 0x80 0x00 for 0, or 0xAC 0x00 for 44).
            if shift > 0 && byte == 0 {
                return Err(Error::MalformedVarLen);
            }
            return Ok(Some(value));
        }
        shift += 7;
        if shift > 21 {
            return Err(Error::MalformedVarLen);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};

    // Encode value, return the resulting bytes.
    fn enc(value: u32) -> Bytes {
        let mut buf = BytesMut::new();
        encode(value, &mut buf).unwrap();
        buf.freeze()
    }

    // Decode from a byte slice; panics if result is not Ok(Some(_)).
    fn dec(bytes: &[u8]) -> u32 {
        decode(&mut Bytes::copy_from_slice(bytes)).unwrap().unwrap()
    }

    // ---- encoded_size ----

    #[test]
    fn test_encoded_size() {
        assert_eq!(encoded_size(0), 1);
        assert_eq!(encoded_size(127), 1);
        assert_eq!(encoded_size(128), 2);
        assert_eq!(encoded_size(16_383), 2);
        assert_eq!(encoded_size(16_384), 3);
        assert_eq!(encoded_size(2_097_151), 3);
        assert_eq!(encoded_size(2_097_152), 4);
        assert_eq!(encoded_size(MAX_VARLEN), 4);
    }

    // ---- encode ----

    #[test]
    fn test_encode_single_byte() {
        assert_eq!(&enc(0)[..], &[0x00]);
        assert_eq!(&enc(1)[..], &[0x01]);
        assert_eq!(&enc(127)[..], &[0x7F]);
    }

    #[test]
    fn test_encode_two_bytes() {
        assert_eq!(&enc(128)[..], &[0x80, 0x01]);
        assert_eq!(&enc(16_383)[..], &[0xFF, 0x7F]);
    }

    #[test]
    fn test_encode_three_bytes() {
        assert_eq!(&enc(16_384)[..], &[0x80, 0x80, 0x01]);
        assert_eq!(&enc(2_097_151)[..], &[0xFF, 0xFF, 0x7F]);
    }

    #[test]
    fn test_encode_four_bytes() {
        assert_eq!(&enc(2_097_152)[..], &[0x80, 0x80, 0x80, 0x01]);
        assert_eq!(&enc(MAX_VARLEN)[..], &[0xFF, 0xFF, 0xFF, 0x7F]);
    }

    #[test]
    fn test_encode_overflow() {
        let mut buf = BytesMut::new();
        assert_eq!(encode(MAX_VARLEN + 1, &mut buf), Err(Error::VarLenOverflow));
        assert!(buf.is_empty());
    }

    // ---- decode ----

    #[test]
    fn test_decode_single_byte() {
        assert_eq!(dec(&[0x00]), 0);
        assert_eq!(dec(&[0x01]), 1);
        assert_eq!(dec(&[0x7F]), 127);
    }

    #[test]
    fn test_decode_two_bytes() {
        assert_eq!(dec(&[0x80, 0x01]), 128);
        assert_eq!(dec(&[0xFF, 0x7F]), 16_383);
    }

    #[test]
    fn test_decode_three_bytes() {
        assert_eq!(dec(&[0x80, 0x80, 0x01]), 16_384);
        assert_eq!(dec(&[0xFF, 0xFF, 0x7F]), 2_097_151);
    }

    #[test]
    fn test_decode_four_bytes() {
        assert_eq!(dec(&[0x80, 0x80, 0x80, 0x01]), 2_097_152);
        assert_eq!(dec(&[0xFF, 0xFF, 0xFF, 0x7F]), MAX_VARLEN);
    }

    #[test]
    fn test_decode_partial_returns_none() {
        // Continuation bit set but no more bytes available.
        let result = decode(&mut Bytes::from_static(&[0x80]));
        assert_eq!(result, Ok(None));

        let result = decode(&mut Bytes::from_static(&[0x80, 0x80]));
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_decode_empty_returns_none() {
        let result = decode(&mut Bytes::new());
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_decode_malformed_five_continuation_bytes() {
        // Fifth byte still has continuation bit — malformed.
        let result = decode(&mut Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x01]));
        assert_eq!(result, Err(Error::MalformedVarLen));
    }

    #[test]
    fn test_decode_non_minimal_encoding_is_malformed() {
        // MQTT-1.5.5-1: an encoding longer than necessary is malformed.
        for bytes in [
            &[0x80u8, 0x00][..],       // 0 in two bytes
            &[0xAC, 0x00][..],         // 44 in two bytes (found by fuzzing)
            &[0xFF, 0x80, 0x00][..],   // 127 in three bytes
            &[0x80, 0x80, 0x80, 0x00], // 0 in four bytes
        ] {
            assert_eq!(
                decode(&mut Bytes::copy_from_slice(bytes)),
                Err(Error::MalformedVarLen),
                "{bytes:02x?}"
            );
        }
        // Values whose top byte is legitimately small are still fine.
        assert_eq!(
            decode(&mut Bytes::from_static(&[0x80, 0x01])),
            Ok(Some(128))
        );
        assert_eq!(decode(&mut Bytes::from_static(&[0x00])), Ok(Some(0)));
    }

    #[test]
    fn test_every_encoding_is_minimal_and_round_trips() {
        for value in [
            0u32, 1, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152, MAX_VARLEN,
        ] {
            let mut buf = BytesMut::new();
            encode(value, &mut buf).unwrap();
            assert_eq!(buf.len(), encoded_size(value));
            assert_eq!(decode(&mut buf.freeze()), Ok(Some(value)));
        }
    }

    // ---- round-trip ----

    #[test]
    fn test_round_trip() {
        for value in [
            0, 1, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152, MAX_VARLEN,
        ] {
            let encoded = enc(value);
            assert_eq!(
                encoded.len(),
                encoded_size(value),
                "encoded_size mismatch for {value}"
            );
            let decoded = dec(&encoded);
            assert_eq!(decoded, value, "round-trip mismatch for {value}");
        }
    }

    #[test]
    fn test_decode_ignores_trailing_bytes() {
        // Decoder should stop after a complete value and leave the rest.
        let mut buf = Bytes::from_static(&[0x05, 0xFF]);
        assert_eq!(decode(&mut buf).unwrap(), Some(5));
        assert_eq!(buf.remaining(), 1); // 0xFF still in buffer
    }
}
