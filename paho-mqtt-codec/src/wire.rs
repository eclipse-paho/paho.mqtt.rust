// paho-mqtt-codec/src/wire.rs
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

//! Low-level wire-format primitives shared across packet modules.
//!
//! MQTT uses a handful of recurring encodings — length-prefixed UTF-8 strings,
//! length-prefixed binary data — that every packet module needs. Centralising
//! them here avoids duplication and keeps the per-packet modules focused on
//! their own structure.

use crate::{Error, Result, String};
use bytes::{Buf, BufMut, Bytes};

/// Writes a 2-byte big-endian length prefix followed by the UTF-8 string
/// bytes of `s`.
///
/// Returns [`Error::StringTooLong`] if `s` exceeds 65,535 bytes, or
/// [`Error::MalformedPacket`] if it contains U+0000, which MQTT forbids in
/// any string field (MQTT-1.5.4-2).
pub(crate) fn put_utf8<B: BufMut>(s: &str, dst: &mut B) -> Result<()> {
    let len = u16::try_from(s.len()).map_err(|_| Error::StringTooLong)?;
    if s.as_bytes().contains(&0) {
        return Err(Error::MalformedPacket);
    }
    dst.put_u16(len);
    dst.put_slice(s.as_bytes());
    Ok(())
}

/// Writes a 2-byte big-endian length prefix followed by the raw bytes of `b`.
///
/// Returns [`Error::BinaryTooLong`] if `b` exceeds 65,535 bytes.
pub(crate) fn put_binary<B: BufMut>(b: &[u8], dst: &mut B) -> Result<()> {
    let len = u16::try_from(b.len()).map_err(|_| Error::BinaryTooLong)?;
    dst.put_u16(len);
    dst.put_slice(b);
    Ok(())
}

/// Reads a 2-byte big-endian length prefix then that many UTF-8 bytes from `src`.
///
/// Returns [`Error::MalformedPacket`] if the bytes are not valid UTF-8 or
/// contain U+0000 (MQTT-1.5.4-2).
pub(crate) fn get_utf8<B: Buf>(src: &mut B) -> Result<String> {
    if src.remaining() < 2 {
        return Err(Error::PacketTooShort);
    }
    let len = src.get_u16() as usize;
    if src.remaining() < len {
        return Err(Error::PacketTooShort);
    }
    let bytes = src.copy_to_bytes(len);
    if bytes.contains(&0) {
        return Err(Error::MalformedPacket);
    }
    String::from_utf8(bytes.into()).map_err(|_| Error::MalformedPacket)
}

/// Reads a 2-byte big-endian length prefix then that many raw bytes from `src`.
pub(crate) fn get_binary<B: Buf>(src: &mut B) -> Result<Bytes> {
    if src.remaining() < 2 {
        return Err(Error::PacketTooShort);
    }
    let len = src.get_u16() as usize;
    if src.remaining() < len {
        return Err(Error::PacketTooShort);
    }
    Ok(src.copy_to_bytes(len))
}

/// Checks that a decoder consumed its whole input.
///
/// The remaining length names the packet's exact extent (§2.1.4), so bytes
/// left over once every field has been read mean the packet does not have the
/// structure its type requires — a PINGREQ with a body, a v3 CONNACK longer
/// than two bytes, a CONNECT with data after the password. Returns
/// [`Error::MalformedPacket`] if anything remains.
pub(crate) fn expect_end<B: Buf>(src: &B) -> Result<()> {
    if src.has_remaining() {
        return Err(Error::MalformedPacket);
    }
    Ok(())
}

/// Checks a packet identifier read from, or about to be written to, the wire.
///
/// Zero is never a valid identifier (MQTT-2.2.1-3). Returns
/// [`Error::ProtocolError`] for it.
pub(crate) fn check_packet_id(id: u16) -> Result<u16> {
    if id == 0 {
        return Err(Error::ProtocolError);
    }
    Ok(id)
}

/// Wire size of a length-prefixed UTF-8 string: 2-byte prefix + content bytes.
pub(crate) fn utf8_wire_size(s: &str) -> usize {
    2 + s.len()
}

/// Wire size of a length-prefixed binary blob: 2-byte prefix + data bytes.
pub(crate) fn binary_wire_size(b: &[u8]) -> usize {
    2 + b.len()
}
