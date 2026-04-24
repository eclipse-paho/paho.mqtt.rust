// paho-mqtt-codec/src/wire.rs
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

//! Low-level wire-format primitives shared across packet modules.
//!
//! MQTT uses a handful of recurring encodings — length-prefixed UTF-8 strings,
//! length-prefixed binary data — that every packet module needs. Centralising
//! them here avoids duplication and keeps the per-packet modules focused on
//! their own structure.

use crate::{Error, Result, String};
use bytes::{Buf, BufMut, Bytes};

/// Writes a 2-byte big-endian length prefix followed by the UTF-8 bytes of `s`.
pub(crate) fn put_utf8<B: BufMut>(s: &str, dst: &mut B) {
    dst.put_u16(s.len() as u16);
    dst.put_slice(s.as_bytes());
}

/// Reads a 2-byte big-endian length prefix then that many UTF-8 bytes from `src`.
pub(crate) fn get_utf8<B: Buf>(src: &mut B) -> Result<String> {
    if src.remaining() < 2 {
        return Err(Error::PacketTooShort);
    }
    let len = src.get_u16() as usize;
    if src.remaining() < len {
        return Err(Error::PacketTooShort);
    }
    let bytes = src.copy_to_bytes(len);
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

/// Wire size of a length-prefixed UTF-8 string: 2-byte prefix + content bytes.
pub(crate) fn utf8_wire_size(s: &str) -> usize {
    2 + s.len()
}

/// Wire size of a length-prefixed binary blob: 2-byte prefix + data bytes.
pub(crate) fn binary_wire_size(b: &[u8]) -> usize {
    2 + b.len()
}
