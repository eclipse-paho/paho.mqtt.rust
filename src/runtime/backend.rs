// paho-mqtt/src/runtime/tokio_rt.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2024-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! The interface an async runtime must provide to drive the MQTT session.
//!
//! The protocol logic in [`super::session`] is written once, generic over a
//! [`Backend`].  A backend is a zero-sized marker type that names the
//! runtime's split transport halves and timer, and supplies the handful of
//! I/O operations the session performs: open a transport for a
//! [`ServerAddr`], read into a `BytesMut`, write a frame, flush, and sleep.
//! Every future a backend returns must be `Send` so the event loop can run
//! on a multi-threaded executor.

use std::{io, time::Duration};

use bytes::{Bytes, BytesMut};
use futures::future::FusedFuture;

use super::commands::{ConnectParams, ServerAddr};
use crate::Result;

/// A resettable one-shot timer.
///
/// The session keeps one of these armed for the keep-alive deadline and
/// re-arms it every time a packet goes out, so the timer must survive being
/// reset both before and after it has fired.  It is polled inside
/// `futures::select!`, hence the [`FusedFuture`] and `Unpin` bounds: a timer
/// created with [`never`](Self::never) reports itself terminated so the
/// select skips its branch altogether.
pub(crate) trait Sleep: Future<Output = ()> + FusedFuture + Send + Unpin {
    /// Creates a timer that fires once, `dur` from now.
    fn new(dur: Duration) -> Self;

    /// Creates a timer that never fires and is already terminated.
    fn never() -> Self;

    /// Re-arms the timer to fire `dur` from now, whether or not it has
    /// already fired.
    fn reset(&mut self, dur: Duration);
}

/// The runtime-specific half of the MQTT event loop.
pub(crate) trait Backend: 'static {
    /// Read half of the split transport.
    type Reader: Send + Unpin + 'static;
    /// Write half of the split transport.
    type Writer: Send + Unpin + 'static;
    /// The runtime's timer.
    type Sleep: Sleep;

    /// Opens the transport described by `addr` and returns its split halves.
    ///
    /// This covers the TCP connect and any TLS handshake or WebSocket upgrade,
    /// but not the MQTT CONNECT exchange, which the session performs itself.
    fn open(
        addr: &ServerAddr,
        params: &ConnectParams,
    ) -> impl Future<Output = Result<(Self::Reader, Self::Writer)>> + Send;

    /// Reads whatever is available into `buf`, growing it as needed, and
    /// returns the number of bytes appended (`0` on EOF).
    ///
    /// Must be cancel-safe: if the future is dropped before completion, `buf`
    /// must be unchanged.  The session drops it on every `select!` that is
    /// won by another branch.
    fn read_buf(
        reader: &mut Self::Reader,
        buf: &mut BytesMut,
    ) -> impl Future<Output = io::Result<usize>> + Send;

    /// Writes all of `data`.
    fn write_all(
        writer: &mut Self::Writer,
        data: &[u8],
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Writes `header` followed by `payload` as one frame, without copying
    /// the payload into the header buffer (vectored where the runtime allows).
    fn write_frame(
        writer: &mut Self::Writer,
        header: &[u8],
        payload: Bytes,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Flushes any buffered output to the transport.
    fn flush(writer: &mut Self::Writer) -> impl Future<Output = io::Result<()>> + Send;
}
