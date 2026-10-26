// paho-mqtt/src/runtime/backend.rs
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
//! [`ServerAddr`], read into a `BytesMut`, write, flush, and sleep.
//! Every future a backend returns must be `Send` so the event loop can run
//! on a multi-threaded executor.

use std::{
    io::{self, IoSlice},
    time::Duration,
};

use bytes::BytesMut;
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

/// How many bytes a read is offered when the buffer is nearly full.
///
/// Small packets arrive in bursts — a flood of QoS 0 publishes is about
/// seventy to a 4 KiB read — so the read size decides how many syscalls a
/// burst costs. 16 KiB is four times fewer than the 4 KiB the buffer used
/// to settle at, and still small enough that the chunk a delivered message
/// pins is a fraction of what a slow consumer may hold.
pub(crate) const READ_CHUNK: usize = 16 * 1024;

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
    ///
    /// An implementation should be prepared to take up to [`READ_CHUNK`]
    /// bytes in one call. A read capped well below that costs a syscall for
    /// every few packets of a burst; one much larger buys little, because a
    /// payload is a slice of the buffer it arrived in (the codec's decode is
    /// zero-copy), so a message waiting for a slow consumer keeps its whole
    /// chunk alive.
    fn read_buf(
        reader: &mut Self::Reader,
        buf: &mut BytesMut,
    ) -> impl Future<Output = io::Result<usize>> + Send;

    /// Writes all of `data`.
    ///
    /// Used only for the CONNECT exchange, before the session's run loop
    /// takes over the writer.
    fn write_all(
        writer: &mut Self::Writer,
        data: &[u8],
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Makes one attempt to write `bufs`, in order, and returns the number
    /// of bytes the transport accepted, which may be fewer than offered.
    ///
    /// Must be cancel-safe: if the future is dropped before completion,
    /// nothing was written. The run loop polls it as one branch of a
    /// `select!` and drops it whenever another branch wins.
    fn write_vectored(
        writer: &mut Self::Writer,
        bufs: &[IoSlice<'_>],
    ) -> impl Future<Output = io::Result<usize>> + Send;

    /// Flushes any output the transport has buffered internally, such as a
    /// TLS record or a WebSocket frame.
    ///
    /// Must be cancel-safe: a flush dropped before completion may simply be
    /// started again.
    fn flush(writer: &mut Self::Writer) -> impl Future<Output = io::Result<()>> + Send;
}
