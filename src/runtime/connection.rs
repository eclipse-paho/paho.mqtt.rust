// paho-mqtt/src/runtime/connection.rs
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

//! Shared transport abstraction for the internal async runtime.
//!
//! The [`Connection`] trait is a marker that bundles the async I/O super-traits
//! into a single bound.  Each backend defines it using the I/O traits native to
//! that runtime:
//!
//! | Backend | AsyncRead / AsyncWrite source |
//! |---------|-------------------------------|
//! | `tokio` | `tokio::io`                   |
//! | `smol`  | `futures::io`                 |
//!
//! Concrete connection types (`TcpConnection`, `TlsConnection`, etc.) live in
//! the backend module (`tokio_rt`, `smol_rt`) and implement this trait.

/// A bidirectional async transport used by the internal event loop.
///
/// This is a marker trait that collects the async I/O bounds required by the
/// event loop into a single name.  Concrete implementations are provided by
/// each backend module.
#[cfg(feature = "tokio")]
pub(crate) trait Connection:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static
{
}

/// A bidirectional async transport used by the internal event loop.
///
/// This is a marker trait that collects the async I/O bounds required by the
/// event loop into a single name.  Concrete implementations are provided by
/// each backend module.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) trait Connection:
    futures::io::AsyncRead + futures::io::AsyncWrite + Send + Unpin + 'static
{
}
