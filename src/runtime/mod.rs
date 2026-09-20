// paho-mqtt/src/runtime/mod.rs
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

//! Internal async runtime and transport abstractions.
//!
//! The MQTT protocol logic lives once, in `session`, generic over the
//! `Backend` trait defined in `backend`.  A backend module (`tokio_rt` or
//! `smol_rt`) implements that trait for its runtime — opening transports,
//! reading, writing, and timing — and supplies the transport types
//! (`tokio_conn` / `smol_conn`) behind the `Connection` marker trait.  The
//! `commands` module holds the command/event types that bridge the public
//! API and the event loop.
//!
//! The `tokio` feature (default) selects the tokio backend; the `smol`
//! feature selects the smol backend.  When both are enabled, tokio wins.

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) mod commands;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) mod connection;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) mod backend;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) mod session;

#[cfg(any(feature = "tls", feature = "smol-tls", feature = "openssl"))]
pub(crate) mod tls_config;

#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
pub(crate) mod ws_util;

#[cfg(feature = "tokio")]
pub(crate) mod tokio_conn;

#[cfg(feature = "tokio")]
pub(crate) mod tokio_rt;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) mod smol_conn;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) mod smol_rt;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) use commands::{BufConfig, Command, ConnectParams, Event, ServerAddr};

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) use session::RuntimeHandle;

#[cfg(feature = "tokio")]
pub(crate) use tokio_rt::resolve_runtime;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) use smol_rt::{SmolOwnership, get_or_create_shared_executor};

/// Unified runtime-ownership type for the active backend.
#[cfg(feature = "tokio")]
pub(crate) type RtOwnership = tokio_rt::RuntimeOwnership;

/// Unified runtime-ownership type for the active backend.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) type RtOwnership = smol_rt::SmolOwnership;
