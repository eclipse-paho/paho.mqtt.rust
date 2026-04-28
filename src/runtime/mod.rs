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
//! This module contains the `Connection` trait that abstracts over the
//! async transport layer, runtime-specific implementations, and the
//! command/event types that bridge the public API and the event loop.
//!
//! The `tokio` feature (default) enables the tokio-based runtime.
//! The `smol` feature will enable an alternative smol-based runtime.
//! The `commands` module is shared between both backends.

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) mod commands;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) mod connection;

#[cfg(feature = "tokio")]
pub(crate) mod tokio_rt;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) mod smol_rt;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub(crate) use commands::{BufConfig, Command, ConnectParams, Event, ServerAddr};

#[cfg(feature = "tokio")]
pub(crate) use tokio_rt::{RuntimeHandle, RuntimeOwnership, resolve_runtime};

#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) use smol_rt::{RuntimeHandle, SmolOwnership, get_or_create_shared_executor};

/// Unified runtime-ownership type for the active backend.
#[cfg(feature = "tokio")]
pub(crate) type RtOwnership = tokio_rt::RuntimeOwnership;

/// Unified runtime-ownership type for the active backend.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) type RtOwnership = smol_rt::SmolOwnership;
