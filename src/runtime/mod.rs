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
//! Currently only the `tokio` runtime is supported; `smol` support is
//! planned (Step 10 of the v2 plan).

#[cfg(feature = "tokio")]
pub(crate) mod commands;

#[cfg(feature = "tokio")]
pub(crate) mod tokio_rt;

#[cfg(feature = "tokio")]
pub(crate) use commands::{Command, Event};

#[cfg(feature = "tokio")]
pub(crate) use tokio_rt::RuntimeHandle;
