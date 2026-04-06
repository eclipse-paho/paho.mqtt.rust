// ffi_utils/mod.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// FFI utility types used to bridge Rust data structures to the Paho C library.
//

/*******************************************************************************
 * Copyright (c) 2017-2025 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Internal FFI utility types for bridging Rust data to the Paho C library.

pub(crate) mod c_trace;
pub(crate) mod call_options_ffi;
pub(crate) mod connect_options_ffi;
pub(crate) mod disconnect_options_ffi;
pub(crate) mod message_ffi;
pub(crate) mod name_value_ffi;
pub(crate) mod ssl_options_ffi;
pub(crate) mod string_collection_ffi;
pub(crate) mod subscribe_options_ffi;

pub(crate) use c_trace::{c_trace_level, on_c_trace};
pub(crate) use call_options_ffi::{CallOptionsFfi, CallOptionsFfiBuilder};
pub(crate) use connect_options_ffi::ConnectOptionsFfi;
pub(crate) use disconnect_options_ffi::DisconnectOptionsFfi;
pub(crate) use message_ffi::{MessageFfi, MessageFfiData};
pub(crate) use name_value_ffi::NameValueCollectionFfi;
pub(crate) use ssl_options_ffi::SslOptionsFfi;
pub(crate) use string_collection_ffi::StringCollectionFfi;
pub(crate) use subscribe_options_ffi::SubscribeOptionsFfi;
