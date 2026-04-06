// lib.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2022 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! This is the Eclipse Paho MQTT client library for the Rust language.
//!
//! # Crate Features
//!
//! ### Default
//!
//! * **bundled**
//!   Builds the Paho C library for the target and automatically links it.
//!
//! * **ssl**
//!   Builds the library with OpenSSL support. This allow secure SSL/TLS
//!   connections to a broker along with secure websockets. This requires
//!   OpenSSL to be installed for the target or using the `vendored-ssl`
//!   feature to build it.
//!
//! ### Non-default
//!
//! * **build_bindgen**
//!   Build the bindings to the Paho C library for the target. The library
//!   ships with a number of pre-built bindings for common targets. For
//!   other targets it might be helpful to build the bindings for the
//!   specific target platform, especially if the library segfauls due to
//!   an improper API.
//!
//! * **vendored-ssl**
//!   Download an build OpenSSL for the target.
//!
//! * **tokio**
//!   Just enables some additional tokio examples. The library ie fully
//!   compatible with tokio whether or not this feature is enabled.
//!
//! # Logging
//!
//! The library uses the standard Rust log facility with the target/module
//! "paho_mqtt".
//!
//! In addition, the traces from the underlying Paho C library are captured
//! and redirected to the Rust log facility. Since the C library is a bit
//! cryptic and verbose, the output is controlled separately using a
//! "paho_mqtt_c" target/module. The levels for the C library are
//! approximated to suit the Rust filter level selected for it.
//!
//! The example applications use the simple
//! [environment logger](https://crates.io/crates/env_logger) which can be
//! controlled by the `RUST_LOG` environment variable. To set the logging
//! for the two modules, you can do something like:
//!
//! ```text
//! $ RUST_LOG="pago_mqtt=info,paho_mqtt_c=debug" ./target/debug/examples/async_subscribe
//! Connecting to the MQTT server at 'mqtt://localhost:1883'...
//! [2023-09-11T13:26:18Z DEBUG paho_mqtt::c_trace] =========================================================
//! [2023-09-11T13:26:18Z DEBUG paho_mqtt::c_trace]                    Trace Output
//! [2023-09-11T13:26:18Z DEBUG paho_mqtt::c_trace] Product name: Eclipse Paho Asynchronous MQTT C Client Library
//! [2023-09-11T13:26:18Z DEBUG paho_mqtt::c_trace] Version: 1.3.11
//! ...
//! ```

#![deny(missing_docs)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::unnecessary_cast)]

#[macro_use]
extern crate log;
extern crate paho_mqtt_sys as ffi;

/// Re-export async_channel
pub use async_channel::{self, Receiver as AsyncReceiver};

/// Re-export crossbeam channel as sync components
pub use crossbeam_channel::{self as sync_channel, Receiver as SyncReceiver};

/// The asynchronous API
pub mod async_client;
pub use crate::async_client::{
    AsyncClient, ConnectedCallback, ConnectionLostCallback, DisconnectedCallback,
    MessageArrivedCallback, SslErrorCallback,
};

/// The synchronous API
pub mod client;
pub use crate::client::Client;

pub use crate::connect_options::{ConnectOptions, ConnectOptionsBuilder};
pub use crate::create_options::{CreateOptions, CreateOptionsBuilder};
pub use crate::disconnect_options::{DisconnectOptions, DisconnectOptionsBuilder};
pub use crate::errors::{
    ConnectReturnCode, Error, Result, error_message, error_message_opt,
};
pub use crate::event::Event;
pub use crate::message::{InMessage, Message, MessageBuilder, OutMessage};
pub use crate::persistence::{
    ClientPersistence, ClientPersistenceBox, PERSISTENCE_ERROR, PERSISTENCE_SUCCESS,
    PersistenceType, UserPersistence,
};
pub use crate::properties::{
    Binary, Properties, PropertiesFfi, Property, PropertyCode, PropertyFfi,
    PropertyIterator, PropertyType, StringPairIterator, Value, VbiU32,
};
pub use crate::reason_code::ReasonCode;
pub use crate::server_response::{
    ConnectResponse, RequestResponse, ServerRequest, ServerResponse,
};
pub use crate::ssl_options::{SslOptions, SslOptionsBuilder, SslVersion, path_to_cstring};
pub use crate::subscribe_options::{
    NO_RETAIN_AS_PUBLISHED, RETAIN_AS_PUBLISHED, RetainHandling, SUBSCRIBE_LOCAL,
    SUBSCRIBE_NO_LOCAL, SubscribeOptions, SubscribeOptionsBuilder,
};
pub use crate::token::{
    ConnectToken, DeliveryToken, FailureCallback, SubscribeManyToken, SubscribeToken,
    SuccessCallback, Token, UnsubscribeManyToken, UnsubscribeToken,
};
pub use crate::topic::{SyncTopic, Topic, TopicFilter, is_topic_filter_valid};
pub use crate::topic_matcher::TopicMatcher;
pub use crate::types::{
    MQTT_VERSION_3_1, MQTT_VERSION_3_1_1, MQTT_VERSION_5, MQTT_VERSION_DEFAULT,
    MqttVersion, QOS_0, QOS_1, QOS_2, QoS,
};

use std::{any::Any, os::raw::c_int};

mod macros;

/// Client creation options
pub mod create_options;

/// Options for connecting to the server.
pub mod connect_options;

/// Connect options for creating secure connections to the server.
pub mod ssl_options;

/// Options for disconnecting from the server.
pub mod disconnect_options;

/// Options for subscribing to topics
pub mod subscribe_options;

/// Responses coming back from the server.
pub mod server_response;

/// MQTT 5v properties.
pub mod properties;

/// The message object
pub mod message;

/// Tokens to monitor asynchronous operations
pub mod token;

/// Options for creating topic objects that are associated with a
/// particular server.
pub mod topic;

// Topic matcher struct
pub mod topic_matcher;

/// MQTT v5 Reason Codes
pub mod reason_code;

/// Miscellaneous types
pub mod types;

/// Message persistence for the client.
pub mod persistence;

/// The MQTT errors
pub mod errors;

/// Events emitted by the client
pub mod event;

/// Internal FFI utility types for bridging Rust data to the Paho C library.
mod ffi_utils;

// C log tracing pass-through
use ffi_utils::{c_trace_level, on_c_trace};

// --------------------------------------------------------------------------

/// Generic type for arbitrary user-supplied data.
///
/// The application can use a type compatible with this to store in the
/// client as "user data" to be accessed from callbacks, etc.
pub type UserData = Box<dyn Any + 'static + Send + Sync>;

// --------------------------------------------------------------------------

/// Determines if the URI us for a secure connection.
pub fn is_secure_uri(uri: &str) -> bool {
    for sch in &["ssl://", "tls://", "mqtts://", "wss://"] {
        if uri.starts_with(sch) {
            return true;
        }
    }
    false
}

/// Convert a Rust bool to a Paho C integer boolean
#[inline]
pub fn to_c_bool(on: bool) -> c_int {
    c_int::from(on)
}

/// Converts a C integer boolean to a Rust bool
#[inline]
pub fn from_c_bool(on: c_int) -> bool {
    on != 0
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_c_bool() {
        // C lib is sometimes picky about using 1, specifically, for true.
        assert_eq!(1, to_c_bool(true));
        assert_eq!(0, to_c_bool(false));
    }

    #[test]
    fn test_from_c_bool() {
        assert!(from_c_bool(1));
        assert!(from_c_bool(42));
        assert!(!from_c_bool(0));
    }
}
