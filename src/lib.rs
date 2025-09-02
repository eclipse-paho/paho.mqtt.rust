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
pub use async_channel::Receiver as AsyncReceiver;

/// Re-export crossbeam channel as sync components
pub use crossbeam_channel::{self as sync_channel, Receiver};

/// The asynchronous API
pub mod async_client;
pub use crate::async_client::{
    AsyncClient,
    //AsyncClientBuilder,
    ConnectedCallback,
    ConnectionLostCallback,
    DisconnectedCallback,
    MessageArrivedCallback,
    //SslErrorCallback,
};

/// The synchronous API
pub mod client;
pub use crate::client::Client; //ClientBuilder};

pub use crate::client_persistence::*;
pub use crate::connect_options::*; //{ConnectOptions, ConnectOptionsBuilder, MQTT_VERSION_3_1_1, ...};
pub use crate::create_options::*; //{CreateOptions, CreateOptionsBuilder};
pub use crate::disconnect_options::*; //{DisconnectOptions, DisconnectOptionsBuilder};
pub use crate::errors::*;
pub use crate::message::*; //{Message, MessageBuilder};
pub use crate::name_value::*; //{NameValueCollection};
pub use crate::properties::*; //{Property, Properties};
pub use crate::reason_code::*; //{ReasonCode}
pub use crate::response_options::*; //{ResponseOptions};
pub use crate::server_response::*; //{ServerResponse, CommandResponse};
pub use crate::ssl_options::*; //{SslOptions, SslOptionsBuilder};
pub use crate::subscribe_options::*; //{SubscribeOptions};
pub use crate::token::*; //{Token}
pub use crate::topic::*; //{Topic, TopicFilter, TopicMatcher};
pub use crate::topic_matcher::TopicMatcher;
pub use crate::types::*; //...
pub use crate::will_options::*; //{WillOptions, WillOptionsBuilder}; //{Result, Error, ErrorKind};

use std::{any::Any, os::raw::c_int};

mod macros;

/// Client creation options
pub mod create_options;

/// Options for connecting to the server.
pub mod connect_options;

/// Connect options for the Last Will and Testament (LWT) message.
pub mod will_options;

/// Connect options for creating secure connections to the server.
pub mod ssl_options;

/// Options for disconnecting from the server.
pub mod disconnect_options;

/// Options for subscribing to topics
pub mod subscribe_options;

/// Options for responses coming back from the C lib.
pub mod response_options;

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

/// Miscelaneous types
pub mod types;

/// Definitions for creating user-defined persistence.
pub mod client_persistence;

/// The MQTT errors
pub mod errors;

/// Utility for creating string collections (to pass to the C library).
pub mod string_collection;

/// Utility for creating name/value string pair collections
/// (to pass to the C library).
pub mod name_value;

// C log tracing pass-through
mod c_trace;
use c_trace::{c_trace_level, on_c_trace};

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
pub fn to_c_bool(on: bool) -> c_int {
    c_int::from(on)
}

/// Converts a C integer boolean to a Rust bool
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
