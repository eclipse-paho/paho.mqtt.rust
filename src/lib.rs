// lib.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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
//! * **tokio** *(default)*
//!   Enables the Tokio-based internal async runtime for pure-Rust MQTT I/O.
//!
//! * **tls**
//!   Enables TLS/SSL transport via [`rustls`](https://docs.rs/rustls) and
//!   [`tokio-rustls`](https://docs.rs/tokio-rustls). Adds support for
//!   `ssl://`, `mqtts://`, and `tls://` URIs. Implies the `tokio` feature.
//!
//! * **serde**
//!   Serialization support for messages.
//!
//! # Logging
//!
//! The library uses the standard Rust log facility with the target/module
//! "paho_mqtt".

#![deny(missing_docs)]

#[macro_use]
extern crate log;

/// Re-export async_channel
pub use async_channel::{self, Receiver as AsyncReceiver};

/// Re-export crossbeam channel as sync components
pub use crossbeam_channel::{self as sync_channel, Receiver as SyncReceiver};

/// Re-export the codec
pub use paho_mqtt_codec::{
    self, MQTT_VERSION_3, MQTT_VERSION_3_1_1, MQTT_VERSION_5, MQTT_VERSION_DEFAULT,
    MqttVersion, QOS_0, QOS_1, QOS_2, QoS, ReasonCode,
};

/// The asynchronous API
pub mod async_client;
pub use crate::async_client::{
    AsyncClient, ConnectedCallback, ConnectionLostCallback, DisconnectedCallback,
    MessageArrivedCallback, TlsErrorCallback,
};

/// The synchronous API
pub mod client;
pub use crate::client::Client;

pub use crate::connect_options::{ConnectOptions, ConnectOptionsBuilder};
pub use crate::create_options::{CreateOptions, CreateOptionsBuilder};
pub use crate::disconnect_options::{DisconnectOptions, DisconnectOptionsBuilder};
pub use crate::error::{Error, Result};
pub use crate::event::Event;
pub use crate::message::{Message, MessageBuilder};
pub use crate::persistence::{
    ClientPersistence, ClientPersistenceBox, FilePersistence, PERSISTENCE_ERROR,
    PERSISTENCE_SUCCESS, PersistenceType, UserPersistence,
};
pub use crate::properties::{
    Properties, Property, PropertyCode, PropertyId, PropertyType, PropertyValue,
};
pub use crate::responses::{
    ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse,
};
pub use crate::subscribe_options::{
    NO_RETAIN_AS_PUBLISHED, RETAIN_AS_PUBLISHED, RetainHandling, SUBSCRIBE_LOCAL,
    SUBSCRIBE_NO_LOCAL, SubscribeOptions, SubscribeOptionsBuilder,
};
pub use crate::tls_options::{TlsOptions, TlsOptionsBuilder, TlsVersion};
pub use crate::token::{
    ConnectToken, DeliveryToken, SubscribeManyToken, SubscribeToken, Token,
    UnsubscribeManyToken, UnsubscribeToken,
};
pub use crate::topic::{SyncTopic, Topic, TopicFilter, is_topic_filter_valid};
pub use crate::topic_matcher::TopicMatcher;

use std::any::Any;

mod macros;

/// Client creation options
pub mod create_options;

/// Options for connecting to the server.
pub mod connect_options;

/// Options for creating secure connections to the server.
pub mod tls_options;

/// Options for disconnecting from the server.
pub mod disconnect_options;

/// Options for subscribing to topics
pub mod subscribe_options;

/// Typed response types for MQTT operations.
pub mod responses;

/// MQTT v5 properties.
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

/// Message persistence for the client.
pub mod persistence;

/// The MQTT errors
pub mod error;

/// Events emitted by the client
pub mod event;

/// Internal async runtime and transport abstractions.
mod runtime;

// --------------------------------------------------------------------------

/// Generic type for arbitrary user-supplied data.
///
/// The application can use a type compatible with this to store in the
/// client as "user data" to be accessed from callbacks, etc.
pub type UserData = Box<dyn Any + 'static + Send + Sync>;

// --------------------------------------------------------------------------

/// Determines if the URI is for a secure connection.
pub fn is_secure_uri(uri: &str) -> bool {
    for sch in &["ssl://", "tls://", "mqtts://", "wss://"] {
        if uri.starts_with(sch) {
            return true;
        }
    }
    false
}
