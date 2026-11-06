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

//! The [Eclipse Paho](http://eclipse.org/paho) MQTT client library for Rust.
//!
//! A pure-Rust MQTT v5 / v3.1.1 / v3.1 client: no C library, no build-time
//! toolchain beyond Cargo. It speaks TCP, TLS, WebSocket, and Unix-domain
//! sockets, and run with either [tokio] or [smol].
//!
//! [tokio]: https://tokio.rs
//! [smol]: https://github.com/smol-rs/smol
//!
//! # Choosing an API
//!
//! There are two clients available:
//!
//! * [`AsyncClient`] returns a [`Token<T>`](Token) for every operation.
//!   Tokens are futures, so `.await` them; they can also be waited on from
//!   a thread with [`Token::wait`], which is what the token/wait style of
//!   the v1.x API did.
//! * [`Client`] wraps `AsyncClient` and blocks on every call, for programs
//!   with no async runtime of their own.
//!
//! ```no_run
//! use paho_mqtt as mqtt;
//!
//! # fn main() -> mqtt::Result<()> {
//! # #[cfg(any(feature = "tokio", feature = "smol"))]
//! # {
//! let cli = mqtt::Client::new("mqtt://localhost:1883")?;
//! cli.connect(None)?;
//! cli.publish(mqtt::Message::new("test", "Hello world!", mqtt::QOS_1))?;
//! cli.disconnect(None)?;
//! # }
//! # Ok(())
//! # }
//! ```
//!
//! # Tokens start immediately
//!
//! Unlike most Rust futures, a [`Token`] is not lazy. The operation begins
//! when the method is called, not when the token is polled — so a token may
//! be dropped, awaited later, or handed to another thread, and the publish
//! still happens. A token may also be cloned, and every clone resolves with
//! the same result.
//!
//! ```no_run
//! # use paho_mqtt as mqtt;
//! # #[cfg(any(feature = "tokio", feature = "smol"))]
//! # fn f(cli: &mqtt::AsyncClient, msg: mqtt::Message) -> mqtt::Result<()> {
//! let tok = cli.publish(msg);   // already on its way
//! // ... other work ...
//! tok.wait()?;                  // or `tok.await?`
//! # Ok(())
//! # }
//! ```
//!
//! # Transports
//!
//! The URI scheme picks the transport, and a scheme the build does not
//! support is refused rather than quietly downgraded:
//!
//! | Scheme | Transport | Feature |
//! |---|---|---|
//! | `mqtt://`, `tcp://` | TCP | — |
//! | `mqtts://`, `ssl://`, `tls://` | TLS | `tls`, `openssl`, or `smol-tls` |
//! | `ws://` | WebSocket | `websocket` or `smol-websocket` |
//! | `wss://` | WebSocket over TLS | the two above together |
//! | `unix://` | Unix-domain socket (\*nix) | — |
//!
//! TLS is configured with [`TlsOptions`]; `ws://` and `wss://` can be
//! tunnelled through an HTTP proxy set on [`ConnectOptions`].
//!
//! # Runtimes
//!
//! The protocol runs on an internal async task. Which executor drives it is
//! a build-time choice — the `tokio` feature (default) or `smol` — and
//! [`RuntimeMode`] decides whether that task joins the application's
//! runtime or one the library owns. No public API is an `async fn`, so the
//! crate can be used from any executor whichever backend is compiled in.
//!
//! When [`RuntimeMode::Auto`] borrows the application's runtime, *blocking*
//! on a token from that same runtime can stall the thread the protocol
//! needs; inside async code, await the token instead. [`Client`], whose
//! whole API blocks, uses the library's own runtime for this reason.
//!
//! # Crate Features
//!
//! Default: `tokio`.
//!
//! | Feature | Description |
//! |---|---|
//! | `tokio` *(default)* | Tokio-based internal runtime |
//! | `tls` | TLS via [rustls](https://docs.rs/rustls); implies `tokio` |
//! | `openssl` | TLS via the system OpenSSL; implies `tokio` |
//! | `openssl-vendored` | As `openssl`, building OpenSSL from source |
//! | `websocket` | WebSocket transport; implies `tokio` |
//! | `smol` | Smol-based internal runtime |
//! | `smol-tls` | TLS via rustls; implies `smol` |
//! | `smol-websocket` | WebSocket transport; implies `smol` |
//! | `serde` | `Serialize`/`Deserialize` for [`Message`], the response and options types, and the codec's data types |
//!
//! Exactly one backend at a time: enabling both `tokio` and `smol` is a
//! compile error, since an application could not tell which runtime it was
//! talking to. OpenSSL is tokio-only.
//!
//! ## Building with no runtime
//!
//! `--no-default-features` builds the crate without a backend, and so
//! without a client: [`AsyncClient`], [`Client`], [`Topic`] and [`SyncTopic`]
//! are absent, as is [`CreateOptionsBuilder::create_async_client`] and its
//! blocking counterpart. Everything that does not need one remains — the
//! codec and its types through this crate's re-exports, [`Message`],
//! [`Properties`], [`TopicMatcher`], [`TopicFilter`], [`Token`], the response
//! types, the options types and their builders, persistence, and [`Error`].
//!
//! That is the build for code that handles MQTT data without connecting:
//! a codec-level tool, a library of shared message types, or a configuration
//! crate that reads and writes [`ConnectOptions`] (with `serde`) for an
//! application that links a runtime elsewhere.
//!
//! # MQTT v5
//!
//! Version 5 is the default protocol level. [`Properties`] travel on every
//! packet that allows them, [`ConnectResponse`] reports everything the
//! broker advertised in its CONNACK, and [`ReasonCode`] carries the outcome
//! of each operation. The [`auth`] module covers enhanced authentication.
//! For an earlier level, use [`ConnectOptionsBuilder::new_v3`] or set the
//! version explicitly.
//!
//! # Persistence
//!
//! In-flight QoS 1 and 2 messages survive a dropped connection on their
//! own: the client carries them across the reconnect. A
//! [`ClientPersistence`] store — [`FilePersistence`], [`MemPersistence`],
//! or your own — is what carries them across a restart of the program.
//!
//! # Logging
//!
//! The library logs through the [`log`](https://docs.rs/log) crate under
//! the target `paho_mqtt`.

#![deny(missing_docs)]
// Without a runtime feature there is no client and no event loop, so the
// plumbing they would drive — completers, the authentication handler's
// innards — has no caller inside the crate. It stays as it is, rather than
// being cfg-ed away piece by piece and changing shape between builds.
#![cfg_attr(not(any(feature = "tokio", feature = "smol")), allow(dead_code))]
// The same build has no `AsyncClient`, `Client`, `Topic` or `SyncTopic`, so
// the documentation's links to them cannot resolve. They are checked in every
// build with a backend, which is the one that documents the client.
#![cfg_attr(
    not(any(feature = "tokio", feature = "smol")),
    allow(rustdoc::broken_intra_doc_links)
)]

// One backend at a time: the client would otherwise be driven by a runtime
// the application did not choose, with no way to tell which from the API.
// `_runtime` only marks that one of them is present, for the sake of Cargo's
// `required-features`, which cannot say "tokio or smol" itself.
#[cfg(all(feature = "_runtime", not(any(feature = "tokio", feature = "smol"))))]
compile_error!(
    "`_runtime` is internal and marks that a backend is present — enable `tokio` or `smol` instead"
);

#[cfg(all(feature = "tokio", feature = "smol"))]
compile_error!(
    "the `tokio` and `smol` features are mutually exclusive — enable exactly one \
     (note that `tls`, `openssl` and `websocket` imply `tokio`, while `smol-tls` \
     and `smol-websocket` imply `smol`)"
);

#[cfg(any(feature = "tokio", feature = "smol"))]
#[macro_use]
extern crate log;

/// Re-export `bytes`, which appears in this crate's API: a [`Message`]
/// payload is a [`Bytes`](bytes::Bytes), and the payload arguments take
/// anything convertible into one. Naming the type through this re-export
/// keeps an application on the same version the client uses.
///
/// ```
/// use paho_mqtt as mqtt;
/// use mqtt::bytes::Bytes;
///
/// let payload = Bytes::from_static(b"reused without copying");
/// let msg = mqtt::Message::new("data", payload.clone(), mqtt::QOS_1);
/// assert_eq!(msg.payload(), &payload[..]);
/// ```
pub use bytes;

/// Re-export async_channel
pub use async_channel::{self, Receiver as AsyncReceiver};

/// Re-export crossbeam channel as sync components
pub use crossbeam_channel::{self as sync_channel, Receiver as SyncReceiver};

/// Re-export the codec
pub use paho_mqtt_codec::{
    self, MQTT_VERSION_3, MQTT_VERSION_3_1, MQTT_VERSION_3_1_1, MQTT_VERSION_5,
    MQTT_VERSION_DEFAULT, MqttVersion, QOS_0, QOS_1, QOS_2, QoS, ReasonCode, topic as topic_rules,
};

/// MQTT v5 enhanced authentication
pub mod auth;
pub use crate::auth::{AuthCallback, AuthChallenge, AuthHandler};

/// The asynchronous API
#[cfg(any(feature = "tokio", feature = "smol"))]
pub mod async_client;
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use crate::async_client::{
    AsyncClient, ConnectedCallback, ConnectionLostCallback, DisconnectedCallback,
    MessageArrivedCallback, TlsErrorCallback,
};

/// The synchronous API
#[cfg(any(feature = "tokio", feature = "smol"))]
pub mod client;
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use crate::client::Client;

pub use crate::{
    connect_options::{ConnectOptions, ConnectOptionsBuilder},
    connection_state::ConnectionState,
    create_options::{CreateOptions, CreateOptionsBuilder, RuntimeMode},
    disconnect_options::{DisconnectOptions, DisconnectOptionsBuilder},
    error::{Error, ProxyError, Result, TlsError, TlsErrorKind},
    event::Event,
    message::{IntoPayload, Message, MessageBuilder},
    persistence::{
        ClientPersistence, ClientPersistenceBox, FilePersistence, MemPersistence,
        PERSISTENCE_ERROR, PERSISTENCE_SUCCESS, PersistenceType, UserPersistence,
    },
    properties::{Properties, PropertiesBuilder, Property, PropertyCode, PropertyId, PropertyType},
    responses::{ConnectResponse, PublishResponse, SubscribeResponse, UnsubscribeResponse},
    subscribe_options::{
        NO_RETAIN_AS_PUBLISHED, RETAIN_AS_PUBLISHED, RetainHandling, SUBSCRIBE_LOCAL,
        SUBSCRIBE_NO_LOCAL, SubscribeOptions, SubscribeOptionsBuilder,
    },
    tls_options::{TlsOptions, TlsOptionsBuilder, TlsVersion},
    token::{
        ConnectToken, DeliveryToken, SubscribeManyToken, SubscribeToken, Token,
        UnsubscribeManyToken, UnsubscribeToken,
    },
    topic::{TopicFilter, is_topic_filter_valid},
    topic_matcher::TopicMatcher,
};

#[cfg(any(feature = "tokio", feature = "smol"))]
pub use crate::topic::{SyncTopic, Topic};

use std::any::Any;

mod macros;

/// Client creation options
pub mod create_options;

/// Options for connecting to the server.
pub mod connect_options;

/// The state of the client's connection to the server.
pub mod connection_state;

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
#[cfg(any(feature = "tokio", feature = "smol"))]
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
