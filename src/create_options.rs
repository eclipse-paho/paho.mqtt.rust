// create_options.rs
//
// The set of options for creating an MQTT client.
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2024 Frank Pagliughi <fpagliughi@mindspring.com>
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

#[cfg(any(feature = "tokio", feature = "smol"))]
use crate::{AsyncClient, Client, Result};
use crate::{ClientPersistence, MqttVersion, PersistenceType, UserData, UserPersistence};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::cmp;

/////////////////////////////////////////////////////////////////////////////
//                            RuntimeMode
/////////////////////////////////////////////////////////////////////////////

/// Selects which Tokio runtime the client's internal event loop runs on.
///
/// Each [`AsyncClient`] runs a `run_event_loop` task that handles all network
/// I/O for that client. This enum controls which Tokio runtime hosts that task.
///
/// # Choosing a mode
///
/// | Situation | Recommended mode |
/// |---|---|
/// | Application already uses `#[tokio::main]` or `Runtime::block_on` | [`Auto`] (default) or [`ApplicationRuntime`] |
/// | Pure synchronous application, no existing runtime | [`Auto`] (default) or [`Shared`] |
/// | Multiple clients, want a single shared background runtime | [`Auto`] (default) or [`Shared`] |
/// | Multiple clients, want MQTT isolated from application tasks | [`Shared`] |
/// | Need explicit control over runtime thread count | [`Shared(n)`] |
///
/// Most applications can leave this at the default ([`Auto`]) and get correct
/// behaviour without any configuration.
///
/// # Runtime lifetime and threads
///
/// When the library creates its own runtime ([`Auto`] from a sync context, or
/// [`Shared`]), all clients in the process share a single runtime. The runtime
/// is created on the first client and dropped when the last client is dropped.
/// It runs as a Tokio `multi_thread` runtime with background worker thread(s).
///
/// When the library borrows the application's runtime ([`Auto`] from an async
/// context, or [`ApplicationRuntime`]), no extra threads are created for the
/// runtime itself. The application is responsible for keeping its runtime alive.
///
/// Either way, each client also spawns one lightweight OS thread for
/// dispatching received messages and connection-state callbacks to user code.
///
/// [`Auto`]: RuntimeMode::Auto
/// [`Shared`]: RuntimeMode::Shared
/// [`Shared(n)`]: RuntimeMode::Shared
/// [`ApplicationRuntime`]: RuntimeMode::ApplicationRuntime
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Default)]
pub enum RuntimeMode {
    /// Automatically select the runtime (default).
    ///
    /// Detection happens at [`AsyncClient::new`] time:
    ///
    /// - **Inside an async context** (e.g. `#[tokio::main]`, `Runtime::block_on`,
    ///   or a running Tokio task): the current runtime is borrowed. No extra
    ///   threads are created. MQTT event-loop tasks run as peers of the
    ///   application's own tasks.
    ///
    /// - **Outside any async context** (e.g. a plain `fn main()`, a test, or a
    ///   synchronous thread): the process-wide shared runtime is used, creating
    ///   it on the first call. The runtime is a `multi_thread` Tokio runtime
    ///   with 1 worker thread by default, tuned to check for socket readiness
    ///   every couple of task polls while busy, so that one worker keeps
    ///   latency low under load (see [`Shared`]). It is dropped when the last
    ///   client that owns it is dropped.
    ///
    /// This mode is appropriate for the vast majority of applications. Switch
    /// to [`Shared`] only if you need isolation or explicit thread-count control,
    /// or to [`ApplicationRuntime`] if you need the guarantee that no
    /// library-owned runtime is ever created.
    ///
    /// # Blocking and the borrowed runtime
    ///
    /// When this mode borrows the application's runtime, the event loop runs
    /// as one of that runtime's tasks — so a *blocking* wait made from that
    /// same runtime can stop the thread the event loop needs. On a
    /// `current_thread` runtime, `Token::wait()` never returns; on a small
    /// multi-threaded one it takes a worker out of service, and enough
    /// concurrent waits will stall it.
    ///
    /// Inside async code, `.await` the token rather than waiting on it, and
    /// the question does not arise. [`Client`], whose whole
    /// API blocks, sidesteps it by using [`Shared`] when left on `Auto`.
    ///
    /// [`Shared`]: RuntimeMode::Shared
    /// [`ApplicationRuntime`]: RuntimeMode::ApplicationRuntime
    #[default]
    Auto,

    /// Always use the process-wide shared runtime, regardless of whether the
    /// caller is inside an existing Tokio runtime.
    ///
    /// The `usize` value is the number of worker threads for the shared
    /// runtime. It is honoured only when the runtime is first created;
    /// subsequent clients that specify a different value log a warning and
    /// use the existing runtime unchanged. `0` and `1` both produce a runtime
    /// with 1 worker thread; any larger value produces a runtime with that
    /// many workers.
    ///
    /// **Latency under load.** A busy worker normally checks for socket
    /// readiness only now and then, so with a single worker kept busy by a
    /// heavy publishing load, acknowledgements could wait unseen for
    /// milliseconds. The shared runtime is built to check every couple of
    /// task polls instead (Tokio's `event_interval`), which keeps the
    /// 99th-percentile publish latency at a few milliseconds even at around
    /// 100K msg/s, at no cost when idle. A second worker does as well or
    /// slightly better, since an idle worker waits on readiness directly, at
    /// the price of a thread and some more CPU per message: `Shared(2)` if
    /// tail latency under sustained load matters most.
    ///
    /// **Use this mode when:**
    /// - You want MQTT I/O to run on a dedicated runtime, isolated from the
    ///   application's own async tasks.
    /// - You want to size the runtime thread pool explicitly (e.g. `Shared(4)`
    ///   for a high-throughput gateway with many clients).
    /// - You are mixing synchronous and asynchronous code and want consistent
    ///   runtime behaviour regardless of call site.
    ///
    /// All clients created with `Shared` (or [`Auto`] from a sync context)
    /// share the same runtime instance. The runtime is kept alive by the
    /// last [`AsyncClient`] that holds it; dropping that client shuts the
    /// runtime down (blocking briefly while in-flight tasks complete).
    ///
    /// [`Auto`]: RuntimeMode::Auto
    Shared(usize),

    /// Borrow the Tokio runtime that is current when [`AsyncClient::new`] is
    /// called.
    ///
    /// This behaves like the async-context branch of [`Auto`], but is
    /// **explicit**: it panics immediately if there is no current runtime
    /// rather than falling back to creating one.
    ///
    /// **Use this mode when:**
    /// - You want a hard guarantee that the library never creates its own
    ///   runtime or background threads (beyond the per-client event-dispatch
    ///   thread).
    /// - You are writing a library on top of paho-mqtt and want to document
    ///   that callers must supply their own runtime.
    ///
    /// The application runtime is not affected by client lifetimes: dropping
    /// all clients does not shut down the runtime.
    ///
    /// **Panics** if called outside a Tokio runtime context.
    ///
    /// [`Auto`]: RuntimeMode::Auto
    ApplicationRuntime,
}

/////////////////////////////////////////////////////////////////////////////
//                            Create Options
/////////////////////////////////////////////////////////////////////////////

/// The options for creating an MQTT client.
/// This can be constructed using a
/// [CreateOptionsBuilder](struct.CreateOptionsBuilder.html).
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[derive(Debug)]
pub struct CreateOptions {
    /// The URI of the remote MQTT broker.
    /// This can be used when there's a single server URI.
    /// Alternately, for a High Availability setup, a list of server URIs
    /// can bep specified in the connect options.
    pub server_uri: String,
    /// The client identifier passed to the server when the client connects.
    /// This can be empty, in which case the server will assign a random,
    /// unique identifier.
    pub client_id: String,
    /// Whether to allow messages to be sent when the client library is not
    /// connected.
    pub send_while_disconnected: bool,
    /// The maximum number of messages allowed to be buffered.
    /// This is intended to be used to limit the number of messages queued
    /// while the client is not connected. It also applies when the client is
    /// connected, however, so has to be greater than 0.
    pub max_buffered_msgs: usize,
    /// Allow sending of messages while disconnected before a first
    /// successful connect.
    pub allow_send_any_time: bool,
    /// When the maximum number of buffered messages is reached, delete the
    /// oldest rather than discard the newest.
    pub delete_oldest: bool,
    /// Restore messages from persistence on create - or clear it.
    pub restore_msgs: bool,
    /// Whether a QoS 0 publish made while disconnected is queued for the
    /// next connection. Defaults to `true`.
    ///
    /// Only meaningful together with
    /// [`send_while_disconnected`](Self::send_while_disconnected): it decides
    /// what that buffer does with a QoS 0 message. `true` queues it like any
    /// other, to go out on the next connection; `false` drops it, completing
    /// its token with `Ok` as though it had been sent — which is all QoS 0
    /// ever promises, since a message delivered at most once may be
    /// delivered not at all.
    ///
    /// Set it to `false` for data that is only worth sending while it is
    /// fresh — a periodic reading, say, where the value that arrives after a
    /// long outage is of no use and would only crowd out a current one.
    ///
    /// The name comes from the v1.x API, where the buffer and the
    /// persistence store were the same mechanism. QoS 0 messages are never
    /// written to a [`ClientPersistence`] store by
    /// this client: with nothing to acknowledge, there is no state to
    /// recover.
    pub persist_qos0: bool,
    /// The MQTT protocol version to use for connections from this client.
    /// Defaults to v5, which enables a persistence store compatible with
    /// either v3.x or v5 connections.
    pub mqtt_version: MqttVersion,
    /// The type of persistence used by the client.
    pub persistence: PersistenceType,
    /// User-defined data to store in the client, if any
    ///
    /// Skipped by `serde`: this is a `Box<dyn Any>`, which has no
    /// representation to write or read back.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub user_data: Option<UserData>,
    /// Which Tokio runtime the client's event loop runs on.
    /// See [`RuntimeMode`] for the available options and guidance on choosing.
    pub runtime_mode: RuntimeMode,
}

impl CreateOptions {
    /// Create a new set of create options with the specified URL
    /// and Client ID.
    pub fn new<U, C>(server_uri: U, client_id: C) -> Self
    where
        U: Into<String>,
        C: Into<String>,
    {
        Self {
            server_uri: server_uri.into(),
            client_id: client_id.into(),
            ..Self::default()
        }
    }
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            server_uri: String::new(),
            client_id: String::new(),
            send_while_disconnected: false,
            max_buffered_msgs: 100,
            allow_send_any_time: false,
            delete_oldest: false,
            restore_msgs: true,
            persist_qos0: true,
            mqtt_version: MqttVersion::Default,
            persistence: PersistenceType::default(),
            user_data: None,
            runtime_mode: RuntimeMode::default(),
        }
    }
}

impl From<String> for CreateOptions {
    fn from(server_uri: String) -> Self {
        Self {
            server_uri,
            ..Self::default()
        }
    }
}

impl<'a> From<&'a str> for CreateOptions {
    fn from(server_uri: &'a str) -> Self {
        Self::from(server_uri.to_string())
    }
}

impl From<(String, String)> for CreateOptions {
    /// Constructs the create options from two strings giving the server URI
    /// and Client ID.
    fn from((server_uri, client_id): (String, String)) -> Self {
        let mut opts = Self {
            server_uri,
            client_id,
            ..Self::default()
        };
        // TODO: Is this confusing?
        if !opts.client_id.is_empty() {
            opts.persistence = PersistenceType::File;
        }
        opts
    }
}

impl<'a, 'b> From<(&'a str, &'b str)> for CreateOptions {
    /// Constructs the create options from two string reference giving the server URI
    /// and Client ID.
    fn from((server_uri, client_id): (&'a str, &'b str)) -> Self {
        Self::from((server_uri.to_string(), client_id.to_string()))
    }
}

impl From<CreateOptionsBuilder> for CreateOptions {
    fn from(val: CreateOptionsBuilder) -> Self {
        val.finalize()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                                Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder to construct client creation options.
///
/// # Examples
///
/// ```
/// use paho_mqtt as mqtt;
///
/// let opts = mqtt::CreateOptionsBuilder::new()
///     .server_uri("mqtt://my_server:1883")
///     .client_id("client1")
///     .finalize();
///
/// # #[cfg(any(feature = "tokio", feature = "smol"))]
/// let cli = mqtt::AsyncClient::new(opts).unwrap();
/// ```
#[derive(Debug, Default)]
pub struct CreateOptionsBuilder {
    opts: CreateOptions,
}

impl CreateOptionsBuilder {
    /// Creates a default client create options builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the the URI to the MQTT broker.
    /// Alternately, the application can specify multiple servers via the
    /// connect options.
    ///
    /// # Arguments
    ///
    /// `server_uri` The URI string to specify the server in the form
    ///              _protocol://host:port_, where the protocol can be
    ///              _tcp_ or _ssl_, and the host can be an IP address
    ///              or domain name.
    pub fn server_uri<S: Into<String>>(mut self, server_uri: S) -> Self {
        self.opts.server_uri = server_uri.into();
        self
    }

    /// Sets the client identifier string that is sent to the server.
    /// The client ID is a unique name to identify the client to the server,
    /// which can be used if the client desires the server to hold state
    /// about the session. If the client requests a clean session, this can
    /// be an empty string, in which case the server will assign a random
    /// name for the client.
    ///
    /// The broker is required to honor a client ID of up to 23 bytes, but
    /// could honor longer ones, depending on the broker.
    ///
    /// Note that if this is an empty string, the clean session parameter
    /// *must* be set to _true_.
    ///
    /// # Arguments
    ///
    /// `client_id` A UTF-8 string identifying the client to the server.
    ///
    pub fn client_id<S: Into<String>>(mut self, client_id: S) -> Self {
        self.opts.client_id = client_id.into();
        self
    }

    /// Sets the type of persistence used by the client.
    /// The default is for the library to automatically use file persistence,
    /// although this can be turned off by specify `None` for a more
    /// performant, though possibly less reliable system.
    ///
    /// # Arguments
    ///
    /// `persist` The type of persistence to use.
    ///
    pub fn persistence<P>(mut self, persist: P) -> Self
    where
        P: Into<PersistenceType>,
    {
        self.opts.persistence = persist.into();
        self
    }

    /// Sets a user-defined persistence store.
    /// This sets the persistence to use a custom one defined by the
    /// application. This can be anything that implements the
    /// `ClientPersistence` trait.
    ///
    /// # Arguments
    ///
    /// `persist` An application-defined custom persistence store.
    ///
    pub fn user_persistence<T>(mut self, persistence: T) -> Self
    where
        T: ClientPersistence + 'static,
    {
        let persistence: Box<dyn ClientPersistence> = Box::new(persistence);
        self.opts.persistence = PersistenceType::User(UserPersistence::from(persistence));
        self
    }

    /// Sets the maximum number of messages that can be buffered for delivery.
    ///
    /// When the client is off-line, this specifies the maximum number of
    /// messages that can be buffered. Even while connected, the library
    /// needs a small buffer to queue outbound messages. Setting it to zero
    /// disables off-line buffering but still keeps some slots open for
    /// on-line operation.
    ///
    /// # Arguments
    ///
    /// `n` The maximum number of messages that can be buffered. Setting this
    ///     to zero also disables any off-line buffering.
    ///
    pub fn max_buffered_messages(mut self, n: usize) -> Self {
        // Enforce a small lower bound so the client has at least a few
        // slots available for in-flight operations even when connected.
        self.opts.max_buffered_msgs = cmp::max(4, n);
        if n == 0 {
            self.opts.send_while_disconnected = false;
        }
        self
    }

    /// Allow the application to send (publish) messages while disconnected.
    ///
    /// If this is disabled, then any time the app tries to publish while
    /// disconnected results in a "disconnected" error from the client.
    /// When enabled, the application can queue up to
    /// [`max_buffered_messages()`](Self::max_buffered_messages) while off-line.
    ///
    /// # Arguments
    ///
    /// `on` Whether to allow off-line buffering in the client.
    ///
    pub fn send_while_disconnected(mut self, on: bool) -> Self {
        self.opts.send_while_disconnected = on;
        self
    }

    /// Allow sending of messages while disconnected before a first successful
    /// connect.
    pub fn allow_disconnected_send_at_anytime(mut self, allow: bool) -> Self {
        self.opts.allow_send_any_time = allow;
        self
    }

    /// When the maximum number of buffered messages is reached, delete the
    /// oldest rather than the newest.
    pub fn delete_oldest_messages(mut self, delete_oldest: bool) -> Self {
        self.opts.delete_oldest = delete_oldest;
        self
    }

    /// If set, messages from persistence are restored on create. Defaults to
    /// true.
    pub fn restore_messages(mut self, restore: bool) -> Self {
        self.opts.restore_msgs = restore;
        self
    }

    /// Whether a QoS 0 publish made while disconnected is queued for the
    /// next connection, or dropped. Defaults to `true`.
    ///
    /// See [`CreateOptions::persist_qos0`] for what this does and when to
    /// turn it off.
    pub fn persist_qos0(mut self, persist: bool) -> Self {
        self.opts.persist_qos0 = persist;
        self
    }

    /// Sets the MQTT protocol version for connections from this client.
    pub fn mqtt_version<V: Into<MqttVersion>>(mut self, ver: V) -> Self {
        self.opts.mqtt_version = ver.into();
        self
    }

    /// Sets the user-defined data structure for the client.
    pub fn user_data(mut self, data: UserData) -> Self {
        self.opts.user_data = Some(data);
        self
    }

    /// Use the process-wide shared runtime with `num_threads` worker threads.
    ///
    /// Sets [`RuntimeMode::Shared(num_threads)`](RuntimeMode::Shared). The
    /// thread count is honoured only when the shared runtime is first created;
    /// if it already exists the existing thread count is kept and a warning is
    /// logged. `0` and `1` both produce 1 worker thread.
    ///
    /// One worker suits most applications; `2` can lower tail latency under a
    /// sustained heavy load. See [`RuntimeMode::Shared`].
    pub fn shared_runtime(mut self, num_threads: usize) -> Self {
        self.opts.runtime_mode = RuntimeMode::Shared(num_threads);
        self
    }

    /// Use the Tokio runtime that is current when [`AsyncClient::new`] is
    /// called.
    ///
    /// Sets [`RuntimeMode::ApplicationRuntime`]. Panics at client-creation
    /// time if there is no current Tokio runtime.
    pub fn use_application_runtime(mut self) -> Self {
        self.opts.runtime_mode = RuntimeMode::ApplicationRuntime;
        self
    }

    /// Constructs a set of create options from the builder information.
    pub fn finalize(self) -> CreateOptions {
        self.opts
    }

    /// Finalize the builder and create an asynchronous client.
    ///
    /// Needs a runtime feature (`tokio` or `smol`) to drive the client.
    #[cfg(any(feature = "tokio", feature = "smol"))]
    pub fn create_async_client(self) -> Result<AsyncClient> {
        AsyncClient::new(self.finalize())
    }

    /// Finalize the builder and create a synchronous client.
    ///
    /// Needs a runtime feature (`tokio` or `smol`) to drive the client.
    #[cfg(any(feature = "tokio", feature = "smol"))]
    pub fn create_client(self) -> Result<Client> {
        Client::new(self.finalize())
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let opts = CreateOptions::default();
        assert_eq!("", &opts.server_uri);
        assert_eq!("", &opts.client_id);
        assert!(!opts.send_while_disconnected);
        assert_eq!(100, opts.max_buffered_msgs);
        assert!(matches!(opts.persistence, PersistenceType::None));
    }

    #[test]
    fn test_from_string() {
        const HOST: &str = "localhost";
        let opts = CreateOptions::from(HOST);
        assert_eq!(HOST, &opts.server_uri);
        assert_eq!("", &opts.client_id);
    }

    #[test]
    fn test_from_tuple() {
        const HOST: &str = "localhost";
        const ID: &str = "bubba";
        let opts = CreateOptions::from((HOST, ID));
        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        assert!(matches!(opts.persistence, PersistenceType::File));
    }

    #[test]
    fn test_builder() {
        const HOST: &str = "localhost";
        const ID: &str = "bubba";
        const MAX_BUF_MSGS: usize = 250;

        let opts = CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(ID)
            .max_buffered_messages(MAX_BUF_MSGS)
            .finalize();

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        assert!(!opts.send_while_disconnected);
        assert_eq!(MAX_BUF_MSGS, opts.max_buffered_msgs);

        let opts = CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(ID)
            .send_while_disconnected(true)
            .max_buffered_messages(MAX_BUF_MSGS)
            .finalize();

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        assert!(opts.send_while_disconnected);
        assert_eq!(MAX_BUF_MSGS, opts.max_buffered_msgs);
    }

    /// The configuration round-trips; the user data does not, being a
    /// `Box<dyn Any>`.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_round_trip() {
        let opts = CreateOptionsBuilder::new()
            .server_uri("mqtt://localhost:1883")
            .client_id("cli-1")
            .send_while_disconnected(true)
            .max_buffered_messages(50)
            .delete_oldest_messages(true)
            .persist_qos0(false)
            .mqtt_version(MqttVersion::V3_1_1)
            .persistence(PersistenceType::FilePath("/var/mqtt".into()))
            .shared_runtime(2)
            .user_data(Box::new(42u32))
            .finalize();
        assert!(opts.user_data.is_some());

        let back: CreateOptions =
            serde_json::from_str(&serde_json::to_string(&opts).unwrap()).unwrap();
        assert_eq!(back.server_uri, "mqtt://localhost:1883");
        assert_eq!(back.client_id, "cli-1");
        assert!(back.send_while_disconnected);
        assert_eq!(back.max_buffered_msgs, 50);
        assert!(back.delete_oldest);
        assert!(!back.persist_qos0);
        assert_eq!(back.mqtt_version, MqttVersion::V3_1_1);
        assert!(
            matches!(back.persistence, PersistenceType::FilePath(ref p) if p.as_os_str() == "/var/mqtt")
        );
        assert!(matches!(back.runtime_mode, RuntimeMode::Shared(2)));
        assert!(back.user_data.is_none(), "`dyn Any` cannot be read back");
    }

    /// The two client constructors differ only in the type they hand back;
    /// the annotations below are the point of the test.
    #[cfg(any(feature = "tokio", feature = "smol"))]
    #[test]
    fn test_client_constructors() {
        let opts = || {
            CreateOptionsBuilder::new()
                .server_uri("tcp://localhost:1883")
                .client_id("ctor-test")
        };

        let acli: crate::AsyncClient = opts().create_async_client().unwrap();
        assert_eq!(acli.client_id(), "ctor-test");

        let scli: crate::Client = opts().create_client().unwrap();
        assert_eq!(scli.client_id(), "ctor-test");

        let direct = crate::Client::new(opts().finalize()).unwrap();
        assert_eq!(direct.client_id(), scli.client_id());
    }

    /// A configuration file need only name what it changes.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_partial_input_uses_defaults() {
        let opts: CreateOptions =
            serde_json::from_str(r#"{"server_uri":"mqtt://host:1883"}"#).unwrap();
        let dflt = CreateOptions::default();
        assert_eq!(opts.server_uri, "mqtt://host:1883");
        assert_eq!(opts.max_buffered_msgs, dflt.max_buffered_msgs);
        assert_eq!(opts.mqtt_version, dflt.mqtt_version);
        assert!(matches!(opts.runtime_mode, RuntimeMode::Auto));
    }
}
