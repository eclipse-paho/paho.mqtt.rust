// paho-mqtt/src/async_client.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

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

//! The Asynchronous client module for the Paho MQTT Rust client library.
//!
//! This presents an asynchronous API that can be used with an asynchronous
//! runtime in the application.
//!
//! Asynchronous operations return a `Token` that is a type of future. It
//! can be used to determine if an operation has completed, block and wait
//! for the operation to complete, and obtain the final result.
//! For example, you can start a connection, do something else, and then
//! wait for the connection to complete.
//!
//! ```no_run
//! use paho_mqtt as mqtt;
//!
//! let cli = mqtt::AsyncClient::new("mqtt://localhost:1883").unwrap();
//!
//! // Start an async operation and get the token for it.
//! let tok = cli.connect(mqtt::ConnectOptions::new());
//!
//! // ...do something else...
//!
//! // Wait for the async operation to complete.
//! tok.wait().unwrap();
//! ```

use crate::{
    AsyncReceiver, ConnectOptions, ConnectResponse, ConnectToken, ConnectionState, CreateOptions,
    DeliveryToken, DisconnectOptions, Event, FilePersistence, Message, PersistenceType, Properties,
    ReasonCode, SubscribeManyToken, SubscribeOptions, SubscribeToken, SyncReceiver,
    UnsubscribeManyToken, UnsubscribeToken, UserData,
    error::{Error, Result, TlsError},
    is_topic_filter_valid,
    persistence::ClientPersistence,
    runtime::{
        Command, ConnectParams, OfflineConfig, RtOwnership, ServerAddr, Sink, StateView,
        commands::split_host_port, session::Dispatch,
    },
    token::Token,
};
use async_channel::Sender as AsyncSender;
use bytes::Bytes;
use crossbeam_channel::{self as sync_channel, Sender as SyncSender};
use paho_mqtt_codec::types::*;
use std::{
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    thread,
    time::Duration,
};

/////////////////////////////////////////////////////////////////////////////

/// Constructs a boxed, mutex-wrapped persistence store from a `PersistenceType`.
///
/// Returns `None` when `typ` is `PersistenceType::None`.
/// Checks the topic filters of a SUBSCRIBE or UNSUBSCRIBE before it is sent.
///
/// The codec would refuse to encode either packet with no filters, or with
/// one that breaks the rules of §4.7, so this reports it to the caller
/// instead of leaving it to fail on the token.
fn check_filters<I, S>(filters: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut any = false;
    for f in filters {
        if !is_topic_filter_valid(f.as_ref()) {
            return Err(Error::BadTopicFilter);
        }
        any = true;
    }
    if !any {
        return Err(Error::InvalidArgument("no topic filters".into()));
    }
    Ok(())
}

fn make_persistence(
    typ: PersistenceType,
    sync: bool,
) -> Option<Arc<Mutex<Box<dyn ClientPersistence>>>> {
    let store: Box<dyn ClientPersistence> = match typ {
        PersistenceType::None => return None,
        PersistenceType::File => Box::new(FilePersistence::new(".").with_sync(sync)),
        PersistenceType::FilePath(p) => Box::new(FilePersistence::new(p).with_sync(sync)),
        // A store of the application's own decides its own durability.
        PersistenceType::User(up) => up.into_inner(),
    };
    Some(Arc::new(Mutex::new(store)))
}

// The client is safe to send or share between threads.

/// User callback type for when the client is connected.
pub type ConnectedCallback = dyn FnMut(&AsyncClient, &ConnectResponse) + Send + 'static;

/// User callback type for when the connection is lost from the broker.
pub type ConnectionLostCallback = dyn FnMut(&AsyncClient) + Send + 'static;

/// User callback type for when the client receives a disconnect packet.
pub type DisconnectedCallback = dyn FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static;

/// User callback signature for when subscribed messages are received.
pub type MessageArrivedCallback = dyn FnMut(&AsyncClient, Option<Message>) + Send + 'static;

/// User callback type for a TLS error
pub type TlsErrorCallback = dyn Fn(&AsyncClient, &TlsError) + Send + Sync + 'static;

/////////////////////////////////////////////////////////////////////////////

/// One callback slot, and the bookkeeping that lets it be removed from
/// inside itself.
///
/// A callback is taken out of its slot to run, so that the application can
/// call back into the client — including registering or removing callbacks —
/// without meeting the lock it is already under. Putting it back afterwards
/// must not undo what the application did in the meantime, and an empty slot
/// alone cannot say: a slot is equally empty when nobody touched it and when
/// the callback removed itself. Every `set` and `clear` therefore bumps a
/// counter, and the callback is put back only if the count is where it was.
struct Slot<F: ?Sized> {
    cb: Option<Box<F>>,
    /// Bumped by every `set` and `clear`, never by `take`, which the
    /// application cannot observe.
    generation: u64,
}

// Derived `Default` would demand `F: Default`, which a `dyn FnMut` is not.
impl<F: ?Sized> Default for Slot<F> {
    fn default() -> Self {
        Self {
            cb: None,
            generation: 0,
        }
    }
}

impl<F: ?Sized> Slot<F> {
    /// Registers `cb`, replacing whatever was there, and returns the
    /// generation that now identifies it.
    fn set(&mut self, cb: Box<F>) -> u64 {
        self.cb = Some(cb);
        self.generation = self.generation.wrapping_add(1);
        self.generation
    }

    /// Removes the callback, if any.
    fn clear(&mut self) {
        self.cb = None;
        self.generation = self.generation.wrapping_add(1);
    }

    /// Takes the callback out to be run, with the generation to hand back to
    /// [`restore`](Self::restore).
    fn take(&mut self) -> Option<(Box<F>, u64)> {
        self.cb.take().map(|cb| (cb, self.generation))
    }

    /// Puts a taken callback back, unless the application has since changed
    /// this slot — set another callback, or removed this one from inside it.
    fn restore(&mut self, cb: Box<F>, generation: u64) {
        if self.generation == generation && self.cb.is_none() {
            self.cb = Some(cb);
        }
    }
}

/// The synchronous consumers of one client: the channels
/// [`start_consuming`] and [`start_consuming_events`] handed out.
///
/// These are fed by the dispatcher thread rather than by the event loop,
/// because a `crossbeam` receiver cannot be awaited. A bounded one that is
/// full blocks that thread, which stops it draining the event channel, which
/// is how backpressure from this side reaches the session.
///
/// Held behind an `Arc` the dispatcher clones out from under the lock, so a
/// consumer that blocks does not hold the lock while it does.
///
/// [`start_consuming`]: AsyncClient::start_consuming
/// [`start_consuming_events`]: AsyncClient::start_consuming_events
#[derive(Default)]
struct SyncSinks {
    msgs: Vec<SyncSender<Option<Message>>>,
    events: Vec<SyncSender<Event>>,
}

impl SyncSinks {
    /// Whether any synchronous consumer is registered.
    fn is_empty(&self) -> bool {
        self.msgs.is_empty() && self.events.is_empty()
    }

    /// Sends to every consumer, returning the ones whose receiver is gone
    /// for the caller to prune.
    fn send_message(&self, msg: Message) -> DeadSinks {
        let mut dead = DeadSinks::default();
        let n = self.msgs.len() + self.events.len();
        let mut msg = Some(msg);
        let mut i = 0;
        for tx in &self.msgs {
            i += 1;
            let m = if i == n {
                msg.take().unwrap()
            } else {
                msg.as_ref().unwrap().clone()
            };
            if tx.send(Some(m)).is_err() {
                dead.msgs.push(tx.clone());
            }
        }
        for tx in &self.events {
            i += 1;
            let m = if i == n {
                msg.take().unwrap()
            } else {
                msg.as_ref().unwrap().clone()
            };
            if tx.send(Event::Message(m)).is_err() {
                dead.events.push(tx.clone());
            }
        }
        dead
    }

    /// Sends a connection event to the event consumers.
    fn send_event(&self, ev: &Event) -> DeadSinks {
        let mut dead = DeadSinks::default();
        for tx in &self.events {
            if tx.send(ev.clone()).is_err() {
                dead.events.push(tx.clone());
            }
        }
        dead
    }

    /// Tells every consumer the connection ended: `None` to the message
    /// consumers, as the message callback is given, and `ev` to the event
    /// consumers.
    ///
    /// `ev` is [`Event::ConnectionLost`] when the connection was lost
    /// unexpectedly, or [`Event::Disconnected`] when the server closed it
    /// and said why. A message consumer cannot carry a reason, so it sees
    /// the same `None` either way.
    fn send_session_end(&self, ev: &Event) -> DeadSinks {
        let mut dead = DeadSinks::default();
        for tx in &self.msgs {
            if tx.send(None).is_err() {
                dead.msgs.push(tx.clone());
            }
        }
        let mut rest = self.send_event(ev);
        dead.events.append(&mut rest.events);
        dead
    }
}

/// Consumers whose receiver was dropped, found while sending to them.
#[derive(Default)]
struct DeadSinks {
    msgs: Vec<SyncSender<Option<Message>>>,
    events: Vec<SyncSender<Event>>,
}

impl DeadSinks {
    fn is_empty(&self) -> bool {
        self.msgs.is_empty() && self.events.is_empty()
    }
}

#[derive(Default)]
struct CallbackContext {
    on_connected: Slot<ConnectedCallback>,
    on_connection_lost: Slot<ConnectionLostCallback>,
    on_disconnected: Slot<DisconnectedCallback>,
    on_message_arrived: Slot<MessageArrivedCallback>,
    /// Whether a message callback is registered, including while it is
    /// taken out to be run — `on_message_arrived` is empty for the length
    /// of the call, and the event loop must not conclude from that that
    /// nothing wants messages.
    msg_cb_installed: bool,
}

/////////////////////////////////////////////////////////////////////////////
// AsyncClient

/// An asynchronous MQTT connection client.
#[derive(Clone)]
pub struct AsyncClient {
    pub(crate) inner: Arc<InnerAsyncClient>,
}

/// Implementation details for the asynchronous MQTT connection client.
pub(crate) struct InnerAsyncClient {
    /// Command sender to the internal event loop.
    cmd_tx: AsyncSender<Command>,
    /// The connection state, as the event loop keeps it. Read-only here: the
    /// loop is its only writer, and sets it before the token or event that
    /// reveals a change, so `is_connected()` agrees with a `wait()` that has
    /// just returned.
    state: StateView,
    /// The MQTT version of the connection.
    mqtt_version: AtomicU32,
    /// The client identifier that the server assigned, if the client
    /// connected with an empty one. Set by the event loop before the
    /// connect token resolves.
    assigned_id: Arc<RwLock<Option<String>>>,
    /// The options used to create the client.
    opts: CreateOptions,
    /// The options for the most recent connection attempt.
    conn_opts: RwLock<ConnectOptions>,
    /// User-registered event callbacks.
    callback_context: Mutex<CallbackContext>,
    /// The synchronous consumers: `start_consuming` and
    /// `start_consuming_events`. Fed by the dispatcher thread, because a
    /// `crossbeam` receiver cannot be awaited.
    sync_sinks: RwLock<Arc<SyncSinks>>,
    /// Whether `sync_sinks` holds anything, so that the dispatcher can skip
    /// the lock — and the `Arc` clone it takes out of it — for every
    /// message when there is no synchronous consumer.
    has_sync_sinks: AtomicBool,
    /// What this client shares with its event loop: the gate the dispatcher
    /// reports consumed messages to, and whether anything on this side
    /// wants messages at all.
    dispatch: Dispatch,
    /// Persistence store, if any.
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
    /// Keeps the async runtime alive for the lifetime of this client.
    /// Declared last so it is dropped after `cmd_tx`, ensuring the event-loop
    /// task sees the closed channel before the runtime shuts down.
    _rt_ownership: RtOwnership,
}

impl InnerAsyncClient {
    /// Creates the client state around a running event loop, reached through
    /// `cmd_tx` and `dispatch`, whose connection state is read through `state`.
    ///
    /// Everything else starts out at its default: no assigned
    /// identifier, no callbacks or consumers, and connect options that carry
    /// only the MQTT version from `opts`, so that `reconnect()` and sends
    /// before the first connect use the right one.
    fn new(
        cmd_tx: AsyncSender<Command>,
        opts: CreateOptions,
        dispatch: Dispatch,
        state: StateView,
        persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
        rt_ownership: RtOwnership,
    ) -> Self {
        let ver = opts.mqtt_version;
        Self {
            cmd_tx,
            state,
            mqtt_version: AtomicU32::new(ver as u32),
            assigned_id: Arc::new(RwLock::new(None)),
            opts,
            conn_opts: RwLock::new(ConnectOptions {
                mqtt_version: ver,
                ..ConnectOptions::default()
            }),
            callback_context: Mutex::new(CallbackContext::default()),
            sync_sinks: RwLock::new(Arc::new(SyncSinks::default())),
            has_sync_sinks: AtomicBool::new(false),
            dispatch,
            persist,
            _rt_ownership: rt_ownership,
        }
    }
}

impl AsyncClient {
    // ----- Creation -----

    /// Creates a new MQTT client which can connect to an MQTT broker.
    ///
    /// # Arguments
    ///
    /// * `opts` The create options for the client.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] if the options ask for file persistence
    /// ([`PersistenceType::File`] or [`FilePath`](PersistenceType::FilePath))
    /// with an empty client ID. A client without an ID cannot resume a
    /// session, so there is nothing for the store to keep, and the store is
    /// named after the ID. v1.x switched such a client to no persistence
    /// without saying so.
    ///
    /// [`Error::InvalidArgument`] for
    /// [`RuntimeMode::ApplicationRuntime`](crate::RuntimeMode::ApplicationRuntime)
    /// when there is no runtime to borrow: with the tokio backend, when the
    /// client is not created from within a Tokio runtime; with the smol
    /// backend, always.
    ///
    /// [`Error::Io`] if what the client needs cannot be started: the shared
    /// Tokio runtime cannot be built, no thread can be spawned for the
    /// shared smol executor, or the client's own event-dispatch thread
    /// cannot be spawned.
    ///
    pub fn new<T>(opts: T) -> Result<AsyncClient>
    where
        T: Into<CreateOptions>,
    {
        let mut opts: CreateOptions = opts.into();
        debug!("Create options: {:?}", opts);

        // The file store is named after the client ID, and a client with no
        // ID never resumes a session, so there would be nothing to keep and
        // every such client would share one directory.
        if opts.client_id.is_empty()
            && matches!(
                opts.persistence,
                PersistenceType::File | PersistenceType::FilePath(_)
            )
        {
            return Err(Error::InvalidArgument(
                "file persistence needs a client ID".into(),
            ));
        }

        let offline_cfg = OfflineConfig::from(&opts);

        #[cfg(feature = "tokio")]
        let (rt, rt_ownership) = {
            use crate::runtime::{RuntimeHandle, resolve_runtime};
            let (rt_handle, ownership) = resolve_runtime(&opts.runtime_mode)?;
            (RuntimeHandle::spawn(&rt_handle, offline_cfg), ownership)
        };

        #[cfg(feature = "smol")]
        let (rt, rt_ownership) = {
            use crate::runtime::{RuntimeHandle, SmolOwnership, resolve_executor};
            let executor = resolve_executor(&opts.runtime_mode)?;
            let rt = RuntimeHandle::spawn(&executor, offline_cfg);
            (rt, SmolOwnership::Shared(executor))
        };
        let (cmd_tx, event_rx, dispatch, state) = rt.into_parts();

        // Extract persistence before moving opts into inner, so we can consume
        // PersistenceType::User without cloning the trait object.
        let persist_type = std::mem::replace(&mut opts.persistence, PersistenceType::None);
        let persist = make_persistence(persist_type, opts.persistence_sync);

        let inner = Arc::new(InnerAsyncClient::new(
            cmd_tx,
            opts,
            dispatch.clone(),
            state,
            persist,
            rt_ownership,
        ));

        // A thread that cannot be started is an error from `new`, like the
        // runtime's (see `resolve_runtime`). Returning drops `inner`, and
        // with it the command channel, which ends the event loop task.
        let weak = Arc::downgrade(&inner);
        thread::Builder::new()
            .name("mqtt-events".into())
            .spawn(move || {
                while let Ok(ev) = event_rx.recv_blocking() {
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    let cli = AsyncClient { inner };
                    let is_msg = matches!(ev, crate::runtime::Event::MessageArrived(_));
                    cli.dispatch_event(ev);
                    // Only now, with the callback done and the synchronous
                    // consumers fed, is there room for another: a slow
                    // consumer holds the session back.
                    if is_msg {
                        dispatch.gate.consumed();
                    }
                }
            })?;

        Ok(AsyncClient { inner })
    }

    // ----- Connecting -----

    /// Attempts to connect to an MQTT broker using the specified connect
    /// options, but returns an error immediately if the request can't be
    /// started.
    ///
    /// That is a failure found before anything goes on the wire: invalid
    /// connect options, a server or proxy URI that can't be parsed, a
    /// secure server URI — in the connect options, or the one the client
    /// was created with — and no TLS options ([`Error::MissingTlsOptions`]),
    /// or a client whose background session has shut down
    /// ([`Error::Disconnected`]).
    ///
    /// On success the returned token tracks the connection attempt itself.
    ///
    /// # Arguments
    ///
    /// * `opts` The connect options. This can be `None`, in which case the
    ///   default options are used.
    ///
    pub fn try_connect<T>(&self, opts: T) -> Result<ConnectToken>
    where
        T: Into<Option<ConnectOptions>>,
    {
        let opt_opts = opts.into();

        // None: keep the client's current version unchanged.
        // Some(opts): use opts.mqtt_version, and once the options are known
        // to be good, make it the client's version.
        let ver = match &opt_opts {
            Some(opts) => opts.mqtt_version,
            None => self.mqtt_version(),
        };

        // Normalize: ensure the opts we store and send have the resolved version.
        let mut opts = opt_opts.unwrap_or_default();
        opts.mqtt_version = ver;

        debug!("Connecting with opts: {:?}", opts);
        opts.check()?;

        // Build the ordered server list: explicit list from connect opts, or
        // fall back to the URI given at client creation time.
        let uris: Vec<String> = if opts.server_uris.is_empty() {
            vec![self.inner.opts.server_uri.clone()]
        } else {
            opts.server_uris.clone()
        };
        let mut servers: Vec<(ServerAddr, String)> = Vec::with_capacity(uris.len());
        for uri in uris {
            servers.push((parse_server_uri(&uri)?, uri));
        }
        // `check` above looks at the connect options' own list; the
        // creation URI, used when that is empty, needs the same rule. Without
        // it every attempt, auto-reconnect's included, would fail at the
        // transport instead of this call failing once. (After parsing, so
        // that a build with no TLS backend says that instead.)
        if !opts.has_tls_options() && servers.iter().any(|(_, u)| crate::is_secure_uri(u)) {
            return Err(Error::MissingTlsOptions);
        }

        // A proxy URI is otherwise read only when a WebSocket connection is
        // opened, so a bad one would fail every attempt — auto-reconnect's
        // included — rather than this call.
        #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
        for uri in [&opts.http_proxy, &opts.https_proxy] {
            if !uri.is_empty() {
                crate::runtime::proxy::Proxy::parse(uri)?;
            }
        }

        let keep_alive_secs = opts.keep_alive_secs();

        let will = opts.will.as_ref().map(|msg| paho_mqtt_codec::Will {
            topic: msg.topic.clone(),
            payload: msg.payload.clone(),
            qos: msg.qos,
            retain: msg.retain,
            properties: msg.props.clone(),
        });

        // Everything that can be checked here has been: a connect refused
        // above leaves the client's version and options as they were.
        self.set_mqtt_version(ver);
        let (tok, comp) = ConnectToken::new();
        *self.inner.conn_opts.write().unwrap() = opts.clone();

        let params = ConnectParams {
            servers,
            persist_uri: self.inner.opts.server_uri.clone(),
            client_id: self.inner.opts.client_id.clone(),
            clean_start: opts.clean_start,
            keep_alive_secs,
            user_name: opts.user_name,
            password: opts.password.map(Bytes::from),
            will,
            ver,
            props: opts.props,
            auth: opts.auth_handler,
            #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
            tls: opts.tls,
            #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
            http_headers: opts.http_headers,
            #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
            http_proxy: opts.http_proxy,
            #[cfg(any(
                all(feature = "websocket", any(feature = "tls", feature = "openssl")),
                all(feature = "smol-websocket", feature = "smol-tls")
            ))]
            https_proxy: opts.https_proxy,
            auto_reconnect: opts.auto_reconnect,
            min_retry_interval: opts.min_retry_interval,
            max_retry_interval: opts.max_retry_interval,
            max_inflight: opts.max_inflight as usize,
            connect_timeout: opts.timeout,
        };

        let cmd = Command::Connect {
            params: Box::new(params),
            tx: comp,
            assigned_id: Arc::clone(&self.inner.assigned_id),
            persist: self.inner.persist.as_ref().map(Arc::clone),
        };

        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Err(Error::Disconnected);
        }

        Ok(tok)
    }

    /// Connects to an MQTT broker using the specified connect options.
    ///
    /// Returns a Connect Token to track the progress of the operation.
    /// A failure found before the request is sent — see
    /// [`try_connect`](Self::try_connect) — fails the token instead.
    ///
    /// # Arguments
    ///
    /// * `opts` The connect options. This can be `None`, in which case the
    ///   default options are used.
    ///
    pub fn connect<T>(&self, opts: T) -> ConnectToken
    where
        T: Into<Option<ConnectOptions>>,
    {
        self.try_connect(opts).unwrap_or_else(ConnectToken::from)
    }

    /// Attempts to reconnect to the broker.
    /// This can only be called after a connection was initially made or
    /// attempted. It will retry with the same connect options.
    pub fn reconnect(&self) -> ConnectToken {
        self.try_reconnect().unwrap_or_else(ConnectToken::from)
    }

    /// Attempts to reconnect to the broker with the same connect options,
    /// but returns an error immediately if the request can't be started.
    ///
    /// See [`try_connect`](Self::try_connect) for the failures reported this
    /// way, and [`reconnect`](Self::reconnect) for the rest.
    pub fn try_reconnect(&self) -> Result<ConnectToken> {
        let connopts = self.inner.conn_opts.read().unwrap().clone();
        self.try_connect(connopts)
    }

    /// Attempts to disconnect from the MQTT broker, but returns an error
    /// immediately if the request can't be started.
    ///
    /// That is a failure found before anything goes on the wire: a v5
    /// reason code or v5 properties on a connection of an earlier version
    /// ([`Error::WrongMqttVersion`]), or a client whose background session
    /// has shut down ([`Error::Disconnected`]). Either way the message
    /// stream is left open.
    ///
    /// On success the returned token tracks the disconnect itself.
    ///
    /// # Arguments
    ///
    /// `opts` Optional disconnect options. Specifying `None` will use
    ///        default of immediate (zero timeout) disconnect.
    ///
    pub fn try_disconnect<T>(&self, opts: T) -> Result<Token<()>>
    where
        T: Into<Option<DisconnectOptions>>,
    {
        let opts = opts.into().unwrap_or_default();
        debug!("Disconnecting with opts: {:?}", opts);

        // A v5 reason code and v5 properties cannot go on a v3 connection;
        // the codec refuses to encode either, and the DISCONNECT would never
        // be sent.
        self.require_v5_for_props(&opts.props)?;
        self.require_v5_for_disconnect_reason(opts.reason_code)?;

        let (tok, comp) = Token::<()>::new();

        let cmd = Command::Disconnect {
            timeout: opts.timeout,
            reason_code: opts.reason_code,
            props: opts.props,
            tx: comp,
        };

        // The message callback gets its `None` from the dispatcher thread,
        // when the event loop reports the disconnect done
        // (`Event::DisconnectDone`): after every message the connection
        // delivered, and never alongside one. It used to be called here, on
        // the caller's thread, where it raced the dispatcher.
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Err(Error::Disconnected);
        }
        Ok(tok)
    }

    /// Disconnects from the MQTT broker.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_disconnect`](Self::try_disconnect) — fails the token instead.
    ///
    /// What is still outstanding when the connection closes (after the
    /// timeout, if one was given) is treated differently by kind:
    ///
    /// * A QoS 1 or 2 publish belongs to the session, not the connection.
    ///   If the session outlives the connection, its token stays pending:
    ///   the next [`connect`](Self::connect) re-sends it if the server
    ///   resumes the session, and the token completes with the server's
    ///   answer; if the server does not, it fails with
    ///   [`Error::SessionNotResumed`]. Inbound QoS 2 messages awaiting
    ///   PUBREL are kept the same way.
    /// * If the session ends with the connection — a v3 clean session, or a
    ///   v5 one whose Session Expiry Interval is 0, the default, including
    ///   one set to 0 by these options' properties — the publish fails at
    ///   once with [`Error::SessionNotResumed`], and inbound QoS 2 state is
    ///   dropped. The server has discarded its half of the session.
    /// * A subscribe or unsubscribe fails with [`Error::Disconnected`].
    ///
    /// Dropping the client fails whatever is left with
    /// [`Error::Disconnected`].
    ///
    /// # Arguments
    ///
    /// `opts` Optional disconnect options. Specifying `None` will use
    ///        default of immediate (zero timeout) disconnect.
    ///
    pub fn disconnect<T>(&self, opts: T) -> Token<()>
    where
        T: Into<Option<DisconnectOptions>>,
    {
        self.try_disconnect(opts).unwrap_or_else(Token::from_err)
    }

    /// Disconnect from the MQTT broker with a timeout.
    ///
    /// This will delay the disconnect for up to the specified timeout to
    /// allow in-flight messages to complete. What happens to those still
    /// outstanding after it is described under
    /// [`disconnect`](Self::disconnect).
    ///
    /// # Arguments
    ///
    /// * `timeout` The amount of time to wait for the disconnect. This has
    ///   a resolution in milliseconds.
    ///
    pub fn disconnect_after(&self, timeout: Duration) -> Token<()> {
        self.disconnect(DisconnectOptions::with_timeout(timeout))
    }

    // ----- Publishing -----

    /// Attempts to publish a message to the MQTT broker, but returns an
    /// error immediately if there's a problem creating or queuing the
    /// message.
    ///
    /// On failure returns [`Error::Publish`], which carries the cause and
    /// hands the message back, so that a message which never reached the
    /// broker can be kept and sent again:
    ///
    /// ```no_run
    /// # use paho_mqtt as mqtt;
    /// # fn main() -> mqtt::Result<()> {
    /// # let cli = mqtt::AsyncClient::new("mqtt://localhost:1883")?;
    /// let mut msg = mqtt::Message::new("data", "payload", mqtt::QOS_1);
    /// while let Err(mqtt::Error::Publish(cause, returned)) = cli.try_publish(msg) {
    ///     eprintln!("could not queue the message: {cause}");
    ///     msg = *returned; // ... and try again later
    /// #   break;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note that [`publish`](Self::publish) fails its token with the *cause*
    /// alone, having taken the message.
    ///
    /// A message carrying a Subscription Identifier is refused with
    /// [`Error::InvalidArgument`] as the cause: a client may not send one
    /// (MQTT-3.3.4-6). A message received from a subscription has them, so
    /// one republished on must have them removed first:
    ///
    /// ```no_run
    /// # use paho_mqtt as mqtt;
    /// # fn forward(cli: &mqtt::AsyncClient, mut msg: mqtt::Message) {
    /// msg.props.remove(mqtt::PropertyId::SubscriptionIdentifier);
    /// cli.publish(msg);
    /// # }
    /// ```
    pub fn try_publish(&self, msg: Message) -> Result<DeliveryToken> {
        debug!("Publish: {:?}", msg);

        if let Err(cause) = self.require_v5_for_props(&msg.props) {
            return Err(Error::Publish(Box::new(cause), Box::new(msg)));
        }

        // A server puts Subscription Identifiers in the PUBLISH packets it
        // sends; a client must not (MQTT-3.3.4-6), and the codec, which
        // serves both sides, cannot tell which it is encoding for. A server
        // that gets one disconnects with a Protocol Error. The likely way
        // here is a received message published on unchanged.
        if msg
            .props
            .contains(crate::PropertyId::SubscriptionIdentifier)
        {
            let cause = Error::InvalidArgument(
                "a published message cannot carry a Subscription Identifier (MQTT-3.3.4-6); \
                 remove it from a received message before publishing it on"
                    .into(),
            );
            return Err(Error::Publish(Box::new(cause), Box::new(msg)));
        }

        // Deliberately *not* gated on `msg.props.is_valid()`. It would catch
        // a bad property here, synchronously and with the message handed
        // back — but the session reports the one that matters in practice, an
        // unusable `TopicAlias`, as `ReasonCode(TopicAliasInvalid)`, the code
        // the specification defines for it (§3.3.2.3.4). A blanket check at
        // this boundary would shadow that with a bare `ProtocolError`.

        // Kept aside so that a message handed back is the message given: the
        // command carries only what goes on the wire.
        let (dup, msgid) = (msg.dup, msg.msgid);

        let Message {
            topic,
            payload,
            qos,
            retain,
            props,
            ..
        } = msg;
        let (tok, comp) = DeliveryToken::new();

        let cmd = Command::Publish {
            topic,
            payload,
            qos,
            retain,
            props,
            tx: comp,
        };

        if let Err(err) = self.inner.cmd_tx.send_blocking(cmd) {
            // A closed channel gives the command back, so the message can be
            // reassembled rather than lost with it.
            let Command::Publish {
                topic,
                payload,
                qos,
                retain,
                props,
                ..
            } = err.into_inner()
            else {
                unreachable!("the command sent above is a Publish");
            };
            let msg = Message {
                topic,
                payload,
                qos,
                retain,
                props,
                dup,
                msgid,
            };
            return Err(Error::Publish(Box::new(Error::Disconnected), Box::new(msg)));
        }

        Ok(tok)
    }

    /// Publishes a message to the MQTT broker.
    ///
    /// Returns a Delivery Token to track the progress of the operation.
    ///
    /// A synchronous failure fails the token with the cause — `Disconnected`
    /// or `WrongMqttVersion`. The message is not handed back, this call having
    /// taken it; use [`try_publish`](Self::try_publish) to keep it.
    ///
    /// A message the server said in its CONNACK that it cannot take fails
    /// the token without being sent, with [`Error::ReasonCode`] carrying
    /// `QosNotSupported` (a QoS above the server's Maximum QoS) or
    /// `RetainNotSupported` (RETAIN, when Retain Available is 0); see
    /// [`ConnectResponse::maximum_qos`] and
    /// [`ConnectResponse::retain_available`]. It is not downgraded.
    pub fn publish(&self, msg: Message) -> DeliveryToken {
        match self.try_publish(msg) {
            // Unwrap the message this call has taken ownership of, so the
            // token reports the same cause it always did.
            Err(Error::Publish(cause, _)) => DeliveryToken::from_err(*cause),
            Err(err) => DeliveryToken::from_err(err),
            Ok(tok) => tok,
        }
    }

    // ----- Subscribing -----

    /// Attempts to subscribe to a single topic, but returns an error
    /// immediately if the request can't be started.
    ///
    /// That is a failure found before anything goes on the wire: a topic
    /// filter that isn't valid ([`Error::BadTopicFilter`]), a QoS value that
    /// isn't valid, or a client whose background session has shut down
    /// ([`Error::Disconnected`]).
    ///
    /// On success the returned token tracks the subscription itself.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic name
    /// * `qos` The quality of service requested for messages
    ///
    pub fn try_subscribe<S, Q>(&self, topic: S, qos: Q) -> Result<SubscribeToken>
    where
        S: Into<String>,
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
    {
        self.try_subscribe_with_options(topic, qos, SubscribeOptions::default(), None)
    }

    /// Subscribes to a single topic.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_subscribe`](Self::try_subscribe) — fails the token instead.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic name
    /// * `qos` The quality of service requested for messages
    ///
    pub fn subscribe<S, Q>(&self, topic: S, qos: Q) -> SubscribeToken
    where
        S: Into<String>,
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
    {
        self.try_subscribe(topic, qos)
            .unwrap_or_else(SubscribeToken::from_err)
    }

    /// Attempts to subscribe to a single topic with v5 options, but returns
    /// an error immediately if the request can't be started.
    ///
    /// The failures are those of [`try_subscribe`](Self::try_subscribe), and
    /// [`Error::WrongMqttVersion`] for v5 subscribe options or properties on
    /// a connection of an earlier version.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic name
    /// * `qos` The quality of service requested for messages
    /// * `opts` Options for the subscription
    /// * `props` MQTT v5 properties
    ///
    pub fn try_subscribe_with_options<S, Q, T, P>(
        &self,
        topic: S,
        qos: Q,
        opts: T,
        props: P,
    ) -> Result<SubscribeToken>
    where
        S: Into<String>,
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
        T: Into<SubscribeOptions>,
        P: Into<Option<Properties>>,
    {
        let topic = topic.into();
        check_filters([&topic])?;
        let qos = qos.try_into().map_err(Into::into)?;
        let opts: SubscribeOptions = opts.into();
        let props = props.into().unwrap_or_default();
        self.require_v5_for_sub_opts(&opts)?;
        self.require_v5_for_props(&props)?;

        let (tok, comp) = SubscribeToken::new();
        let cmd = Command::Subscribe {
            single: true,
            filters: vec![(topic, qos, opts)],
            props,
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Err(Error::Disconnected);
        }
        Ok(tok)
    }

    /// Subscribes to a single topic with v5 options.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_subscribe_with_options`](Self::try_subscribe_with_options) —
    /// fails the token instead.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic name
    /// * `qos` The quality of service requested for messages
    /// * `opts` Options for the subscription
    /// * `props` MQTT v5 properties
    ///
    pub fn subscribe_with_options<S, Q, T, P>(
        &self,
        topic: S,
        qos: Q,
        opts: T,
        props: P,
    ) -> SubscribeToken
    where
        S: Into<String>,
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
        T: Into<SubscribeOptions>,
        P: Into<Option<Properties>>,
    {
        self.try_subscribe_with_options(topic, qos, opts, props)
            .unwrap_or_else(SubscribeToken::from_err)
    }

    /// Attempts to subscribe to multiple topics simultaneously, but returns
    /// an error immediately if the request can't be started.
    ///
    /// The failures are those of [`try_subscribe`](Self::try_subscribe), and
    /// [`Error::InvalidArgument`] if `topics` is empty or `topics` and `qos`
    /// differ in length.
    ///
    /// # Arguments
    ///
    /// * `topics` The collection of topic names
    /// * `qos` The quality of service requested for messages
    ///
    pub fn try_subscribe_many<T, Q>(&self, topics: &[T], qos: &[Q]) -> Result<SubscribeManyToken>
    where
        T: AsRef<str>,
        Q: TryInto<QoS> + Copy,
        Q::Error: Into<Error>,
    {
        self.try_subscribe_many_with_options(topics, qos, &[], None)
    }

    /// Subscribes to multiple topics simultaneously.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_subscribe_many`](Self::try_subscribe_many) — fails the token
    /// instead.
    ///
    /// # Arguments
    ///
    /// * `topics` The collection of topic names
    /// * `qos` The quality of service requested for messages
    ///
    pub fn subscribe_many<T, Q>(&self, topics: &[T], qos: &[Q]) -> SubscribeManyToken
    where
        T: AsRef<str>,
        Q: TryInto<QoS> + Copy,
        Q::Error: Into<Error>,
    {
        self.try_subscribe_many(topics, qos)
            .unwrap_or_else(SubscribeManyToken::from_err)
    }

    /// Attempts to subscribe to multiple topics simultaneously using the
    /// same QoS for all of them, but returns an error immediately if the
    /// request can't be started.
    ///
    /// The failures are those of [`try_subscribe`](Self::try_subscribe), and
    /// [`Error::InvalidArgument`] if `topics` is empty.
    ///
    /// # Arguments
    ///
    /// * `topics` The collection of topic names
    /// * `qos` The quality of service requested for all messages
    ///
    pub fn try_subscribe_many_same_qos<T, Q>(
        &self,
        topics: &[T],
        qos: Q,
    ) -> Result<SubscribeManyToken>
    where
        T: AsRef<str>,
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
    {
        let qos = qos.try_into().map_err(Into::into)?;
        let qos = vec![qos; topics.len()];
        self.try_subscribe_many(topics, &qos)
    }

    /// Subscribes to multiple topics simultaneously using the same QoS
    /// for all of them.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_subscribe_many_same_qos`](Self::try_subscribe_many_same_qos) —
    /// fails the token instead.
    ///
    /// # Arguments
    ///
    /// * `topics` The collection of topic names
    /// * `qos` The quality of service requested for all messages
    ///
    pub fn subscribe_many_same_qos<T, Q>(&self, topics: &[T], qos: Q) -> SubscribeManyToken
    where
        T: AsRef<str>,
        Q: TryInto<QoS>,
        Q::Error: Into<Error>,
    {
        self.try_subscribe_many_same_qos(topics, qos)
            .unwrap_or_else(SubscribeManyToken::from_err)
    }

    /// Attempts to subscribe to multiple topics simultaneously with
    /// options, but returns an error immediately if the request can't be
    /// started.
    ///
    /// The failures are those of
    /// [`try_subscribe_with_options`](Self::try_subscribe_with_options), and
    /// [`Error::InvalidArgument`] if `topics` is empty or `qos`, or a
    /// non-empty `opts`, differs in length from `topics`.
    ///
    /// # Arguments
    ///
    /// * `topics` The collection of topic names
    /// * `qos` The quality of service requested for messages
    /// * `opts` Subscribe options (one per topic)
    /// * `props` MQTT v5 properties
    ///
    pub fn try_subscribe_many_with_options<T, Q, P>(
        &self,
        topics: &[T],
        qos: &[Q],
        opts: &[SubscribeOptions],
        props: P,
    ) -> Result<SubscribeManyToken>
    where
        T: AsRef<str>,
        Q: TryInto<QoS> + Copy,
        Q::Error: Into<Error>,
        P: Into<Option<Properties>>,
    {
        let n = topics.len();
        if n != qos.len() {
            return Err(Error::InvalidArgument("topics/qos length mismatch".into()));
        }
        if !opts.is_empty() && opts.len() != n {
            return Err(Error::InvalidArgument("topics/opts length mismatch".into()));
        }
        check_filters(topics)?;

        let props = props.into().unwrap_or_default();
        self.require_v5_for_props(&props)?;

        let filters = topics
            .iter()
            .zip(qos.iter())
            .enumerate()
            .map(|(i, (t, q))| {
                let sub_opts = opts.get(i).copied().unwrap_or_default();
                self.require_v5_for_sub_opts(&sub_opts)?;
                let qos = (*q).try_into().map_err(Into::into)?;
                Ok((t.as_ref().to_string(), qos, sub_opts))
            })
            .collect::<Result<Vec<_>>>()?;

        let (tok, comp) = SubscribeManyToken::new();
        let cmd = Command::Subscribe {
            single: false,
            filters,
            props,
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Err(Error::Disconnected);
        }
        Ok(tok)
    }

    /// Subscribes to multiple topics simultaneously with options.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_subscribe_many_with_options`](Self::try_subscribe_many_with_options)
    /// — fails the token instead.
    ///
    /// # Arguments
    ///
    /// * `topics` The collection of topic names
    /// * `qos` The quality of service requested for messages
    /// * `opts` Subscribe options (one per topic)
    /// * `props` MQTT v5 properties
    ///
    pub fn subscribe_many_with_options<T, Q, P>(
        &self,
        topics: &[T],
        qos: &[Q],
        opts: &[SubscribeOptions],
        props: P,
    ) -> SubscribeManyToken
    where
        T: AsRef<str>,
        Q: TryInto<QoS> + Copy,
        Q::Error: Into<Error>,
        P: Into<Option<Properties>>,
    {
        self.try_subscribe_many_with_options(topics, qos, opts, props)
            .unwrap_or_else(SubscribeManyToken::from_err)
    }

    /// Attempts to unsubscribe from a single topic, but returns an error
    /// immediately if the request can't be started.
    ///
    /// That is a failure found before anything goes on the wire: a topic
    /// filter that isn't valid ([`Error::BadTopicFilter`]), or a client whose
    /// background session has shut down ([`Error::Disconnected`]).
    ///
    /// On success the returned token tracks the unsubscribe itself.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic to unsubscribe. It must match a topic from a
    ///   previous subscribe.
    ///
    pub fn try_unsubscribe<S>(&self, topic: S) -> Result<UnsubscribeToken>
    where
        S: Into<String>,
    {
        self.try_unsubscribe_with_options(topic, Properties::default())
    }

    /// Unsubscribes from a single topic.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_unsubscribe`](Self::try_unsubscribe) — fails the token instead.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic to unsubscribe. It must match a topic from a
    ///   previous subscribe.
    ///
    pub fn unsubscribe<S>(&self, topic: S) -> UnsubscribeToken
    where
        S: Into<String>,
    {
        self.try_unsubscribe(topic)
            .unwrap_or_else(UnsubscribeToken::from_err)
    }

    /// Attempts to unsubscribe from a single topic with v5 options, but
    /// returns an error immediately if the request can't be started.
    ///
    /// The failures are those of [`try_unsubscribe`](Self::try_unsubscribe),
    /// and [`Error::WrongMqttVersion`] for v5 properties on a connection of
    /// an earlier version.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic to unsubscribe.
    /// * `props` MQTT v5 properties for the unsubscribe.
    ///
    pub fn try_unsubscribe_with_options<S>(
        &self,
        topic: S,
        props: Properties,
    ) -> Result<UnsubscribeToken>
    where
        S: Into<String>,
    {
        let topic = topic.into();
        check_filters([&topic])?;
        self.require_v5_for_props(&props)?;

        let (tok, comp) = UnsubscribeToken::new();
        let cmd = Command::Unsubscribe {
            single: true,
            filters: vec![topic],
            props,
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Err(Error::Disconnected);
        }
        Ok(tok)
    }

    /// Unsubscribes from a single topic with v5 options.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_unsubscribe_with_options`](Self::try_unsubscribe_with_options)
    /// — fails the token instead.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic to unsubscribe.
    /// * `props` MQTT v5 properties for the unsubscribe.
    ///
    pub fn unsubscribe_with_options<S>(&self, topic: S, props: Properties) -> UnsubscribeToken
    where
        S: Into<String>,
    {
        self.try_unsubscribe_with_options(topic, props)
            .unwrap_or_else(UnsubscribeToken::from_err)
    }

    /// Attempts to unsubscribe from multiple topics simultaneously, but
    /// returns an error immediately if the request can't be started.
    ///
    /// The failures are those of [`try_unsubscribe`](Self::try_unsubscribe),
    /// and [`Error::InvalidArgument`] if `topics` is empty.
    ///
    /// # Arguments
    ///
    /// * `topics` The topics to unsubscribe. Each must match a topic from a
    ///   previous subscribe.
    ///
    pub fn try_unsubscribe_many<T>(&self, topics: &[T]) -> Result<UnsubscribeManyToken>
    where
        T: AsRef<str>,
    {
        self.try_unsubscribe_many_with_options(topics, Properties::default())
    }

    /// Unsubscribes from multiple topics simultaneously.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_unsubscribe_many`](Self::try_unsubscribe_many) — fails the
    /// token instead.
    ///
    /// # Arguments
    ///
    /// * `topics` The topics to unsubscribe. Each must match a topic from a
    ///   previous subscribe.
    ///
    pub fn unsubscribe_many<T>(&self, topics: &[T]) -> UnsubscribeManyToken
    where
        T: AsRef<str>,
    {
        self.try_unsubscribe_many(topics)
            .unwrap_or_else(UnsubscribeManyToken::from_err)
    }

    /// Attempts to unsubscribe from multiple topics simultaneously with v5
    /// options, but returns an error immediately if the request can't be
    /// started.
    ///
    /// The failures are those of
    /// [`try_unsubscribe_with_options`](Self::try_unsubscribe_with_options),
    /// and [`Error::InvalidArgument`] if `topics` is empty.
    ///
    /// # Arguments
    ///
    /// * `topics` The topics to unsubscribe.
    /// * `props` MQTT v5 properties for the unsubscribe.
    ///
    pub fn try_unsubscribe_many_with_options<T>(
        &self,
        topics: &[T],
        props: Properties,
    ) -> Result<UnsubscribeManyToken>
    where
        T: AsRef<str>,
    {
        check_filters(topics)?;
        self.require_v5_for_props(&props)?;

        let (tok, comp) = UnsubscribeManyToken::new();
        let filters = topics.iter().map(|t| t.as_ref().to_string()).collect();
        let cmd = Command::Unsubscribe {
            single: false,
            filters,
            props,
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Err(Error::Disconnected);
        }
        Ok(tok)
    }

    /// Unsubscribes from multiple topics simultaneously with v5 options.
    ///
    /// Returns a token to track the progress of the operation. A failure
    /// found before the request is sent — see
    /// [`try_unsubscribe_many_with_options`](Self::try_unsubscribe_many_with_options)
    /// — fails the token instead.
    ///
    /// # Arguments
    ///
    /// * `topics` The topics to unsubscribe.
    /// * `props` MQTT v5 properties for the unsubscribe.
    ///
    pub fn unsubscribe_many_with_options<T>(
        &self,
        topics: &[T],
        props: Properties,
    ) -> UnsubscribeManyToken
    where
        T: AsRef<str>,
    {
        self.try_unsubscribe_many_with_options(topics, props)
            .unwrap_or_else(UnsubscribeManyToken::from_err)
    }

    // ----- State and identity -----

    /// Determines if this client is currently connected to an MQTT broker.
    ///
    /// The same as `state() == ConnectionState::Connected`; see
    /// [`state`](Self::state).
    pub fn is_connected(&self) -> bool {
        self.state() == ConnectionState::Connected
    }

    /// Where the client's connection to the broker stands: connected,
    /// connecting, disconnecting, retrying after a lost connection, and so on.
    ///
    /// The state is kept by the client's event loop and changes as it works,
    /// so it can lag a call that has just been made — straight after
    /// `connect()` returns it may still read
    /// [`Disconnected`](ConnectionState::Disconnected) for a moment. What is
    /// guaranteed is that it has changed by the time the operation's token
    /// resolves, and before the callback or event announcing the change runs.
    pub fn state(&self) -> ConnectionState {
        self.inner.state.get()
    }

    /// Returns the client ID used for the client instance.
    ///
    /// Normally the identifier given at creation. A client that connects
    /// with an empty ID is assigned one by the server (MQTT v5 Assigned
    /// Client Identifier); from the moment that connection completes, this
    /// returns the assigned identifier, which is also what a reconnect uses.
    pub fn client_id(&self) -> String {
        match self.inner.assigned_id.read().unwrap().as_ref() {
            Some(id) => id.clone(),
            None => self.inner.opts.client_id.clone(),
        }
    }

    /// Returns the server URI the client was created with.
    ///
    /// This is the URI from the [`CreateOptions`]. A connection made with a
    /// list of servers in its [`ConnectOptions`] may be to another of them;
    /// [`current_server_uri`](Self::current_server_uri) names that one.
    pub fn server_uri(&self) -> &str {
        &self.inner.opts.server_uri
    }

    /// Returns the URI of the server the client is connected to, or `None`
    /// if it is not connected.
    ///
    /// When the connect options list several servers, this is the one that
    /// accepted the connection — after an automatic reconnect, the one that
    /// accepted that. It is the same URI as the
    /// [`ConnectResponse::server_uri`](crate::ConnectResponse::server_uri) of
    /// the connection, and is set by the time that response is delivered: when
    /// the connect token resolves, or the connected callback or
    /// [`Event::Connected`] runs.
    ///
    /// It follows [`state`](Self::state): `Some` exactly when the state is
    /// [`Connected`](ConnectionState::Connected).
    pub fn current_server_uri(&self) -> Option<String> {
        self.inner.state.server_uri()
    }

    /// Gets the most recent MQTT version for the client.
    pub fn mqtt_version(&self) -> MqttVersion {
        MqttVersion::try_from(self.mqtt_version_raw())
            .expect("stored MQTT version always originates from a valid MqttVersion")
    }

    /// The raw, integer value of the MQTT version.
    pub fn mqtt_version_raw(&self) -> u32 {
        self.inner.mqtt_version.load(Ordering::SeqCst)
    }

    /// Sets the current MQTT version.
    pub(crate) fn set_mqtt_version<V>(&self, ver: V)
    where
        V: Into<MqttVersion>,
    {
        let ver = ver.into() as u32;
        trace!("Updating client MQTT version: {}", ver);
        self.inner.mqtt_version.store(ver, Ordering::SeqCst);
    }

    /// Get access to the user-defined data in the client.
    pub fn user_data(&self) -> Option<&UserData> {
        self.inner.opts.user_data.as_ref()
    }

    /// Gets a copy of the options used in the last connection attempt.
    pub fn connect_options(&self) -> ConnectOptions {
        self.inner.conn_opts.read().unwrap().clone()
    }

    // ----- Callbacks -----

    /// Sets the callback for when the connection is established with the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///   function or a closure.
    ///
    /// The callback runs outside the lock on its slot, so it may call back
    /// into the client — including registering a different callback, or
    /// removing itself. Either sticks.
    pub fn set_connected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, &crate::ConnectResponse) + Send + 'static,
    {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_connected
            .set(Box::new(cb));
    }

    /// Removes the callback for when the connection is established.
    pub fn remove_connected_callback(&self) {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_connected
            .clear();
    }

    /// Sets the callback for when the connection is lost with the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///   function or a closure.
    ///
    /// The callback runs outside the lock on its slot, so it may call back
    /// into the client — including registering a different callback, or
    /// removing itself. Either sticks.
    pub fn set_connection_lost_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient) + Send + 'static,
    {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_connection_lost
            .set(Box::new(cb));
    }

    /// Removes the callback for when the connection is lost.
    pub fn remove_connection_lost_callback(&self) {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_connection_lost
            .clear();
    }

    /// Sets the callback for when a disconnect message arrives from the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///   function or a closure.
    ///
    /// The callback runs outside the lock on its slot, so it may call back
    /// into the client — including registering a different callback, or
    /// removing itself. Either sticks.
    pub fn set_disconnected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static,
    {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_disconnected
            .set(Box::new(cb));
    }

    /// Removes the callback for when a disconnect message is received.
    pub fn remove_disconnected_callback(&self) {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_disconnected
            .clear();
    }

    /// Sets the callback for when a message arrives from the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///   function or a closure.
    ///
    /// The callback runs outside the lock on its slot, so it may call back
    /// into the client — including registering a different callback, or
    /// removing itself. Either sticks.
    ///
    /// It is called with `None` when a connection ends: when it is lost,
    /// when the server disconnects, and when a [`disconnect`](Self::disconnect)
    /// completes. Every call comes from the client's dispatcher thread, one
    /// at a time, so the `None` follows every message that connection
    /// delivered.
    pub fn set_message_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Option<Message>) + Send + 'static,
    {
        {
            let mut cbctx = self.inner.callback_context.lock().unwrap();
            cbctx.on_message_arrived.set(Box::new(cb));
            cbctx.msg_cb_installed = true;
        }
        self.refresh_wants_messages(None);
    }

    /// Removes the callback for when a message arrives from the broker.
    ///
    /// The application's streams and consumers are unaffected: they are not
    /// callbacks, and never were.
    pub fn remove_message_callback(&self) {
        {
            let mut cbctx = self.inner.callback_context.lock().unwrap();
            cbctx.on_message_arrived.clear();
            cbctx.msg_cb_installed = false;
        }
        self.refresh_wants_messages(None);
    }

    /// Removes all registered callbacks atomically under a single lock.
    ///
    /// A callback may call this from inside itself; it is not reinstated when
    /// it returns.
    pub fn remove_callbacks(&self) {
        {
            let mut cbctx = self.inner.callback_context.lock().unwrap();
            cbctx.on_connected.clear();
            cbctx.on_connection_lost.clear();
            cbctx.on_disconnected.clear();
            cbctx.on_message_arrived.clear();
            cbctx.msg_cb_installed = false;
        }
        self.refresh_wants_messages(None);
    }

    // ----- Consumers and streams -----

    /// Starts the client consuming messages for a blocking (non-async) app.
    ///
    /// Returns the receiving-end of the channel for the application to
    /// get the messages. The channel is unbounded, so nothing is dropped
    /// and nothing is held back; a consumer that falls behind grows it.
    ///
    /// This is independent of [`set_message_callback`] and of the async
    /// streams: each receives every message. Consuming stops when the
    /// receiver is dropped, or on [`stop_consuming`].
    ///
    /// [`set_message_callback`]: AsyncClient::set_message_callback
    /// [`stop_consuming`]: AsyncClient::stop_consuming
    pub fn start_consuming(&self) -> SyncReceiver<Option<Message>> {
        let (tx, rx) = sync_channel::unbounded();
        self.update_sync_sinks(|sinks| sinks.msgs.push(tx));
        rx
    }

    /// Stops the client from consuming messages.
    ///
    /// Closes the channels handed out by [`start_consuming`], and nothing
    /// else: an application message callback, the async streams and the
    /// event consumers are left as they are.
    ///
    /// [`start_consuming`]: AsyncClient::start_consuming
    pub fn stop_consuming(&self) {
        self.update_sync_sinks(|sinks| sinks.msgs.clear());
    }

    /// Creates a synchronous event stream.
    ///
    /// Returns the receive end of a channel that receives [`Event`] values:
    /// the messages and the connection events, in one sequence.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if `buffer_lim` is `Some`, or an unbounded channel if
    /// `None`. A full bounded channel is **not** dropped into: the client
    /// waits for room, and stops reading from the broker until it has it.
    pub fn start_consuming_events<L>(&self, buffer_lim: L) -> SyncReceiver<Event>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => sync_channel::bounded(lim),
            None => sync_channel::unbounded(),
        };
        self.update_sync_sinks(|sinks| sinks.events.push(tx));
        rx
    }

    /// Creates an async message stream.
    ///
    /// The event loop delivers into it directly, so a message reaches an
    /// application waiting on the stream without passing through the
    /// callback dispatcher first.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if `buffer_lim` is `Some`, or an unbounded channel if
    /// `None`. A full bounded stream is **not** dropped into: the client
    /// stops reading from the broker until the application takes what is
    /// there, and an unacknowledged QoS 1 or 2 message stays unacknowledged
    /// meanwhile. An unbounded stream never blocks the client but grows
    /// without limit behind a consumer that falls behind.
    ///
    /// Several streams may be live at once, and each receives every
    /// message; so does a message callback, if one is registered. A stream
    /// ends when its receiver is dropped, or on [`stop_stream`].
    ///
    /// [`stop_stream`]: AsyncClient::stop_stream
    pub fn get_stream<L>(&mut self, buffer_lim: L) -> AsyncReceiver<Option<Message>>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => async_channel::bounded(lim),
            None => async_channel::unbounded(),
        };
        self.add_sink(Sink::Messages(tx));
        rx
    }

    /// Creates an async event stream.
    ///
    /// Receives the messages *and* the connection events, in one sequence,
    /// delivered by the event loop itself. The capacity and backpressure
    /// are as for [`get_stream`].
    ///
    /// [`get_stream`]: AsyncClient::get_stream
    pub fn get_event_stream<L>(&self, buffer_lim: L) -> AsyncReceiver<Event>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => async_channel::bounded(lim),
            None => async_channel::unbounded(),
        };
        self.add_sink(Sink::Events(tx));
        rx
    }

    /// Stops every stream and consumer this client feeds, closing their
    /// channels: those from [`get_stream`], [`get_event_stream`],
    /// [`start_consuming`] and [`start_consuming_events`].
    ///
    /// Callbacks the application registered are left alone — they are the
    /// application's, and nothing the library does displaces them. Use
    /// [`remove_callbacks`] to clear those.
    ///
    /// A stream also ends simply by being dropped, which needs no call at
    /// all; this is for closing one whose receiver is held elsewhere.
    ///
    /// [`get_stream`]: AsyncClient::get_stream
    /// [`get_event_stream`]: AsyncClient::get_event_stream
    /// [`start_consuming`]: AsyncClient::start_consuming
    /// [`start_consuming_events`]: AsyncClient::start_consuming_events
    /// [`remove_callbacks`]: AsyncClient::remove_callbacks
    pub fn stop_stream(&self) {
        let _ = self.inner.cmd_tx.try_send(Command::ClearSinks);
        self.update_sync_sinks(|sinks| {
            sinks.msgs.clear();
            sinks.events.clear();
        });
    }

    /// Registers an async delivery sink with the event loop, which feeds it
    /// directly — no callback, and no dispatcher thread in the way.
    fn add_sink(&self, sink: Sink) {
        if self.inner.cmd_tx.try_send(Command::AddSink(sink)).is_err() {
            warn!("Client is closed; the stream will receive nothing");
        }
    }

    /// Applies `f` to the synchronous consumers and republishes them.
    ///
    /// The list is copy-on-write: the dispatcher thread clones the `Arc`
    /// out and sends from that, so it never holds the lock while a full
    /// consumer blocks it.
    fn update_sync_sinks<F>(&self, f: F)
    where
        F: FnOnce(&mut SyncSinks),
    {
        let mut guard = self.inner.sync_sinks.write().unwrap();
        let mut next = SyncSinks {
            msgs: guard.msgs.clone(),
            events: guard.events.clone(),
        };
        f(&mut next);
        let any = !next.is_empty();
        *guard = Arc::new(next);
        self.inner.has_sync_sinks.store(any, Ordering::Release);
        drop(guard);
        // Messages reach the callbacks and these consumers through the
        // dispatcher thread; while neither exists the event loop skips it
        // and feeds the streams alone.
        self.refresh_wants_messages(Some(any));
    }

    /// Tells the event loop whether anything on this side still wants
    /// messages: a message callback, or a synchronous consumer.
    ///
    /// `sync` saves re-reading the consumer list when the caller has just
    /// rebuilt it.
    fn refresh_wants_messages(&self, sync: Option<bool>) {
        let cb = self.inner.callback_context.lock().unwrap().msg_cb_installed;
        let sync = sync.unwrap_or_else(|| !self.inner.sync_sinks.read().unwrap().is_empty());
        self.inner
            .dispatch
            .wants_messages
            .store(cb || sync, Ordering::SeqCst);
    }

    // ----- Internal helpers -----

    /// Returns `Err(WrongMqttVersion)` if v5 properties are non-empty but the
    /// current connection is not MQTT v5.
    fn require_v5_for_props(&self, props: &Properties) -> Result<()> {
        if !props.is_empty() && self.mqtt_version() < MqttVersion::V5 {
            Err(Error::WrongMqttVersion)
        } else {
            Ok(())
        }
    }

    /// Returns `Err(WrongMqttVersion)` if a DISCONNECT reason code other than
    /// `NormalDisconnection` is requested on a connection that is not MQTT v5.
    ///
    /// A v3 DISCONNECT has no body to carry one, so the codec refuses to
    /// encode it and the DISCONNECT would never be sent.
    fn require_v5_for_disconnect_reason(&self, rc: ReasonCode) -> Result<()> {
        if rc != ReasonCode::NormalDisconnection && self.mqtt_version() < MqttVersion::V5 {
            Err(Error::WrongMqttVersion)
        } else {
            Ok(())
        }
    }

    /// Returns `Err(WrongMqttVersion)` if any v5-only subscription option is
    /// set but the current connection is not MQTT v5.
    fn require_v5_for_sub_opts(&self, opts: &SubscribeOptions) -> Result<()> {
        if self.mqtt_version() < MqttVersion::V5
            && (opts.no_local
                || opts.retain_as_published
                || opts.retain_handling != crate::subscribe_options::RetainHandling::default())
        {
            Err(Error::WrongMqttVersion)
        } else {
            Ok(())
        }
    }

    /// Dispatches an event from the runtime to the registered callbacks.
    /// Sends to the synchronous consumers, outside the lock, and forgets
    /// the ones whose receiver has been dropped.
    ///
    /// A bounded consumer that is full blocks this thread until it has
    /// room, which stops the event channel draining and, through the gate,
    /// stops the session reading. That is the backpressure a blocking
    /// consumer can apply; nothing is dropped.
    fn to_sync_sinks<F>(&self, send: F)
    where
        F: FnOnce(&SyncSinks) -> DeadSinks,
    {
        // Checked before the lock: this runs for every message, and most
        // applications have no synchronous consumer at all.
        if !self.inner.has_sync_sinks.load(Ordering::Acquire) {
            return;
        }
        let sinks = self.inner.sync_sinks.read().unwrap().clone();
        if sinks.is_empty() {
            return;
        }
        let dead = send(&sinks);
        if !dead.is_empty() {
            self.update_sync_sinks(|sinks| {
                sinks
                    .msgs
                    .retain(|tx| !dead.msgs.iter().any(|d| tx.same_channel(d)));
                sinks
                    .events
                    .retain(|tx| !dead.events.iter().any(|d| tx.same_channel(d)));
            });
        }
    }

    fn dispatch_event(&self, ev: crate::runtime::Event) {
        use crate::runtime::Event as Ev;
        match ev {
            Ev::Connected(response) => {
                self.to_sync_sinks(|sinks| sinks.send_event(&Event::Connected(response.clone())));
                // Take the callback out before invoking so we don't hold the
                // lock across the call — a callback that calls set_*_callback
                // or remove_callbacks would otherwise deadlock. `restore`
                // then leaves the slot alone if it did either.
                let taken = self
                    .inner
                    .callback_context
                    .lock()
                    .unwrap()
                    .on_connected
                    .take();
                if let Some((mut cb, generation)) = taken {
                    cb(self, &response);
                    self.inner
                        .callback_context
                        .lock()
                        .unwrap()
                        .on_connected
                        .restore(cb, generation);
                }
            }
            Ev::ConnectionLost(_err) => {
                self.to_sync_sinks(|sinks| sinks.send_session_end(&Event::ConnectionLost));
                let (msg_cb, lost_cb) = {
                    let mut cbctx = self.inner.callback_context.lock().unwrap();
                    (
                        cbctx.on_message_arrived.take(),
                        cbctx.on_connection_lost.take(),
                    )
                };
                if let Some((mut cb, generation)) = msg_cb {
                    cb(self, None);
                    self.inner
                        .callback_context
                        .lock()
                        .unwrap()
                        .on_message_arrived
                        .restore(cb, generation);
                }
                if let Some((mut cb, generation)) = lost_cb {
                    cb(self);
                    self.inner
                        .callback_context
                        .lock()
                        .unwrap()
                        .on_connection_lost
                        .restore(cb, generation);
                }
            }
            Ev::Disconnected { reason_code, props } => {
                // The server closing the connection ends it just as surely
                // as losing it does, so the message consumers get their
                // `None` here too — this is the end of their connection,
                // and no `ConnectionLost` is coming to deliver it. The
                // event consumers get the reason along with it.
                self.to_sync_sinks(|sinks| {
                    sinks.send_session_end(&Event::Disconnected {
                        props: props.clone(),
                        reason_code,
                    })
                });
                let (msg_cb, disc_cb) = {
                    let mut cbctx = self.inner.callback_context.lock().unwrap();
                    (
                        cbctx.on_message_arrived.take(),
                        cbctx.on_disconnected.take(),
                    )
                };
                if let Some((mut cb, generation)) = msg_cb {
                    cb(self, None);
                    self.inner
                        .callback_context
                        .lock()
                        .unwrap()
                        .on_message_arrived
                        .restore(cb, generation);
                }
                if let Some((mut cb, generation)) = disc_cb {
                    cb(self, props, reason_code);
                    self.inner
                        .callback_context
                        .lock()
                        .unwrap()
                        .on_disconnected
                        .restore(cb, generation);
                }
            }
            // A `disconnect()` has completed: the end of the message stream.
            Ev::DisconnectDone => {
                let taken = self
                    .inner
                    .callback_context
                    .lock()
                    .unwrap()
                    .on_message_arrived
                    .take();
                if let Some((mut cb, generation)) = taken {
                    cb(self, None);
                    self.inner
                        .callback_context
                        .lock()
                        .unwrap()
                        .on_message_arrived
                        .restore(cb, generation);
                }
            }
            Ev::MessageArrived(msg) => {
                let taken = self
                    .inner
                    .callback_context
                    .lock()
                    .unwrap()
                    .on_message_arrived
                    .take();
                // The consumers first, so that a callback which blocks
                // cannot hold them up; neither displaces the other. The
                // message is copied only when both want it — with just one
                // of them, which is the usual case, it is handed over.
                let msg = match (
                    self.inner.has_sync_sinks.load(Ordering::Acquire),
                    taken.is_some(),
                ) {
                    (true, true) => {
                        self.to_sync_sinks(|sinks| sinks.send_message(msg.clone()));
                        msg
                    }
                    (true, false) => {
                        self.to_sync_sinks(|sinks| sinks.send_message(msg));
                        return;
                    }
                    (false, true) => msg,
                    (false, false) => return,
                };
                if let Some((mut cb, generation)) = taken {
                    cb(self, Some(msg));
                    self.inner
                        .callback_context
                        .lock()
                        .unwrap()
                        .on_message_arrived
                        .restore(cb, generation);
                }
            }
            Ev::TlsError(err) => {
                // Clone the Arc out of the RwLock before invoking so we don't
                // hold the lock across the (potentially blocking) callback.
                let cb = self
                    .inner
                    .conn_opts
                    .read()
                    .unwrap()
                    .tls
                    .as_ref()
                    .and_then(|tls| tls.on_tls_error.clone());
                if let Some(cb) = cb {
                    cb(self, &err);
                }
            }
        }
    }
}

/// Extracts `(host, port, path)` from the authority+path portion of a URI.
///
/// `rest` is the URI with its scheme prefix stripped (e.g. `"broker:1883/mqtt"`).
/// `default_port` is used when no explicit port appears.
///
/// The authority ends at the first `/`, `?` or `#` (RFC 3986 §3.2). What
/// follows it is the request target of a WebSocket connection (`http_target`):
/// its path and query, `"/"` when there are none. A plain MQTT connection has
/// no request to put one in, so there it must be empty or a bare `/`. A
/// fragment is refused either way, as HTTP never sends one, and so is user
/// information (`user@host`): MQTT credentials go in the CONNECT packet,
/// from the connect options, and nothing reads them from the URI.
///
/// The host may be an IPv6 address in brackets (`[::1]:1883`), and is
/// returned without them; see [`split_host_port`].
fn parse_host_port_path(
    rest: &str,
    default_port: u16,
    http_target: bool,
) -> Result<(String, u16, String)> {
    let bad = |why: &str| Error::InvalidArgument(format!("{why} (server URI: {rest})"));

    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    if authority.contains('@') {
        return Err(bad(
            "credentials in a server URI are not used; set the user name and password \
             in the connect options",
        ));
    }
    if tail.contains('#') {
        return Err(bad("a server URI cannot have a fragment ('#')"));
    }
    let path = match tail {
        "" => "/".to_string(),
        t if t.starts_with('?') => format!("/{t}"),
        t => t.to_string(),
    };
    if !http_target && path != "/" {
        return Err(bad(
            "only a WebSocket server URI can have a path or query; \
             an MQTT connection has nowhere to send one",
        ));
    }

    let (host, port) = split_host_port(authority, default_port)
        .map_err(|e| Error::InvalidArgument(format!("{e} (server URI: {rest})")))?;
    Ok((host, port, path))
}

/// Parses an MQTT URI into a [`ServerAddr`].
///
/// Recognises:
/// - `unix://` or `unix:` — Unix-domain socket path
/// - `mqtt://`, `tcp://` — plain TCP (default port 1883)
/// - `ws://` — WebSocket over TCP (default port 80, HTTP's); [`ServerAddr::Ws`] when
///   the `websocket` feature is enabled, plain TCP otherwise
/// - `mqtts://`, `ssl://`, `tls://` — TLS TCP (default port 8883)
/// - `wss://` — WebSocket over TLS (default port 443, HTTPS's); [`ServerAddr::Wss`]
///   when both `websocket` and `tls` features are enabled, TLS TCP otherwise
///
/// If no explicit port is present the default for the scheme is used. An
/// IPv6 host is written in brackets (`tcp://[::1]:1883`) and held without
/// them.
fn parse_server_uri(uri: &str) -> Result<ServerAddr> {
    // Unix-domain socket: unix:///path/to/sock or unix:/path/to/sock
    #[cfg(unix)]
    if let Some(rest) = uri
        .strip_prefix("unix://")
        .or_else(|| uri.strip_prefix("unix:"))
    {
        return Ok(ServerAddr::Unix {
            path: rest.to_string(),
        });
    }

    // WebSocket transports (feature-gated).
    #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
    if let Some(rest) = uri.strip_prefix("ws://") {
        // A WebSocket URI is an HTTP one (RFC 6455 §3), so its default
        // port is HTTP's, as it was in v1.x.
        let (host, port, path) = parse_host_port_path(rest, 80, true)?;
        return Ok(ServerAddr::Ws { host, port, path });
    }

    #[cfg(any(
        all(feature = "websocket", any(feature = "tls", feature = "openssl")),
        all(feature = "smol-websocket", feature = "smol-tls")
    ))]
    if let Some(rest) = uri.strip_prefix("wss://") {
        let (host, port, path) = parse_host_port_path(rest, 443, true)?;
        return Ok(ServerAddr::Wss { host, port, path });
    }

    // wss:// with websocket feature but without a TLS backend is an error.
    #[cfg(any(
        all(feature = "websocket", not(any(feature = "tls", feature = "openssl"))),
        all(feature = "smol-websocket", not(feature = "smol-tls"))
    ))]
    if uri.starts_with("wss://") {
        return Err(Error::TlsNotSupported);
    }

    // Without a WebSocket feature these schemes must not fall through to the
    // raw TCP/TLS parsing below, which would speak plain MQTT to an HTTP
    // listener.
    #[cfg(not(any(feature = "websocket", feature = "smol-websocket")))]
    if uri.starts_with("ws://") || uri.starts_with("wss://") {
        return Err(Error::WebSocketNotSupported);
    }
    // Plain TCP / TLS transports (wss:// falls through to TLS when websocket
    // feature is absent).
    let (default_port, is_tls, rest) = if let Some(s) = uri
        .strip_prefix("mqtts://")
        .or_else(|| uri.strip_prefix("ssl://"))
        .or_else(|| uri.strip_prefix("tls://"))
    {
        (8883u16, true, s)
    } else if let Some(s) = uri
        .strip_prefix("mqtt://")
        .or_else(|| uri.strip_prefix("tcp://"))
    {
        (1883u16, false, s)
    } else {
        (1883u16, false, uri)
    };

    let (host, port, _) = parse_host_port_path(rest, default_port, false)?;

    if is_tls {
        #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
        return Ok(ServerAddr::Tls { host, port });
        #[cfg(not(any(feature = "tls", feature = "openssl", feature = "smol-tls")))]
        return Err(Error::TlsNotSupported);
    }

    Ok(ServerAddr::Tcp { host, port })
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_options::CreateOptionsBuilder;
    use std::sync::{Arc, Mutex, RwLock};

    // Makes sure than when a client is moved, the inner struct stays at
    // the same address (on the heap) since that inner struct is used as
    // the context pointer for callbacks.
    // GitHub Issue #17
    #[test]
    fn test_context() {
        let cli = AsyncClient::new("mqtt://localhost:1883").unwrap();
        cli.set_message_callback(|_, _| {});

        let pctx = {
            let ctx: &InnerAsyncClient = &cli.inner;
            ctx as *const _ as usize
        };

        let new_cli = cli;
        let new_pctx = {
            let ctx: &InnerAsyncClient = &new_cli.inner;
            ctx as *const _ as usize
        };

        assert_eq!(pctx, new_pctx);
    }

    #[test]
    fn test_create() {
        let cli = AsyncClient::new("mqtt://localhost:1883");
        assert!(cli.is_ok(), "Error creating async client");
    }

    // A client built from options carries the identifier it was given.
    // Whether that identifier reaches the broker is a question for
    // `test_connect_uses_configured_client_id` in tests/integration.rs.
    #[test]
    fn test_with_client_id() {
        let options = CreateOptionsBuilder::new().client_id("test1").finalize();
        let cli = AsyncClient::new(options).expect("creating a client with a client_id");
        assert_eq!(cli.client_id(), "test1");
    }

    // Test immutable user data without any lock
    #[test]
    fn test_user_data() {
        const DATA_STR: &str = "Hello world!";

        let cli = CreateOptionsBuilder::new()
            .server_uri("mqtt://localhost:1883")
            .user_data(Box::new(DATA_STR))
            .create_async_client()
            .unwrap();

        let data = cli.user_data();

        assert!(data.is_some());
        assert_eq!(&DATA_STR, data.unwrap().downcast_ref::<&str>().unwrap());
    }

    // Test writable user data using a mutex.
    #[test]
    fn test_locked_user_data() {
        let data_vec = vec!["zero", "one", "two"];
        let data = Box::new(Mutex::new(data_vec));

        let cli = CreateOptionsBuilder::new()
            .server_uri("mqtt://localhost:1883")
            .user_data(data)
            .create_async_client()
            .unwrap();

        let data = cli.user_data();
        assert!(data.is_some());

        let lock = data.unwrap().downcast_ref::<Mutex<Vec<&str>>>().unwrap();
        let mut v = lock.lock().unwrap();
        assert_eq!(3, v.len());
        assert_eq!("zero", v[0]);
        assert_eq!("one", v[1]);
        assert_eq!("two", v[2]);

        v.push("three");
        assert_eq!(4, v.len());
        assert_eq!("three", v[3]);
    }

    #[test]
    fn test_rw_user_data() {
        let data_vec = vec!["zero", "one", "two"];
        let data = Box::new(RwLock::new(data_vec));

        let cli = CreateOptionsBuilder::new()
            .server_uri("mqtt://localhost:1883")
            .user_data(data)
            .create_async_client()
            .unwrap();

        let data = cli.user_data();
        assert!(data.is_some());
        let data = data.unwrap();

        let lock = data.downcast_ref::<RwLock<Vec<&str>>>().unwrap();
        {
            let v = lock.read().unwrap();
            assert_eq!(3, v.len());
            assert_eq!("zero", v[0]);
            assert_eq!("one", v[1]);
            assert_eq!("two", v[2]);
        }
        {
            let mut v = lock.write().unwrap();
            v.push("three");
            assert_eq!(4, v.len());
            assert_eq!("three", v[3]);
        }
    }

    // Determine that a client can be sent across threads.
    #[test]
    fn test_send() {
        let cli = AsyncClient::new("mqtt://localhost:1883").unwrap();
        let thr = thread::spawn(move || {
            assert!(!cli.is_connected());
        });
        thr.join().unwrap();
    }

    // Determine that a client can be shared across threads using an Arc.
    #[test]
    fn test_send_arc() {
        let cli = AsyncClient::new("mqtt://localhost:1883").unwrap();

        let cli = Arc::new(cli);
        let cli2 = cli.clone();

        let thr = thread::spawn(move || {
            assert!(!cli.is_connected());
        });
        assert!(!cli2.is_connected());
        thr.join().unwrap();
    }

    #[test]
    fn test_get_client_id() {
        let c_id = "test_client_id";
        let options = CreateOptionsBuilder::new().client_id(c_id).finalize();
        let cli = AsyncClient::new(options);
        assert!(cli.is_ok(), "Error in creating async client with client_id");
        let cli = cli.unwrap();
        let retrieved = cli.client_id();
        assert_eq!(retrieved, c_id);
    }

    #[test]
    fn test_get_server_uri() {
        let server_uri = "mqtt://localhost:1883";
        let cli = CreateOptionsBuilder::new()
            .server_uri(server_uri)
            .create_async_client();
        assert!(
            cli.is_ok(),
            "Error in creating async client with server_uri"
        );
        let cli = cli.unwrap();
        let retrieved = cli.server_uri();
        assert_eq!(retrieved, server_uri);
    }

    #[test]
    fn test_file_persistence_needs_a_client_id() {
        let create = |id: &str, persistence: PersistenceType| {
            AsyncClient::new(
                CreateOptionsBuilder::new()
                    .server_uri("mqtt://localhost:1883")
                    .client_id(id)
                    .persistence(persistence)
                    .finalize(),
            )
        };
        let dir = std::env::temp_dir().join("paho-test-no-id");

        for persistence in [
            PersistenceType::File,
            PersistenceType::FilePath(dir.clone()),
        ] {
            assert!(matches!(
                create("", persistence),
                Err(Error::InvalidArgument(msg)) if msg.contains("client ID")
            ));
        }

        // No store, or one of the application's own, is its business.
        assert!(create("", PersistenceType::None).is_ok());
        let mem: Box<dyn ClientPersistence> = Box::new(crate::MemPersistence::new());
        assert!(create("", PersistenceType::User(mem.into())).is_ok());

        // And with an ID the file store is fine. Nothing is written until a
        // connect opens it.
        assert!(create("an-id", PersistenceType::FilePath(dir)).is_ok());
    }

    /// `ApplicationRuntime` with no runtime to borrow is an error from
    /// `new`, as the rest of its configuration errors are. It used to panic
    /// in the runtime layer. With tokio, called from within a runtime, it
    /// borrows that one.
    #[test]
    fn test_application_runtime_needs_one() {
        let opts = || {
            crate::CreateOptionsBuilder::new()
                .server_uri("tcp://localhost:1883")
                .use_application_runtime()
                .finalize()
        };
        assert!(matches!(
            AsyncClient::new(opts()),
            Err(Error::InvalidArgument(msg)) if msg.contains("ApplicationRuntime")
        ));

        #[cfg(feature = "tokio")]
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                AsyncClient::new(opts()).expect("borrows the current runtime");
            });
    }

    /// A client created with a secure URI needs TLS options to connect, as
    /// one given secure URIs in its connect options already did. It used
    /// to get no further than the transport, on every attempt.
    #[test]
    fn test_secure_creation_uri_needs_tls_options() {
        for uri in [
            "ssl://localhost:8883",
            "mqtts://localhost",
            "tls://localhost",
        ] {
            let cli = AsyncClient::new(uri).unwrap();
            let res = cli.try_connect(None);
            #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
            assert!(matches!(res, Err(Error::MissingTlsOptions)), "{uri}");
            // Without a TLS backend, that is the problem to report.
            #[cfg(not(any(feature = "tls", feature = "openssl", feature = "smol-tls")))]
            assert!(matches!(res, Err(Error::TlsNotSupported)), "{uri}");
            // Refused before anything changed.
            assert!(!cli.connect_options().has_tls_options());
        }

        // The connect options' own list takes the creation URI's place.
        let cli = AsyncClient::new("ssl://localhost:8883").unwrap();
        let opts = crate::ConnectOptionsBuilder::new()
            .server_uris(&["tcp://localhost:1"])
            .connect_timeout(Duration::from_millis(100))
            .finalize();
        assert!(cli.try_connect(opts).is_ok());

        #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
        {
            let cli = AsyncClient::new("ssl://localhost:1").unwrap();
            let opts = crate::ConnectOptionsBuilder::new()
                .tls_options(crate::TlsOptions::default())
                .connect_timeout(Duration::from_millis(100))
                .finalize();
            assert!(
                cli.try_connect(opts).is_ok(),
                "with TLS options it goes ahead"
            );
        }
    }

    #[test]
    fn test_bad_arguments_are_invalid_argument() {
        assert!(matches!(
            parse_server_uri("tcp://localhost:notaport"),
            Err(Error::InvalidArgument(msg)) if msg.contains("notaport")
        ));

        let cli = AsyncClient::new("mqtt://localhost:1883").unwrap();
        assert!(matches!(
            cli.subscribe_many(&["a", "b"], &[QoS::QoS1]).wait(),
            Err(Error::InvalidArgument(_))
        ));
    }

    // Checked by `connect()` itself. The server is plain TCP, whose transport
    // never reads the proxy options, so without that check this would fail
    // only as a refused connection.
    #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
    #[test]
    fn test_connect_rejects_a_bad_proxy_uri() {
        let cli = AsyncClient::new("tcp://localhost:1").unwrap();
        for opts in [
            crate::ConnectOptionsBuilder::new_ws()
                .http_proxy("http://proxy:notaport")
                .finalize(),
            crate::ConnectOptionsBuilder::new_ws()
                .https_proxy("http://")
                .finalize(),
            crate::ConnectOptionsBuilder::new_ws()
                .http_proxy("http://user:p%zz@proxy:3128")
                .finalize(),
        ] {
            assert!(matches!(
                cli.connect(opts).wait_for(Duration::from_secs(1)),
                Err(Error::InvalidArgument(msg)) if msg.contains("proxy")
            ));
        }

        // So is an HTTP header that HTTP does not allow, which the
        // WebSocket request used to drop.
        let opts = crate::ConnectOptionsBuilder::new_ws()
            .http_headers_from_coll(&[("Bad Name", "x")])
            .finalize();
        assert!(matches!(
            cli.connect(opts).wait_for(Duration::from_secs(1)),
            Err(Error::InvalidArgument(msg)) if msg.contains("header")
        ));
    }

    #[test]
    fn test_parse_server_uri() {
        assert!(matches!(
            parse_server_uri("mqtt://localhost:1883").unwrap(),
            ServerAddr::Tcp { host, port } if host == "localhost" && port == 1883
        ));
        // IPv6 literals, held without their brackets.
        assert!(matches!(
            parse_server_uri("tcp://[::1]:1884").unwrap(),
            ServerAddr::Tcp { host, port } if host == "::1" && port == 1884
        ));
        assert!(matches!(
            parse_server_uri("tcp://[2001:db8::7]").unwrap(),
            ServerAddr::Tcp { host, port } if host == "2001:db8::7" && port == 1883
        ));
        #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
        assert!(matches!(
            parse_server_uri("ssl://[::1]").unwrap(),
            ServerAddr::Tls { host, port } if host == "::1" && port == 8883
        ));
        #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
        assert!(matches!(
            parse_server_uri("ws://[::1]:9001/mqtt").unwrap(),
            ServerAddr::Ws { host, port, path } if host == "::1" && port == 9001 && path == "/mqtt"
        ));
        // Where the authority ends, and what may follow it.
        let invalid = |uri: &str, why: &str| {
            assert!(
                matches!(parse_server_uri(uri), Err(Error::InvalidArgument(ref m)) if m.contains(why)),
                "{uri}: {:?}",
                parse_server_uri(uri).err()
            );
        };
        assert!(matches!(
            parse_server_uri("tcp://broker:1883/").unwrap(),
            ServerAddr::Tcp { port: 1883, .. }
        ));
        invalid("tcp://broker:1883?x", "WebSocket");
        invalid("tcp://broker/mqtt", "WebSocket");
        invalid("tcp://broker#frag", "fragment");
        invalid("tcp://user@broker", "credentials");
        invalid("mqtt://user:pass@broker:1883", "credentials");
        #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
        {
            let ws_path = |uri: &str| match parse_server_uri(uri).unwrap() {
                ServerAddr::Ws { host, port, path } => (host, port, path),
                _ => panic!("{uri}"),
            };
            assert_eq!(
                ws_path("ws://broker:9001?x"),
                ("broker".into(), 9001, "/?x".into())
            );
            assert_eq!(
                ws_path("ws://broker/mqtt?a=1&b=2"),
                ("broker".into(), 80, "/mqtt?a=1&b=2".into())
            );
            assert_eq!(ws_path("ws://[::1]?x"), ("::1".into(), 80, "/?x".into()));
            assert_eq!(
                ws_path("ws://broker/a@b"),
                ("broker".into(), 80, "/a@b".into())
            );
            invalid("ws://broker/mqtt#frag", "fragment");
            invalid("ws://user@broker/mqtt", "credentials");
        }
        for bad in ["tcp://::1:1883", "tcp://[::1", "tcp://", "tcp://:1883"] {
            assert!(
                matches!(parse_server_uri(bad), Err(Error::InvalidArgument(_))),
                "{bad}"
            );
        }
        assert!(matches!(
            parse_server_uri("tcp://broker.example.com:1883").unwrap(),
            ServerAddr::Tcp { host, port } if host == "broker.example.com" && port == 1883
        ));
        #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
        assert!(matches!(
            parse_server_uri("mqtts://broker.example.com:8883").unwrap(),
            ServerAddr::Tls { host, port } if host == "broker.example.com" && port == 8883
        ));
        #[cfg(not(any(feature = "tls", feature = "openssl", feature = "smol-tls")))]
        assert!(matches!(
            parse_server_uri("mqtts://broker.example.com:8883"),
            Err(Error::TlsNotSupported)
        ));
        assert!(matches!(
            parse_server_uri("mqtt://localhost").unwrap(),
            ServerAddr::Tcp { host, port } if host == "localhost" && port == 1883
        ));
        #[cfg(any(feature = "tls", feature = "openssl", feature = "smol-tls"))]
        assert!(matches!(
            parse_server_uri("mqtts://secure.broker.io").unwrap(),
            ServerAddr::Tls { host, port } if host == "secure.broker.io" && port == 8883
        ));
        #[cfg(not(any(feature = "tls", feature = "openssl", feature = "smol-tls")))]
        assert!(matches!(
            parse_server_uri("mqtts://secure.broker.io"),
            Err(Error::TlsNotSupported)
        ));
        // Without the websocket feature ws:// and wss:// are refused rather
        // than silently downgraded to raw TCP / TLS.
        #[cfg(not(any(feature = "websocket", feature = "smol-websocket")))]
        {
            assert!(matches!(
                parse_server_uri("ws://localhost:8080/mqtt"),
                Err(Error::WebSocketNotSupported)
            ));
            assert!(matches!(
                parse_server_uri("wss://localhost:8081/mqtt"),
                Err(Error::WebSocketNotSupported)
            ));
        }
        // With the websocket feature ws:// maps to a WebSocket address.
        #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
        assert!(matches!(
            parse_server_uri("ws://localhost:8080/mqtt").unwrap(),
            ServerAddr::Ws { ref host, port, ref path }
                if host == "localhost" && port == 8080 && path == "/mqtt"
        ));
        // A WebSocket URI without a port takes HTTP's, 80, or HTTPS's, 443,
        // as in v1.x — not MQTT's.
        #[cfg(any(feature = "websocket", feature = "smol-websocket"))]
        assert!(matches!(
            parse_server_uri("ws://broker/mqtt").unwrap(),
            ServerAddr::Ws { port: 80, .. }
        ));
        #[cfg(any(
            all(feature = "websocket", any(feature = "tls", feature = "openssl")),
            all(feature = "smol-websocket", feature = "smol-tls")
        ))]
        {
            assert!(matches!(
                parse_server_uri("wss://broker/mqtt").unwrap(),
                ServerAddr::Wss { port: 443, .. }
            ));
            assert!(matches!(
                parse_server_uri("wss://broker:8884/mqtt").unwrap(),
                ServerAddr::Wss { port: 8884, .. }
            ));
        }
        #[cfg(unix)]
        assert!(matches!(
            parse_server_uri("unix:///tmp/mqtt.sock").unwrap(),
            ServerAddr::Unix { path } if path == "/tmp/mqtt.sock"
        ));
        #[cfg(unix)]
        assert!(matches!(
            parse_server_uri("unix:/tmp/mqtt.sock").unwrap(),
            ServerAddr::Unix { path } if path == "/tmp/mqtt.sock"
        ));
    }

    // ----------------------------------------------------------------------
    // A callback runs outside the lock on its own slot, so it may call back
    // into the client. What it does there must stick.
    // ----------------------------------------------------------------------

    /// A client that talks to nothing. `dispatch_event` is driven directly,
    /// so no broker, socket or event loop is involved.
    fn test_client() -> AsyncClient {
        AsyncClient::new("mqtt://localhost:1883").unwrap()
    }

    fn counter() -> Arc<Mutex<u32>> {
        Arc::new(Mutex::new(0))
    }

    /// A callback that removes itself stays removed. The dispatcher takes the
    /// callback out of its slot to run it, and used to put it back whenever
    /// the slot was empty afterwards — which is exactly the state a
    /// self-removal leaves behind, so the removal was undone.
    #[test]
    fn test_connected_callback_can_remove_itself() {
        let cli = test_client();
        let calls = counter();

        let c = calls.clone();
        cli.set_connected_callback(move |cli, _| {
            *c.lock().unwrap() += 1;
            cli.remove_connected_callback();
        });

        let resp = crate::ConnectResponse::default();
        cli.dispatch_event(crate::runtime::Event::Connected(resp.clone()));
        cli.dispatch_event(crate::runtime::Event::Connected(resp));

        assert_eq!(*calls.lock().unwrap(), 1, "the callback was reinstated");
        assert!(
            cli.inner
                .callback_context
                .lock()
                .unwrap()
                .on_connected
                .cb
                .is_none()
        );
    }

    /// The same for the message callback, which has the same shape.
    #[test]
    fn test_message_callback_can_remove_itself() {
        let cli = test_client();
        let calls = counter();

        let c = calls.clone();
        cli.set_message_callback(move |cli, _| {
            *c.lock().unwrap() += 1;
            cli.remove_message_callback();
        });

        let msg = Message::new("t", "p", crate::QOS_0);
        cli.dispatch_event(crate::runtime::Event::MessageArrived(msg.clone()));
        cli.dispatch_event(crate::runtime::Event::MessageArrived(msg));

        assert_eq!(*calls.lock().unwrap(), 1);
    }

    /// `remove_callbacks()` from inside a callback removes that one too.
    #[test]
    fn test_callback_can_remove_all_callbacks() {
        let cli = test_client();
        let calls = counter();

        let c = calls.clone();
        cli.set_disconnected_callback(move |cli, _, _| {
            *c.lock().unwrap() += 1;
            cli.remove_callbacks();
        });

        for _ in 0..2 {
            cli.dispatch_event(crate::runtime::Event::Disconnected {
                reason_code: ReasonCode::NormalDisconnection,
                props: Properties::new(),
            });
        }
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    /// A callback that replaces itself hands over to the new one, which is
    /// not then overwritten by the one that just ran.
    #[test]
    fn test_callback_can_replace_itself() {
        let cli = test_client();
        let first = counter();
        let second = counter();

        let f = first.clone();
        let s = second.clone();
        cli.set_connected_callback(move |cli, _| {
            *f.lock().unwrap() += 1;
            let s = s.clone();
            cli.set_connected_callback(move |_, _| {
                *s.lock().unwrap() += 1;
            });
        });

        let resp = crate::ConnectResponse::default();
        cli.dispatch_event(crate::runtime::Event::Connected(resp.clone()));
        cli.dispatch_event(crate::runtime::Event::Connected(resp.clone()));
        cli.dispatch_event(crate::runtime::Event::Connected(resp));

        assert_eq!(*first.lock().unwrap(), 1, "the first callback ran again");
        assert_eq!(*second.lock().unwrap(), 2);
    }

    /// A callback that touches a *different* slot keeps its own place.
    #[test]
    fn test_callback_setting_another_slot_keeps_its_own() {
        let cli = test_client();
        let calls = counter();

        let c = calls.clone();
        cli.set_connected_callback(move |cli, _| {
            *c.lock().unwrap() += 1;
            cli.set_message_callback(|_, _| {});
            cli.remove_disconnected_callback();
        });

        let resp = crate::ConnectResponse::default();
        cli.dispatch_event(crate::runtime::Event::Connected(resp.clone()));
        cli.dispatch_event(crate::runtime::Event::Connected(resp));

        assert_eq!(*calls.lock().unwrap(), 2, "it removed itself by accident");
    }

    /// A callback that does nothing to its slot is still there next time.
    #[test]
    fn test_untouched_callback_is_restored() {
        let cli = test_client();
        let calls = counter();

        let c = calls.clone();
        cli.set_connected_callback(move |_, _| {
            *c.lock().unwrap() += 1;
        });

        let resp = crate::ConnectResponse::default();
        for _ in 0..3 {
            cli.dispatch_event(crate::runtime::Event::Connected(resp.clone()));
        }
        assert_eq!(*calls.lock().unwrap(), 3);
    }

    /// `disconnect()` ends the message stream by calling the message callback
    /// with `None`. That call used to happen while holding the callback lock,
    /// so a callback touching any slot deadlocked; it now runs outside it.
    ///
    /// It also comes from the dispatcher thread, once the event loop has
    /// finished the disconnect, rather than from the caller's thread, where
    /// it raced the dispatcher: a message arriving meanwhile could find the
    /// slot empty and be dropped, though already acknowledged.
    #[test]
    fn test_message_callback_may_touch_the_client_during_disconnect() {
        let cli = test_client();
        let calls = counter();
        let caller = std::thread::current().id();

        let c = calls.clone();
        cli.set_message_callback(move |cli, msg| {
            assert!(msg.is_none(), "disconnect signals the end of the stream");
            assert_ne!(std::thread::current().id(), caller, "on the dispatcher");
            cli.remove_message_callback();
            *c.lock().unwrap() += 1;
        });

        cli.disconnect(None).wait().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while *calls.lock().unwrap() == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(*calls.lock().unwrap(), 1);

        // It removed itself, so a second disconnect calls nothing.
        cli.disconnect(None).wait().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    // ----------------------------------------------------------------------
    // try_connect reports a failure to start the connect synchronously
    // ----------------------------------------------------------------------

    /// A server URI that can't be parsed is caught before anything is sent.
    #[test]
    fn test_try_connect_rejects_a_bad_server_uri() {
        let cli = test_client();
        let opts = crate::ConnectOptionsBuilder::new()
            .server_uris(&["tcp://localhost:notaport"])
            .finalize();
        assert!(matches!(
            cli.try_connect(opts),
            Err(Error::InvalidArgument(msg)) if msg.contains("notaport")
        ));
    }

    /// With the command channel closed the connect can't be queued.
    #[test]
    fn test_try_connect_fails_when_disconnected() {
        let cli = test_client();
        assert!(cli.inner.cmd_tx.close(), "the channel was already closed");
        assert!(matches!(cli.try_connect(None), Err(Error::Disconnected)));
    }

    /// A refused connect leaves the client's version as it was, rather than
    /// switching it to the version of options that were never used.
    #[test]
    fn test_try_connect_failure_keeps_the_mqtt_version() {
        let cli = test_client();
        let before = cli.mqtt_version();
        let other = if before == MqttVersion::V5 {
            MqttVersion::V3_1_1
        } else {
            MqttVersion::V5
        };
        let opts = crate::ConnectOptionsBuilder::with_mqtt_version(other)
            .server_uris(&["tcp://localhost:notaport"])
            .finalize();
        assert!(cli.try_connect(opts).is_err());
        assert_eq!(cli.mqtt_version(), before);
    }

    /// `try_reconnect` reports the same synchronous failures.
    #[test]
    fn test_try_reconnect_fails_when_disconnected() {
        let cli = test_client();
        assert!(cli.inner.cmd_tx.close());
        assert!(matches!(cli.try_reconnect(), Err(Error::Disconnected)));
    }

    /// A v5 reason code can't go on a v3 connection, so the disconnect is
    /// refused before it is queued, and the message stream is not ended.
    #[test]
    fn test_try_disconnect_rejects_a_v5_reason_on_v3() {
        let cli = AsyncClient::new(
            CreateOptionsBuilder::new()
                .server_uri("mqtt://localhost:1883")
                .mqtt_version(MqttVersion::V3_1_1)
                .finalize(),
        )
        .unwrap();
        let calls = counter();
        let c = calls.clone();
        cli.set_message_callback(move |_, _| *c.lock().unwrap() += 1);

        let opts = crate::DisconnectOptionsBuilder::new()
            .reason_code(ReasonCode::DisconnectWithWillMessage)
            .finalize();
        assert!(matches!(
            cli.try_disconnect(opts),
            Err(Error::WrongMqttVersion)
        ));
        assert_eq!(*calls.lock().unwrap(), 0, "the stream was ended");
    }

    /// With the command channel closed the disconnect can't be queued.
    #[test]
    fn test_try_disconnect_fails_when_disconnected() {
        let cli = test_client();
        assert!(cli.inner.cmd_tx.close());
        assert!(matches!(cli.try_disconnect(None), Err(Error::Disconnected)));
        let err = cli.disconnect(None).wait().unwrap_err();
        assert!(matches!(err, Error::Disconnected), "{err}");
    }

    /// A QoS that isn't valid is caught before anything is queued.
    #[test]
    fn test_try_subscribe_rejects_a_bad_qos() {
        let cli = test_client();
        assert!(cli.try_subscribe("a", 3).is_err());
        assert!(cli.try_subscribe_many_same_qos(&["a", "b"], 3).is_err());
        assert!(cli.try_subscribe_many(&["a", "b"], &[1, 3]).is_err());
    }

    /// Mismatched slices are refused with `InvalidArgument`.
    #[test]
    fn test_try_subscribe_many_rejects_mismatched_lengths() {
        let cli = test_client();
        assert!(matches!(
            cli.try_subscribe_many(&["a", "b"], &[QoS::QoS1]),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            cli.try_subscribe_many_with_options(
                &["a", "b"],
                &[QoS::QoS1, QoS::QoS1],
                &[SubscribeOptions::default()],
                None
            ),
            Err(Error::InvalidArgument(_))
        ));
    }

    /// A filter that breaks the rules, or an empty list, is refused before
    /// anything is queued, by every (un)subscribe call. Each used to reach
    /// the session and end the connection.
    #[test]
    fn test_try_subscribe_and_unsubscribe_check_filters() {
        let cli = test_client();
        let bad = |r: Result<_>| matches!(r, Err(Error::BadTopicFilter));
        let empty = |r: Result<_>| matches!(r, Err(Error::InvalidArgument(_)));
        let none: [&str; 0] = [];

        for f in ["", "a/#/b", "a#", "a+/b", "$share//a", "a\0b"] {
            assert!(bad(cli.try_subscribe(f, 1).map(drop)), "{f:?}");
            assert!(bad(cli.try_unsubscribe(f).map(drop)), "{f:?}");
            assert!(bad(cli.try_subscribe_many(&["ok", f], &[1, 1]).map(drop)));
            assert!(bad(cli.try_unsubscribe_many(&["ok", f]).map(drop)));
        }
        assert!(empty(
            cli.try_subscribe_many(&none, &[] as &[i32]).map(drop)
        ));
        assert!(empty(cli.try_subscribe_many_same_qos(&none, 1).map(drop)));
        assert!(empty(cli.try_unsubscribe_many(&none).map(drop)));

        // The token-returning calls fail their tokens the same way.
        let err = cli.subscribe("a/#/b", 1).wait().unwrap_err();
        assert!(matches!(err, Error::BadTopicFilter), "{err}");
        let err = cli.unsubscribe_many(&none).wait().unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err}");

        // Wildcards and shared subscriptions in their places are fine.
        for f in ["#", "+", "a/+/b", "a/#", "$share/g/a/#", "/"] {
            assert!(cli.try_subscribe(f, 1).is_ok(), "{f:?}");
        }
    }

    /// v5 subscribe options can't go on a v3 connection.
    #[test]
    fn test_try_subscribe_rejects_v5_options_on_v3() {
        let cli = AsyncClient::new(
            CreateOptionsBuilder::new()
                .server_uri("mqtt://localhost:1883")
                .mqtt_version(MqttVersion::V3_1_1)
                .finalize(),
        )
        .unwrap();
        let opts = SubscribeOptions {
            no_local: true,
            ..Default::default()
        };
        assert!(matches!(
            cli.try_subscribe_with_options("a", QoS::QoS1, opts, None),
            Err(Error::WrongMqttVersion)
        ));
        assert!(matches!(
            cli.try_subscribe_many_with_options(&["a"], &[QoS::QoS1], &[opts], None),
            Err(Error::WrongMqttVersion)
        ));
    }

    /// With the command channel closed no variant can queue its subscribe,
    /// and the token-returning ones report the same failure.
    #[test]
    fn test_try_subscribe_fails_when_disconnected() {
        let cli = test_client();
        assert!(cli.inner.cmd_tx.close());
        let (topics, qos) = (["a", "b"], [QoS::QoS1, QoS::QoS1]);

        assert!(matches!(
            cli.try_subscribe("a", QoS::QoS1),
            Err(Error::Disconnected)
        ));
        assert!(matches!(
            cli.try_subscribe_with_options("a", QoS::QoS1, SubscribeOptions::default(), None),
            Err(Error::Disconnected)
        ));
        assert!(matches!(
            cli.try_subscribe_many(&topics, &qos),
            Err(Error::Disconnected)
        ));
        assert!(matches!(
            cli.try_subscribe_many_same_qos(&topics, QoS::QoS1),
            Err(Error::Disconnected)
        ));
        assert!(matches!(
            cli.try_subscribe_many_with_options(&topics, &qos, &[], None),
            Err(Error::Disconnected)
        ));

        let err = cli.subscribe("a", QoS::QoS1).wait().unwrap_err();
        assert!(matches!(err, Error::Disconnected), "{err}");
        let err = cli.subscribe_many(&topics, &qos).wait().unwrap_err();
        assert!(matches!(err, Error::Disconnected), "{err}");
    }

    /// v5 properties can't go on a v3 unsubscribe.
    #[test]
    fn test_try_unsubscribe_rejects_v5_props_on_v3() {
        let cli = AsyncClient::new(
            CreateOptionsBuilder::new()
                .server_uri("mqtt://localhost:1883")
                .mqtt_version(MqttVersion::V3_1_1)
                .finalize(),
        )
        .unwrap();
        let mut props = Properties::new();
        props.push(crate::Property::UserProperty("k".into(), "v".into()));

        assert!(matches!(
            cli.try_unsubscribe_with_options("a", props.clone()),
            Err(Error::WrongMqttVersion)
        ));
        assert!(matches!(
            cli.try_unsubscribe_many_with_options(&["a", "b"], props),
            Err(Error::WrongMqttVersion)
        ));
    }

    /// With the command channel closed no variant can queue its unsubscribe,
    /// and the token-returning ones report the same failure.
    #[test]
    fn test_try_unsubscribe_fails_when_disconnected() {
        let cli = test_client();
        assert!(cli.inner.cmd_tx.close());
        let topics = ["a", "b"];

        assert!(matches!(cli.try_unsubscribe("a"), Err(Error::Disconnected)));
        assert!(matches!(
            cli.try_unsubscribe_with_options("a", Properties::new()),
            Err(Error::Disconnected)
        ));
        assert!(matches!(
            cli.try_unsubscribe_many(&topics),
            Err(Error::Disconnected)
        ));
        assert!(matches!(
            cli.try_unsubscribe_many_with_options(&topics, Properties::new()),
            Err(Error::Disconnected)
        ));

        let err = cli.unsubscribe("a").wait().unwrap_err();
        assert!(matches!(err, Error::Disconnected), "{err}");
        let err = cli.unsubscribe_many(&topics).wait().unwrap_err();
        assert!(matches!(err, Error::Disconnected), "{err}");
    }

    /// `connect` reports the same failure through its token.
    #[test]
    fn test_connect_token_reports_a_synchronous_failure() {
        let cli = test_client();
        assert!(cli.inner.cmd_tx.close());
        let err = cli.connect(None).wait().unwrap_err();
        assert!(matches!(err, Error::Disconnected), "{err}");
    }

    // ----------------------------------------------------------------------
    // try_publish hands the message back on a synchronous failure
    // ----------------------------------------------------------------------

    /// A message carrying the protocol's own bookkeeping, to prove the one
    /// handed back is the one given rather than a rebuild that lost fields.
    fn marked_message() -> Message {
        let mut msg = Message::new_retained("data/t", "payload", crate::QOS_2);
        msg.dup = true;
        msg.msgid = Some(7);
        msg
    }

    fn assert_same(back: &Message, orig: &Message) {
        assert_eq!(back.topic(), orig.topic());
        assert_eq!(back.payload(), orig.payload());
        assert_eq!(back.qos(), orig.qos());
        assert_eq!(back.retain(), orig.retain());
        assert_eq!(back.properties(), orig.properties());
        assert_eq!(back.dup(), orig.dup(), "dup was dropped");
        assert_eq!(back.msgid(), orig.msgid(), "msgid was dropped");
    }

    /// With the command channel closed the message cannot be queued, and
    /// comes back with the cause so the caller can send it again.
    #[test]
    fn test_try_publish_returns_the_message_when_disconnected() {
        let cli = test_client();
        assert!(cli.inner.cmd_tx.close(), "the channel was already closed");

        let msg = marked_message();
        match cli.try_publish(msg.clone()) {
            Err(Error::Publish(cause, back)) => {
                assert!(matches!(*cause, Error::Disconnected), "{cause}");
                assert_same(&back, &msg);
            }
            Ok(_) => panic!("the message should not have been queued"),
            Err(err) => panic!("expected the message back, got {err}"),
        }
    }

    /// The same for a message the connection's version cannot carry, which
    /// fails before the message is taken apart at all.
    #[test]
    fn test_try_publish_returns_the_message_on_a_version_mismatch() {
        let cli = AsyncClient::new(
            CreateOptionsBuilder::new()
                .server_uri("mqtt://localhost:1883")
                .mqtt_version(MqttVersion::V3_1_1)
                .finalize(),
        )
        .unwrap();

        let mut msg = marked_message();
        msg.props.push(crate::Property::MessageExpiryInterval(30));

        match cli.try_publish(msg.clone()) {
            Err(Error::Publish(cause, back)) => {
                assert!(matches!(*cause, Error::WrongMqttVersion), "{cause}");
                assert_same(&back, &msg);
            }
            Ok(_) => panic!("the message should not have been queued"),
            Err(err) => panic!("expected the message back, got {err}"),
        }
    }

    /// A client may not publish a Subscription Identifier (MQTT-3.3.4-6),
    /// as a received message republished as is would carry. It is refused
    /// with the message handed back, or on `publish`'s token, and goes out
    /// once the property is removed. It used to be sent, and a server
    /// answers that with DISCONNECT (Protocol Error).
    #[test]
    fn test_publish_refuses_a_subscription_identifier() {
        let cli = AsyncClient::new(
            CreateOptionsBuilder::new()
                .server_uri("mqtt://localhost:1883")
                .mqtt_version(MqttVersion::V5)
                .finalize(),
        )
        .unwrap();

        let mut msg = marked_message();
        msg.props.push(crate::Property::SubscriptionIdentifier(7));
        msg.props.push(crate::Property::MessageExpiryInterval(30));

        match cli.try_publish(msg.clone()) {
            Err(Error::Publish(cause, back)) => {
                assert!(
                    matches!(&*cause, Error::InvalidArgument(m) if m.contains("Subscription Identifier")),
                    "{cause}"
                );
                assert_same(&back, &msg);
            }
            Ok(_) => panic!("the message should not have been queued"),
            Err(err) => panic!("expected the message back, got {err}"),
        }
        let err = cli.publish(msg.clone()).wait().unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err}");

        // Without it, the message is accepted (and waits for a connection).
        msg.props.remove(crate::PropertyId::SubscriptionIdentifier);
        assert!(cli.try_publish(msg).is_ok());
    }

    /// `publish` has taken the message, so its token reports the cause alone
    /// rather than an `Error::Publish` holding a message the caller cannot
    /// use.
    #[test]
    fn test_publish_token_reports_the_cause_alone() {
        let cli = test_client();
        assert!(cli.inner.cmd_tx.close());

        let err = cli.publish(marked_message()).wait().unwrap_err();
        assert!(matches!(err, Error::Disconnected), "{err}");
    }

    /// The error prints as its cause, so a message that is only logged reads
    /// the same as it did when the variant carried a string.
    #[test]
    fn test_publish_error_displays_the_cause() {
        let err = Error::Publish(Box::new(Error::Disconnected), Box::new(marked_message()));
        assert_eq!(err.to_string(), Error::Disconnected.to_string());
    }

    // ----------------------------------------------------------------------
    // stop_stream removes the stream's callbacks, and only those
    // ----------------------------------------------------------------------

    /// Registers one counting callback in each of the four slots and returns
    /// the counters, in the order connected, lost, disconnected, message.
    fn count_all_callbacks(cli: &AsyncClient) -> [Arc<Mutex<u32>>; 4] {
        let counts: [Arc<Mutex<u32>>; 4] = [counter(), counter(), counter(), counter()];

        let c = counts[0].clone();
        cli.set_connected_callback(move |_, _| *c.lock().unwrap() += 1);
        let c = counts[1].clone();
        cli.set_connection_lost_callback(move |_| *c.lock().unwrap() += 1);
        let c = counts[2].clone();
        cli.set_disconnected_callback(move |_, _, _| *c.lock().unwrap() += 1);
        let c = counts[3].clone();
        cli.set_message_callback(move |_, _| *c.lock().unwrap() += 1);
        counts
    }

    /// Dispatches one of each event the callbacks answer, in an order a
    /// connection could produce them: established, a message, then the two
    /// ways one ends.
    ///
    /// Note that a real connection ends *either* by being lost or by the
    /// server disconnecting, never both; this sends both only to exercise
    /// all four callbacks from one helper. Each of those two also calls the
    /// message callback with `None` to mark the end of the message stream,
    /// so a message callback that is still registered sees three calls in
    /// all: the message and two ends.
    fn dispatch_one_of_each(cli: &AsyncClient) {
        use crate::runtime::Event as Ev;
        cli.dispatch_event(Ev::Connected(crate::ConnectResponse::default()));
        cli.dispatch_event(Ev::MessageArrived(Message::new("t", "p", crate::QOS_0)));
        // Both clear the connected flag and call the message callback.
        cli.dispatch_event(Ev::Disconnected {
            reason_code: ReasonCode::NormalDisconnection,
            props: Properties::new(),
        });
        cli.dispatch_event(Ev::ConnectionLost(Error::Disconnected));
    }

    /// Streams and consumers are not callbacks. Starting one leaves the
    /// application's handlers alone, and so does stopping it — which is the
    /// whole point of the sinks: the library and the application no longer
    /// bid for the same four slots.
    #[test]
    fn test_streams_do_not_displace_the_applications_callbacks() {
        let mut cli = test_client();
        let counts = count_all_callbacks(&cli);
        let _rx = cli.get_stream(10);
        let _events = cli.get_event_stream(10);
        let _sync = cli.start_consuming();

        cli.stop_stream();
        dispatch_one_of_each(&cli);

        assert_eq!(*counts[0].lock().unwrap(), 1, "connected");
        assert_eq!(*counts[1].lock().unwrap(), 1, "connection lost");
        assert_eq!(*counts[2].lock().unwrap(), 1, "disconnected");
        // Three times: the arriving message, and the `None` that each of
        // the two ways a connection can end sends to mark it.
        assert_eq!(*counts[3].lock().unwrap(), 3, "message");
    }

    /// A message callback and a synchronous consumer both receive every
    /// message. Before the sinks, whichever was registered second silently
    /// took the other's place.
    #[test]
    fn test_callback_and_consumer_both_receive() {
        let cli = test_client();
        let calls = counter();
        let c = calls.clone();
        cli.set_message_callback(move |_, _| *c.lock().unwrap() += 1);
        let rx = cli.start_consuming();

        cli.dispatch_event(crate::runtime::Event::MessageArrived(Message::new(
            "t",
            "p",
            crate::QOS_0,
        )));

        assert_eq!(*calls.lock().unwrap(), 1, "the callback");
        assert_eq!(rx.try_recv().unwrap().unwrap().topic, "t", "the consumer");
    }

    /// `stop_consuming` closes what `start_consuming` opened and nothing
    /// else; it used to remove the application's message callback, which it
    /// had never installed.
    #[test]
    fn test_stop_consuming_leaves_the_callback() {
        let cli = test_client();
        let calls = counter();
        let c = calls.clone();
        cli.set_message_callback(move |_, _| *c.lock().unwrap() += 1);
        let rx = cli.start_consuming();

        cli.stop_consuming();
        cli.dispatch_event(crate::runtime::Event::MessageArrived(Message::new(
            "t",
            "p",
            crate::QOS_0,
        )));

        assert_eq!(*calls.lock().unwrap(), 1, "the callback still runs");
        assert!(rx.try_recv().is_err(), "the consumer was closed");
    }

    /// `remove_callbacks` clears the callbacks — which is what it says —
    /// and leaves the consumers running.
    #[test]
    fn test_remove_callbacks_leaves_the_consumers() {
        let cli = test_client();
        let counts = count_all_callbacks(&cli);
        let rx = cli.start_consuming();

        cli.remove_callbacks();
        dispatch_one_of_each(&cli);

        for (i, count) in counts.iter().enumerate() {
            assert_eq!(*count.lock().unwrap(), 0, "callback {i}");
        }
        assert_eq!(rx.try_recv().unwrap().unwrap().topic, "t");
    }

    /// The event loop is told whether anything on this side wants messages
    /// at all. While nothing does it delivers to the streams alone, with no
    /// dispatcher thread in the way.
    #[test]
    fn test_wants_messages_tracks_callbacks_and_consumers() {
        let cli = test_client();
        let wants = || cli.inner.dispatch.wants_messages.load(Ordering::SeqCst);
        assert!(!wants(), "a fresh client has neither");

        cli.set_message_callback(|_, _| {});
        assert!(wants(), "a callback wants them");
        cli.remove_message_callback();
        assert!(!wants());

        let rx = cli.start_consuming();
        assert!(wants(), "a consumer wants them");
        cli.set_message_callback(|_, _| {});
        cli.stop_consuming();
        assert!(wants(), "the callback is still there");
        cli.remove_callbacks();
        assert!(!wants());
        drop(rx);
    }

    /// A synchronous event consumer sees the connection events and the
    /// messages in one sequence, in order.
    #[test]
    fn test_event_consumer_sees_events_and_messages() {
        let cli = test_client();
        let rx = cli.start_consuming_events(None);

        cli.dispatch_event(crate::runtime::Event::Connected(
            crate::ConnectResponse::default(),
        ));
        cli.dispatch_event(crate::runtime::Event::MessageArrived(Message::new(
            "t",
            "p",
            crate::QOS_0,
        )));
        cli.dispatch_event(crate::runtime::Event::ConnectionLost(Error::Disconnected));

        assert!(matches!(rx.try_recv().unwrap(), Event::Connected(_)));
        assert!(matches!(rx.try_recv().unwrap(), Event::Message(m) if m.topic == "t"));
        assert!(matches!(rx.try_recv().unwrap(), Event::ConnectionLost));
    }

    /// `stop_stream` closes every stream and consumer. The synchronous ones
    /// are held here and close at once; the async streams live in the event
    /// loop and close once it has seen the command.
    #[test]
    fn test_stop_stream_closes_streams_and_consumers() {
        let mut cli = test_client();
        let sync_rx = cli.start_consuming();
        let stream = cli.get_stream(10);
        let events = cli.get_event_stream(10);

        cli.stop_stream();

        assert!(sync_rx.try_recv().is_err(), "the consumer closed at once");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while (!stream.is_closed() || !events.is_closed()) && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(stream.is_closed(), "the message stream was not closed");
        assert!(events.is_closed(), "the event stream was not closed");
    }

    /// With no stream at all there is nothing to remove.
    #[test]
    fn test_stop_stream_without_a_stream_removes_nothing() {
        let cli = test_client();
        let counts = count_all_callbacks(&cli);

        cli.stop_stream();
        dispatch_one_of_each(&cli);

        assert_eq!(*counts[0].lock().unwrap(), 1, "connected");
        assert_eq!(*counts[1].lock().unwrap(), 1, "connection lost");
        assert_eq!(*counts[2].lock().unwrap(), 1, "disconnected");
        // Three times: the arriving message, and the `None` that each of
        // the two ways a connection can end sends to mark it.
        assert_eq!(*counts[3].lock().unwrap(), 3, "message");
    }

    /// `remove_callbacks` clears the callbacks and leaves a stream open:
    /// the stream is not a callback, and was never the library's to close.
    #[test]
    fn test_remove_callbacks_leaves_a_stream_open() {
        let cli = test_client();
        let counts = count_all_callbacks(&cli);
        let rx = cli.get_event_stream(10);

        cli.remove_callbacks();
        dispatch_one_of_each(&cli);

        for (i, count) in counts.iter().enumerate() {
            assert_eq!(*count.lock().unwrap(), 0, "callback {i}");
        }
        assert!(!rx.is_closed(), "the stream was closed with the callbacks");
    }

    /// A server DISCONNECT ends the connection on the callback route and
    /// for the synchronous consumers, and it is the *only* notice sent:
    /// `connection_lost` means the connection was lost unexpectedly, which
    /// this was not. Every established connection gets one or the other,
    /// never both and never neither. (The async streams are fed by the
    /// event loop's sinks, not by the dispatcher; see
    /// `runtime::sinks::tests::test_server_disconnect_ends_the_message_streams_too`.)
    #[test]
    fn test_server_disconnect_ends_every_dispatched_route() {
        use crate::runtime::Event as Ev;

        let cli = test_client();
        let counts = count_all_callbacks(&cli);
        let sync_msgs = cli.start_consuming();
        let sync_events = cli.start_consuming_events(10);

        cli.dispatch_event(Ev::Disconnected {
            reason_code: ReasonCode::ServerShuttingDown,
            props: Properties::new(),
        });

        // The callback route: `disconnected` with the reason, the message
        // callback with `None`, and nothing on `connection_lost`.
        assert_eq!(*counts[2].lock().unwrap(), 1, "disconnected");
        assert_eq!(*counts[3].lock().unwrap(), 1, "message callback given None");
        assert_eq!(
            *counts[1].lock().unwrap(),
            0,
            "connection_lost must not fire"
        );

        // The synchronous consumers end the same way.
        assert!(
            matches!(sync_msgs.try_recv(), Ok(None)),
            "a sync consumer ends with None"
        );
        assert!(
            matches!(
                sync_events.try_recv(),
                Ok(Event::Disconnected { reason_code, .. })
                    if reason_code == ReasonCode::ServerShuttingDown
            ),
            "a sync event consumer is given the reason"
        );

        // Nothing is left behind on either.
        assert!(sync_msgs.try_recv().is_err());
        assert!(sync_events.try_recv().is_err());
    }

    /// The counterpart: a connection lost without a DISCONNECT reports
    /// `connection_lost` and not `disconnected`.
    #[test]
    fn test_lost_connection_does_not_report_disconnected() {
        use crate::runtime::Event as Ev;

        let cli = test_client();
        let counts = count_all_callbacks(&cli);
        let sync_msgs = cli.start_consuming();
        let sync_events = cli.start_consuming_events(10);

        cli.dispatch_event(Ev::ConnectionLost(Error::Disconnected));

        assert_eq!(*counts[1].lock().unwrap(), 1, "connection lost");
        assert_eq!(*counts[2].lock().unwrap(), 0, "disconnected must not fire");
        assert_eq!(*counts[3].lock().unwrap(), 1, "message callback given None");
        assert!(matches!(sync_msgs.try_recv(), Ok(None)));
        assert!(matches!(sync_events.try_recv(), Ok(Event::ConnectionLost)));
    }
}
