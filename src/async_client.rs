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
    AsyncReceiver, ConnectOptions, ConnectResponse, ConnectToken, CreateOptions, DeliveryToken,
    DisconnectOptions, Event, FilePersistence, Message, PersistenceType, Properties, ReasonCode,
    SubscribeManyToken, SubscribeOptions, SubscribeToken, SyncReceiver, UnsubscribeManyToken,
    UnsubscribeToken, UserData,
    error::{Error, Result},
    persistence::ClientPersistence,
    runtime::{BufConfig, Command, ConnectParams, RtOwnership, ServerAddr},
    token::Token,
};
use bytes::Bytes;
use crossbeam_channel as sync_channel;
use paho_mqtt_codec::types::*;
use std::{
    result::Result as StdResult,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};

/////////////////////////////////////////////////////////////////////////////

/// Constructs a boxed, mutex-wrapped persistence store from a `PersistenceType`.
///
/// Returns `None` when `typ` is `PersistenceType::None`.
fn make_persistence(typ: PersistenceType) -> Option<Arc<Mutex<Box<dyn ClientPersistence>>>> {
    let store: Box<dyn ClientPersistence> = match typ {
        PersistenceType::None => return None,
        PersistenceType::File => Box::new(FilePersistence::new(".")),
        PersistenceType::FilePath(p) => Box::new(FilePersistence::new(p)),
        PersistenceType::User(up) => up.into_inner(),
    };
    Some(Arc::new(Mutex::new(store)))
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
    cmd_tx: async_channel::Sender<Command>,
    /// Whether the client is currently connected.
    /// Wrapped in Arc so the event-loop thread can set it before the token
    /// resolves, eliminating any race between `wait()` and `is_connected()`.
    connected: Arc<AtomicBool>,
    /// The MQTT version of the connection.
    mqtt_version: AtomicU32,
    /// The identifier the server assigned, when the client connected with an
    /// empty one. Set by the event loop before the connect token resolves.
    assigned_id: Arc<RwLock<Option<String>>>,
    /// The options used to create the client.
    opts: CreateOptions,
    /// The options for the most recent connection attempt.
    conn_opts: RwLock<ConnectOptions>,
    /// User-registered event callbacks.
    callback_context: Mutex<CallbackContext>,
    /// Persistence store, if any.
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
    /// Keeps the async runtime alive for the lifetime of this client.
    /// Declared last so it is dropped after `cmd_tx`, ensuring the event-loop
    /// task sees the closed channel before the runtime shuts down.
    _rt_ownership: RtOwnership,
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
pub type TlsErrorCallback = dyn Fn(&AsyncClient, &str) + Send + Sync + 'static;

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

    /// Removes the callback only if the slot still holds the one identified
    /// by `generation`, leaving anything the application put there since.
    fn clear_if(&mut self, generation: u64) {
        if self.generation == generation {
            self.clear();
        }
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

/// The generations of the callbacks a message or event stream installed.
///
/// `stop_stream` closes a stream's channel by dropping the senders the stream
/// put in the callback slots. It must drop *those* and nothing else: the
/// application's own handlers are not the stream's to remove, and a callback
/// the application has since replaced is no longer the stream's either.
#[derive(Default)]
struct StreamSlots {
    connected: Option<u64>,
    connection_lost: Option<u64>,
    disconnected: Option<u64>,
    message: Option<u64>,
}

#[derive(Default)]
struct CallbackContext {
    on_connected: Slot<ConnectedCallback>,
    on_connection_lost: Slot<ConnectionLostCallback>,
    on_disconnected: Slot<DisconnectedCallback>,
    on_message_arrived: Slot<MessageArrivedCallback>,
    /// What the current stream, if any, installed.
    stream: StreamSlots,
}

/////////////////////////////////////////////////////////////////////////////

impl AsyncClient {
    /// Creates a new MQTT client which can connect to an MQTT broker.
    ///
    /// # Arguments
    ///
    /// * `opts` The create options for the client.
    ///
    pub fn new<T>(opts: T) -> Result<AsyncClient>
    where
        T: Into<CreateOptions>,
    {
        let mut opts: CreateOptions = opts.into();
        debug!("Create options: {:?}", opts);

        let buf_cfg = BufConfig {
            send_while_disconnected: opts.send_while_disconnected,
            max_buffered_msgs: opts.max_buffered_msgs,
            allow_send_any_time: opts.allow_send_any_time,
            delete_oldest: opts.delete_oldest,
            restore_msgs: opts.restore_msgs,
            persist_qos0: opts.persist_qos0,
        };

        #[cfg(feature = "tokio")]
        let (rt, rt_ownership) = {
            use crate::runtime::{RuntimeHandle, resolve_runtime};
            let (rt_handle, ownership) = resolve_runtime(&opts.runtime_mode);
            (RuntimeHandle::spawn(&rt_handle, buf_cfg), ownership)
        };

        #[cfg(feature = "smol")]
        let (rt, rt_ownership) = {
            use crate::runtime::{RuntimeHandle, SmolOwnership, resolve_executor};
            let executor = resolve_executor(&opts.runtime_mode);
            let rt = RuntimeHandle::spawn(&executor, buf_cfg);
            (rt, SmolOwnership::Shared(executor))
        };
        let (cmd_tx, event_rx, gate) = rt.into_parts();

        // Seed the stored connect opts with the client's initial MQTT version
        // so that reconnect() and pre-connect sends use the right version.
        let create_ver = opts.mqtt_version;
        let initial_conn_opts = ConnectOptions {
            mqtt_version: create_ver,
            ..ConnectOptions::default()
        };

        // Extract persistence before moving opts into inner, so we can consume
        // PersistenceType::User without cloning the trait object.
        let persist_type = std::mem::replace(&mut opts.persistence, PersistenceType::None);
        let persist = make_persistence(persist_type);

        let inner = Arc::new(InnerAsyncClient {
            cmd_tx,
            connected: Arc::new(AtomicBool::new(false)),
            mqtt_version: AtomicU32::new(create_ver as u32),
            assigned_id: Arc::new(RwLock::new(None)),
            opts,
            conn_opts: RwLock::new(initial_conn_opts),
            callback_context: Mutex::new(CallbackContext::default()),
            persist,
            _rt_ownership: rt_ownership,
        });

        let weak = Arc::downgrade(&inner);
        std::thread::Builder::new()
            .name("mqtt-events".into())
            .spawn(move || {
                while let Ok(ev) = event_rx.recv_blocking() {
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    let cli = AsyncClient { inner };
                    let is_msg = matches!(ev, crate::runtime::Event::MessageArrived(_));
                    cli.dispatch_event(ev);
                    // Only now, with the callback done, is there room for
                    // another: a slow consumer holds the session back.
                    if is_msg {
                        gate.consumed();
                    }
                }
            })
            .expect("spawn mqtt event thread");

        Ok(AsyncClient { inner })
    }

    /// Connects to an MQTT broker using the specified connect options.
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
        let explicit: Option<ConnectOptions> = opts.into();
        debug!("Connecting with opts: {:?}", explicit);

        // None: keep the client's current version unchanged.
        // Some(opts): use opts.mqtt_version and update the stored version.
        let ver = match &explicit {
            None => self.mqtt_version(),
            Some(o) => {
                self.set_mqtt_version(o.mqtt_version);
                o.mqtt_version
            }
        };

        // Normalize: ensure the opts we store and send have the resolved version.
        let mut opts = explicit.unwrap_or_default();
        opts.mqtt_version = ver;

        if let Err(err) = opts.check() {
            return ConnectToken::from(err);
        }

        if !opts.props.is_empty() && ver < MqttVersion::V5 {
            return ConnectToken::from(Error::WrongMqttVersion);
        }

        // Build the ordered server list: explicit list from connect opts, or
        // fall back to the URI given at client creation time.
        let uris: Vec<String> = if opts.server_uris.is_empty() {
            vec![self.inner.opts.server_uri.clone()]
        } else {
            opts.server_uris.clone()
        };
        let mut servers: Vec<(ServerAddr, String)> = Vec::with_capacity(uris.len());
        for uri in uris {
            match parse_server_uri(&uri) {
                Ok(addr) => servers.push((addr, uri)),
                Err(e) => return ConnectToken::from(e),
            }
        }

        let keep_alive_secs = opts.keep_alive_interval.as_secs().min(u16::MAX as u64) as u16;

        // The Will's v5 properties (delay interval, expiry, content type, …)
        // travel with it; like connect properties they need a v5 connection.
        if let Some(msg) = &opts.will
            && !msg.props.is_empty()
            && ver < MqttVersion::V5
        {
            return ConnectToken::from(Error::WrongMqttVersion);
        }
        let will = opts.will.as_ref().map(|msg| paho_mqtt_codec::Will {
            topic: msg.topic.clone(),
            payload: msg.payload.clone(),
            qos: msg.qos,
            retain: msg.retain,
            properties: msg.props.clone(),
        });

        let (tok, comp) = ConnectToken::new();
        *self.inner.conn_opts.write().unwrap() = opts.clone();

        let params = ConnectParams {
            servers,
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
            connected_flag: Arc::clone(&self.inner.connected),
            assigned_id: Arc::clone(&self.inner.assigned_id),
            persist: self.inner.persist.as_ref().map(Arc::clone),
        };

        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return ConnectToken::from(Error::Disconnected);
        }

        tok
    }

    /// Disconnects from the MQTT broker.
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
        let opts = opts.into().unwrap_or_default();
        debug!("Disconnecting with opts: {:?}", opts);

        // A v5 reason code and v5 properties cannot go on a v3 connection;
        // the codec refuses to encode either, and the DISCONNECT would never
        // be sent.
        if let Err(e) = self
            .require_v5_for_props(&opts.props)
            .and_then(|()| self.require_v5_for_disconnect_reason(opts.reason_code))
        {
            return Token::<()>::from_err(e);
        }

        let (tok, comp) = Token::<()>::new();

        let cmd = Command::Disconnect {
            timeout: opts.timeout,
            reason_code: opts.reason_code,
            props: opts.props,
            tx: comp,
            connected_flag: Arc::clone(&self.inner.connected),
        };

        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Token::<()>::from_err(Error::Disconnected);
        }

        // Signal the end of the message stream. The callback runs outside
        // the lock, so it may call back into the client — including removing
        // itself.
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

        tok
    }

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
    pub fn try_publish(&self, msg: Message) -> Result<DeliveryToken> {
        debug!("Publish: {:?}", msg);

        if let Err(cause) = self.require_v5_for_props(&msg.props) {
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

        if let Err(e) = self.inner.cmd_tx.send_blocking(cmd) {
            // A closed channel gives the command back, so the message can be
            // reassembled rather than lost with it.
            let Command::Publish {
                topic,
                payload,
                qos,
                retain,
                props,
                ..
            } = e.into_inner()
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
    pub fn publish(&self, msg: Message) -> DeliveryToken {
        match self.try_publish(msg) {
            // Unwrap the message this call has taken ownership of, so the
            // token reports the same cause it always did.
            Err(Error::Publish(cause, _)) => DeliveryToken::from_err(*cause),
            Err(e) => DeliveryToken::from_err(e),
            Ok(tok) => tok,
        }
    }

    /// Subscribes to a single topic.
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
        let qos = match qos.try_into() {
            Ok(q) => q,
            Err(e) => return SubscribeToken::from_err(e.into()),
        };
        let (tok, comp) = SubscribeToken::new();
        let cmd = Command::Subscribe {
            single: true,
            filters: vec![(topic.into(), qos, SubscribeOptions::default())],
            props: Default::default(),
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return SubscribeToken::from_err(Error::Disconnected);
        }
        tok
    }

    /// Subscribes to a single topic with v5 options.
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
        let qos = match qos.try_into() {
            Ok(q) => q,
            Err(e) => return SubscribeToken::from_err(e.into()),
        };
        let opts: SubscribeOptions = opts.into();
        let props = props.into().unwrap_or_default();
        if let Err(e) = self.require_v5_for_sub_opts(&opts) {
            return SubscribeToken::from_err(e);
        }
        if let Err(e) = self.require_v5_for_props(&props) {
            return SubscribeToken::from_err(e);
        }
        let (tok, comp) = SubscribeToken::new();
        let cmd = Command::Subscribe {
            single: true,
            filters: vec![(topic.into(), qos, opts)],
            props,
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return SubscribeToken::from_err(Error::Disconnected);
        }
        tok
    }

    /// Subscribes to multiple topics simultaneously.
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
        let n = topics.len();
        if n != qos.len() {
            return SubscribeManyToken::from_err(Error::General("topics/qos length mismatch"));
        }

        let filters: Result<Vec<_>> = topics
            .iter()
            .zip(qos.iter())
            .map(|(t, q)| {
                let qos = (*q).try_into().map_err(Into::into)?;
                Ok((t.as_ref().to_string(), qos, SubscribeOptions::default()))
            })
            .collect();
        let filters = match filters {
            Ok(f) => f,
            Err(e) => return SubscribeManyToken::from_err(e),
        };

        let (tok, comp) = SubscribeManyToken::new();

        let cmd = Command::Subscribe {
            single: false,
            filters,
            props: Default::default(),
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return SubscribeManyToken::from_err(Error::Disconnected);
        }
        tok
    }

    /// Subscribes to multiple topics simultaneously using the same QoS
    /// for all of them.
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
        let qos = match qos.try_into() {
            Ok(q) => q,
            Err(e) => return SubscribeManyToken::from_err(e.into()),
        };
        let qos = vec![qos; topics.len()];
        self.subscribe_many(topics, &qos)
    }

    /// Subscribes to multiple topics simultaneously with options.
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
        let n = topics.len();
        if n != qos.len() {
            return SubscribeManyToken::from_err(Error::General("topics/qos length mismatch"));
        }
        if !opts.is_empty() && opts.len() != n {
            return SubscribeManyToken::from_err(Error::General("topics/opts length mismatch"));
        }

        let props = props.into().unwrap_or_default();
        if let Err(e) = self.require_v5_for_props(&props) {
            return SubscribeManyToken::from_err(e);
        }

        let filters: Result<Vec<_>> = topics
            .iter()
            .zip(qos.iter())
            .enumerate()
            .map(|(i, (t, q))| {
                let sub_opts = opts.get(i).copied().unwrap_or_default();
                self.require_v5_for_sub_opts(&sub_opts)?;
                let qos = (*q).try_into().map_err(Into::into)?;
                Ok((t.as_ref().to_string(), qos, sub_opts))
            })
            .collect();

        let filters = match filters {
            Ok(f) => f,
            Err(e) => return SubscribeManyToken::from_err(e),
        };

        let (tok, comp) = SubscribeManyToken::new();
        let cmd = Command::Subscribe {
            single: false,
            filters,
            props,
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return SubscribeManyToken::from_err(Error::Disconnected);
        }
        tok
    }

    /// Unsubscribes from a single topic.
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
        let (tok, comp) = UnsubscribeToken::new();
        let cmd = Command::Unsubscribe {
            single: true,
            filters: vec![topic.into()],
            props: Default::default(),
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return UnsubscribeToken::from_err(Error::Disconnected);
        }
        tok
    }

    /// Unsubscribes from a single topic with v5 options.
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
        if let Err(e) = self.require_v5_for_props(&props) {
            return UnsubscribeToken::from_err(e);
        }
        let (tok, comp) = UnsubscribeToken::new();
        let cmd = Command::Unsubscribe {
            single: true,
            filters: vec![topic.into()],
            props,
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return UnsubscribeToken::from_err(Error::Disconnected);
        }
        tok
    }

    /// Unsubscribes from multiple topics simultaneously.
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
        let (tok, comp) = UnsubscribeManyToken::new();
        let filters = topics.iter().map(|t| t.as_ref().to_string()).collect();
        let cmd = Command::Unsubscribe {
            single: false,
            filters,
            props: Default::default(),
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return UnsubscribeManyToken::from_err(Error::Disconnected);
        }
        tok
    }

    /// Unsubscribes from multiple topics simultaneously with v5 options.
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
        if let Err(e) = self.require_v5_for_props(&props) {
            return UnsubscribeManyToken::from_err(e);
        }
        let (tok, comp) = UnsubscribeManyToken::new();
        let filters = topics.iter().map(|t| t.as_ref().to_string()).collect();
        let cmd = Command::Unsubscribe {
            single: false,
            filters,
            props,
            tx: comp,
        };
        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return UnsubscribeManyToken::from_err(Error::Disconnected);
        }
        tok
    }

    /// Determines if this client is currently connected to an MQTT broker.
    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::SeqCst)
    }

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
    fn dispatch_event(&self, ev: crate::runtime::Event) {
        use crate::runtime::Event as Ev;
        match ev {
            Ev::Connected(response) => {
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
                self.inner.connected.store(false, Ordering::SeqCst);
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
                self.inner.connected.store(false, Ordering::SeqCst);
                let taken = self
                    .inner
                    .callback_context
                    .lock()
                    .unwrap()
                    .on_disconnected
                    .take();
                if let Some((mut cb, generation)) = taken {
                    cb(self, props, reason_code);
                    self.inner
                        .callback_context
                        .lock()
                        .unwrap()
                        .on_disconnected
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
            Ev::TlsError(msg) => {
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
                    cb(self, &msg);
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Common API
/////////////////////////////////////////////////////////////////////////////

impl AsyncClient {
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

    /// Attempts to reconnect to the broker.
    /// This can only be called after a connection was initially made or
    /// attempted. It will retry with the same connect options.
    pub fn reconnect(&self) -> ConnectToken {
        let connopts = self.inner.conn_opts.read().unwrap().clone();
        self.connect(connopts)
    }

    /// Disconnect from the MQTT broker with a timeout.
    ///
    /// This will delay the disconnect for up to the specified timeout to
    /// allow in-flight messages to complete.
    ///
    /// # Arguments
    ///
    /// * `timeout` The amount of time to wait for the disconnect. This has
    ///   a resolution in milliseconds.
    ///
    pub fn disconnect_after(&self, timeout: Duration) -> Token<()> {
        self.disconnect(DisconnectOptions::with_timeout(timeout))
    }

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
    pub fn set_message_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Option<Message>) + Send + 'static,
    {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_message_arrived
            .set(Box::new(cb));
    }

    /// Removes the callback for when a message arrives from the broker.
    pub fn remove_message_callback(&self) {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_message_arrived
            .clear();
    }

    /// Removes all registered callbacks atomically under a single lock.
    ///
    /// A callback may call this from inside itself; it is not reinstated when
    /// it returns.
    pub fn remove_callbacks(&self) {
        let mut cbctx = self.inner.callback_context.lock().unwrap();
        cbctx.stream = StreamSlots::default();
        cbctx.on_connected.clear();
        cbctx.on_connection_lost.clear();
        cbctx.on_disconnected.clear();
        cbctx.on_message_arrived.clear();
    }

    /// Starts the client consuming messages for a blocking (non-async) app.
    ///
    /// Returns the receiving-end of the channel for the application to
    /// get the messages.
    pub fn start_consuming(&self) -> SyncReceiver<Option<Message>> {
        let (tx, rx) = sync_channel::unbounded();

        self.set_stream_message_callback(move |_, optmsg| {
            log_channel_err(tx.try_send(optmsg));
        });

        rx
    }

    /// Stops the client from consuming messages.
    pub fn stop_consuming(&self) {
        self.remove_message_callback();
    }

    /// Creates a synchronous event stream.
    ///
    /// Returns the receive end of a channel that receives [`Event`] values.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if `buffer_lim` is `Some`, or an unbounded channel if `None`.
    pub fn start_consuming_events<L>(&self, buffer_lim: L) -> SyncReceiver<Event>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => sync_channel::bounded(lim),
            None => sync_channel::unbounded(),
        };

        self.set_stream_connected_callback({
            let tx = tx.clone();
            move |_cli, rsp| {
                log_channel_err(tx.try_send(Event::Connected(rsp.clone())));
            }
        });

        self.set_stream_message_callback({
            let tx = tx.clone();
            move |_cli, msg| {
                if let Some(msg) = msg {
                    log_channel_err(tx.try_send(Event::Message(msg)));
                }
            }
        });

        self.set_stream_connection_lost_callback({
            let tx = tx.clone();
            move |_cli| {
                log_channel_err(tx.try_send(Event::ConnectionLost));
            }
        });

        self.set_stream_disconnected_callback({
            let tx = tx.clone();
            move |_cli, props, reason_code| {
                log_channel_err(tx.try_send(Event::Disconnected { props, reason_code }));
            }
        });

        rx
    }

    // ---- Callback installation on behalf of a stream ----
    //
    // Each of these registers a sender-holding callback and records which
    // callback the stream owns, so `stop_stream` can take back exactly those.

    fn set_stream_connected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, &crate::ConnectResponse) + Send + 'static,
    {
        let mut cbctx = self.inner.callback_context.lock().unwrap();
        cbctx.stream.connected = Some(cbctx.on_connected.set(Box::new(cb)));
    }

    fn set_stream_connection_lost_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient) + Send + 'static,
    {
        let mut cbctx = self.inner.callback_context.lock().unwrap();
        cbctx.stream.connection_lost = Some(cbctx.on_connection_lost.set(Box::new(cb)));
    }

    fn set_stream_disconnected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static,
    {
        let mut cbctx = self.inner.callback_context.lock().unwrap();
        cbctx.stream.disconnected = Some(cbctx.on_disconnected.set(Box::new(cb)));
    }

    fn set_stream_message_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Option<Message>) + Send + 'static,
    {
        let mut cbctx = self.inner.callback_context.lock().unwrap();
        cbctx.stream.message = Some(cbctx.on_message_arrived.set(Box::new(cb)));
    }

    /// Creates an async message stream.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if `buffer_lim` is `Some`, or an unbounded channel if `None`.
    pub fn get_stream<L>(&mut self, buffer_lim: L) -> AsyncReceiver<Option<Message>>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => async_channel::bounded(lim),
            None => async_channel::unbounded(),
        };

        self.set_stream_message_callback(move |_, msg| {
            log_stream_err(tx.try_send(msg));
        });

        rx
    }

    /// Creates an async event stream.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if `buffer_lim` is `Some`, or an unbounded channel if `None`.
    pub fn get_event_stream<L>(&self, buffer_lim: L) -> AsyncReceiver<Event>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => async_channel::bounded(lim),
            None => async_channel::unbounded(),
        };

        self.set_stream_connected_callback({
            let tx = tx.clone();
            move |_cli, rsp| {
                log_stream_err(tx.try_send(Event::Connected(rsp.clone())));
            }
        });

        self.set_stream_message_callback({
            let tx = tx.clone();
            move |_cli, msg| {
                if let Some(msg) = msg {
                    log_stream_err(tx.try_send(Event::Message(msg)));
                }
            }
        });

        self.set_stream_connection_lost_callback({
            let tx = tx.clone();
            move |_cli| {
                log_stream_err(tx.try_send(Event::ConnectionLost));
            }
        });

        self.set_stream_disconnected_callback({
            let tx = tx.clone();
            move |_cli, props, reason_code| {
                log_stream_err(tx.try_send(Event::Disconnected { props, reason_code }));
            }
        });

        rx
    }

    /// Stops the stream created by [`get_stream`], [`get_event_stream`],
    /// [`start_consuming`] or [`start_consuming_events`], closing its channel.
    ///
    /// Removes the callbacks that stream installed, dropping the last
    /// channel-sender clones so the receiver observes the channel closing. A
    /// message stream holds one callback and an event stream four, so this
    /// removes one or four accordingly.
    ///
    /// Callbacks the application registered itself are left alone, as is a
    /// callback the application has replaced since the stream started — the
    /// stream no longer owns that slot. Use [`remove_callbacks`] to clear
    /// everything.
    ///
    /// [`get_stream`]: AsyncClient::get_stream
    /// [`get_event_stream`]: AsyncClient::get_event_stream
    /// [`start_consuming`]: AsyncClient::start_consuming
    /// [`start_consuming_events`]: AsyncClient::start_consuming_events
    /// [`remove_callbacks`]: AsyncClient::remove_callbacks
    pub fn stop_stream(&self) {
        let mut cbctx = self.inner.callback_context.lock().unwrap();
        let stream = std::mem::take(&mut cbctx.stream);

        if let Some(generation) = stream.connected {
            cbctx.on_connected.clear_if(generation);
        }
        if let Some(generation) = stream.connection_lost {
            cbctx.on_connection_lost.clear_if(generation);
        }
        if let Some(generation) = stream.disconnected {
            cbctx.on_disconnected.clear_if(generation);
        }
        if let Some(generation) = stream.message {
            cbctx.on_message_arrived.clear_if(generation);
        }
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

    /// Returns the server URI used for connection.
    pub fn server_uri(&self) -> &str {
        &self.inner.opts.server_uri
    }
}

// The client is safe to send or share between threads.

fn log_send_err(is_full: bool) {
    if is_full {
        warn!("Event stream full. Losing messages");
    } else {
        warn!("Event stream closed");
    }
}

fn log_channel_err<T>(res: StdResult<(), sync_channel::TrySendError<T>>) {
    if let Err(e) = res {
        log_send_err(e.is_full());
    }
}

fn log_stream_err<T>(res: StdResult<(), async_channel::TrySendError<T>>) {
    if let Err(e) = res {
        log_send_err(e.is_full());
    }
}

/// Extracts `(host, port, path)` from the authority+path portion of a URI.
///
/// `rest` is the URI with its scheme prefix stripped (e.g. `"broker:1883/mqtt"`).
/// `default_port` is used when no explicit port appears. `path` is `"/"` when
/// the URI has no path component.
fn parse_host_port_path(rest: &str, default_port: u16) -> Result<(String, u16, String)> {
    let authority = rest.split('/').next().unwrap_or(rest);
    let path = if authority.len() < rest.len() {
        rest[authority.len()..].to_string()
    } else {
        "/".to_string()
    };

    let (host, port) = if let Some(colon) = authority.rfind(':') {
        let host = authority[..colon].to_string();
        let port = authority[colon + 1..]
            .parse::<u16>()
            .map_err(|_| Error::General("invalid port in server URI"))?;
        (host, port)
    } else {
        (authority.to_string(), default_port)
    };

    Ok((host, port, path))
}

/// Parses an MQTT URI into a [`ServerAddr`].
///
/// Recognises:
/// - `unix://` or `unix:` — Unix-domain socket path
/// - `mqtt://`, `tcp://` — plain TCP (default port 1883)
/// - `ws://` — WebSocket over TCP (default port 1883); [`ServerAddr::Ws`] when
///   the `websocket` feature is enabled, plain TCP otherwise
/// - `mqtts://`, `ssl://`, `tls://` — TLS TCP (default port 8883)
/// - `wss://` — WebSocket over TLS (default port 8883); [`ServerAddr::Wss`]
///   when both `websocket` and `tls` features are enabled, TLS TCP otherwise
///
/// If no explicit port is present the default for the scheme is used.
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
        let (host, port, path) = parse_host_port_path(rest, 1883)?;
        return Ok(ServerAddr::Ws { host, port, path });
    }

    #[cfg(any(
        all(feature = "websocket", any(feature = "tls", feature = "openssl")),
        all(feature = "smol-websocket", feature = "smol-tls")
    ))]
    if let Some(rest) = uri.strip_prefix("wss://") {
        let (host, port, path) = parse_host_port_path(rest, 8883)?;
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

    let (host, port, _) = parse_host_port_path(rest, default_port)?;

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
    use std::thread;

    // Makes sure than when a client is moved, the inner struct stays at
    // the same address (on the heap) since that inner struct is used as
    // the context pointer for callbacks.
    // GitHub Issue #17
    #[test]
    fn test_context() {
        let cli = AsyncClient::new("tcp://localhost:1883").unwrap();
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
        let cli = AsyncClient::new("tcp://localhost:1883");
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
            .server_uri("tcp://localhost:1883")
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
            .server_uri("tcp://localhost:1883")
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
            .server_uri("tcp://localhost:1883")
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
        let cli = AsyncClient::new("tcp://localhost:1883").unwrap();
        let thr = thread::spawn(move || {
            assert!(!cli.is_connected());
        });
        thr.join().unwrap();
    }

    // Determine that a client can be shared across threads using an Arc.
    #[test]
    fn test_send_arc() {
        let cli = AsyncClient::new("tcp://localhost:1883").unwrap();

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
        let server_uri = "tcp://localhost:1883";
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
    fn test_parse_server_uri() {
        assert!(matches!(
            parse_server_uri("mqtt://localhost:1883").unwrap(),
            ServerAddr::Tcp { host, port } if host == "localhost" && port == 1883
        ));
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
        AsyncClient::new("tcp://localhost:1883").unwrap()
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
    #[test]
    fn test_message_callback_may_touch_the_client_during_disconnect() {
        let cli = test_client();
        let calls = counter();

        let c = calls.clone();
        cli.set_message_callback(move |cli, msg| {
            *c.lock().unwrap() += 1;
            assert!(msg.is_none(), "disconnect signals the end of the stream");
            cli.remove_message_callback();
        });

        let _tok = cli.disconnect(None);
        assert_eq!(*calls.lock().unwrap(), 1);
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
            Err(e) => panic!("expected the message back, got {e}"),
        }
    }

    /// The same for a message the connection's version cannot carry, which
    /// fails before the message is taken apart at all.
    #[test]
    fn test_try_publish_returns_the_message_on_a_version_mismatch() {
        let cli = AsyncClient::new(
            CreateOptionsBuilder::new()
                .server_uri("tcp://localhost:1883")
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
            Err(e) => panic!("expected the message back, got {e}"),
        }
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

    /// Dispatches one of each event the callbacks answer. Note that a lost
    /// connection calls the message callback too, with `None`, so a message
    /// callback that is still registered sees two calls in all.
    fn dispatch_one_of_each(cli: &AsyncClient) {
        use crate::runtime::Event as Ev;
        cli.dispatch_event(Ev::Connected(crate::ConnectResponse::default()));
        cli.dispatch_event(Ev::Disconnected {
            reason_code: ReasonCode::NormalDisconnection,
            props: Properties::new(),
        });
        cli.dispatch_event(Ev::MessageArrived(Message::new("t", "p", crate::QOS_0)));
        // Last: it clears the connected flag and calls two callbacks.
        cli.dispatch_event(Ev::ConnectionLost(Error::Disconnected));
    }

    /// A message stream owns one callback, so stopping it must not take the
    /// application's connection handlers with it. `stop_stream` used to call
    /// `remove_callbacks()`, which removed all four.
    #[test]
    fn test_stop_stream_keeps_the_applications_callbacks() {
        let mut cli = test_client();
        let counts = count_all_callbacks(&cli);
        let _rx = cli.get_stream(10);

        cli.stop_stream();
        dispatch_one_of_each(&cli);

        assert_eq!(*counts[0].lock().unwrap(), 1, "connected");
        assert_eq!(*counts[1].lock().unwrap(), 1, "connection lost");
        assert_eq!(*counts[2].lock().unwrap(), 1, "disconnected");
        assert_eq!(
            *counts[3].lock().unwrap(),
            0,
            "the stream replaced the message callback and then removed it"
        );
    }

    /// An event stream owns all four, so stopping it drops every sender and
    /// the receiver sees the channel close.
    #[test]
    fn test_stop_stream_closes_an_event_stream() {
        let cli = test_client();
        let rx = cli.get_event_stream(10);
        assert!(!rx.is_closed());

        cli.stop_stream();
        assert!(rx.is_closed(), "senders still held in the callback slots");

        dispatch_one_of_each(&cli);
        assert!(rx.try_recv().is_err(), "the stream is still fed");
    }

    /// A callback the application installed *after* starting a stream is not
    /// the stream's to remove.
    #[test]
    fn test_stop_stream_leaves_a_replaced_callback_alone() {
        let mut cli = test_client();
        let _rx = cli.get_stream(10);

        let calls = counter();
        let c = calls.clone();
        cli.set_message_callback(move |_, _| *c.lock().unwrap() += 1);

        cli.stop_stream();
        cli.dispatch_event(crate::runtime::Event::MessageArrived(Message::new(
            "t",
            "p",
            crate::QOS_0,
        )));
        assert_eq!(*calls.lock().unwrap(), 1, "the replacement was removed");
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
        // Twice: the arriving message, and the `None` that a lost connection
        // sends to mark the end of the message stream.
        assert_eq!(*counts[3].lock().unwrap(), 2, "message");
    }

    /// `remove_callbacks` still clears everything, stream or not.
    #[test]
    fn test_remove_callbacks_clears_a_stream_too() {
        let cli = test_client();
        let counts = count_all_callbacks(&cli);
        let rx = cli.get_event_stream(10);

        cli.remove_callbacks();
        assert!(rx.is_closed());
        dispatch_one_of_each(&cli);

        for (i, count) in counts.iter().enumerate() {
            assert_eq!(*count.lock().unwrap(), 0, "callback {i}");
        }
    }
}
