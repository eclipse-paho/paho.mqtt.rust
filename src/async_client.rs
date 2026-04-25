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
//! This presents an asynchronous API that is similar to the other Paho MQTT
//! clients, but uses Token objects that implement the Futures trait, so
//! can be used in much more flexible ways than the other language clients.
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
    AsyncReceiver, ConnectOptions, ConnectResponse, ConnectToken, CreateOptions,
    DeliveryToken, DisconnectOptions, Event, FilePersistence, Message, PersistenceType,
    Properties, ReasonCode, SubscribeManyToken, SubscribeOptions, SubscribeToken,
    SyncReceiver, UnsubscribeManyToken, UnsubscribeToken, UserData,
    error::{Error, Result},
    persistence::ClientPersistence,
    runtime::{BufConfig, Command, ConnectParams, RuntimeHandle, ServerAddr},
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
fn make_persistence(
    typ: PersistenceType,
) -> Option<Arc<Mutex<Box<dyn ClientPersistence>>>> {
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
    /// The options used to create the client.
    opts: CreateOptions,
    /// The options for the most recent connection attempt.
    conn_opts: RwLock<ConnectOptions>,
    /// User-registered event callbacks.
    callback_context: Mutex<CallbackContext>,
    /// Persistence store, if any.
    persist: Option<Arc<Mutex<Box<dyn ClientPersistence>>>>,
}

// The client is safe to send or share between threads.
unsafe impl Send for InnerAsyncClient {}
unsafe impl Sync for InnerAsyncClient {}

/// User callback type for when the client is connected.
pub type ConnectedCallback = dyn FnMut(&AsyncClient, &ConnectResponse) + Send + 'static;

/// User callback type for when the connection is lost from the broker.
pub type ConnectionLostCallback = dyn FnMut(&AsyncClient) + Send + 'static;

/// User callback type for when the client receives a disconnect packet.
pub type DisconnectedCallback =
    dyn FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static;

/// User callback signature for when subscribed messages are received.
pub type MessageArrivedCallback = dyn FnMut(&AsyncClient, Option<Message>) + Send + 'static;

/// User callback type for a TLS error
pub type TlsErrorCallback = dyn Fn(&AsyncClient, &str) + Send + Sync + 'static;

#[derive(Default)]
struct CallbackContext {
    on_connected: Option<Box<ConnectedCallback>>,
    on_connection_lost: Option<Box<ConnectionLostCallback>>,
    on_disconnected: Option<Box<DisconnectedCallback>>,
    on_message_arrived: Option<Box<MessageArrivedCallback>>,
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

        let rt = RuntimeHandle::spawn(BufConfig {
            send_while_disconnected: opts.send_while_disconnected,
            max_buffered_msgs: opts.max_buffered_msgs,
            allow_send_any_time: opts.allow_send_any_time,
            delete_oldest: opts.delete_oldest,
            restore_msgs: opts.restore_msgs,
            persist_qos0: opts.persist_qos0,
        });
        let (cmd_tx, event_rx) = rt.into_parts();

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
            opts,
            conn_opts: RwLock::new(initial_conn_opts),
            callback_context: Mutex::new(CallbackContext::default()),
            persist,
        });

        let weak = Arc::downgrade(&inner);
        std::thread::Builder::new()
            .name("mqtt-events".into())
            .spawn(move || {
                while let Ok(ev) = event_rx.recv_blocking() {
                    let Some(inner) = weak.upgrade()
                    else {
                        break;
                    };
                    let cli = AsyncClient { inner };
                    cli.dispatch_event(ev);
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

        // None → keep the client's current version unchanged.
        // Some(opts) → use opts.mqtt_version and update the stored version.
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
        }
        else {
            opts.server_uris.clone()
        };
        let mut servers: Vec<(ServerAddr, String)> = Vec::with_capacity(uris.len());
        for uri in uris {
            match parse_server_uri(&uri) {
                Ok(addr) => servers.push((addr, uri)),
                Err(e) => return ConnectToken::from(e),
            }
        }

        let keep_alive_secs =
            opts.keep_alive_interval.as_secs().min(u16::MAX as u64) as u16;

        let will = opts.will.as_ref().map(|msg| {
            paho_mqtt_codec::Will::new(
                msg.topic.clone(),
                msg.payload.clone(),
                msg.qos,
                msg.retain,
            )
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
            tls: opts.tls,
            #[cfg(feature = "websocket")]
            http_headers: opts.http_headers,
            auto_reconnect: opts.auto_reconnect,
            min_retry_interval: opts.min_retry_interval,
            max_retry_interval: opts.max_retry_interval,
            max_inflight: opts.max_inflight as usize,
        };

        let cmd = Command::Connect {
            params: Box::new(params),
            tx: comp,
            connected_flag: Arc::clone(&self.inner.connected),
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
        debug!("Disconnecting");

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

        if let Some(ref mut cb) = self
            .inner
            .callback_context
            .lock()
            .unwrap()
            .on_message_arrived
        {
            cb(self, None);
        }

        tok
    }

    /// Attempts to publish a message to the MQTT broker, but returns an
    /// error immediately if there's a problem creating or queuing the
    /// message.
    ///
    /// Returns a Publish Error on failure so that the original message
    /// can be recovered and sent again.
    pub fn try_publish(&self, msg: Message) -> Result<DeliveryToken> {
        debug!("Publish: {:?}", msg);

        self.require_v5_for_props(&msg.props)?;

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

        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Err(Error::Disconnected);
        }

        Ok(tok)
    }

    /// Publishes a message to the MQTT broker.
    ///
    /// Returns a Delivery Token to track the progress of the operation.
    ///
    pub fn publish(&self, msg: Message) -> DeliveryToken {
        match self.try_publish(msg) {
            Ok(tok) => tok,
            Err(e) => DeliveryToken::from_err(e),
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
        Q: Into<QoS>,
    {
        let (tok, comp) = SubscribeToken::new();
        let cmd = Command::Subscribe {
            filters: vec![(topic.into(), qos.into(), SubscribeOptions::default())],
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
        Q: Into<QoS>,
        T: Into<SubscribeOptions>,
        P: Into<Option<Properties>>,
    {
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
            filters: vec![(topic.into(), qos.into(), opts)],
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
        Q: Into<QoS> + Copy,
    {
        let n = topics.len();
        if n != qos.len() {
            return SubscribeManyToken::from_err(Error::General(
                "topics/qos length mismatch",
            ));
        }

        let (tok, comp) = SubscribeManyToken::new();
        let filters = topics
            .iter()
            .zip(qos.iter())
            .map(|(t, q)| {
                (
                    t.as_ref().to_string(),
                    (*q).into(),
                    SubscribeOptions::default(),
                )
            })
            .collect();

        let cmd = Command::Subscribe {
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
    pub fn subscribe_many_same_qos<T>(&self, topics: &[T], qos: i32) -> SubscribeManyToken
    where
        T: AsRef<str>,
    {
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
        Q: Into<QoS> + Copy,
        P: Into<Option<Properties>>,
    {
        let n = topics.len();
        if n != qos.len() {
            return SubscribeManyToken::from_err(Error::General(
                "topics/qos length mismatch",
            ));
        }
        if !opts.is_empty() && opts.len() != n {
            return SubscribeManyToken::from_err(Error::General(
                "topics/opts length mismatch",
            ));
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
                Ok((t.as_ref().to_string(), (*q).into(), sub_opts))
            })
            .collect();

        let filters = match filters {
            Ok(f) => f,
            Err(e) => return SubscribeManyToken::from_err(e),
        };

        let (tok, comp) = SubscribeManyToken::new();
        let cmd = Command::Subscribe {
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
    pub fn unsubscribe_with_options<S>(
        &self,
        topic: S,
        props: Properties,
    ) -> UnsubscribeToken
    where
        S: Into<String>,
    {
        if let Err(e) = self.require_v5_for_props(&props) {
            return UnsubscribeToken::from_err(e);
        }
        let (tok, comp) = UnsubscribeToken::new();
        let cmd = Command::Unsubscribe {
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
        }
        else {
            Ok(())
        }
    }

    /// Returns `Err(WrongMqttVersion)` if any v5-only subscription option is
    /// set but the current connection is not MQTT v5.
    fn require_v5_for_sub_opts(&self, opts: &SubscribeOptions) -> Result<()> {
        if self.mqtt_version() < MqttVersion::V5
            && (opts.no_local
                || opts.retain_as_published
                || opts.retain_handling
                    != crate::subscribe_options::RetainHandling::default())
        {
            Err(Error::WrongMqttVersion)
        }
        else {
            Ok(())
        }
    }

    /// Dispatches an event from the runtime to the registered callbacks.
    fn dispatch_event(&self, ev: crate::runtime::Event) {
        use crate::runtime::Event as Ev;
        match ev {
            Ev::Connected(response) => {
                // connected flag already set by event loop before token resolved
                let mut cbctx = self.inner.callback_context.lock().unwrap();
                if let Some(ref mut cb) = cbctx.on_connected {
                    cb(self, &response);
                }
            }
            Ev::ConnectionLost(_err) => {
                self.inner.connected.store(false, Ordering::SeqCst);
                let mut cbctx = self.inner.callback_context.lock().unwrap();
                if let Some(ref mut cb) = cbctx.on_message_arrived {
                    cb(self, None);
                }
                if let Some(ref mut cb) = cbctx.on_connection_lost {
                    cb(self);
                }
            }
            Ev::MessageArrived(msg) => {
                let mut cbctx = self.inner.callback_context.lock().unwrap();
                if let Some(ref mut cb) = cbctx.on_message_arrived {
                    cb(self, Some(msg));
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
        MqttVersion::from(self.mqtt_version_raw())
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
    pub fn set_connected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, &crate::ConnectResponse) + Send + 'static,
    {
        self.inner.callback_context.lock().unwrap().on_connected = Some(Box::new(cb));
    }

    /// Removes the callback for when the connection is established.
    pub fn remove_connected_callback(&self) {
        self.inner.callback_context.lock().unwrap().on_connected = None;
    }

    /// Sets the callback for when the connection is lost with the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///   function or a closure.
    pub fn set_connection_lost_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient) + Send + 'static,
    {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_connection_lost = Some(Box::new(cb));
    }

    /// Removes the callback for when the connection is lost.
    pub fn remove_connection_lost_callback(&self) {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_connection_lost = None;
    }

    /// Sets the callback for when a disconnect message arrives from the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///   function or a closure.
    pub fn set_disconnected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static,
    {
        self.inner.callback_context.lock().unwrap().on_disconnected = Some(Box::new(cb));
    }

    /// Removes the callback for when a disconnect message is received.
    pub fn remove_disconnected_callback(&self) {
        self.inner.callback_context.lock().unwrap().on_disconnected = None;
    }

    /// Sets the callback for when a message arrives from the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///   function or a closure.
    ///
    pub fn set_message_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Option<Message>) + Send + 'static,
    {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_message_arrived = Some(Box::new(cb));
    }

    /// Removes the callback for when a message arrives from the broker.
    pub fn remove_message_callback(&self) {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_message_arrived = None;
    }

    /// Starts the client consuming messages for a blocking (non-async) app.
    ///
    /// Returns the receiving-end of the channel for the application to
    /// get the messages.
    pub fn start_consuming(&self) -> SyncReceiver<Option<Message>> {
        let (tx, rx) = sync_channel::unbounded();

        self.set_message_callback(move |_, optmsg| {
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
    pub fn start_consuming_events<L>(&mut self, buffer_lim: L) -> SyncReceiver<Event>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => sync_channel::bounded(lim),
            None => sync_channel::unbounded(),
        };

        self.set_connected_callback({
            let tx = tx.clone();
            move |_cli, rsp| {
                log_channel_err(tx.try_send(Event::Connected(rsp.clone())));
            }
        });

        self.set_message_callback({
            let tx = tx.clone();
            move |_cli, msg| {
                if let Some(msg) = msg {
                    log_channel_err(tx.try_send(Event::Message(msg)));
                }
            }
        });

        self.set_connection_lost_callback({
            let tx = tx.clone();
            move |_cli| {
                log_channel_err(tx.try_send(Event::ConnectionLost));
            }
        });

        self.set_disconnected_callback({
            let tx = tx.clone();
            move |_cli, props, reason_code| {
                log_channel_err(tx.try_send(Event::Disconnected { props, reason_code }));
            }
        });

        rx
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

        self.set_message_callback(move |_, msg| {
            log_stream_err(tx.try_send(msg));
        });

        rx
    }

    /// Creates an async event stream.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if `buffer_lim` is `Some`, or an unbounded channel if `None`.
    pub fn get_event_stream<L>(&mut self, buffer_lim: L) -> AsyncReceiver<Event>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => async_channel::bounded(lim),
            None => async_channel::unbounded(),
        };

        self.set_connected_callback({
            let tx = tx.clone();
            move |_cli, rsp| {
                log_stream_err(tx.try_send(Event::Connected(rsp.clone())));
            }
        });

        self.set_message_callback({
            let tx = tx.clone();
            move |_cli, msg| {
                if let Some(msg) = msg {
                    log_stream_err(tx.try_send(Event::Message(msg)));
                }
            }
        });

        self.set_connection_lost_callback({
            let tx = tx.clone();
            move |_cli| {
                log_stream_err(tx.try_send(Event::ConnectionLost));
            }
        });

        self.set_disconnected_callback({
            let tx = tx.clone();
            move |_cli, props, reason_code| {
                log_stream_err(tx.try_send(Event::Disconnected { props, reason_code }));
            }
        });

        rx
    }

    /// Stops the client from streaming messages in.
    pub fn stop_stream(&self) {
        self.remove_message_callback();
    }

    /// Returns the client ID used for the client instance.
    pub fn client_id(&self) -> &str {
        &self.inner.opts.client_id
    }

    /// Returns the server URI used for connection.
    pub fn server_uri(&self) -> &str {
        &self.inner.opts.server_uri
    }
}

// The client is safe to send or share between threads.
unsafe impl Send for AsyncClient {}
unsafe impl Sync for AsyncClient {}

fn log_send_err(is_full: bool) {
    if is_full {
        warn!("Event stream full. Losing messages");
    }
    else {
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
    }
    else {
        "/".to_string()
    };

    let (host, port) = if let Some(colon) = authority.rfind(':') {
        let host = authority[..colon].to_string();
        let port = authority[colon + 1..]
            .parse::<u16>()
            .map_err(|_| Error::General("invalid port in server URI"))?;
        (host, port)
    }
    else {
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
    #[cfg(feature = "websocket")]
    if let Some(rest) = uri.strip_prefix("ws://") {
        let (host, port, path) = parse_host_port_path(rest, 1883)?;
        return Ok(ServerAddr::Ws { host, port, path });
    }

    #[cfg(all(feature = "websocket", any(feature = "tls", feature = "openssl")))]
    if let Some(rest) = uri.strip_prefix("wss://") {
        let (host, port, path) = parse_host_port_path(rest, 8883)?;
        return Ok(ServerAddr::Wss { host, port, path });
    }

    // wss:// with websocket but without a TLS backend is an error.
    #[cfg(all(feature = "websocket", not(any(feature = "tls", feature = "openssl"))))]
    if uri.starts_with("wss://") {
        return Err(Error::TlsNotSupported);
    }

    // Plain TCP / TLS transports (wss:// falls through to TLS when websocket
    // feature is absent).
    let (default_port, is_tls, rest) = if let Some(s) = uri
        .strip_prefix("mqtts://")
        .or_else(|| uri.strip_prefix("ssl://"))
        .or_else(|| uri.strip_prefix("tls://"))
        .or_else(|| uri.strip_prefix("wss://"))
    {
        (8883u16, true, s)
    }
    else if let Some(s) = uri
        .strip_prefix("mqtt://")
        .or_else(|| uri.strip_prefix("tcp://"))
        .or_else(|| uri.strip_prefix("ws://"))
    {
        (1883u16, false, s)
    }
    else {
        (1883u16, false, uri)
    };

    let (host, port, _) = parse_host_port_path(rest, default_port)?;

    if is_tls {
        #[cfg(any(feature = "tls", feature = "openssl"))]
        return Ok(ServerAddr::Tls { host, port });
        #[cfg(not(any(feature = "tls", feature = "openssl")))]
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

    #[test]
    fn test_with_client_id() {
        let options = CreateOptionsBuilder::new().client_id("test1").finalize();
        let client = AsyncClient::new(options);
        assert!(
            client.is_ok(),
            "Error in creating async client with client_id"
        );
        let tok = client.unwrap().connect(None);
        match tok.wait() {
            Ok(_) => (),
            Err(e) => println!("(Error) {}", e),
        }
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
        #[cfg(any(feature = "tls", feature = "openssl"))]
        assert!(matches!(
            parse_server_uri("mqtts://broker.example.com:8883").unwrap(),
            ServerAddr::Tls { host, port } if host == "broker.example.com" && port == 8883
        ));
        #[cfg(not(any(feature = "tls", feature = "openssl")))]
        assert!(matches!(
            parse_server_uri("mqtts://broker.example.com:8883"),
            Err(Error::TlsNotSupported)
        ));
        assert!(matches!(
            parse_server_uri("mqtt://localhost").unwrap(),
            ServerAddr::Tcp { host, port } if host == "localhost" && port == 1883
        ));
        #[cfg(any(feature = "tls", feature = "openssl"))]
        assert!(matches!(
            parse_server_uri("mqtts://secure.broker.io").unwrap(),
            ServerAddr::Tls { host, port } if host == "secure.broker.io" && port == 8883
        ));
        #[cfg(not(any(feature = "tls", feature = "openssl")))]
        assert!(matches!(
            parse_server_uri("mqtts://secure.broker.io"),
            Err(Error::TlsNotSupported)
        ));
        // Without the websocket feature ws:// maps to plain TCP.
        #[cfg(not(feature = "websocket"))]
        assert!(matches!(
            parse_server_uri("ws://localhost:8080/mqtt").unwrap(),
            ServerAddr::Tcp { host, port } if host == "localhost" && port == 8080
        ));
        // With the websocket feature ws:// maps to a WebSocket address.
        #[cfg(feature = "websocket")]
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
}
