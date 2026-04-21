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

use crate::event::Event;
use crate::{
    AsyncReceiver, ConnectOptions, ConnectToken, CreateOptions, DeliveryToken,
    DisconnectOptions, Message, Properties, ReasonCode, SyncReceiver, UserData,
    errors::{Error, Result},
    token::Token,
    types::*,
};
use crossbeam_channel as sync_channel;
use std::{
    result::Result as StdResult,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

// C-library imports — only needed when the tokio runtime is not active.
#[cfg(not(feature = "tokio"))]
use crate::{
    PersistenceType, ServerRequest, SubscribeManyToken, SubscribeOptions, SubscribeToken,
    UnsubscribeManyToken, UnsubscribeToken, errors, ffi,
    ffi_utils::{
        CallOptionsFfi, CallOptionsFfiBuilder, ConnectOptionsFfi, DisconnectOptionsFfi,
        MessageFfi, StringCollectionFfi,
    },
    responses::{
        connect_convert, publish_convert, subscribe_convert, unit_convert,
        unsubscribe_convert,
    },
    token::TokenInner,
};
#[cfg(not(feature = "tokio"))]
use std::{
    ffi::{CStr, CString},
    mem,
    os::raw::{c_char, c_int, c_void},
    ptr, slice, str,
    sync::Once,
    sync::atomic::AtomicU32 as _AtomicU32,
};

// Runtime imports — only when the tokio feature is active.
#[cfg(feature = "tokio")]
use crate::{
    SubscribeManyToken, SubscribeOptions, SubscribeToken, UnsubscribeManyToken,
    UnsubscribeToken,
    runtime::{Command, RuntimeHandle},
};
#[cfg(feature = "tokio")]
use std::sync::atomic::AtomicBool;

/////////////////////////////////////////////////////////////////////////////
// AsynClient

/// An asynchronous MQTT connection client.
#[derive(Clone)]
pub struct AsyncClient {
    pub(crate) inner: Arc<InnerAsyncClient>,
}

/// Implementation details for the asynchronous MQTT connection client.
pub(crate) struct InnerAsyncClient {
    // ---- Pure-Rust runtime fields (tokio feature) ----
    /// Command sender to the internal event loop.
    #[cfg(feature = "tokio")]
    cmd_tx: async_channel::Sender<Command>,
    /// Whether the client is currently connected.
    #[cfg(feature = "tokio")]
    connected: AtomicBool,

    // ---- C-library handle (no tokio feature) ----
    /// The handle to the Paho C client.
    #[cfg(not(feature = "tokio"))]
    handle: ffi::MQTTAsync,

    // ---- Common fields ----
    /// The MQTT version of the connection.
    mqtt_version: AtomicU32,
    /// The options used to create the client.
    opts: CreateOptions,
    /// The options for the most recent connection attempt.
    conn_opts: RwLock<ConnectOptions>,
    /// User-registered event callbacks.
    callback_context: Mutex<CallbackContext>,
}

// The client is safe to send or share between threads.
unsafe impl Send for InnerAsyncClient {}
unsafe impl Sync for InnerAsyncClient {}

/// User callback type for when the client is connected.
pub type ConnectedCallback = dyn FnMut(&AsyncClient) + Send + 'static;

/// User callback type for when the connection is lost from the broker.
pub type ConnectionLostCallback = dyn FnMut(&AsyncClient) + Send + 'static;

/// User callback type for when the client receives a disconnect packet.
pub type DisconnectedCallback =
    dyn FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static;

/// User callback signature for when subscribed messages are received.
pub type MessageArrivedCallback = dyn FnMut(&AsyncClient, Option<Message>) + Send + 'static;

/// User callback type for an SSL/TLS error
pub type SslErrorCallback = dyn Fn(&AsyncClient, &str) + Send + Sync + 'static;

#[derive(Default)]
struct CallbackContext {
    on_connected: Option<Box<ConnectedCallback>>,
    on_connection_lost: Option<Box<ConnectionLostCallback>>,
    on_disconnected: Option<Box<DisconnectedCallback>>,
    on_message_arrived: Option<Box<MessageArrivedCallback>>,
}

/////////////////////////////////////////////////////////////////////////////
// C-library init guard — only used by the non-tokio path.
#[cfg(not(feature = "tokio"))]
static C_LIB_INIT: Once = Once::new();

/////////////////////////////////////////////////////////////////////////////
// Tokio (pure-Rust) path
/////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "tokio")]
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
        let opts = opts.into();
        debug!("Create options: {:?}", opts);

        let rt = RuntimeHandle::spawn();
        let (cmd_tx, event_rx) = rt.into_parts();

        let inner = Arc::new(InnerAsyncClient {
            cmd_tx,
            connected: AtomicBool::new(false),
            mqtt_version: AtomicU32::new(opts.mqtt_version as u32),
            opts,
            conn_opts: RwLock::new(ConnectOptions::new()),
            callback_context: Mutex::new(CallbackContext::default()),
        });

        // Spawn a thread to dispatch events from the runtime to callbacks.
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
        let opts = opts.into().unwrap_or_default();
        debug!("Connecting with opts: {:?}", opts);

        if let Err(err) = opts.check() {
            return ConnectToken::from(err);
        }

        let ver = if opts.mqtt_version == MqttVersion::Default {
            MqttVersion::V3_1_1
        }
        else {
            opts.mqtt_version
        };

        let (host, port) = match parse_server_uri(&self.inner.opts.server_uri) {
            Ok(v) => v,
            Err(e) => return ConnectToken::from(e),
        };

        let keep_alive_secs =
            opts.keep_alive_interval.as_secs().min(u16::MAX as u64) as u16;

        let will = opts.will.as_ref().map(|msg| {
            paho_mqtt_codec::Will::new(
                msg.topic().to_string(),
                msg.payload().to_vec(),
                msg.qos(),
                msg.retain(),
            )
        });

        let (tok, comp) = ConnectToken::new();
        self.set_mqtt_version(ver);
        *self.inner.conn_opts.write().unwrap() = opts.clone();

        let cmd = Command::Connect {
            host,
            port,
            client_id: self.inner.opts.client_id.clone(),
            clean_start: opts.clean_start,
            keep_alive_secs,
            user_name: opts.user_name,
            password: opts.password,
            will,
            ver,
            props: Default::default(),
            tx: comp,
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
        let _opts = opts.into().unwrap_or_default();
        debug!("Disconnecting");

        let (tok, comp) = Token::<()>::new();

        let cmd = Command::Disconnect {
            reason_code: paho_mqtt_codec::ReasonCode::NormalDisconnection,
            props: Default::default(),
            tx: comp,
        };

        if self.inner.cmd_tx.send_blocking(cmd).is_err() {
            return Token::<()>::from_err(Error::Disconnected);
        }

        // Push a None into the message stream to cleanly shutdown consumers.
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
        use bytes::Bytes;
        debug!("Publish: {:?}", msg);

        let qos = msg.qos();
        let (tok, comp) = DeliveryToken::new();

        let cmd = Command::Publish {
            topic: msg.topic().to_string(),
            payload: Bytes::copy_from_slice(msg.payload()),
            qos,
            retain: msg.retain(),
            props: Default::default(),
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
            filters: vec![(topic.into(), qos.into())],
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
        _opts: T,
        _props: P,
    ) -> SubscribeToken
    where
        S: Into<String>,
        Q: Into<QoS>,
        T: Into<SubscribeOptions>,
        P: Into<Option<Properties>>,
    {
        self.subscribe(topic, qos)
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
            .map(|(t, q)| (t.as_ref().to_string(), (*q).into()))
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
        _opts: &[SubscribeOptions],
        _props: P,
    ) -> SubscribeManyToken
    where
        T: AsRef<str>,
        Q: Into<QoS> + Copy,
        P: Into<Option<Properties>>,
    {
        self.subscribe_many(topics, qos)
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
        _props: Properties,
    ) -> UnsubscribeToken
    where
        S: Into<String>,
    {
        self.unsubscribe(topic)
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
        _props: Properties,
    ) -> UnsubscribeManyToken
    where
        T: AsRef<str>,
    {
        self.unsubscribe_many(topics)
    }

    /// Determines if this client is currently connected to an MQTT broker.
    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::SeqCst)
    }

    /// Dispatches an event from the runtime to the registered callbacks.
    fn dispatch_event(&self, ev: crate::runtime::Event) {
        use crate::runtime::Event as Ev;
        match ev {
            Ev::Connected(_response) => {
                self.inner.connected.store(true, Ordering::SeqCst);
                let mut cbctx = self.inner.callback_context.lock().unwrap();
                if let Some(ref mut cb) = cbctx.on_connected {
                    cb(self);
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
            Ev::MessageArrived(recv) => {
                use crate::message::OutMessage;
                let msg = Message::Out(OutMessage {
                    topic: recv.topic,
                    payload: recv.payload.into(),
                    qos: recv.qos,
                    retain: recv.retain,
                    dup: recv.dup,
                    msgid: recv.packet_id,
                    props: Properties::default(),
                });
                let mut cbctx = self.inner.callback_context.lock().unwrap();
                if let Some(ref mut cb) = cbctx.on_message_arrived {
                    cb(self, Some(msg));
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// C-library path
/////////////////////////////////////////////////////////////////////////////

#[cfg(not(feature = "tokio"))]
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
        C_LIB_INIT.call_once(|| {
            if let Some(lvl) = crate::c_trace_level() {
                debug!("Setting Paho C log level to {}", lvl);
                unsafe {
                    ffi::MQTTAsync_setTraceCallback(Some(crate::on_c_trace));
                    ffi::MQTTAsync_setTraceLevel(lvl);
                }
            }
        });

        let opts = opts.into();
        debug!("Create options: {:?}", opts);

        let mut cli = InnerAsyncClient {
            handle: ptr::null_mut(),
            mqtt_version: AtomicU32::new(opts.mqtt_version as u32),
            opts,
            conn_opts: RwLock::new(ConnectOptions::new()),
            callback_context: Mutex::new(CallbackContext::default()),
        };

        let file_path;
        let mut persist_copts;

        let (ptype, pptr) = match &cli.opts.persistence {
            PersistenceType::None => (ffi::MQTTCLIENT_PERSISTENCE_NONE, ptr::null_mut()),
            PersistenceType::File => (ffi::MQTTCLIENT_PERSISTENCE_DEFAULT, ptr::null_mut()),
            PersistenceType::FilePath(path) => {
                let s = path.to_str().ok_or(errors::Error::PersistenceError)?;
                file_path = CString::new(s).unwrap_or_default();
                let pptr = file_path.as_ptr() as *mut c_void;
                (ffi::MQTTCLIENT_PERSISTENCE_DEFAULT, pptr)
            }
            PersistenceType::User(user_persist) => {
                persist_copts = user_persist.c_struct();
                let pptr = &mut persist_copts as *mut _ as *mut c_void;
                (ffi::MQTTCLIENT_PERSISTENCE_USER, pptr)
            }
        };

        debug!("Creating client with persistence: {}", ptype);

        let server_uri = CString::new(cli.opts.server_uri.clone())?;
        let client_id = CString::new(cli.opts.client_id.clone())?;
        let mut copts = cli.opts.c_struct();

        let rc = unsafe {
            ffi::MQTTAsync_createWithOptions(
                &mut cli.handle as *mut *mut c_void,
                server_uri.as_ptr(),
                client_id.as_ptr(),
                ptype as c_int,
                pptr,
                &mut copts,
            ) as i32
        };

        if rc != 0 {
            warn!("Create failure: {}", rc);
            return Err(rc.into());
        }

        let cli = AsyncClient {
            inner: Arc::new(cli),
        };

        debug!(
            "AsyncClient w/ Inner {:?} and Handle: {:?}",
            Arc::as_ptr(&cli.inner),
            cli.inner.handle
        );
        Ok(cli)
    }

    /// Constructs a client from a raw pointer to the inner structure.
    pub(crate) unsafe fn from_raw(ptr: *mut c_void) -> AsyncClient {
        AsyncClient {
            inner: unsafe { Arc::from_raw(ptr as *mut InnerAsyncClient) },
        }
    }

    /// Consumes the client, returning the inner wrapped value.
    pub(crate) fn into_raw(self) -> *mut c_void {
        Arc::into_raw(self.inner) as *mut c_void
    }

    /// Gets the client "C" handle, normally for diagnostics.
    pub(crate) fn handle(&self) -> ffi::MQTTAsync {
        self.inner.handle
    }

    // C callback: client connected.
    unsafe extern "C" fn on_connected(context: *mut c_void, _cause: *mut c_char) {
        if context.is_null() {
            warn!("on_connected: null client context.");
            return;
        }
        debug!("Connected! Client {:?}", context);
        let cli = unsafe { AsyncClient::from_raw(context) };
        if let Some(ref mut cb) = cli.inner.callback_context.lock().unwrap().on_connected {
            trace!("Invoking connected callback");
            cb(&cli);
        }
        let _ = cli.into_raw();
    }

    // C callback: connection lost.
    unsafe extern "C" fn on_connection_lost(context: *mut c_void, _cause: *mut c_char) {
        if context.is_null() {
            warn!("on_connection_lost: null client context");
            return;
        }
        warn!("Connection lost. Client: {:?}", context);
        let cli = unsafe { AsyncClient::from_raw(context) };
        {
            let mut cbctx = cli.inner.callback_context.lock().unwrap();
            if let Some(ref mut cb) = cbctx.on_message_arrived {
                trace!("Invoking message callback with None");
                cb(&cli, None);
            }
            if let Some(ref mut cb) = cbctx.on_connection_lost {
                trace!("Invoking connection lost callback");
                cb(&cli);
            }
        }
        let _ = cli.into_raw();
    }

    // C callback: DISCONNECT packet arrived.
    unsafe extern "C" fn on_disconnected(
        context: *mut c_void,
        cprops: *mut ffi::MQTTProperties,
        reason: ffi::MQTTReasonCodes,
    ) {
        if context.is_null() {
            warn!("on_disconnected: null client context");
            return;
        }
        debug!(
            "Disconnected with reason code: {}. Client: {:?}",
            reason, context
        );
        let cli = unsafe { AsyncClient::from_raw(context) };
        {
            let mut cbctx = cli.inner.callback_context.lock().unwrap();
            if let Some(ref mut cb) = cbctx.on_message_arrived {
                trace!("Invoking message callback with None");
                cb(&cli, None);
            }
            if let Some(ref mut cb) = cbctx.on_disconnected {
                let reason_code = ReasonCode::from(reason);
                let props = unsafe { Properties::from(&*cprops) };
                trace!("Invoking disconnected callback");
                cb(&cli, props, reason_code);
            }
        }
        let _ = cli.into_raw();
    }

    // C callback: message arrived.
    unsafe extern "C" fn on_message_arrived(
        context: *mut c_void,
        topic: *mut c_char,
        topic_len: c_int,
        cmsg: *mut ffi::MQTTAsync_message,
    ) -> c_int {
        if context.is_null() || topic.is_null() || cmsg.is_null() {
            warn!("on_message_arrived: null pointer.");
            return 1;
        }
        debug!(
            "Message arrived. Client: {:?}, topic: {:?} len {:?} cmsg: {:?}",
            context,
            topic,
            topic_len,
            unsafe { *cmsg }
        );
        let cli = unsafe { AsyncClient::from_raw(context) };
        if let Some(ref mut cb) = cli
            .inner
            .callback_context
            .lock()
            .unwrap()
            .on_message_arrived
        {
            let len = match topic_len {
                0 => unsafe { CStr::from_ptr(topic).to_bytes().len() },
                len if len < 0 => 0,
                _ => topic_len as usize,
            };
            let msg = Message::from_c_parts(topic, len, cmsg);
            trace!("Invoking message callback");
            cb(&cli, Some(msg));
        }
        let _ = cli.into_raw();
        1
    }

    // C callback: SSL error.
    unsafe extern "C" fn on_ssl_error(
        err: *const c_char,
        len: usize,
        context: *mut c_void,
    ) -> i32 {
        if context.is_null() {
            warn!("on_ssl_error: null client context");
            return 0;
        }
        if err.is_null() || len == 0 {
            warn!("on_ssl_error: no error provided");
            return 0;
        }
        debug!("SSL Error for Client: {:?}", context);
        let cli = unsafe { AsyncClient::from_raw(context) };
        {
            if let Some(ref ssl) = cli.inner.conn_opts.read().unwrap().ssl
                && let Some(ref cb) = ssl.on_ssl_error
            {
                let s =
                    str::from_utf8(unsafe { slice::from_raw_parts(err as *mut u8, len) })
                        .map(str::trim)
                        .unwrap_or_default();
                cb(&cli, s);
            }
        }
        let _ = cli.into_raw();
        0
    }

    // Registers the C-library disconnection callbacks.
    fn set_disconnection_callbacks(&self) {
        let inner: &InnerAsyncClient = &self.inner;
        unsafe {
            ffi::MQTTAsync_setConnectionLostCallback(
                inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_connection_lost),
            );
            ffi::MQTTAsync_setDisconnected(
                inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_disconnected),
            );
        }
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
        let opts = opts.into().unwrap_or_default();
        debug!(
            "Connecting. Handle: {:?}, opts: {:?}",
            self.inner.handle, opts
        );

        if crate::is_secure_uri(self.server_uri()) && !opts.has_ssl_options() {
            return ConnectToken::from(Error::MissingSslOptions);
        }

        if let Err(err) = opts.check() {
            return ConnectToken::from(err);
        }

        if opts.mqtt_version() != MqttVersion::Default {
            self.set_mqtt_version(opts.mqtt_version());
        }

        let tok = ConnectToken::from_request(self, ServerRequest::Connect, connect_convert);
        let mut ffi_opts = ConnectOptionsFfi::from(&opts);
        ffi_opts.set_token(tok.clone());

        if let Some(ssl) = unsafe { ffi_opts.copts.ssl.as_mut() } {
            let inner: &InnerAsyncClient = &self.inner;
            ssl.ssl_error_cb = Some(AsyncClient::on_ssl_error);
            ssl.ssl_error_context = inner as *const _ as *mut c_void;
        }

        *self.inner.conn_opts.write().unwrap() = opts;

        let rc = unsafe { ffi::MQTTAsync_connect(self.inner.handle, &ffi_opts.copts) };
        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(ffi_opts.copts.context) });
            return ConnectToken::from_error(rc, connect_convert);
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
        debug!("Disconnecting client: {:?}", self.inner.handle);

        let opts = opts.into().unwrap_or_default();
        trace!("Disconnect options: {:?}", opts);

        let mut ffi_opts = DisconnectOptionsFfi::from(&opts);
        let tok = Token::<()>::with_client(self, ServerRequest::Disconnect, unit_convert);
        ffi_opts.copts.context = tok.clone().into_raw();

        if self.mqtt_version() < MqttVersion::V5 {
            ffi_opts.copts.onSuccess = Some(TokenInner::on_success);
            ffi_opts.copts.onFailure = Some(TokenInner::on_failure);
        }
        else {
            ffi_opts.copts.onSuccess5 = Some(TokenInner::on_success5);
            ffi_opts.copts.onFailure5 = Some(TokenInner::on_failure5);
        }

        let rc = unsafe { ffi::MQTTAsync_disconnect(self.inner.handle, &ffi_opts.copts) };
        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(ffi_opts.copts.context) });
            return Token::<()>::from_error(rc, unit_convert);
        }

        if let Some(ref mut cb) = self
            .inner
            .callback_context
            .lock()
            .unwrap()
            .on_message_arrived
        {
            trace!("Invoking message callback with None");
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

        let ver = self.mqtt_version();
        let ffi_msg = MessageFfi::try_from(msg.clone())?;

        let tok = DeliveryToken::new_pending(publish_convert);
        let mut call_opts = CallOptionsFfi::new(ver, tok.inner());

        let rc = unsafe {
            ffi::MQTTAsync_sendMessage(
                self.inner.handle,
                ffi_msg.topic().as_ptr() as *const c_char,
                ffi_msg.c_struct(),
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return Err(Error::Publish(rc, msg));
        }

        tok.set_msgid(call_opts.copts.token as i16);
        Ok(tok)
    }

    /// Publishes a message to the MQTT broker.
    ///
    /// Returns a Delivery Token to track the progress of the operation.
    ///
    pub fn publish(&self, msg: Message) -> DeliveryToken {
        match self.try_publish(msg) {
            Ok(tok) => tok,
            Err(Error::Publish(rc, _)) => DeliveryToken::from_error(rc, publish_convert),
            Err(e) => DeliveryToken::new_err(e, publish_convert),
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
        let ver = self.mqtt_version();
        let tok =
            SubscribeToken::from_request(None, ServerRequest::Subscribe, subscribe_convert);
        let mut call_opts = CallOptionsFfi::new(ver, tok.inner());
        let topic = CString::new(topic.into()).unwrap();
        let qos = qos.into() as i32;
        debug!("Subscribe to '{:?}' @ QOS {}", topic, qos);

        let rc = unsafe {
            ffi::MQTTAsync_subscribe(
                self.inner.handle,
                topic.as_ptr(),
                qos,
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return SubscribeToken::from_error(rc, subscribe_convert);
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
        debug_assert!(self.mqtt_version() >= MqttVersion::V5);

        let tok =
            SubscribeToken::from_request(None, ServerRequest::Subscribe, subscribe_convert);
        let mut call_opts = CallOptionsFfiBuilder::new()
            .token(tok.inner())
            .subscribe_options(opts.into())
            .properties(props.into().unwrap_or_default())
            .finalize();

        let topic = CString::new(topic.into()).unwrap();
        let qos = qos.into() as i32;
        debug!("Subscribe to '{:?}' @ QOS {}", topic, qos);

        let rc = unsafe {
            ffi::MQTTAsync_subscribe(
                self.inner.handle,
                topic.as_ptr(),
                qos,
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return SubscribeToken::from_error(rc, subscribe_convert);
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
            return SubscribeManyToken::from_error(-1, subscribe_convert);
        }

        let ver = self.mqtt_version();
        let tok = SubscribeManyToken::from_request(
            None,
            ServerRequest::SubscribeMany(n),
            subscribe_convert,
        );
        let mut call_opts = CallOptionsFfi::new(ver, tok.inner());
        let topics = StringCollectionFfi::new(topics);
        let qos: Vec<i32> = qos.iter().map(|q| (*q).into() as i32).collect();
        debug!("Subscribe to '{:?}' @ QOS {:?}", topics, qos);

        let rc = unsafe {
            ffi::MQTTAsync_subscribeMany(
                self.inner.handle,
                n as c_int,
                topics.as_c_arr_mut_ptr(),
                qos.as_ptr(),
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return SubscribeManyToken::from_error(rc, subscribe_convert);
        }
        tok
    }

    /// Subscribes to multiple topics simultaneously using the same QoS.
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
        debug_assert!(self.mqtt_version() >= MqttVersion::V5);

        let n = topics.len();
        let tok = SubscribeManyToken::from_request(
            None,
            ServerRequest::SubscribeMany(n),
            subscribe_convert,
        );
        let mut call_opts = CallOptionsFfiBuilder::new()
            .token(tok.inner())
            .subscribe_many_options(opts)
            .properties(props.into().unwrap_or_default())
            .finalize();

        let topics = StringCollectionFfi::new(topics);
        let qos: Vec<i32> = qos.iter().map(|q| (*q).into() as i32).collect();
        debug!(
            "Subscribe to '{:?}' @ QOS {:?} w/ opts: {:?}",
            topics, qos, opts
        );
        trace!("Subscribe call/response opts: {:?}", call_opts);

        let rc = unsafe {
            ffi::MQTTAsync_subscribeMany(
                self.inner.handle,
                n as c_int,
                topics.as_c_arr_mut_ptr(),
                qos.as_ptr(),
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return SubscribeManyToken::from_error(rc, subscribe_convert);
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
        let ver = self.mqtt_version();
        let tok = UnsubscribeToken::from_request(
            None,
            ServerRequest::Unsubscribe,
            unsubscribe_convert,
        );
        let mut call_opts = CallOptionsFfi::new(ver, tok.inner());
        let topic = CString::new(topic.into()).unwrap();
        debug!("Unsubscribe from '{:?}'", topic);

        let rc = unsafe {
            ffi::MQTTAsync_unsubscribe(
                self.inner.handle,
                topic.as_ptr(),
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return UnsubscribeToken::from_error(rc, unsubscribe_convert);
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
        debug_assert!(self.mqtt_version() >= MqttVersion::V5);

        let tok = UnsubscribeToken::from_request(
            None,
            ServerRequest::Unsubscribe,
            unsubscribe_convert,
        );
        let mut call_opts = CallOptionsFfiBuilder::new()
            .token(tok.inner())
            .properties(props)
            .finalize();

        let topic = CString::new(topic.into()).unwrap();
        debug!("Unsubscribe from '{:?}'", topic);

        let rc = unsafe {
            ffi::MQTTAsync_unsubscribe(
                self.inner.handle,
                topic.as_ptr(),
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return UnsubscribeToken::from_error(rc, unsubscribe_convert);
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
        let ver = self.mqtt_version();
        let n = topics.len();
        let tok = UnsubscribeManyToken::from_request(
            None,
            ServerRequest::UnsubscribeMany(n),
            unsubscribe_convert,
        );
        let mut call_opts = CallOptionsFfi::new(ver, tok.inner());
        let topics = StringCollectionFfi::new(topics);
        debug!("Unsubscribe from '{:?}'", topics);

        let rc = unsafe {
            ffi::MQTTAsync_unsubscribeMany(
                self.inner.handle,
                n as c_int,
                topics.as_c_arr_mut_ptr(),
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return UnsubscribeManyToken::from_error(rc, unsubscribe_convert);
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
        debug_assert!(self.mqtt_version() >= MqttVersion::V5);

        let n = topics.len();
        let tok = UnsubscribeManyToken::from_request(
            None,
            ServerRequest::UnsubscribeMany(n),
            unsubscribe_convert,
        );
        let mut call_opts = CallOptionsFfiBuilder::new()
            .token(tok.inner())
            .properties(props)
            .finalize();

        let topics = StringCollectionFfi::new(topics);
        debug!("Unsubscribe from '{:?}'", topics);

        let rc = unsafe {
            ffi::MQTTAsync_unsubscribeMany(
                self.inner.handle,
                n as c_int,
                topics.as_c_arr_mut_ptr(),
                &mut call_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { TokenInner::from_raw(call_opts.copts.context) });
            return UnsubscribeManyToken::from_error(rc, unsubscribe_convert);
        }
        tok
    }

    /// Determines if this client is currently connected to an MQTT broker.
    pub fn is_connected(&self) -> bool {
        unsafe { ffi::MQTTAsync_isConnected(self.inner.handle) != 0 }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Common API — works for both runtime paths
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
        F: FnMut(&AsyncClient) + Send + 'static,
    {
        let inner: &InnerAsyncClient = &self.inner;
        inner.callback_context.lock().unwrap().on_connected = Some(Box::new(cb));

        #[cfg(not(feature = "tokio"))]
        unsafe {
            ffi::MQTTAsync_setConnected(
                inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_connected),
            );
        }
    }

    /// Removes the callback for when the connection is established.
    pub fn remove_connected_callback(&self) {
        self.inner.callback_context.lock().unwrap().on_connected = None;

        #[cfg(not(feature = "tokio"))]
        unsafe {
            ffi::MQTTAsync_setConnected(self.inner.handle, ptr::null_mut(), None);
        }
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
        let inner: &InnerAsyncClient = &self.inner;
        inner.callback_context.lock().unwrap().on_connection_lost = Some(Box::new(cb));

        #[cfg(not(feature = "tokio"))]
        unsafe {
            ffi::MQTTAsync_setConnectionLostCallback(
                inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_connection_lost),
            );
        }
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
        let inner: &InnerAsyncClient = &self.inner;
        inner.callback_context.lock().unwrap().on_disconnected = Some(Box::new(cb));

        #[cfg(not(feature = "tokio"))]
        unsafe {
            ffi::MQTTAsync_setDisconnected(
                inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_disconnected),
            );
        }
    }

    /// Removes the callback for when a disconnect message is received.
    pub fn remove_disconnected_callback(&self) {
        self.inner.callback_context.lock().unwrap().on_disconnected = None;

        #[cfg(not(feature = "tokio"))]
        unsafe {
            ffi::MQTTAsync_setDisconnected(self.inner.handle, ptr::null_mut(), None);
        }
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
        let inner: &InnerAsyncClient = &self.inner;
        inner.callback_context.lock().unwrap().on_message_arrived = Some(Box::new(cb));

        #[cfg(not(feature = "tokio"))]
        unsafe {
            ffi::MQTTAsync_setMessageArrivedCallback(
                self.inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_message_arrived),
            );
        }
    }

    /// Removes the callback for when a message arrives from the broker.
    pub fn remove_message_callback(&self) {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_message_arrived = None;

        #[cfg(not(feature = "tokio"))]
        unsafe {
            ffi::MQTTAsync_setMessageArrivedCallback(
                self.inner.handle,
                ptr::null_mut(),
                None,
            );
        }
    }

    /// Starts the client consuming messages for a blocking (non-async) app.
    ///
    /// Returns the receiving-end of the channel for the application to
    /// get the messages.
    pub fn start_consuming(&self) -> SyncReceiver<Option<Message>> {
        let (tx, rx) = sync_channel::unbounded();

        #[cfg(not(feature = "tokio"))]
        self.set_disconnection_callbacks();

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
            move |_cli| {
                log_channel_err(tx.try_send(Event::Connected));
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

        #[cfg(not(feature = "tokio"))]
        self.set_disconnection_callbacks();

        self.set_message_callback(move |_, msg| match tx.try_send(msg) {
            Err(err) if err.is_full() => warn!("Message stream full. Losing messages"),
            Err(_) => error!("Message stream closed"),
            _ => (),
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
            move |_cli| {
                log_stream_err(tx.try_send(Event::Connected));
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

impl Drop for InnerAsyncClient {
    fn drop(&mut self) {
        #[cfg(not(feature = "tokio"))]
        if !self.handle.is_null() {
            unsafe {
                ffi::MQTTAsync_destroy(&mut self.handle as *mut *mut c_void);
            }
        }
    }
}

fn log_channel_err<T>(res: StdResult<(), sync_channel::TrySendError<T>>) {
    match res {
        Err(err) if err.is_full() => warn!("Event stream full. Losing messages"),
        Err(_) => warn!("Event stream closed"),
        _ => (),
    }
}

fn log_stream_err<T>(res: StdResult<(), async_channel::TrySendError<T>>) {
    match res {
        Err(err) if err.is_full() => warn!("Event stream full. Losing messages"),
        Err(_) => warn!("Event stream closed"),
        _ => (),
    }
}

/// Parses an MQTT URI into (host, port).
///
/// Recognises the schemes `mqtt://`, `mqtts://`, `tcp://`, `ssl://`, `tls://`,
/// `ws://`, `wss://`.  If no explicit port is present the default for the
/// scheme is used (8883 for secure, 1883 otherwise).
#[cfg(feature = "tokio")]
fn parse_server_uri(uri: &str) -> Result<(String, u16)> {
    let (default_port, rest) = if let Some(s) = uri
        .strip_prefix("mqtts://")
        .or_else(|| uri.strip_prefix("ssl://"))
        .or_else(|| uri.strip_prefix("tls://"))
        .or_else(|| uri.strip_prefix("wss://"))
    {
        (8883u16, s)
    }
    else if let Some(s) = uri
        .strip_prefix("mqtt://")
        .or_else(|| uri.strip_prefix("tcp://"))
        .or_else(|| uri.strip_prefix("ws://"))
    {
        (1883u16, s)
    }
    else {
        (1883u16, uri)
    };

    // Strip a trailing path component (e.g. WebSocket paths)
    let authority = rest.split('/').next().unwrap_or(rest);

    if let Some(colon) = authority.rfind(':') {
        let host = authority[..colon].to_string();
        let port = authority[colon + 1..]
            .parse::<u16>()
            .map_err(|_| Error::General("invalid port in server URI"))?;
        Ok((host, port))
    }
    else {
        Ok((authority.to_string(), default_port))
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MessageBuilder;
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

    // Test publish before connect only applies to the C-library path where
    // `send_while_disconnected` can queue outbound messages.
    #[cfg(not(feature = "tokio"))]
    #[test]
    fn test_publish_before_connect() {
        let server_uri = "tcp://localhost:1883";
        let client = CreateOptionsBuilder::new()
            .server_uri(server_uri)
            .mqtt_version(MqttVersion::V5)
            .send_while_disconnected(true)
            .allow_disconnected_send_at_anytime(true)
            .create_async_client()
            .unwrap();
        let res = client.try_publish(
            MessageBuilder::new()
                .topic("test")
                .payload([])
                .qos(1)
                .finalize(),
        );
        assert!(res.is_ok(), "Failed to publish message before connecting");
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn test_parse_server_uri() {
        assert_eq!(
            parse_server_uri("mqtt://localhost:1883").unwrap(),
            ("localhost".to_string(), 1883)
        );
        assert_eq!(
            parse_server_uri("tcp://broker.example.com:1883").unwrap(),
            ("broker.example.com".to_string(), 1883)
        );
        assert_eq!(
            parse_server_uri("mqtts://broker.example.com:8883").unwrap(),
            ("broker.example.com".to_string(), 8883)
        );
        assert_eq!(
            parse_server_uri("mqtt://localhost").unwrap(),
            ("localhost".to_string(), 1883)
        );
        assert_eq!(
            parse_server_uri("mqtts://secure.broker.io").unwrap(),
            ("secure.broker.io".to_string(), 8883)
        );
        assert_eq!(
            parse_server_uri("ws://localhost:8080/mqtt").unwrap(),
            ("localhost".to_string(), 8080)
        );
    }
}
