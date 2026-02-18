// paho-mqtt/src/async_client.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

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
//! ```
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
    client_persistence::UserPersistence,
    connect_options::ConnectOptions,
    create_options::{CreateOptions, PersistenceType},
    disconnect_options::{DisconnectOptions, DisconnectOptionsBuilder},
    errors::{self, Error, Result},
    ffi,
    message::Message,
    properties::Properties,
    reason_code::ReasonCode,
    response_options::{ResponseOptions, ResponseOptionsBuilder},
    server_response::ServerRequest,
    string_collection::StringCollection,
    subscribe_options::SubscribeOptions,
    token::{ConnectToken, DeliveryToken, SubscribeManyToken, SubscribeToken, Token},
    types::*,
    AsyncReceiver, Event, SyncReceiver, UserData,
};
use crossbeam_channel as sync_channel;
use std::{
    ffi::{CStr, CString},
    mem,
    os::raw::{c_char, c_int, c_void},
    ptr,
    result::Result as StdResult,
    slice, str,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, Once,
    },
    time::Duration,
};

/////////////////////////////////////////////////////////////////////////////
// AsynClient

/// An asynchronous MQTT connection client.
#[derive(Clone)]
pub struct AsyncClient {
    pub(crate) inner: Arc<InnerAsyncClient>,
}

/// Implementation details for the asynchronous MQTT connection client.
pub(crate) struct InnerAsyncClient {
    // The handle to the Paho C client
    handle: ffi::MQTTAsync,
    // The MQTT version of the connection
    mqtt_version: AtomicU32,
    // The options for connecting to the broker
    opts: Mutex<ConnectOptions>,
    // The context to give to the C callbacks
    callback_context: Mutex<CallbackContext>,
    // The server URI
    server_uri: CString,
    // The MQTT client ID name
    client_id: CString,
    // The user persistence (if any)
    user_persistence: Option<Box<UserPersistence>>,
    // Arbitrary, user-supplied data
    user_data: Option<UserData>,
}

// The client is safe to send or share between threads.
unsafe impl Send for InnerAsyncClient {}
unsafe impl Sync for InnerAsyncClient {}

/// User callback type for when the client is connected.
pub type ConnectedCallback = dyn FnMut(&AsyncClient) + Send + 'static;

/// User callback type for when the connection is lost from the broker.
pub type ConnectionLostCallback = dyn FnMut(&AsyncClient) + Send + 'static;

/// User callback type for when the client receives a disconnect packet.
pub type DisconnectedCallback = dyn FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static;

/// User callback signature for when subscribed messages are received.
pub type MessageArrivedCallback = dyn FnMut(&AsyncClient, Option<Message>) + Send + 'static;

// The context provided for the client callbacks.
//
// Originally these needed to be kept together and managed with a single
// context in the C lib. Now, we just keep them together to easily manage
// for thread-protection with a Mutex.
// These are now independent, so don't need to be kept inside a single mutex.
// Even better, it would be nice to be able to run the callbacks lock-free.
#[derive(Default)]
struct CallbackContext {
    /// Callback for when the client successfully connects.
    on_connected: Option<Box<ConnectedCallback>>,
    /// Callback for when the client loses connection to the server.
    on_connection_lost: Option<Box<ConnectionLostCallback>>,
    /// Callback for when the client receives a disconnect packet.
    on_disconnected: Option<Box<DisconnectedCallback>>,
    /// Callback for when a message arrives from the server.
    on_message_arrived: Option<Box<MessageArrivedCallback>>,
}

// Runs code to initialize the underlying C library
static C_LIB_INIT: Once = Once::new();

impl AsyncClient {
    /// Creates a new MQTT client which can connect to an MQTT broker.
    ///
    /// # Arguments
    ///
    /// `opts` The create options for the client.
    ///
    pub fn new<T>(opts: T) -> Result<AsyncClient>
    where
        T: Into<CreateOptions>,
    {
        // Do any initialization of the C lib
        C_LIB_INIT.call_once(|| {
            if let Some(lvl) = crate::c_trace_level() {
                debug!("Setting Paho C log level to {}", lvl);
                unsafe {
                    ffi::MQTTAsync_setTraceCallback(Some(crate::on_c_trace));
                    ffi::MQTTAsync_setTraceLevel(lvl);
                }
            }
        });

        // Create the client
        let mut opts = opts.into();
        debug!("Create options: {:?}", opts);

        let mut cli = InnerAsyncClient {
            handle: ptr::null_mut(),
            mqtt_version: AtomicU32::new(MQTT_VERSION_DEFAULT),
            opts: Mutex::new(ConnectOptions::new()),
            callback_context: Mutex::new(CallbackContext::default()),
            server_uri: CString::new(opts.server_uri)?,
            client_id: CString::new(opts.client_id)?,
            user_persistence: None,
            user_data: opts.user_data,
        };

        // We might need this for file persistence path
        let file_path;

        let (ptype, pptr) = match opts.persistence {
            PersistenceType::None => (ffi::MQTTCLIENT_PERSISTENCE_NONE, ptr::null_mut()),
            PersistenceType::File => (ffi::MQTTCLIENT_PERSISTENCE_DEFAULT, ptr::null_mut()),
            PersistenceType::FilePath(path) => {
                let s = path.to_str().ok_or(errors::Error::PersistenceError)?;
                file_path = CString::new(s).unwrap_or_default();
                let pptr = file_path.as_ptr() as *mut c_void;
                (ffi::MQTTCLIENT_PERSISTENCE_DEFAULT, pptr)
            }
            PersistenceType::User(cli_persist) => {
                let mut user_persistence = Box::new(UserPersistence::new(cli_persist));
                let pptr = &mut user_persistence.copts as *mut _ as *mut c_void;
                cli.user_persistence = Some(user_persistence);
                (ffi::MQTTCLIENT_PERSISTENCE_USER, pptr)
            }
        };

        debug!("Creating client with persistence: {}", ptype);

        let rc = unsafe {
            ffi::MQTTAsync_createWithOptions(
                &mut cli.handle as *mut *mut c_void,
                cli.server_uri.as_ptr(),
                cli.client_id.as_ptr(),
                ptype as c_int,
                pptr,
                &mut opts.copts,
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
    /// This is how the client is normally reconstructed from a context
    /// pointer coming back from the C lib.
    pub(crate) unsafe fn from_raw(ptr: *mut c_void) -> AsyncClient {
        AsyncClient {
            inner: Arc::from_raw(ptr as *mut InnerAsyncClient),
        }
    }

    /// Consumes the client, returning the inner wrapped value.
    /// This is how a client can be passed to the C lib as a context pointer.
    pub(crate) fn into_raw(self) -> *mut c_void {
        Arc::into_raw(self.inner) as *mut c_void
    }

    /// Gets the client "C" handle, normally for diagnostics
    pub(crate) fn handle(&self) -> ffi::MQTTAsync {
        self.inner.handle
    }

    // Low-level callback from the C library when the client is connected.
    // We just pass the call on to the handler registered with the client, if any.
    unsafe extern "C" fn on_connected(context: *mut c_void, _cause: *mut c_char) {
        debug!("Connected! Client {:?}", context);

        if !context.is_null() {
            let cli = AsyncClient::from_raw(context);

            if let Some(ref mut cb) = cli.inner.callback_context.lock().unwrap().on_connected {
                trace!("Invoking connected callback");
                cb(&cli);
            }

            let _ = cli.into_raw();
        }
    }

    // Low-level callback from the C library when the connection is lost.
    // We pass the call on to the handler registered with the client, if any.
    unsafe extern "C" fn on_connection_lost(context: *mut c_void, _cause: *mut c_char) {
        warn!("Connection lost. Client: {:?}", context);

        if !context.is_null() {
            let cli = AsyncClient::from_raw(context);
            {
                let mut cbctx = cli.inner.callback_context.lock().unwrap();

                // Push a None into the message stream to cleanly
                // shutdown any consumers.
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
    }

    // Low-level callback from the C library for when a disconnect packet arrives.
    unsafe extern "C" fn on_disconnected(
        context: *mut c_void,
        cprops: *mut ffi::MQTTProperties,
        reason: ffi::MQTTReasonCodes,
    ) {
        debug!(
            "Disconnected with reason code: {}. Client: {:?}",
            reason, context
        );

        if !context.is_null() {
            let cli = AsyncClient::from_raw(context);
            {
                let mut cbctx = cli.inner.callback_context.lock().unwrap();

                // Push a None into the message stream to cleanly
                // shutdown any consumers.
                if let Some(ref mut cb) = cbctx.on_message_arrived {
                    trace!("Invoking message callback with None");
                    cb(&cli, None);
                }

                if let Some(ref mut cb) = cbctx.on_disconnected {
                    let reason_code = ReasonCode::from(reason);
                    let props = Properties::from_c_struct(&*cprops);
                    trace!("Invoking disconnected callback");
                    cb(&cli, props, reason_code);
                }
            }
            let _ = cli.into_raw();
        }
    }

    // Low-level callback from the C library when a message arrives from the broker.
    // We pass the call on to the handler registered with the client, if any.
    unsafe extern "C" fn on_message_arrived(
        context: *mut c_void,
        topic_name: *mut c_char,
        topic_len: c_int,
        mut cmsg: *mut ffi::MQTTAsync_message,
    ) -> c_int {
        debug!(
            "Message arrived. Client: {:?}, topic: {:?} len {:?} cmsg: {:?}: {:?}",
            context, topic_name, topic_len, cmsg, *cmsg
        );

        if !context.is_null() {
            let cli = AsyncClient::from_raw(context);

            if let Some(ref mut cb) = cli
                .inner
                .callback_context
                .lock()
                .unwrap()
                .on_message_arrived
            {
                let len = topic_len as usize;
                let topic = if len == 0 {
                    // Zero-len topic means it's a NUL-terminated C string
                    CStr::from_ptr(topic_name).to_owned()
                }
                else {
                    // If we get a len for the topic, then there's no NUL terminator.
                    // TODO: Handle UTF-8 error(s)
                    let tp =
                        str::from_utf8(slice::from_raw_parts(topic_name as *mut u8, len)).unwrap();
                    CString::new(tp).unwrap()
                };
                let msg = Message::from_c_parts(topic, &*cmsg);

                trace!("Invoking message callback");
                cb(&cli, Some(msg));
            }

            let _ = cli.into_raw();
        }

        ffi::MQTTAsync_freeMessage(&mut cmsg);
        ffi::MQTTAsync_free(topic_name as *mut c_void);
        1
    }

    // Set the disconnection callbacks, usually to prepare for creating
    // an input channel/stream of messages.
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

    /// Gets the most recent MQTT version for the client.
    ///
    /// This is the version of the current connection, or the most recent
    /// connection if currently disconnected. Before an initial connection
    /// is made, this will report MQTT_VERSION_DEFAULT (0).
    pub fn mqtt_version(&self) -> MqttVersion {
        MqttVersion::from(self.mqtt_version_raw())
    }

    /// The raw, integer value of the MQTT version
    pub fn mqtt_version_raw(&self) -> u32 {
        self.inner.mqtt_version.load(Ordering::SeqCst)
    }

    /// Sets the current MQTT version.
    /// This is set when a connection is requested or established.
    pub(crate) fn set_mqtt_version<V>(&self, ver: V)
    where
        V: Into<MqttVersion>,
    {
        let ver = ver.into() as u32;
        trace!("Updating client MQTT version: {}", ver);
        self.inner.mqtt_version.store(ver, Ordering::SeqCst);
    }

    /// Get access to the user-defined data in the client.
    ///
    /// This returns a reference to a read/write lock around the user data so
    /// that the application can access the data, as needed from any outside
    /// thread or a callback.
    ///
    /// Note that it's up to the application to ensure that it doesn't
    /// deadlock the callback thread when accessing the user data.
    pub fn user_data(&self) -> Option<&UserData> {
        self.inner.user_data.as_ref()
    }

    /// Gets a copy of the options used in the last connection attempt.
    pub fn connect_options(&self) -> ConnectOptions {
        self.inner.opts.lock().unwrap().clone()
    }

    /// Connects to an MQTT broker using the specified connect options.
    ///
    /// # Arguments
    ///
    /// * `opts` The connect options. This can be `None`, in which case the
    ///          default options are used.
    ///
    pub fn connect<T>(&self, opts: T) -> ConnectToken
    where
        T: Into<Option<ConnectOptions>>,
    {
        debug!("Connecting. Handle: {:?}", self.inner.handle);

        let mut opts = opts.into().unwrap_or_default();

        if crate::is_secure_uri(&self.server_uri()) && !opts.has_ssl_options() {
            return ConnectToken::from(Error::MissingSslOptions);
        }

        if let Err(err) = opts.check() {
            return ConnectToken::from(err);
        }

        self.set_mqtt_version(opts.mqtt_version());

        let tok = Token::from_request(self, ServerRequest::Connect);
        opts.set_token(tok.clone());

        debug!("Connect options: {:?}", opts);
        let mut lkopts = self.inner.opts.lock().unwrap();
        *lkopts = opts;

        let rc = unsafe { ffi::MQTTAsync_connect(self.inner.handle, &lkopts.copts) };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(lkopts.copts.context) });
            return ConnectToken::from_error(rc);
        }

        tok
    }

    /// Connects to an MQTT broker using the specified connect options.
    ///
    /// # Arguments
    ///
    /// * `opts` The connect options
    /// * `success_cb` The callback for a successful connection.
    /// * `failure_cb` The callback for a failed connection attempt.
    pub fn connect_with_callbacks<FS, FF>(
        &self,
        mut opts: ConnectOptions,
        success_cb: FS,
        failure_cb: FF,
    ) -> ConnectToken
    where
        FS: Fn(&AsyncClient, u16) + Send + 'static,
        FF: Fn(&AsyncClient, u16, i32) + Send + 'static,
    {
        debug!(
            "Connecting with callbacks. Handle: {:?}, opts: {:?}",
            self.inner.handle, opts
        );
        unsafe {
            if !opts.copts.will.is_null() {
                debug!("Will: {:?}", *(opts.copts.will));
            }
        }

        if crate::is_secure_uri(&self.server_uri()) && !opts.has_ssl_options() {
            return ConnectToken::from(Error::MissingSslOptions);
        }

        if let Err(err) = opts.check() {
            return ConnectToken::from(err);
        }

        self.set_mqtt_version(opts.mqtt_version());

        let tok = Token::from_client(self, ServerRequest::Connect, success_cb, failure_cb);
        opts.set_token(tok.clone());

        let mut lkopts = self.inner.opts.lock().unwrap();
        *lkopts = opts;

        let rc = unsafe { ffi::MQTTAsync_connect(self.inner.handle, &lkopts.copts) };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(lkopts.copts.context) });
            return ConnectToken::from_error(rc);
        }

        tok
    }

    /// Attempts to reconnect to the broker.
    /// This can only be called after a connection was initially made or
    /// attempted. It will retry with the same connect options.
    ///
    pub fn reconnect(&self) -> ConnectToken {
        let connopts = self.inner.opts.lock().unwrap().clone();
        self.connect(connopts)
    }

    /// Attempts to reconnect to the broker, using callbacks to signal
    /// completion.
    /// This can only be called after a connection was initially made or
    /// attempted. It will retry with the same connect options.
    ///
    /// # Arguments
    ///
    /// * `success_cb` The callback for a successful connection.
    /// * `failure_cb` The callback for a failed connection attempt.
    ///
    pub fn reconnect_with_callbacks<FS, FF>(&self, success_cb: FS, failure_cb: FF) -> ConnectToken
    where
        FS: Fn(&AsyncClient, u16) + Send + 'static,
        FF: Fn(&AsyncClient, u16, i32) + Send + 'static,
    {
        let connopts = self.inner.opts.lock().unwrap().clone();
        self.connect_with_callbacks(connopts, success_cb, failure_cb)
    }

    /// Disconnects from the MQTT broker.
    ///
    /// # Arguments
    ///
    /// `opt_opts` Optional disconnect options. Specifying `None` will use
    ///            default of immediate (zero timeout) disconnect.
    ///
    pub fn disconnect<T>(&self, opt_opts: T) -> Token
    where
        T: Into<Option<DisconnectOptions>>,
    {
        let mut opts = opt_opts.into().unwrap_or_default();
        debug!("Disconnecting.  Handle: {:?}", self.inner.handle);
        trace!("Disconnect options: {:?}", opts);

        let tok = Token::new();
        opts.set_token(tok.clone());

        let rc = unsafe { ffi::MQTTAsync_disconnect(self.inner.handle, &opts.copts) };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(opts.copts.context) });
            return Token::from_error(rc);
        }

        // Push a None into the message stream to cleanly
        // shutdown any consumers.
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

    /// Disconnect from the MQTT broker with a timeout.
    /// This will delay the disconnect for up to the specified timeout to
    /// allow in-flight messages to complete.
    /// This is the same as calling disconnect with options specifying a
    /// timeout.
    ///
    /// # Arguments
    ///
    /// `timeout` The amount of time to wait for the disconnect. This has
    ///           a resolution in milliseconds.
    ///
    pub fn disconnect_after(&self, timeout: Duration) -> Token {
        let disconn_opts = DisconnectOptionsBuilder::new().timeout(timeout).finalize();
        self.disconnect(disconn_opts)
    }

    /// Determines if this client is currently connected to an MQTT broker.
    pub fn is_connected(&self) -> bool {
        unsafe { ffi::MQTTAsync_isConnected(self.inner.handle) != 0 }
    }

    /// Sets the callback for when the connection is established with the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///     function or a closure.
    pub fn set_connected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient) + Send + 'static,
    {
        // A pointer to the inner client will serve as the callback context
        let inner: &InnerAsyncClient = &self.inner;

        // This should be protected by a mutex if we'll have a thread-safe client
        inner.callback_context.lock().unwrap().on_connected = Some(Box::new(cb));

        unsafe {
            ffi::MQTTAsync_setConnected(
                inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_connected),
            );
        }
    }

    /// Removes the callback for when the conection is established
    pub fn remove_connected_callback(&self) {
        self.inner.callback_context.lock().unwrap().on_connected = None;

        unsafe {
            ffi::MQTTAsync_setConnected(self.inner.handle, ptr::null_mut(), None);
        }
    }

    /// Sets the callback for when the connection is lost with the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///     function or a closure.
    pub fn set_connection_lost_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient) + Send + 'static,
    {
        // A pointer to the inner client will serve as the callback context
        let inner: &InnerAsyncClient = &self.inner;

        inner.callback_context.lock().unwrap().on_connection_lost = Some(Box::new(cb));

        unsafe {
            ffi::MQTTAsync_setConnectionLostCallback(
                inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_connection_lost),
            );
        }
    }

    /// Removes the callback for when the connection is lost
    pub fn remove_connection_lost_callback(&self) {
        self.inner
            .callback_context
            .lock()
            .unwrap()
            .on_connection_lost = None;

        // TODO: We should only remove the C handler if we know that
        // we're not consuming or streaming. For now, keeping it is a
        // very minor performance hit.
        /*
        unsafe {
            ffi::MQTTAsync_setConnectionLostCallback(
                self.inner.handle,
                ptr::null_mut(),
                None
            );
        }
        */
    }

    /// Sets the callback for when a disconnect message arrives from the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///     function or a closure.
    pub fn set_disconnected_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Properties, ReasonCode) + Send + 'static,
    {
        // A pointer to the inner client will serve as the callback context
        let inner: &InnerAsyncClient = &self.inner;

        // This should be protected by a mutex if we'll have a thread-safe client
        inner.callback_context.lock().unwrap().on_disconnected = Some(Box::new(cb));

        unsafe {
            ffi::MQTTAsync_setDisconnected(
                inner.handle,
                inner as *const _ as *mut c_void,
                Some(AsyncClient::on_disconnected),
            );
        }
    }

    /// Removes the callback for when a disconnect message is received from the broker.
    pub fn remove_disconnected_callback(&self) {
        self.inner.callback_context.lock().unwrap().on_disconnected = None;

        unsafe {
            ffi::MQTTAsync_setDisconnected(self.inner.handle, ptr::null_mut(), None);
        }
    }

    /// Sets the callback for when a message arrives from the broker.
    ///
    /// # Arguments
    ///
    /// * `cb` The callback to register with the library. This can be a
    ///     function or a closure.
    ///
    pub fn set_message_callback<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Option<Message>) + Send + 'static,
    {
        // A pointer to the inner client will serve as the callback context
        let inner: &InnerAsyncClient = &self.inner;

        // This should be protected by a mutex if we'll have a thread-safe client
        inner.callback_context.lock().unwrap().on_message_arrived = Some(Box::new(cb));

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

        unsafe {
            ffi::MQTTAsync_setMessageArrivedCallback(self.inner.handle, ptr::null_mut(), None);
        }
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
        let tok = DeliveryToken::new(msg);
        let mut rsp_opts = ResponseOptions::new(ver, tok.clone());

        let rc = unsafe {
            let msg = tok.message();
            ffi::MQTTAsync_sendMessage(
                self.inner.handle,
                msg.topic().as_ptr() as *const c_char,
                &msg.cmsg,
                &mut rsp_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            let msg: Message = tok.into();
            return Err(Error::Publish(rc, msg));
        }

        tok.set_msgid(rsp_opts.copts.token as i16);
        Ok(tok)
    }

    /// Publishes a message to the MQTT broker.
    ///
    /// Returns a Delivery Token to track the progress of the operation.
    ///
    pub fn publish(&self, msg: Message) -> DeliveryToken {
        match self.try_publish(msg) {
            Ok(tok) => tok,
            Err(Error::Publish(rc, msg)) => DeliveryToken::from_error(msg, rc),
            _ => panic!("Unknown publish error"),
        }
    }

    /// Subscribes to a single topic.
    ///
    /// # Arguments
    ///
    /// `topic` The topic name
    /// `qos` The quality of service requested for messages
    ///
    pub fn subscribe<S, Q>(&self, topic: S, qos: Q) -> SubscribeToken
    where
        S: Into<String>,
        Q: Into<QoS>,
    {
        let ver = self.mqtt_version();
        let tok = Token::from_request(None, ServerRequest::Subscribe);
        let mut rsp_opts = ResponseOptions::new(ver, tok.clone());
        let topic = CString::new(topic.into()).unwrap();
        let qos = qos.into() as i32;

        debug!("Subscribe to '{:?}' @ QOS {}", topic, qos);

        let rc = unsafe {
            ffi::MQTTAsync_subscribe(self.inner.handle, topic.as_ptr(), qos, &mut rsp_opts.copts)
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            return SubscribeToken::from_error(rc);
        }

        tok
    }

    /// Subscribes to a single topic with v5 options
    ///
    /// # Arguments
    ///
    /// `topic` The topic name
    /// `qos` The quality of service requested for messages
    /// `opts` Options for the subscription
    /// `props` MQTT v5 properties
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

        let tok = Token::from_request(None, ServerRequest::Subscribe);
        let mut rsp_opts = ResponseOptionsBuilder::new()
            .token(tok.clone())
            .subscribe_options(opts.into())
            .properties(props.into().unwrap_or_default())
            .finalize();

        let topic = CString::new(topic.into()).unwrap();
        let qos = qos.into() as i32;

        debug!("Subscribe to '{:?}' @ QOS {}", topic, qos);

        let rc = unsafe {
            ffi::MQTTAsync_subscribe(self.inner.handle, topic.as_ptr(), qos, &mut rsp_opts.copts)
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            return SubscribeToken::from_error(rc);
        }

        tok
    }

    /// Subscribes to multiple topics simultaneously.
    ///
    /// # Arguments
    ///
    /// `topics` The collection of topic names
    /// `qos` The quality of service requested for messages
    ///
    pub fn subscribe_many<T, Q>(&self, topics: &[T], qos: &[Q]) -> SubscribeManyToken
    where
        T: AsRef<str>,
        Q: Into<QoS> + Copy,
    {
        let n = topics.len();

        // The length of the slices must match
        if n != qos.len() {
            return SubscribeManyToken::from_error(-1);
        }

        let ver = self.mqtt_version();
        let tok = Token::from_request(None, ServerRequest::SubscribeMany(n));
        let mut rsp_opts = ResponseOptions::new(ver, tok.clone());
        let topics = StringCollection::new(topics);
        let qos: Vec<i32> = qos.iter().map(|q| (*q).into() as i32).collect();

        debug!("Subscribe to '{:?}' @ QOS {:?}", topics, qos);

        let rc = unsafe {
            ffi::MQTTAsync_subscribeMany(
                self.inner.handle,
                n as c_int,
                topics.as_c_arr_mut_ptr(),
                qos.as_ptr(),
                &mut rsp_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            return SubscribeManyToken::from_error(rc);
        }

        tok
    }

    /// Subscribes to multiple topics simultaneously using the same QoS
    /// for all of them.
    ///
    /// # Arguments
    ///
    /// `topics` The collection of topic names
    /// `qos` The quality of service requested for all messages
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
    /// `topics` The collection of topic names
    /// `qos` The quality of service requested for messages
    /// `opts` Subscribe options (one per topic)
    /// `props` MQTT v5 properties
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
        // TOOD: Make sure topics & qos are same length (or use min)
        let tok = Token::from_request(None, ServerRequest::SubscribeMany(n));
        let mut rsp_opts = ResponseOptionsBuilder::new()
            .token(tok.clone())
            .subscribe_many_options(opts)
            .properties(props.into().unwrap_or_default())
            .finalize();

        let topics = StringCollection::new(topics);
        let qos: Vec<i32> = qos.iter().map(|q| (*q).into() as i32).collect();

        debug!(
            "Subscribe to '{:?}' @ QOS {:?} w/ opts: {:?}",
            topics, qos, opts
        );
        trace!("Subscribe call/response opts: {:?}", rsp_opts);

        let rc = unsafe {
            ffi::MQTTAsync_subscribeMany(
                self.inner.handle,
                n as c_int,
                topics.as_c_arr_mut_ptr(),
                qos.as_ptr(),
                &mut rsp_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            return SubscribeManyToken::from_error(rc);
        }

        tok
    }

    /// Unsubscribes from a single topic.
    ///
    /// # Arguments
    ///
    /// `topic` The topic to unsubscribe. It must match a topic from a
    ///         previous subscribe.
    ///
    pub fn unsubscribe<S>(&self, topic: S) -> Token
    where
        S: Into<String>,
    {
        let ver = self.mqtt_version();
        let tok = Token::from_request(None, ServerRequest::Unsubscribe);
        let mut rsp_opts = ResponseOptions::new(ver, tok.clone());
        let topic = CString::new(topic.into()).unwrap();

        debug!("Unsubscribe from '{:?}'", topic);

        let rc = unsafe {
            ffi::MQTTAsync_unsubscribe(self.inner.handle, topic.as_ptr(), &mut rsp_opts.copts)
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            return Token::from_error(rc);
        }

        tok
    }

    /// Unsubscribes from a single topic.
    ///
    /// # Arguments
    ///
    /// `topic` The topic to unsubscribe. It must match a topic from a
    ///         previous subscribe.
    /// `props` MQTT v5 properties for the unsubscribe.
    ///
    pub fn unsubscribe_with_options<S>(&self, topic: S, props: Properties) -> Token
    where
        S: Into<String>,
    {
        debug_assert!(self.mqtt_version() >= MqttVersion::V5);

        let tok = Token::from_request(None, ServerRequest::Unsubscribe);
        let mut rsp_opts = ResponseOptionsBuilder::new()
            .token(tok.clone())
            .properties(props)
            .finalize();

        let topic = CString::new(topic.into()).unwrap();

        debug!("Unsubscribe from '{:?}'", topic);

        let rc = unsafe {
            ffi::MQTTAsync_unsubscribe(self.inner.handle, topic.as_ptr(), &mut rsp_opts.copts)
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            return Token::from_error(rc);
        }

        tok
    }

    /// Unsubscribes from multiple topics simultaneously.
    ///
    /// # Arguments
    ///
    /// `topic` The topics to unsubscribe. Each must match a topic from a
    ///         previous subscribe.
    ///
    pub fn unsubscribe_many<T>(&self, topics: &[T]) -> Token
    where
        T: AsRef<str>,
    {
        let ver = self.mqtt_version();

        let n = topics.len();
        let tok = Token::from_request(None, ServerRequest::UnsubscribeMany(n));
        let mut rsp_opts = ResponseOptions::new(ver, tok.clone());
        let topics = StringCollection::new(topics);

        debug!("Unsubscribe from '{:?}'", topics);

        let rc = unsafe {
            ffi::MQTTAsync_unsubscribeMany(
                self.inner.handle,
                n as c_int,
                topics.as_c_arr_mut_ptr(),
                &mut rsp_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            return Token::from_error(rc);
        }

        tok
    }

    /// Unsubscribes from multiple topics simultaneously.
    ///
    /// # Arguments
    ///
    /// `topic` The topics to unsubscribe. Each must match a topic from a
    ///         previous subscribe.
    /// `props` MQTT v5 properties for the unsubscribe.
    ///
    pub fn unsubscribe_many_with_options<T>(&self, topics: &[T], props: Properties) -> Token
    where
        T: AsRef<str>,
    {
        debug_assert!(self.mqtt_version() >= MqttVersion::V5);

        let n = topics.len();
        let tok = Token::from_request(None, ServerRequest::UnsubscribeMany(n));
        let mut rsp_opts = ResponseOptionsBuilder::new()
            .token(tok.clone())
            .properties(props)
            .finalize();

        let topics = StringCollection::new(topics);

        debug!("Unsubscribe from '{:?}'", topics);

        let rc = unsafe {
            ffi::MQTTAsync_unsubscribeMany(
                self.inner.handle,
                n as c_int,
                topics.as_c_arr_mut_ptr(),
                &mut rsp_opts.copts,
            )
        };

        if rc != 0 {
            mem::drop(unsafe { Token::from_raw(rsp_opts.copts.context) });
            return Token::from_error(rc);
        }

        tok
    }

    /// Starts the client consuming messages for a blocking (non-async) app.
    ///
    /// This starts the client receiving messages and placing them into a
    /// channel. It returns the receiving-end of the channel for the
    /// application to get the messages.
    ///
    /// This should normall be called before the client is connected,
    /// especially if the application is requesting a persistent session.
    /// With a clean session, this should be called before subscribing to
    /// ensure that messages are not lost.
    pub fn start_consuming(&self) -> SyncReceiver<Option<Message>> {
        let (tx, rx) = sync_channel::unbounded();

        // Make sure at least the low-level connection_lost handler is in
        // place to notify us when the connection is lost (sends a 'None' to
        // the receiver).
        self.set_disconnection_callbacks();

        // Message callback just queues incoming messages.
        self.set_message_callback(move |_, optmsg| {
            log_channel_err(tx.try_send(optmsg));
        });

        rx
    }

    /// Stops the client from consuming messages.
    pub fn stop_consuming(&self) {
        self.remove_message_callback();
    }

    /// Creates a futures stream for consuming events.
    ///
    /// This will install an internal callback to receive the incoming
    /// events from the client, and return the receive side of the channel.
    /// The stream will stay open for the life of the client.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if 'buffer_sz' is 'Some' or will rely on an unbounded channel
    /// if 'buffer_sz' is 'None'.
    ///
    /// It's a best practice to open the stream _before_ connecting to the
    /// server. When using persistent (non-clean) sessions, messages could
    /// arriving as soon as the connection is made - even before the
    /// connect() call returns.
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

    /// Creates a futures stream for consuming messages.
    ///
    /// This will install an internal callback to receive the incoming
    /// messages from the client, and return the receive side of the channel.
    /// The stream will stay open for the life of the client. If the client
    /// gets disconnected, it will insert `None` into the channel to signal
    /// the app about the disconnect.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if 'buffer_sz' is 'Some' or will rely on an unbounded channel
    /// if 'buffer_sz' is 'None'.
    ///
    /// It's a best practice to open the stream _before_ connecting to the
    /// server. When using persistent (non-clean) sessions, messages could
    /// arriving as soon as the connection is made - even before the
    /// connect() call returns.
    pub fn get_stream<L>(&mut self, buffer_lim: L) -> AsyncReceiver<Option<Message>>
    where
        L: Into<Option<usize>>,
    {
        let (tx, rx) = match buffer_lim.into() {
            Some(lim) => async_channel::bounded(lim),
            None => async_channel::unbounded(),
        };

        // Make sure at least the low-level connection-lost handlers are in
        // place to notify us when the connection is lost (sends a 'None' to
        // the receiver).
        self.set_disconnection_callbacks();

        self.set_message_callback(move |_, msg| match tx.try_send(msg) {
            Err(err) if err.is_full() => warn!("Message stream full. Losing messages"),
            Err(_) => error!("Message stream closed"),
            _ => (),
        });

        rx
    }

    /// Creates a futures stream for consuming events.
    ///
    /// This will install an internal callback to receive the incoming
    /// events from the client, and return the receive side of the channel.
    /// The stream will stay open for the life of the client.
    ///
    /// The stream will rely on a bounded channel with the given buffer
    /// capacity if 'buffer_sz' is 'Some' or will rely on an unbounded channel
    /// if 'buffer_sz' is 'None'.
    ///
    /// It's a best practice to open the stream _before_ connecting to the
    /// server. When using persistent (non-clean) sessions, messages could
    /// arriving as soon as the connection is made - even before the
    /// connect() call returns.
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

    /// Returns client ID used for client instance
    ///
    /// Client ID is returned as a rust String as set in a
    /// CreateOptionsBuilder for symmetry
    pub fn client_id(&self) -> String {
        self.inner.client_id.clone().into_string().unwrap()
    }

    /// Returns server URI used for connection
    ///
    /// Server URI is returned as a rust String as set in a
    /// CreateOptionsBuilder for symmetry
    pub fn server_uri(&self) -> String {
        self.inner.server_uri.clone().into_string().unwrap()
    }
}

// The client is safe to send or share between threads.
unsafe impl Send for AsyncClient {}
unsafe impl Sync for AsyncClient {}

impl Drop for InnerAsyncClient {
    /// Drops the client by closing dpen all the underlying, dependent objects
    fn drop(&mut self) {
        // Destroy the underlying C client.
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

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_options::CreateOptionsBuilder;
    use std::sync::{Arc, Mutex, RwLock};
    use std::thread;

    // Makes sure than when a client is moved, the inner struct stayes at
    // the same address (on the heap) since that inner struct is used as
    // the context pointer for callbacks
    // GitHub Issue #17
    #[test]
    fn test_context() {
        let cli = AsyncClient::new("tcp://localhost:1883").unwrap();
        cli.set_message_callback(|_, _| {});

        // Get a context pointer to the inner struct
        let pctx = {
            let ctx: &InnerAsyncClient = &cli.inner;
            ctx as *const _ as *mut c_void
        };

        // Move the client, then get a context pointer to inner
        let new_cli = cli;
        let new_pctx = {
            let ctx: &InnerAsyncClient = &new_cli.inner;
            ctx as *const _ as *mut c_void
        };

        // They should match (inner didn't move)
        assert_eq!(pctx, new_pctx);
    }

    #[test]
    fn test_create() {
        let cli = AsyncClient::new("tcp://localhost:1883");
        assert!(cli.is_ok(), "Error in creating simple async client, do you have a running MQTT server on localhost:1883?");
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
            .create_client()
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
            .create_client()
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
            .create_client()
            .unwrap();

        let data = cli.user_data();
        assert!(data.is_some());
        let data = data.unwrap();

        let lock = data.downcast_ref::<RwLock<Vec<&str>>>().unwrap();
        // Try reading
        {
            let v = lock.read().unwrap();
            assert_eq!(3, v.len());
            assert_eq!("zero", v[0]);
            assert_eq!("one", v[1]);
            assert_eq!("two", v[2]);
        }

        // Now try writing
        {
            let mut v = lock.write().unwrap();
            v.push("three");
            assert_eq!(4, v.len());
            assert_eq!("three", v[3]);
        }
    }

    // Determine that a client can be sent across threads.
    // As long as it compiles, this indicates that AsyncClient implements
    // the Send trait.
    #[test]
    fn test_send() {
        let cli = AsyncClient::new("tcp://localhost:1883").unwrap();
        let thr = thread::spawn(move || {
            assert!(!cli.is_connected());
        });
        let _ = thr.join().unwrap();
    }

    // Determine that a client can be shared across threads using an Arc.
    // As long as it compiles, this indicates that AsyncClient implements the
    // Send trait.
    // This is a bit redundant with the previous test, but explicitly
    // addresses GitHub Issue #31.
    #[test]
    fn test_send_arc() {
        let cli = AsyncClient::new("tcp://localhost:1883").unwrap();

        let cli = Arc::new(cli);
        let cli2 = cli.clone();

        let thr = thread::spawn(move || {
            assert!(!cli.is_connected());
        });
        assert!(!cli2.is_connected());
        let _ = thr.join().unwrap();
    }

    #[test]
    fn test_get_client_id() {
        let c_id = "test_client_id_can_be_retrieved";
        let options = CreateOptionsBuilder::new().client_id(c_id).finalize();
        let client = AsyncClient::new(options);
        assert!(
            client.is_ok(),
            "Error in creating async client with client_id"
        );
        let retrieved = client.unwrap().client_id();
        assert_eq!(retrieved, c_id.to_string());
    }

    #[test]
    fn test_get_server_uri() {
        let server_uri = "tcp://localhost:1883";
        let client = CreateOptionsBuilder::new()
            .server_uri(server_uri)
            .create_client();
        assert!(
            client.is_ok(),
            "Error in creating async client with server_uri"
        );
        let retrieved = client.unwrap().server_uri();
        assert_eq!(retrieved, server_uri.to_string());
    }
}
