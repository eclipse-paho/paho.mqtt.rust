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

use std::{
    cmp, fmt,
    os::raw::c_int,
    path::{Path, PathBuf},
};

use crate::{
    ffi, AsyncClient, Client, ClientPersistence, MqttVersion, Result, UserData, UserPersistence,
};

/*
    Remember the C constants (c_uint)
        MQTTCLIENT_PERSISTENCE_DEFAULT = 0
        MQTTCLIENT_PERSISTENCE_NONE    = 1
        MQTTCLIENT_PERSISTENCE_USER    = 2
*/

/// The type of persistence for the client.
///
/// There is a built-in mechanism for using the file system to persist
/// messages and data. The `File` selection will use that in the current
/// working directory (CWD). A path can also be specified, with `FilePath`
/// and then the persistence store will be created under that directory.
/// Each client will create a directory under that using the MQTT client
/// ID and server URL to name the directory. Therefore, a client ID is
/// required by the library when using persistence.
///
/// The persistence type can be created from any string or path to indicate
/// file persistence to that directory. If a directory with that name can't
/// be found or created by the library, then a persistence error is returned
/// when attempting to create the MQTT client object.
#[derive(Default)]
pub enum PersistenceType {
    /// Messages are persisted to files in a local directory (default).
    File,
    /// Messages are persisted to files under the specified directory.
    FilePath(PathBuf),
    /// No persistence is used.
    #[default]
    None,
    /// A user-defined persistence provided by the application.
    User(UserPersistence),
}

impl fmt::Debug for PersistenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PersistenceType::*;
        match *self {
            File => write!(f, "File"),
            FilePath(_) => write!(f, "File with Path"),
            None => write!(f, "None"),
            User(_) => write!(f, "User"),
        }
    }
}

impl From<&str> for PersistenceType {
    /// A string slice can be used to create a file path persistence.
    fn from(path: &str) -> Self {
        PersistenceType::FilePath(PathBuf::from(path))
    }
}

impl From<String> for PersistenceType {
    /// A string can be used to create a file path persistence.
    fn from(path: String) -> Self {
        PersistenceType::FilePath(PathBuf::from(path))
    }
}

impl From<&Path> for PersistenceType {
    /// A path can be used to create a file path persistence.
    fn from(path: &Path) -> Self {
        PersistenceType::FilePath(path.into())
    }
}

impl From<PathBuf> for PersistenceType {
    /// A path buffer can be used to create a file path persistence.
    fn from(path: PathBuf) -> Self {
        PersistenceType::FilePath(path)
    }
}

impl From<Option<PersistenceType>> for PersistenceType {
    fn from(opt: Option<PersistenceType>) -> Self {
        match opt {
            Some(typ) => typ,
            None => PersistenceType::None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                            Create Options
/////////////////////////////////////////////////////////////////////////////

/// The options for creating an MQTT client.
/// This can be constructed using a
/// [CreateOptionsBuilder](struct.CreateOptionsBuilder.html).
#[derive(Debug)]
pub struct CreateOptions {
    /// The URI of the remote MQTT broker
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
    /// oldest rather than the newest.
    pub delete_oldest: bool,
    /// Restore messages from persistence on create - or clear it.
    pub restore_msgs: bool,
    /// Whether to persist QoS0 publish commands - an option to not persist them.
    /// Defaults to true.
    pub persist_qos0: bool,
    /// The type of persistence used by the client.
    pub persistence: PersistenceType,
    /// User-defined data to store in the client, if any
    pub user_data: Option<UserData>,
}

impl CreateOptions {
    pub(crate) fn c_struct(&self) -> ffi::MQTTAsync_createOptions {
        ffi::MQTTAsync_createOptions {
            sendWhileDisconnected: self.send_while_disconnected as c_int,
            maxBufferedMessages: self.max_buffered_msgs as c_int,
            MQTTVersion: MqttVersion::V5 as i32,
            allowDisconnectedSendAtAnyTime: self.allow_send_any_time as c_int,
            deleteOldestMessages: self.delete_oldest as c_int,
            restoreMessages: self.restore_msgs as c_int,
            persistQoS0: self.persist_qos0 as c_int,
            ..ffi::MQTTAsync_createOptions::default()
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
            persistence: PersistenceType::default(),
            user_data: None,
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
///     .server_uri("tcp://localhost:1883")
///     .client_id("client1")
///     .finalize();
///
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
        // Note that the C lib seems to need at least a single slot
        // to send messages, even when connected. For sanity we put
        // a small lower limit.
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

    /// If set, QoS0 publish commands are persisted. Defaults to true.
    pub fn persist_qos0(mut self, persist: bool) -> Self {
        self.opts.persist_qos0 = persist;
        self
    }

    /// Sets the user-defined data structure for the client.
    pub fn user_data(mut self, data: UserData) -> Self {
        self.opts.user_data = Some(data);
        self
    }

    /// Constructs a set of create options from the builder information.
    pub fn finalize(self) -> CreateOptions {
        self.opts
    }

    /// Finalize the builder and create an asynchronous client.
    pub fn create_async_client(self) -> Result<AsyncClient> {
        AsyncClient::new(self.finalize())
    }

    /// Finalize the builder and create an asynchronous client.
    pub fn create_client(self) -> Result<Client> {
        Client::new(self.finalize())
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
///     .server_uri("tcp://localhost:1883")
///     .client_id("client1")
///     .finalize();
///
/// let cli = mqtt::AsyncClient::new(opts).unwrap();
/// ```

#[derive(Default)]
pub struct CreateOptionsBuilder {
    copts: ffi::MQTTAsync_createOptions,
    server_uri: String,
    client_id: String,
    persistence: PersistenceType,
    user_data: Option<UserData>,
}

impl CreateOptionsBuilder {
    /// Constructs a builder for a client that can connect using MQTT v3.x
    /// or v5.
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs a builder for a client that can only connect using MQTT v3.x.
    pub fn new_v3() -> Self {
        Self {
            copts: ffi::MQTTAsync_createOptions::new_v3(),
            ..Self::default()
        }
    }

    /// Sets the the URI to the MQTT broker.
    /// Alternately, the application can specify multiple servers via the
    /// connect options.
    ///
    /// # Arguments
    ///
    /// * `server_uri` The URI string to specify the server in the form
    ///              _protocol://host:port_, where the protocol can be
    ///              _tcp_ or _ssl_, and the host can be an IP address
    ///              or domain name.
    pub fn server_uri<S>(mut self, server_uri: S) -> Self
    where
        S: Into<String>,
    {
        self.server_uri = server_uri.into();
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
    /// * `client_id` A UTF-8 string identifying the client to the server.
    ///
    pub fn client_id<S>(mut self, client_id: S) -> Self
    where
        S: Into<String>,
    {
        self.client_id = client_id.into();
        self
    }

    /// Sets the type of persistence used by the client.
    /// The default is for the library to automatically use file persistence,
    /// although this can be turned off by specify `None` for a more
    /// performant, though possibly less reliable system.
    ///
    /// # Arguments
    ///
    /// * `persist` The type of persistence to use.
    ///
    pub fn persistence<P>(mut self, persist: P) -> Self
    where
        P: Into<PersistenceType>,
    {
        self.persistence = persist.into();
        self
    }

    /// Sets a user-defined persistence store.
    /// This sets the persistence to use a custom one defined by the
    /// application. This can be anything that implements the
    /// `ClientPersistence` trait.
    ///
    /// # Arguments
    ///
    /// * `persist` An application-defined custom persistence store.
    ///
    pub fn user_persistence<T>(mut self, persistence: T) -> Self
    where
        T: ClientPersistence + Send + 'static,
    {
        let persistence: Box<Box<dyn ClientPersistence + Send>> = Box::new(Box::new(persistence));
        self.persistence = PersistenceType::User(persistence);
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
    /// * `n` The maximum number of messages that can be buffered. Setting this
    ///   to zero also disables any off-line buffering.
    ///
    pub fn max_buffered_messages(mut self, n: i32) -> Self {
        // Note that the C lib seems to need at least a single slot
        // to send messages, even when connected. For sanity we put
        // a small lower limit.
        self.copts.maxBufferedMessages = cmp::max(4, n);
        if n == 0 {
            self.copts.sendWhileDisconnected = 0;
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
    /// * `on` Whether to allow off-line buffering in the client.
    ///
    pub fn send_while_disconnected(mut self, on: bool) -> Self {
        self.copts.sendWhileDisconnected = to_c_bool(on);
        self
    }

    /// Sets the version of MQTT to use on the connect.
    ///
    /// # Arguments
    ///
    /// * `ver` The version of MQTT to use when connecting to the broker.
    ///       * (0) try the latest version (3.1.1) and work backwards
    ///       * (3) only try v3.1
    ///       * (4) only try v3.1.1
    ///       * (5) only try v5
    ///
    pub fn mqtt_version<V>(mut self, ver: V) -> Self
    where
        V: Into<MqttVersion>,
    {
        self.copts.MQTTVersion = ver.into() as c_int;
        self
    }

    /// Allow sending of messages while disconnected before a first successful
    /// connect.
    pub fn allow_disconnected_send_at_anytime(mut self, allow: bool) -> Self {
        self.copts.allowDisconnectedSendAtAnyTime = to_c_bool(allow);
        self
    }

    /// When the maximum number of buffered messages is reached, delete the
    /// oldest rather than the newest.
    pub fn delete_oldest_messages(mut self, delete_oldest: bool) -> Self {
        self.copts.deleteOldestMessages = to_c_bool(delete_oldest);
        self
    }

    /// If set, messages from persistence are restored on create. Defaults to
    /// true.
    pub fn restore_messages(mut self, restore: bool) -> Self {
        self.copts.restoreMessages = to_c_bool(restore);
        self
    }

    /// If set, QoS0 publish commands are persisted. Defaults to true.
    pub fn persist_qos0(mut self, persist: bool) -> Self {
        self.copts.persistQoS0 = to_c_bool(persist);
        self
    }

    /// Sets the user-defined data structure for the client.
    pub fn user_data(mut self, data: UserData) -> Self {
        self.user_data = Some(data);
        self
    }

    /// Constructs a set of create options from the builder information.
    pub fn finalize(self) -> CreateOptions {
        let mut opts = CreateOptions {
            copts: self.copts,
            server_uri: self.server_uri,
            client_id: self.client_id,
            persistence: self.persistence,
            user_data: self.user_data,
        };
        match opts.persistence {
            PersistenceType::File if opts.client_id.is_empty() => {
                opts.persistence = PersistenceType::None
            }
            _ => (),
        }
        opts
    }

    /// Finalize the builder and create an asynchronous client.
    pub fn create_client(self) -> Result<AsyncClient> {
        AsyncClient::new(self.finalize())
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::raw::c_char;

    // The identifier for the create options structure
    const STRUCT_ID: [c_char; 4] = [
        b'M' as c_char,
        b'Q' as c_char,
        b'C' as c_char,
        b'O' as c_char,
    ];

    // The currently supported create struct version
    const STRUCT_VERSION: i32 = ffi::CREATE_OPTIONS_STRUCT_VERSION;

    // Rust options should be the same as the C options
    #[test]
    fn test_default() {
        let opts = CreateOptions::default();
        let copts = opts.c_struct();

        // Get default C options for comparison
        let dflt_copts = ffi::MQTTAsync_createOptions::default();

        // First, make sure C options valid
        assert_eq!(STRUCT_ID, dflt_copts.struct_id);
        assert_eq!(STRUCT_VERSION, dflt_copts.struct_version);

        assert_eq!(dflt_copts.struct_id, copts.struct_id);
        assert_eq!(dflt_copts.struct_version, copts.struct_version);
        assert_eq!(
            dflt_copts.sendWhileDisconnected,
            copts.sendWhileDisconnected
        );
        assert_eq!(dflt_copts.maxBufferedMessages, copts.maxBufferedMessages);

        // TODO: memcmp strings in copts throughout
        assert_eq!("", &opts.server_uri);
        assert_eq!("", &opts.client_id);

        if !matches!(opts.persistence, PersistenceType::None) {
            unreachable!();
        }
    }

    #[test]
    fn test_from_string() {
        const HOST: &str = "localhost";

        let opts = CreateOptions::from(HOST);
        let copts = opts.c_struct();

        let dflt_copts = ffi::MQTTAsync_createOptions::default();

        assert_eq!(STRUCT_ID, copts.struct_id);
        assert_eq!(STRUCT_VERSION, copts.struct_version);
        assert_eq!(
            dflt_copts.sendWhileDisconnected,
            copts.sendWhileDisconnected
        );
        assert_eq!(dflt_copts.maxBufferedMessages, copts.maxBufferedMessages);

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!("", &opts.client_id);
        //assert_eq!(PersistenceType::File, opts.persistence);
    }

    #[test]
    fn test_from_tuple() {
        const HOST: &str = "localhost";
        const ID: &str = "bubba";

        let opts = CreateOptions::from((HOST, ID));
        let copts = opts.c_struct();

        let dflt_copts = ffi::MQTTAsync_createOptions::default();

        assert_eq!(STRUCT_ID, copts.struct_id);
        assert_eq!(STRUCT_VERSION, copts.struct_version);
        assert_eq!(
            dflt_copts.sendWhileDisconnected,
            copts.sendWhileDisconnected
        );
        assert_eq!(dflt_copts.maxBufferedMessages, copts.maxBufferedMessages);

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        assert!(matches!(opts.persistence, PersistenceType::File));
    }

    #[test]
    fn test_default_builder() {
        let opts = CreateOptionsBuilder::new().finalize();
        let copts = opts.c_struct();

        let dflt_copts = ffi::MQTTAsync_createOptions::default();

        // First, make sure C options valid
        assert_eq!(STRUCT_ID, copts.struct_id);
        assert_eq!(STRUCT_VERSION, copts.struct_version);

        assert_eq!(
            dflt_copts.sendWhileDisconnected,
            copts.sendWhileDisconnected
        );
        assert_eq!(dflt_copts.maxBufferedMessages, copts.maxBufferedMessages);

        assert_eq!("", &opts.server_uri);
        assert_eq!("", &opts.client_id);
        //assert_eq!(PersistenceType::File, opts.persistence);
    }

    #[test]
    fn test_builder() {
        const HOST: &str = "localhost";
        const ID: &str = "bubba";
        const MAX_BUF_MSGS: usize = 250;

        // TODO: Test persistence

        let opts = CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(ID)
            .max_buffered_messages(MAX_BUF_MSGS)
            .finalize();
        let copts = opts.c_struct();

        assert_eq!(STRUCT_ID, copts.struct_id);
        assert_eq!(STRUCT_VERSION, copts.struct_version);

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        assert_eq!(0, copts.sendWhileDisconnected);
        assert_eq!(MAX_BUF_MSGS as i32, copts.maxBufferedMessages);

        let opts = CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(ID)
            .send_while_disconnected(true)
            .max_buffered_messages(MAX_BUF_MSGS)
            .finalize();
        let copts = opts.c_struct();

        assert_eq!(STRUCT_ID, copts.struct_id);
        assert_eq!(STRUCT_VERSION, copts.struct_version);

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        assert!(0 != copts.sendWhileDisconnected);
        assert_eq!(MAX_BUF_MSGS as i32, copts.maxBufferedMessages);
    }
}
