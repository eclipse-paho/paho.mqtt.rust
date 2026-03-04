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
    async_client::AsyncClient, client_persistence::ClientPersistence, ffi, to_c_bool, MqttVersion,
    Result, UserData,
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
    User(Box<Box<dyn ClientPersistence + Send>>),
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
#[derive(Debug, Default)]
pub struct CreateOptions {
    /// The underlying C option structure
    pub(crate) copts: ffi::MQTTAsync_createOptions,
    /// The URI for the MQTT broker.
    pub(crate) server_uri: String,
    /// The unique name for the client.
    /// This can be left empty for the server to assign a random name.
    pub(crate) client_id: String,
    /// The type of persistence used by the client.
    pub(crate) persistence: PersistenceType,
    /// User-defined data, if any
    pub(crate) user_data: Option<UserData>,
}

impl CreateOptions {
    /// Create options for a client that can connect using MQTT v3.x or v5.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create options for a client that can only connect using MQTT v3.x.
    pub fn new_v3() -> Self {
        Self {
            copts: ffi::MQTTAsync_createOptions::new_v3(),
            ..Self::default()
        }
    }

    /// Gets the MQTT protocol version used when creating the client.
    ///
    /// This version is used by the client as the default when connecting.
    /// It can be overridden in the connect options to request a different
    /// version, but typically this is the highest version that can be used
    /// by the client.
    pub fn mqtt_version(&self) -> MqttVersion {
        MqttVersion::from(self.copts.MQTTVersion)
    }

    /// Gets the raw, integer value of the MQTT version.
    pub fn mqtt_version_raw(&self) -> u32 {
        self.copts.MQTTVersion as u32
    }
}

impl<'a> From<&'a str> for CreateOptions {
    fn from(server_uri: &'a str) -> Self {
        Self {
            server_uri: server_uri.to_string(),
            ..Self::default()
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

impl<'a, 'b> From<(&'a str, &'b str)> for CreateOptions {
    /// Constructs the create options from two string reference giving the server URI
    /// and Client ID.
    fn from((server_uri, client_id): (&'a str, &'b str)) -> Self {
        let mut opts = Self::from(server_uri);
        opts.client_id = client_id.to_string();
        if !opts.client_id.is_empty() {
            opts.persistence = PersistenceType::File;
        }
        opts
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
        if !opts.client_id.is_empty() {
            opts.persistence = PersistenceType::File;
        }
        opts
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
        // Get default C options for comparison
        let copts = ffi::MQTTAsync_createOptions::default();

        // First, make sure C options valid
        assert_eq!(STRUCT_ID, copts.struct_id);
        assert_eq!(STRUCT_VERSION, copts.struct_version);

        assert_eq!(copts.struct_id, opts.copts.struct_id);
        assert_eq!(copts.struct_version, opts.copts.struct_version);
        assert_eq!(
            copts.sendWhileDisconnected,
            opts.copts.sendWhileDisconnected
        );
        assert_eq!(copts.maxBufferedMessages, opts.copts.maxBufferedMessages);

        assert_eq!("", &opts.server_uri);
        assert_eq!("", &opts.client_id);

        match opts.persistence {
            PersistenceType::None => (),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_from_string() {
        const HOST: &str = "localhost";

        let opts = CreateOptions::from(HOST);
        let copts = ffi::MQTTAsync_createOptions::default();

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);
        assert_eq!(
            copts.sendWhileDisconnected,
            opts.copts.sendWhileDisconnected
        );
        assert_eq!(copts.maxBufferedMessages, opts.copts.maxBufferedMessages);

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!("", &opts.client_id);
        //assert_eq!(PersistenceType::File, opts.persistence);
    }

    #[test]
    fn test_from_tuple() {
        const HOST: &str = "localhost";
        const ID: &str = "bubba";

        let opts = CreateOptions::from((HOST, ID));
        let copts = ffi::MQTTAsync_createOptions::default();

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);
        assert_eq!(
            copts.sendWhileDisconnected,
            opts.copts.sendWhileDisconnected
        );
        assert_eq!(copts.maxBufferedMessages, opts.copts.maxBufferedMessages);

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        //assert_eq!(PersistenceType::File, opts.persistence);
    }

    #[test]
    fn test_default_builder() {
        let opts = CreateOptionsBuilder::new().finalize();
        let copts = ffi::MQTTAsync_createOptions::default();

        // First, make sure C options valid
        assert_eq!(STRUCT_ID, copts.struct_id);
        assert_eq!(STRUCT_VERSION, copts.struct_version);

        assert_eq!(copts.struct_id, opts.copts.struct_id);
        assert_eq!(copts.struct_version, opts.copts.struct_version);
        assert_eq!(
            copts.sendWhileDisconnected,
            opts.copts.sendWhileDisconnected
        );
        assert_eq!(copts.maxBufferedMessages, opts.copts.maxBufferedMessages);

        assert_eq!("", &opts.server_uri);
        assert_eq!("", &opts.client_id);
        //assert_eq!(PersistenceType::File, opts.persistence);
    }

    #[test]
    fn test_builder() {
        const HOST: &str = "localhost";
        const ID: &str = "bubba";
        const MAX_BUF_MSGS: i32 = 250;

        // TODO: Test persistence

        let opts = CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(ID)
            .max_buffered_messages(MAX_BUF_MSGS)
            .finalize();

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        assert_eq!(0, opts.copts.sendWhileDisconnected);
        assert_eq!(MAX_BUF_MSGS, opts.copts.maxBufferedMessages);

        let opts = CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(ID)
            .send_while_disconnected(true)
            .max_buffered_messages(MAX_BUF_MSGS)
            .finalize();

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);

        assert_eq!(HOST, &opts.server_uri);
        assert_eq!(ID, &opts.client_id);
        assert!(0 != opts.copts.sendWhileDisconnected);
        assert_eq!(MAX_BUF_MSGS, opts.copts.maxBufferedMessages);
    }
}
