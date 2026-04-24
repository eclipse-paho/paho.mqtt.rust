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

use crate::{
    AsyncClient, Client, ClientPersistence, MqttVersion, PersistenceType, Result, UserData,
    UserPersistence,
};
use std::cmp;

/////////////////////////////////////////////////////////////////////////////
//                            Create Options
/////////////////////////////////////////////////////////////////////////////

/// The options for creating an MQTT client.
/// This can be constructed using a
/// [CreateOptionsBuilder](struct.CreateOptionsBuilder.html).
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
    /// Whether to persist QoS0 publish commands - an option to not persist them.
    /// Defaults to true.
    pub persist_qos0: bool,
    /// The MQTT protocol version to use for connections from this client.
    /// Defaults to v5, which enables a persistence store compatible with
    /// either v3.x or v5 connections.
    pub mqtt_version: MqttVersion,
    /// The type of persistence used by the client.
    pub persistence: PersistenceType,
    /// User-defined data to store in the client, if any
    pub user_data: Option<UserData>,
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

    /// If set, QoS0 publish commands are persisted. Defaults to true.
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

    /// Constructs a set of create options from the builder information.
    pub fn finalize(self) -> CreateOptions {
        self.opts
    }

    /// Finalize the builder and create an asynchronous client.
    pub fn create_async_client(self) -> Result<AsyncClient> {
        AsyncClient::new(self.finalize())
    }

    /// Finalize the builder and create a synchronous client.
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
}
