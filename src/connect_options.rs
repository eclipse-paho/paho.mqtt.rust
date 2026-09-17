// connect_options.rs
//
// The set of options for connecting to an MQTT client.
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

//! Connect options for the Paho MQTT Rust client library.
//!
//! This contains the structures to define the options for connecting to the
//! MQTT broker/server.

use crate::{Error, Message, Properties, Result, TlsOptions};
use paho_mqtt_codec::types::*;
use std::{collections::HashMap, time::Duration};

/// The default keep alive interval for MQTT (TCP) & MQTTS (TLS) connections.
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// The default keep alive interval for Websocket connections.
const DEFAULT_KEEP_ALIVE_INTERVAL_WS: Duration = Duration::from_secs(45);

/////////////////////////////////////////////////////////////////////////////
// ConnectOptions

/// The options for connecting to an MQTT broker.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// The maximum time that should pass without communications between
    /// the client and server. Resolution in sec.
    pub keep_alive_interval: Duration,
    /// The clean start/session flag.
    pub clean_start: bool,
    /// The maximum number of messages that can be in flight at once.
    pub max_inflight: u16,
    /// The Last Will and Testament (LWT) message
    pub will: Option<Message>,
    /// The TLS options
    pub tls: Option<TlsOptions>,
    /// The user name for authentication
    pub user_name: Option<String>,
    /// The password for authentication
    pub password: Option<Vec<u8>>,
    /// The timeout for the connection attempt
    pub timeout: Duration,
    /// The servers to which the client will connect, as URIs.
    pub server_uris: Vec<String>,
    /// The MQTT protocol for the connection
    pub mqtt_version: MqttVersion,
    /// Whether the server should automatically reconnect on a connection lost.
    pub auto_reconnect: bool,
    /// The minimum retry interval for auto reconnects. Resolution in sec.
    pub min_retry_interval: Duration,
    /// The maximum retry interval for auto reconnects. Resolution in sec.
    pub max_retry_interval: Duration,
    /// The connect properties (MQTT v5 only)
    pub props: Properties,
    /// HTTP headers for websocket connections
    pub http_headers: HashMap<String, String>,
    /// The HTTP proxy (insecure), if any
    pub http_proxy: String,
    /// The HTTPS proxy (secure), if any
    pub https_proxy: String,
}

impl ConnectOptions {
    /// Creates a new, default set of connect options, currently for MQTT v5
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new, default set of connect options for MQTT v3.1.1
    pub fn new_v3() -> Self {
        Self {
            mqtt_version: MqttVersion::V3_1_1,
            ..Self::default()
        }
    }

    /// Creates a new, default set of connect options for MQTT v5
    pub fn new_v5() -> Self {
        Self {
            mqtt_version: MqttVersion::V5,
            ..Self::default()
        }
    }

    /// Creates a new, default set of connect options for connecting over
    /// websockets.
    pub fn new_ws() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL_WS,
            ..Self::default()
        }
    }

    /// Creates a new, default set of connect options for connecting MQTT v3.1.1
    /// over websockets.
    pub fn new_ws_v3() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL_WS,
            mqtt_version: MqttVersion::V3_1_1,
            ..Self::default()
        }
    }

    /// Creates a new, default set of connect options for connecting MQTT v5
    /// over websockets.
    pub fn new_ws_v5() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL_WS,
            ..Self::new_v5()
        }
    }

    /// Creates a new set of connect options for the specified MQTT
    /// protocol version (v3.1, v3.1.1, or v5).
    pub fn with_mqtt_version<V: Into<MqttVersion>>(ver: V) -> Self {
        Self {
            mqtt_version: ver.into(),
            ..Self::default()
        }
    }

    /// Does a check of the options to determine if they're valid.
    ///
    /// Rules:
    /// - If you specify a secure URI ("tls://", "wss://", etc), you must
    ///   supply SSL options.
    pub fn check(&self) -> Result<()> {
        if !self.server_uris.is_empty() {
            let uris = &self.server_uris;
            let any_secure = uris.iter().any(|s| crate::is_secure_uri(s));

            if any_secure && !self.has_tls_options() {
                return Err(Error::MissingTlsOptions);
            }
        }
        Ok(())
    }

    /// Gets the MQTT protocol version that should be used for the
    /// connection.
    pub fn mqtt_version(&self) -> MqttVersion {
        self.mqtt_version
    }

    /// Gets the MQTT protocol version as a raw integer.
    pub fn mqtt_version_raw(&self) -> u32 {
        self.mqtt_version as u32
    }

    /// Gets the "clean session" setting in the options.
    ///
    /// This is only used in MQTT v3 connections.
    pub fn clean_session(&self) -> bool {
        self.clean_start && self.mqtt_version < MqttVersion::V5
    }

    /// This sets the "clean session" behavior for connecting to the server.
    ///
    /// When set to true, this directs the server to throw away any state
    /// related to the client, as determined by the client identifier.
    /// When set to false, the server keeps the state information and
    /// resumes the previous session.
    ///
    /// This is only used in MQTT v3 connections.
    pub fn set_clean_session(&mut self, clean: bool) {
        self.clean_start = clean;
    }

    /// Gets the "clean start" setting in the options.
    ///
    /// This is only used in MQTT v5 connections.
    pub fn clean_start(&self) -> bool {
        self.clean_start && self.mqtt_version >= MqttVersion::V5
    }

    /// This sets the "clean start" behavior for connecting to the server.
    ///
    /// When set to true, this directs the server to throw away any state
    /// related to the client, as determined by the client identifier.
    /// When set to false, the server keeps the state information and
    /// resumes the previous session.
    ///
    /// This is only used in MQTT v5 connections.
    pub fn set_clean_start(&mut self, clean: bool) {
        self.clean_start = clean;
    }

    /// Whether the options have TLS options for a secure connection
    pub fn has_tls_options(&self) -> bool {
        self.tls.is_some()
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL,
            clean_start: true,
            max_inflight: 65535,
            will: None,
            tls: None,
            user_name: None,
            password: None,
            timeout: Duration::from_secs(30),
            server_uris: Vec::new(),
            mqtt_version: MqttVersion::Default,
            auto_reconnect: false,
            min_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(60),
            props: Properties::default(),
            http_headers: HashMap::new(),
            http_proxy: String::new(),
            https_proxy: String::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder to create the options to connect to the MQTT server.
#[derive(Default, Clone)]
pub struct ConnectOptionsBuilder(ConnectOptions);

impl ConnectOptionsBuilder {
    /// Creates a new `ConnectOptionsBuilder` for MQTT v3.x
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `ConnectOptionsBuilder` explicitly for MQTT v3.x
    /// (although this is currently the default).
    pub fn new_v3() -> Self {
        Self(ConnectOptions::new_v3())
    }

    /// Creates a new `ConnectOptionsBuilder` for MQTT v5
    pub fn new_v5() -> Self {
        Self(ConnectOptions::new_v5())
    }

    /// Creates a new `ConnectOptionsBuilder` for connecting over websockets.
    pub fn new_ws() -> Self {
        Self(ConnectOptions::new_ws())
    }

    /// Creates a new `ConnectOptionsBuilder` for connecting to MQTT v3.1.1
    /// over websockets.
    pub fn new_ws_v3() -> Self {
        Self(ConnectOptions::new_ws_v3())
    }

    /// Creates a new `ConnectOptionsBuilder` for connecting to MQTT v5
    /// over websockets.
    pub fn new_ws_v5() -> Self {
        Self(ConnectOptions::new_ws_v5())
    }

    /// Creates a new set of `ConnectOptionsBuilder` for the specified MQTT
    /// protocol version.
    ///
    /// Note that this will initialize the options with the requested version,
    /// even if it's not currently known. The server will likely reject a
    /// request with a bad protocol version, however.
    pub fn with_mqtt_version<V>(ver: V) -> Self
    where
        V: Into<MqttVersion>,
    {
        Self(ConnectOptions::with_mqtt_version(ver))
    }

    /// Sets the keep alive interval for the client session.
    ///
    /// # Arguments
    ///
    /// `keep_alive_interval` The maximum time that should pass without
    ///                       communication between the client and server.
    ///                       This has a resolution in seconds.
    pub fn keep_alive_interval(&mut self, keep_alive_interval: Duration) -> &mut Self {
        // TODO: Set a lower limit?
        self.0.keep_alive_interval = keep_alive_interval;
        self
    }

    /// Sets the 'clean session' flag to send to the broker.
    ///
    /// This is for MQTT v3.x connections only, and if set, will set the
    /// other options to be compatible with v3.
    ///
    /// # Arguments
    ///
    /// `clean` Whether the broker should remove any previously-stored
    ///         information for this client.
    pub fn clean_session(&mut self, clean: bool) -> &mut Self {
        self.0.clean_start = clean;
        self
    }

    /// Sets the 'clean start' flag to send to the broker.
    ///
    /// This is for MQTT v5 connections only, and if set, will set the
    /// other options to be compatible with v5.
    ///
    /// # Arguments
    ///
    /// `clean` Whether the broker should remove any previously-stored
    ///         information for this client.
    pub fn clean_start(&mut self, clean: bool) -> &mut Self {
        self.0.clean_start = clean;
        self
    }

    /// Sets the maximum number of in-flight messages that can be
    /// simultaneously handled by this client.
    ///
    /// # Arguments
    ///
    /// `max_inflight` The maximum number of messages that can be in-flight
    ///                at any given time with this client.
    pub fn max_inflight(&mut self, max_inflight: u16) -> &mut Self {
        self.0.max_inflight = max_inflight;
        self
    }

    /// Sets the LWT message for the connection.
    ///
    /// # Arguments
    ///
    /// `will` The LWT options for the connection.
    pub fn will_message<M>(&mut self, will: M) -> &mut Self
    where
        M: Into<Option<Message>>,
    {
        self.0.will = will.into();
        self
    }

    /// Sets the TLS options for the connection.
    ///
    /// # Arguments
    ///
    /// `tls` The TLS options for the connection.
    pub fn tls_options<S>(&mut self, tls: S) -> &mut Self
    where
        S: Into<Option<TlsOptions>>,
    {
        self.0.tls = tls.into();
        self
    }

    /// Sets the user name for authentication with the broker.
    /// This works with the password.
    ///
    /// # Arguments
    ///
    /// `user_name` The user name to send to the broker.
    ///
    pub fn user_name<S>(&mut self, user_name: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.user_name = Some(user_name.into());
        self
    }

    /// Clears the user name.
    /// This sets the option to `None`
    pub fn clear_user_name(&mut self) -> &mut Self {
        self.0.user_name = None;
        self
    }

    /// Sets the password for authentication with the broker.
    /// This works with the user name.
    ///
    /// # Arguments
    ///
    /// `password` The password to send to the broker.
    ///
    pub fn password<P>(&mut self, password: P) -> &mut Self
    where
        P: Into<Vec<u8>>,
    {
        self.0.password = Some(password.into());
        self
    }

    /// Clear the password.
    /// This sets the option to `None`
    pub fn clear_password(&mut self) -> &mut Self {
        self.0.password = None;
        self
    }

    /// Sets the user name and password for authentication with the broker.
    ///
    /// # Arguments
    ///
    /// `user_name` The user name to send to the broker.
    /// `password` The password to send to the broker.
    ///
    pub fn user_name_password<S, P>(&mut self, user_name: S, password: P) -> &mut Self
    where
        S: Into<String>,
        P: Into<Vec<u8>>,
    {
        self.0.user_name = Some(user_name.into());
        self.0.password = Some(password.into());
        self
    }

    /// Clears the user name and password.
    /// This sets both options to `None`
    pub fn clear_user_name_password(&mut self) -> &mut Self {
        self.0.user_name = None;
        self.0.password = None;
        self
    }

    /// Sets the time interval to allow the connection to complete.
    ///
    /// # Arguments
    ///
    /// `timeout` The time interval to allow the connection to
    ///           complete. This has a resolution of seconds.
    ///
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        // TODO: Lower limit of 1 sec?
        self.0.timeout = timeout;
        self
    }

    /// Specify the servers to which the client will connect.
    ///
    /// # Arguments
    ///
    /// `server_uris` The addresses of the brokers to which this client
    ///               should connect.
    //
    pub fn server_uris<T>(&mut self, server_uris: &[T]) -> &mut Self
    where
        T: AsRef<str>,
    {
        self.0.server_uris = server_uris
            .iter()
            .map(|s| String::from(s.as_ref()))
            .collect();
        self
    }

    /// Sets the client to automatically reconnect if the connection is lost.
    ///
    /// # Arguments
    ///
    /// `min_retry_interval` The minimum retry interval. Doubled on each
    ///                      failed retry. This has a resolution in seconds.
    /// `max_retry_interval` The maximum retry interval. Doubling stops here
    ///                      on failed retries. This has a resolution in
    ///                      seconds.
    pub fn automatic_reconnect(
        &mut self,
        min_retry_interval: Duration,
        max_retry_interval: Duration,
    ) -> &mut Self {
        self.0.auto_reconnect = true;
        // TODO: Lower limit? Ensure max >= min?
        self.0.min_retry_interval = min_retry_interval;
        self.0.max_retry_interval = max_retry_interval;
        self
    }

    /// Sets the collection of properties for the connections.
    ///
    /// # Arguments
    ///
    /// `props` The collection of properties to include with the connect message.
    pub fn properties(&mut self, props: Properties) -> &mut Self {
        // TODO: Error on version mismatch
        self.0.props = props;
        self
    }

    /// Sets the additional HTTP headers that will be sent in the
    /// WebSocket opening handshake.
    pub fn http_headers(&mut self, headers: HashMap<String, String>) -> &mut Self {
        self.0.http_headers = headers;
        self
    }

    /// Sets the additional HTTP headers that will be sent in the
    /// WebSocket opening handshake.
    pub fn http_headers_from_coll<N, V>(&mut self, headers: &[(N, V)]) -> &mut Self
    where
        N: AsRef<str>,
        V: AsRef<str>,
    {
        let mut hdrs = HashMap::new();
        for (n, v) in headers {
            hdrs.insert(n.as_ref().into(), v.as_ref().into());
        }
        self.http_headers(hdrs)
    }

    /// Sets an HTTP proxy for websockets.
    ///
    /// # Arguments
    ///
    /// `proxy` The HTTP proxy
    ///
    pub fn http_proxy<S>(&mut self, proxy: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.http_proxy = proxy.into();
        self
    }

    /// Sets a secure HTTPS proxy for websockets.
    ///
    /// # Arguments
    ///
    /// `proxy` The HTTPS proxy
    ///
    pub fn https_proxy<S>(&mut self, proxy: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.https_proxy = proxy.into();
        self
    }

    /// Finalize the builder to create the connect options.
    pub fn finalize(&self) -> ConnectOptions {
        self.0.clone()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QOS_1;

    const NAME: &str = "some-random-name";
    const PSWD: &str = "some-random-password";

    fn assert_implements_send<T: Send>() {}
    fn assert_implements_sync<T: Sync>() {}

    #[test]
    fn test_new_versions() {
        // new() defaults to v5: clean start, no clean session
        let opts = ConnectOptions::new();
        assert_eq!(MqttVersion::V5, opts.mqtt_version());
        assert!(!opts.clean_session());
        assert!(opts.clean_start());

        // new_v3() explicitly selects v3.1.1: clean session, no clean start
        let opts = ConnectOptions::new_v3();
        assert_eq!(MqttVersion::V3_1_1, opts.mqtt_version());
        assert!(opts.clean_session());
        assert!(!opts.clean_start());

        // new_v5() explicitly selects v5: clean start, no clean session
        let opts = ConnectOptions::new_v5();
        assert_eq!(MqttVersion::V5, opts.mqtt_version());
        assert!(!opts.clean_session());
        assert!(opts.clean_start());
    }

    #[test]
    fn test_with_mqtt_version_stores_requested_version() {
        for ver in [MqttVersion::V3_1, MqttVersion::V3_1_1, MqttVersion::V5] {
            assert_eq!(ConnectOptions::with_mqtt_version(ver).mqtt_version(), ver);
            assert_eq!(
                ConnectOptionsBuilder::with_mqtt_version(ver)
                    .finalize()
                    .mqtt_version(),
                ver
            );
        }
    }

    // Determine that the options can be sent across threads.
    // As long as it compiles, this indicates that ConnectOptions
    // implements the traits.
    #[test]
    fn test_send() {
        assert_implements_send::<ConnectOptions>();
        assert_implements_sync::<ConnectOptions>();
    }

    #[test]
    fn test_set_and_clear() {
        let msg = Message::new("lwt", "disconnected", QOS_1);
        let ssl = TlsOptions::default();

        // user_name: set then clear
        let opts = ConnectOptionsBuilder::new().user_name(NAME).finalize();
        assert_eq!(opts.user_name.as_deref(), Some(NAME));

        let opts = ConnectOptionsBuilder::new()
            .user_name(NAME)
            .clear_user_name()
            .finalize();
        assert!(opts.user_name.is_none());

        // password: set then clear
        let opts = ConnectOptionsBuilder::new().password(PSWD).finalize();
        assert_eq!(opts.password.as_deref(), Some(PSWD.as_bytes()));

        let opts = ConnectOptionsBuilder::new()
            .password(PSWD)
            .clear_password()
            .finalize();
        assert!(opts.password.is_none());

        // will_message: set then clear with None
        let opts = ConnectOptionsBuilder::new()
            .will_message(msg.clone())
            .finalize();
        assert!(opts.will.is_some());

        let opts = ConnectOptionsBuilder::new()
            .will_message(msg)
            .will_message(None)
            .finalize();
        assert!(opts.will.is_none());

        // tls_options: set then clear with None
        let opts = ConnectOptionsBuilder::new().tls_options(ssl).finalize();
        assert!(opts.tls.is_some());

        let opts = ConnectOptionsBuilder::new()
            .tls_options(TlsOptions::default())
            .tls_options(None)
            .finalize();
        assert!(opts.tls.is_none());
    }
}
