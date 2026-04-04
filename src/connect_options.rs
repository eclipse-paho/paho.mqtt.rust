// connect_options.rs
//
// The set of options for connecting to an MQTT client.
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

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

//! Connect options for the Paho MQTT Rust client library.
//!
//! This contains the structures to define the options for connecting to the
//! MQTT broker/server.

use crate::{
    ffi, name_value::NameValueCollection, string_collection::StringCollection, to_c_bool,
    types::*, ConnectToken, Error, Message, Properties, PropertiesFfi, Result, SslOptions,
    SslOptionsFfi, Token, TokenInner, WillOptions,
};
use std::{
    collections::HashMap,
    ffi::{c_int, c_void, CString},
    pin::Pin,
    ptr,
    time::Duration,
};

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
    /// The TLS/SSL options
    pub ssl: Option<SslOptions>,
    /// The user name for authentication
    pub user_name: Option<String>,
    /// The password for authentication
    pub password: Option<Vec<u8>>,
    /// The timeout for the connection attempt
    pub timeout: Duration,
    /// MQTT v3 publish retry interval. (Ignored for MQTT v5)
    pub retry_interval: Option<Duration>,
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
    http_proxy: String,
    /// The HTTPS proxy (secure), if any
    https_proxy: String,
}

impl ConnectOptions {
    /// Creates a new, default set of connect options for MQTT v3.x
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new, default set of connect options for MQTT v3.x
    pub fn new_v3() -> Self {
        Self::default()
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

    /// Creates a new, default set of connect options for connecting over
    /// websockets.
    pub fn new_ws_v3() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL_WS,
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
    /// protocol version.
    ///
    /// Note that this will return the options with the requested version,
    /// even if it's not currently known. The server will likely reject a
    /// request with a bad protocol version, however.
    pub fn with_mqtt_version<V: Into<MqttVersion>>(ver: V) -> Self {
        if ver.into() < MqttVersion::V5 {
            Self::new()
        }
        else {
            Self::new_v5()
        }
    }

    /// Does a check of the options to determine if their valid.
    pub fn check(&self) -> Result<()> {
        if !self.server_uris.is_empty() {
            let uris = &self.server_uris;
            let any_secure = uris.iter().any(|s| crate::is_secure_uri(s));

            if any_secure && !self.has_ssl_options() {
                return Err(Error::MissingSslOptions);
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

    /// Whether the options have SSL/TLS options for a secure connection
    pub fn has_ssl_options(&self) -> bool {
        self.ssl.is_some()
    }

    // TODO: Do we need this?
    /*
    /// Sets the token to ber used for connect completion callbacks.
    ///
    /// Note that we leak the token to give to the C lib. When we're
    /// done with it, we must recover and drop it (i.e. in the completion
    /// callback).
    pub fn set_token(&mut self, tok: ConnectToken) {
        let tok: Token = tok;
        self.copts.context = tok.into_raw();
        self.fix_callbacks();
    }
    */
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL,
            clean_start: true,
            max_inflight: 65535,
            will: None,
            ssl: None,
            user_name: None,
            password: None,
            timeout: Duration::from_secs(30),
            retry_interval: None,
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
#[derive(Default)]
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

    /// Creates a new `ConnectOptionsBuilder` for connecting to MQTT v3.x
    /// over websockets.
    pub fn new_ws() -> Self {
        Self(ConnectOptions::new_ws())
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
        // TODO: Error on version mismatch?
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
        // TODO: Error on version mismatch?
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

    /// Sets the SSL options for the connection.
    ///
    /// # Arguments
    ///
    /// `ssl` The SSL options for the connection.
    pub fn ssl_options<S>(&mut self, ssl: S) -> &mut Self
    where
        S: Into<Option<SslOptions>>,
    {
        self.0.ssl = ssl.into();
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
        // TODO: Into option?
        self.0.user_name = Some(user_name.into());
        self
    }

    /// Sets the password for authentication with the broker.
    /// This works with the user name.
    ///
    /// # Arguments
    ///
    /// `password` The password to send to the broker.
    ///
    pub fn password<S>(&mut self, password: S) -> &mut Self
    where
        S: Into<Vec<u8>>,
    {
        // TODO: Into option?
        self.0.password = Some(password.into());
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

    /// Sets the publish retry interval for MQTT v3 connections.
    ///
    /// This is ignored for MQTT v5 connections, as retrying is only allowed
    /// on a reconnect in v5.
    ///
    /// # Arguments
    ///
    /// `interval` The publish retry interval. This has a resolution of seconds.
    ///
    pub fn retry_interval<D>(&mut self, interval: D) -> &mut Self
    where
        D: Into<Option<Duration>>,
    {
        self.0.retry_interval = interval.into();
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

    /// Gets the FFI representation of the connect options.
    /// This is a convenience function for the unit tests.
    #[cfg(test)]
    pub(crate) fn to_ffi(&self) -> ConnectOptionsFfi {
        ConnectOptionsFfi::from(&self.0)
    }

    /// Finalize the builder to create the connect options.
    pub fn finalize(&self) -> ConnectOptions {
        self.0.clone()
    }
}

/////////////////////////////////////////////////////////////////////////////
// ConnectOptionsFfi

/// The collection of options for connecting to a broker throught the Paho C
/// FFI layer.
///
/// This is constructed from [`ConnectOptions`] internally in the client.
#[derive(Debug)]
pub(crate) struct ConnectOptionsFfi {
    /// The underlying C options structure.
    /// The 'will', 'ssl', 'username', and 'password' fields should
    /// be NULL (not empty) if unused.
    pub(crate) copts: ffi::MQTTAsync_connectOptions,
    /// The pinned data cache
    #[allow(dead_code)]
    data: Pin<Box<ConnectOptionsFfiData>>,
}

/// Cached data for the connect options.
#[derive(Debug, Default)]
struct ConnectOptionsFfiData {
    will: Option<WillOptions>,
    ssl: Option<SslOptionsFfi>,
    user_name: Option<CString>,
    password: Option<Vec<u8>>,
    server_uris: StringCollection,
    props: PropertiesFfi,
    will_props: PropertiesFfi,
    http_headers: NameValueCollection,
    http_proxy: CString,
    https_proxy: CString,
}

impl ConnectOptionsFfi {
    // Creates a set of options from a C struct and cached values.
    // Fixes up the underlying C struct to point to the cached values,
    // then returns a new options object with them combined.
    fn from_data(
        mut copts: ffi::MQTTAsync_connectOptions,
        data: ConnectOptionsFfiData,
    ) -> Self {
        let mut data = Box::pin(data);

        copts.will = match data.will {
            Some(ref mut will_opts) => &mut will_opts.copts,
            _ => ptr::null_mut(),
        };

        copts.ssl = match data.ssl {
            Some(ref mut ssl_opts) => &mut ssl_opts.copts,
            _ => ptr::null_mut(),
        };

        copts.username = match data.user_name {
            Some(ref user_name) => user_name.as_ptr(),
            _ => ptr::null(),
        };

        use crate::ffi::MQTTAsync_connectOptions__bindgen_ty_1 as MQTTAsync_connectOptions__binarypwd;

        copts.binarypwd = match data.password {
            Some(ref binary_password) => MQTTAsync_connectOptions__binarypwd {
                len: binary_password.len() as c_int,
                data: binary_password.as_ptr() as *const c_void,
            },
            None => MQTTAsync_connectOptions__binarypwd {
                len: 0,
                data: ptr::null(),
            },
        };

        if !data.server_uris.is_empty() {
            copts.serverURIs = data.server_uris.as_c_arr_mut_ptr();
            copts.serverURIcount = data.server_uris.len() as c_int;
        }
        else {
            copts.serverURIs = ptr::null();
            copts.serverURIcount = 0;
        }

        let is_v5 = copts.MQTTVersion >= MQTT_VERSION_5 as i32;

        copts.connectProperties = if is_v5 && !data.props.is_empty() {
            &mut data.props.cprops
        }
        else {
            ptr::null_mut()
        };

        copts.willProperties = if is_v5 && !data.will_props.is_empty() {
            &mut data.will_props.cprops
        }
        else {
            ptr::null_mut()
        };

        copts.httpHeaders = match &data.http_headers {
            h if !h.is_empty() => h.as_c_arr_ptr(),
            _ => ptr::null(),
        };

        copts.httpProxy = match &data.http_proxy {
            s if !s.to_bytes().is_empty() => s.as_ptr(),
            _ => ptr::null(),
        };

        copts.httpsProxy = match &data.https_proxy {
            s if !s.to_bytes().is_empty() => s.as_ptr(),
            _ => ptr::null(),
        };

        Self { copts, data }
    }

    // Sets the proper callbacks depending on the MQTT version and the
    // presence of a context in the C opts.
    fn fix_callbacks(&mut self) {
        self.copts.onSuccess = None;
        self.copts.onFailure = None;
        self.copts.onSuccess5 = None;
        self.copts.onFailure5 = None;

        if !self.copts.context.is_null() {
            if self.copts.MQTTVersion < ffi::MQTTVERSION_5 as i32 {
                self.copts.onSuccess = Some(TokenInner::on_success);
                self.copts.onFailure = Some(TokenInner::on_failure);
            }
            else {
                self.copts.onSuccess5 = Some(TokenInner::on_success5);
                self.copts.onFailure5 = Some(TokenInner::on_failure5);
            }
        }
    }

    /// Sets the token to be used for connect completion callbacks.
    ///
    /// Note that we leak the token to give to the C lib. When we're
    /// done with it, we must recover and drop it (i.e. in the completion
    /// callback).
    pub fn set_token(&mut self, tok: ConnectToken) {
        let tok: Token = tok;
        self.copts.context = tok.into_raw();
        self.fix_callbacks();
    }
}

impl Default for ConnectOptionsFfi {
    fn default() -> Self {
        Self::from_data(
            ffi::MQTTAsync_connectOptions::default(),
            ConnectOptionsFfiData::default(),
        )
    }
}

unsafe impl Send for ConnectOptionsFfi {}
unsafe impl Sync for ConnectOptionsFfi {}

impl From<&ConnectOptions> for ConnectOptionsFfi {
    fn from(opts: &ConnectOptions) -> Self {
        let mut copts = ffi::MQTTAsync_connectOptions::default();

        let is_v5 = opts.mqtt_version >= MqttVersion::V5;

        let mut secs = opts.keep_alive_interval.as_secs();
        copts.keepAliveInterval = if secs == 0 { 1 } else { secs as i32 };

        copts.cleansession = to_c_bool(!is_v5 && opts.clean_start);
        copts.cleanstart = to_c_bool(is_v5 && opts.clean_start);

        copts.maxInflight = opts.max_inflight as c_int;

        secs = opts.timeout.as_secs();
        copts.connectTimeout = if secs == 0 { 1 } else { secs as i32 };

        copts.MQTTVersion = opts.mqtt_version as c_int;

        if opts.auto_reconnect {
            copts.automaticReconnect = to_c_bool(true);

            secs = opts.min_retry_interval.as_secs();
            copts.minRetryInterval = if secs == 0 { 1 } else { secs as i32 };

            secs = opts.max_retry_interval.as_secs();
            copts.maxRetryInterval = if secs == 0 { 1 } else { secs as i32 };
        }

        let data = ConnectOptionsFfiData::from(opts);
        Self::from_data(copts, data)
    }
}

impl From<ConnectOptions> for ConnectOptionsFfi {
    fn from(opts: ConnectOptions) -> Self {
        Self::from(&opts)
    }
}

impl From<&ConnectOptions> for ConnectOptionsFfiData {
    fn from(opts: &ConnectOptions) -> Self {
        let mut data = ConnectOptionsFfiData::default();

        if let Some(msg) = &opts.will {
            data.will = Some(WillOptions::from(msg.clone()));
            //data.will_props = msg.properties().clone(); //PropertiesFfi::from(msg.properties());
            data.will_props = PropertiesFfi::from(msg.properties());
        }
        if let Some(ssl) = &opts.ssl {
            // TODO: Return the error
            let ssl_ffi = SslOptionsFfi::try_from(ssl).unwrap();
            data.ssl = Some(ssl_ffi);
        }
        if let Some(user_name) = &opts.user_name {
            data.user_name = Some(CString::new(user_name.clone()).unwrap());
        }
        if let Some(password) = &opts.password {
            data.password = Some(password.clone());
        }
        if !opts.server_uris.is_empty() {
            data.server_uris = StringCollection::from(opts.server_uris.as_slice());
        }
        data.props = PropertiesFfi::from(&opts.props);

        if !opts.http_headers.is_empty() {
            data.http_headers = NameValueCollection::from(opts.http_headers.clone());
        }
        if !opts.http_proxy.is_empty() {
            data.http_proxy = CString::new(opts.http_proxy.clone()).unwrap();
        }
        if !opts.https_proxy.is_empty() {
            data.https_proxy = CString::new(opts.https_proxy.clone()).unwrap();
        }

        data
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{message::MessageBuilder, properties::*, ssl_options::SslOptionsBuilder};
    use std::{ffi::CStr, os::raw::c_char};

    const NAME: &str = "some-random-name";
    const PSWD: &str = "some-random-password";
    const BIN_PSWD: &[u8] = b"some-random-binary-password";
    const TRUST_STORE: &str = "some_file.crt";

    // Identifier fo a C connect options struct
    const STRUCT_ID: [c_char; 4] = [
        b'M' as c_char,
        b'Q' as c_char,
        b'T' as c_char,
        b'C' as c_char,
    ];

    // The currently supported connect struct version
    const STRUCT_VERSION: i32 = ffi::CONNECT_OPTIONS_STRUCT_VERSION;

    #[test]
    fn test_new() {
        let opts = ConnectOptionsFfi::from(&ConnectOptions::new());

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);
        assert!(opts.copts.will.is_null());
        assert_eq!(1, opts.copts.cleansession);

        assert!(opts.copts.username.is_null());
        assert!(opts.copts.password.is_null());
        assert!(opts.copts.binarypwd.data.is_null());
        assert!(opts.copts.binarypwd.len == 0);
        assert!(opts.copts.ssl.is_null());

        assert!(opts.copts.context.is_null());

        assert_eq!(0, opts.copts.serverURIcount);
        assert!(opts.copts.serverURIs.is_null());

        assert_eq!(0, opts.copts.MQTTVersion);
        assert_eq!(0, opts.copts.automaticReconnect);

        assert!(opts.copts.httpHeaders.is_null());
        assert_eq!(0, opts.copts.cleanstart);

        assert!(opts.copts.connectProperties.is_null());
        assert!(opts.copts.willProperties.is_null());
    }

    #[test]
    fn test_new_versions() {
        // v3 defaults to clean session, but no clean start
        let opts = ConnectOptions::new();
        assert_eq!(MqttVersion::Default, opts.mqtt_version());
        assert!(opts.clean_session());
        assert!(!opts.clean_start());

        // v5 defaults to clean start, but no clean session
        let opts = ConnectOptions::new_v5();
        assert_eq!(MqttVersion::V5, opts.mqtt_version());
        assert!(!opts.clean_session());
        assert!(opts.clean_start());
    }

    #[test]
    fn test_ssl() {
        let ssl_opts = SslOptionsBuilder::new().trust_store(TRUST_STORE).finalize();

        let opts = ConnectOptionsBuilder::new().ssl_options(ssl_opts).to_ffi();

        assert!(!opts.copts.ssl.is_null());

        let ssl_opts = opts.data.ssl.as_ref().unwrap();
        // TODO: Test that ssl_opts.get_trust_store() is TRUST_STORE?
        assert_eq!(&ssl_opts.copts as *const _, opts.copts.ssl);
        let ts = unsafe { CStr::from_ptr((*opts.copts.ssl).trustStore) };
        assert_eq!(TRUST_STORE, ts.to_str().unwrap());
    }

    #[test]
    fn test_user_name() {
        const NAME: &str = "some-random-name";

        let opts = ConnectOptionsBuilder::new().user_name(NAME).to_ffi();
        assert!(!opts.copts.username.is_null());

        let user_name = opts.data.user_name.as_deref().unwrap();
        assert_eq!(NAME, user_name.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.username) };
        assert_eq!(NAME, s.to_str().unwrap());
    }

    #[test]
    fn test_password() {
        const PSWD: &str = "some-random-password";

        //assert!(!opts.copts.binarypwd.data.is_null());
        //assert_eq!(opts.copts.binarypwd.len, PSWD.len() as c_int);

        let opts = ConnectOptionsBuilder::new().password(PSWD).to_ffi();
        assert!(!opts.copts.password.is_null());

        let s = unsafe {
            std::slice::from_raw_parts(
                opts.copts.binarypwd.data as *const u8,
                opts.copts.binarypwd.len as usize,
            )
        };
        assert_eq!(PSWD.as_bytes(), s);
    }

    #[test]
    fn test_binary_password() {
        let opts = ConnectOptionsBuilder::new().password(BIN_PSWD).finalize();

        assert!(!opts.copts.binarypwd.data.is_null());
        assert_eq!(opts.copts.binarypwd.len, BIN_PSWD.len() as c_int);

        let binary_password = unsafe {
            std::slice::from_raw_parts(
                opts.copts.binarypwd.data as *const u8,
                opts.copts.binarypwd.len as usize,
            )
        };
        assert_eq!(BIN_PSWD, binary_password);

        let s = unsafe {
            std::slice::from_raw_parts(
                opts.copts.binarypwd.data as *const u8,
                opts.copts.binarypwd.len as usize,
            )
        };

        assert_eq!(s, BIN_PSWD);
    }

    #[test]
    fn test_server_uris() {
        let servers = ["tcp://server1:1883", "ssl://server2:1885"];

        let opts = ConnectOptionsBuilder::new().server_uris(&servers).to_ffi();

        assert_eq!(servers.len() as i32, opts.copts.serverURIcount);

        // Compare the strings to the C-arrays in copts
        for (i, svr) in servers.iter().enumerate() {
            let s = unsafe { CStr::from_ptr(*opts.copts.serverURIs.add(i)) };
            assert_eq!(&svr[..], s.to_str().unwrap());
        }
    }

    #[test]
    fn test_mqtt_version() {
        let opts = ConnectOptionsBuilder::new().to_ffi();

        assert_eq!(MQTT_VERSION_DEFAULT as c_int, opts.copts.MQTTVersion);

        let opts = ConnectOptionsBuilder::new_v3().to_ffi();
        assert_eq!(MQTT_VERSION_DEFAULT as c_int, opts.copts.MQTTVersion);

        let opts = ConnectOptionsBuilder::new_v5().to_ffi();
        assert_eq!(MQTT_VERSION_5 as c_int, opts.copts.MQTTVersion);
    }

    // Test that we can set each of the HTTP and HTTPS proxy values, but also
    // make sure we don't mist them up with cut-and-paste errors.
    #[test]
    fn test_proxies() {
        const HTTP: &str = "http://some_server:80";
        const HTTPS: &str = "https://some_other_server:443";

        let opts = ConnectOptionsBuilder::new().http_proxy(HTTP).to_ffi();
        assert!(!opts.copts.httpProxy.is_null());
        assert!(opts.copts.httpsProxy.is_null());

        assert!(opts.data.https_proxy.is_empty());
        assert_eq!(HTTP, opts.data.http_proxy.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.httpProxy) };
        assert_eq!(HTTP, s.to_str().unwrap());

        let opts = ConnectOptionsBuilder::new().https_proxy(HTTPS).to_ffi();

        assert!(!opts.copts.httpsProxy.is_null());
        assert!(opts.copts.httpProxy.is_null());

        assert!(opts.data.http_proxy.is_empty());
        assert_eq!(HTTPS, opts.data.https_proxy.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.httpsProxy) };
        assert_eq!(HTTPS, s.to_str().unwrap());
    }

    /*
    #[test]
    fn test_opts_check() {
        // Default must pass check
        let opts = ConnectOptionsBuilder::new().finalize();
        assert!(opts.check().is_ok());

        // Non-secure connections w/o SSL opts should pass
        let servers = ["tcp://server1:1883", "mqtt://server2:2883"];

        let opts = ConnectOptionsBuilder::new()
            .server_uris(&servers)
            .finalize();

        assert!(opts.check().is_ok());

        // Secure connections w/o SSL opts should fail
        let servers = ["tcp://server1:1883", "mqtts://server2:8883"];

        let opts = ConnectOptionsBuilder::new()
            .server_uris(&servers)
            .finalize();

        println!("{:?}", opts.check());
        assert!(matches!(opts.check(), Err(Error::MissingSslOptions)));

        // Secure connections with SSL opts should pass
        let servers = ["tcp://server1:1883", "wss://server2:8883/ws"];

        let ssl_opts = SslOptionsBuilder::new()
            .trust_store(TRUST_STORE)
            .unwrap()
            .finalize();

        let opts = ConnectOptionsBuilder::new()
            .server_uris(&servers)
            .ssl_options(ssl_opts)
            .finalize();

        assert!(opts.check().is_ok());
    }

    #[test]
    fn test_assign() {
        const KEEP_ALIVE_SECS: u64 = 30;
        const MAX_INFLIGHT: i32 = 25;
        const USER_NAME: &str = "some-name";
        const PASSWORD: &str = "some-password";
        const CONNECT_TIMEOUT_SECS: u64 = 120;

        let org_opts = ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::new(KEEP_ALIVE_SECS, 0))
            .clean_session(false)
            .max_inflight(MAX_INFLIGHT)
            .user_name(USER_NAME)
            .password(PASSWORD)
            .connect_timeout(Duration::new(CONNECT_TIMEOUT_SECS, 0))
            .finalize();

        let opts = org_opts;

        assert_eq!(KEEP_ALIVE_SECS as c_int, opts.copts.keepAliveInterval);
        assert_eq!(0, opts.copts.cleansession);
        assert_eq!(MAX_INFLIGHT, opts.copts.maxInflight);

        assert_eq!(
            USER_NAME,
            opts.data.user_name.as_ref().unwrap().to_str().unwrap()
        );
        assert_eq!(PASSWORD.as_bytes(), opts.data.password.as_ref().unwrap());

        let user_name = opts.data.user_name.as_deref().unwrap();
        assert_eq!(user_name.as_ptr(), opts.copts.username);

        let password = opts.data.password.as_deref().unwrap();
        assert_eq!(
            password.as_ptr() as *const c_void,
            opts.copts.binarypwd.data
        );

        assert_eq!(CONNECT_TIMEOUT_SECS as i32, opts.copts.connectTimeout);
    }

    #[test]
    fn test_clone() {
        const KEEP_ALIVE_SECS: u64 = 30;
        const MAX_INFLIGHT: i32 = 25;
        const USER_NAME: &str = "some-name";
        const PASSWORD: &str = "some-password";
        const CONNECT_TIMEOUT_SECS: u64 = 120;

        let org_opts = ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::new(KEEP_ALIVE_SECS, 0))
            .clean_session(false)
            .max_inflight(MAX_INFLIGHT)
            .user_name(USER_NAME)
            .password(PASSWORD)
            .connect_timeout(Duration::new(CONNECT_TIMEOUT_SECS, 0))
            .finalize();

        let opts = org_opts.clone();

        // Test original still intact

        assert_eq!(KEEP_ALIVE_SECS as c_int, org_opts.copts.keepAliveInterval);
        assert_eq!(0, org_opts.copts.cleansession);
        assert_eq!(MAX_INFLIGHT, org_opts.copts.maxInflight);

        // Now test the copy

        assert_eq!(KEEP_ALIVE_SECS as c_int, opts.copts.keepAliveInterval);
        assert_eq!(0, opts.copts.cleansession);
        assert_eq!(MAX_INFLIGHT, opts.copts.maxInflight);

        assert_eq!(
            USER_NAME,
            opts.data.user_name.as_ref().unwrap().to_str().unwrap()
        );
        assert_eq!(PASSWORD.as_bytes(), opts.data.password.as_ref().unwrap(),);

        let user_name = opts.data.user_name.as_deref().unwrap();
        assert_eq!(user_name.as_ptr(), opts.copts.username);

        let password = opts.data.password.as_deref().unwrap();
        assert_eq!(
            password.as_ptr() as *const c_void,
            opts.copts.binarypwd.data
        );

        assert_eq!(CONNECT_TIMEOUT_SECS as i32, opts.copts.connectTimeout);
    }
    */

    #[test]
    fn test_connect_properties() {
        let props = Properties::with_property(Property::SessionExpiryInterval(60));

        // Remember, you can only set properties on a v5 connection.
        let opts = ConnectOptionsBuilder::new_v5().properties(props).to_ffi();

        assert_eq!(1, props.len());
        assert_eq!(
            Some(60),
            props.get_uint(PropertyCode::SessionExpiryInterval)
        );

        /*
        assert!(!opts.copts.connectProperties.is_null());
        assert_eq!(
            &props.cprops as *const _ as *mut ffi::MQTTProperties,
            opts.copts.connectProperties
        );
        */
    }

    #[test]
    fn test_will_properties() {
        let props = Properties::with_property(Property::WillDelayInterval(60));

        let lwt = MessageBuilder::new()
            .topic("event/failure")
            .properties(props)
            .finalize();

        // Remember, you can only set properties on a v5 connection.
        let opts = ConnectOptionsBuilder::new_v5().will_message(lwt).to_ffi();

        let will_props = &opts.data.will_props;
        assert_eq!(1, will_props.len());
        assert_eq!(
            Some(60),
            will_props.get_int(PropertyCode::WillDelayInterval)
        );

        assert!(!opts.copts.willProperties.is_null());
        assert_eq!(
            &will_props.cprops as *const _ as *mut ffi::MQTTProperties,
            opts.copts.willProperties
        );
    }

    /*
        #[test]
        fn test_clone() {
            const TRUST_STORE: &str = "some_file.crt";
            // Make sure the original goes out of scope
            // before testing the clone.
            let opts = {
                let org_opts = SslOptionsBuilder::new()
                    .trust_store(TRUST_STORE)
                    .finalize();

                org_opts.clone()
            };

            assert_eq!(TRUST_STORE, opts.trust_store.to_str().unwrap());
            let ts = unsafe { CStr::from_ptr(opts.copts.trustStore) };
            assert_eq!(TRUST_STORE, ts.to_str().unwrap());
        }
    */

    /*
    // Determine that the options can be sent across threads.
    // As long as it compiles, this indicates that ConnectOptions implements
    // the Send trait.
    #[test]
    fn test_send() {
        let opts = ConnectOptions::new();

        // TODO: Fill in some values and check them.
        let thr = thread::spawn(move || {
            assert_eq!(STRUCT_ID, opts.copts.struct_id);
        });
        thr.join().unwrap();
    }
    */
}
