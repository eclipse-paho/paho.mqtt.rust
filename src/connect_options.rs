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
    ffi, from_c_bool,
    message::Message,
    name_value::NameValueCollection,
    properties::Properties,
    ssl_options::SslOptions,
    string_collection::StringCollection,
    to_c_bool,
    token::{ConnectToken, Token, TokenInner},
    types::*,
    will_options::WillOptions,
    Error, Result,
};
use std::{
    ffi::CString,
    os::raw::{c_int, c_void},
    pin::Pin,
    ptr,
    time::Duration,
};

/////////////////////////////////////////////////////////////////////////////
// Connections

/// The collection of options for connecting to a broker.
///
/// This can be constructed using a [`ConnectOptionsBuilder`] to set all the
/// options.
#[derive(Debug)]
pub struct ConnectOptions {
    /// The underlying C options structure.
    /// The 'will', 'ssl', 'username', and 'password' fields should
    /// be NULL (not empty) if unused.
    pub(crate) copts: ffi::MQTTAsync_connectOptions,
    /// The pinned data cache
    data: Pin<Box<ConnectOptionsData>>,
}

/// Cached data for the connect options.
#[derive(Debug, Default, Clone)]
struct ConnectOptionsData {
    will: Option<WillOptions>,
    ssl: Option<SslOptions>,
    user_name: Option<CString>,
    password: Option<Vec<u8>>,
    server_uris: StringCollection,
    props: Option<Properties>,
    will_props: Option<Properties>,
    http_headers: Option<NameValueCollection>,
    http_proxy: Option<CString>,
    https_proxy: Option<CString>,
}

impl ConnectOptions {
    /// Creates a new, default set of connect options for MQTT v3.x
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new, default set of connect options for MQTT v5
    pub fn new_v5() -> Self {
        Self::from_data(
            ffi::MQTTAsync_connectOptions::new_v5(),
            ConnectOptionsData::default(),
        )
    }

    /// Creates a new, default set of connect options for connecting over
    /// websockets.
    pub fn new_ws() -> Self {
        Self::from_data(
            ffi::MQTTAsync_connectOptions::new_ws(),
            ConnectOptionsData::default(),
        )
    }

    /// Creates a new, default set of connect options for connecting MQTT v5
    /// over websockets.
    pub fn new_ws_v5() -> Self {
        Self::from_data(
            ffi::MQTTAsync_connectOptions::new_ws_v5(),
            ConnectOptionsData::default(),
        )
    }

    /// Creates a new set of connect options for the specified MQTT
    /// protocol version.
    ///
    /// Note that this will return the options with the requested version,
    /// even if it's not currently known. The server will likely reject a
    /// request with a bad protocol version, however.
    pub fn with_mqtt_version<V>(ver: V) -> Self
    where
        V: Into<MqttVersion>,
    {
        let ver = ver.into();
        let mut opts = if ver < MqttVersion::V5 {
            Self::new()
        }
        else {
            Self::new_v5()
        };
        opts.copts.MQTTVersion = ver as i32;
        opts
    }

    // Creates a set of options from a C struct and cached values.
    // Fixes up the underlying C struct to point to the cached values,
    // then returns a new options object with them combined.
    fn from_data(mut copts: ffi::MQTTAsync_connectOptions, data: ConnectOptionsData) -> Self {
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

        let n = data.server_uris.len();

        if n != 0 {
            copts.serverURIs = data.server_uris.as_c_arr_mut_ptr();
            copts.serverURIcount = n as c_int;
        }
        else {
            copts.serverURIs = ptr::null();
            copts.serverURIcount = 0;
        }

        copts.connectProperties = ptr::null_mut();
        if copts.MQTTVersion >= MQTT_VERSION_5 as i32 {
            if let Some(ref mut props) = data.props {
                copts.connectProperties = &mut props.cprops
            }
        }

        copts.willProperties = ptr::null_mut();
        if copts.MQTTVersion >= MQTT_VERSION_5 as i32 {
            if let Some(ref mut will_props) = data.will_props {
                copts.willProperties = &mut will_props.cprops
            }
        }

        copts.httpHeaders = match data.http_headers {
            Some(ref mut http_headers) => http_headers.as_c_arr_ptr(),
            _ => ptr::null(),
        };

        copts.httpHeaders = match data.http_headers {
            Some(ref mut http_headers) => http_headers.as_c_arr_ptr(),
            _ => ptr::null(),
        };

        copts.httpProxy = match data.http_proxy {
            Some(ref proxy) => proxy.as_ptr(),
            _ => ptr::null(),
        };

        copts.httpsProxy = match data.https_proxy {
            Some(ref proxy) => proxy.as_ptr(),
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

    /// Does a check of the options to determine if their valid.
    pub fn check(&self) -> Result<()> {
        if !self.data.server_uris.is_empty() {
            let uris: Vec<String> = (&self.data.server_uris).try_into()?;
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
        MqttVersion::from(self.copts.MQTTVersion)
    }

    /// Gets the MQTT protocol version as a raw integer.
    pub fn mqtt_version_raw(&self) -> u32 {
        self.copts.MQTTVersion as u32
    }

    /// Gets the "clean session" setting in the options.
    ///
    /// This is only used in MQTT v3 connections.
    pub fn clean_session(&self) -> bool {
        from_c_bool(self.copts.cleansession)
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
        self.copts.cleansession = to_c_bool(clean);
        if clean {
            self.copts.cleanstart = 0;
        }
    }

    /// Gets the "clean start" setting in the options.
    ///
    /// This is only used in MQTT v5 connections.
    pub fn clean_start(&self) -> bool {
        from_c_bool(self.copts.cleanstart)
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
        self.copts.cleanstart = to_c_bool(clean);
        if clean {
            self.copts.cleansession = 0;
        }
    }

    /// Whether the options have SSL/TLS options for a secure connection
    pub fn has_ssl_options(&self) -> bool {
        self.data.ssl.is_some()
    }

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
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self::from_data(
            ffi::MQTTAsync_connectOptions::default(),
            ConnectOptionsData::default(),
        )
    }
}

impl Clone for ConnectOptions {
    fn clone(&self) -> Self {
        Self::from_data(self.copts, (*self.data).clone())
    }
}

unsafe impl Send for ConnectOptions {}
unsafe impl Sync for ConnectOptions {}

/////////////////////////////////////////////////////////////////////////////
//                              Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder to create the options to connect to the MQTT server.
#[derive(Default)]
pub struct ConnectOptionsBuilder {
    copts: ffi::MQTTAsync_connectOptions,
    data: ConnectOptionsData,
}

impl ConnectOptionsBuilder {
    /// Creates a new `ConnectOptionsBuilder` for MQTT v3.x
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `ConnectOptionsBuilder` explicitly for MQTT v3.x
    /// (although this is currently the default).
    pub fn new_v3() -> Self {
        Self::default()
    }

    /// Creates a new `ConnectOptionsBuilder` for MQTT v5
    pub fn new_v5() -> Self {
        Self {
            copts: ffi::MQTTAsync_connectOptions::new_v5(),
            ..Self::default()
        }
    }

    /// Creates a new `ConnectOptionsBuilder` for connecting to MQTT v3.x
    /// over websockets.
    pub fn new_ws() -> Self {
        Self {
            copts: ffi::MQTTAsync_connectOptions::new_ws(),
            ..Self::default()
        }
    }

    /// Creates a new `ConnectOptionsBuilder` for connecting to MQTT v5
    /// over websockets.
    pub fn new_ws_v5() -> Self {
        Self {
            copts: ffi::MQTTAsync_connectOptions::new_ws_v5(),
            ..Self::default()
        }
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
        let ver = ver.into();
        let mut copts = if ver < MqttVersion::V5 {
            ffi::MQTTAsync_connectOptions::new_v3()
        }
        else {
            ffi::MQTTAsync_connectOptions::new_v5()
        };
        copts.MQTTVersion = ver as i32;

        Self {
            copts,
            ..Self::default()
        }
    }

    /// Sets the keep alive interval for the client session.
    ///
    /// # Arguments
    ///
    /// `keep_alive_interval` The maximum time that should pass without
    ///                       communication between the client and server.
    ///                       This has a resolution in seconds.
    pub fn keep_alive_interval(&mut self, keep_alive_interval: Duration) -> &mut Self {
        let secs = keep_alive_interval.as_secs();
        self.copts.keepAliveInterval = if secs == 0 { 1 } else { secs as i32 };
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
        self.copts.cleansession = to_c_bool(clean);

        // Force the options to those compatible with v3 if set
        self.copts.cleanstart = 0;
        if self.copts.MQTTVersion >= ffi::MQTTVERSION_5 as i32 {
            self.copts.MQTTVersion = 0;
        }
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
        self.copts.cleanstart = to_c_bool(clean);

        // Force the options to those compatible with v5 if set
        self.copts.cleansession = 0;
        if self.copts.MQTTVersion < 5 {
            self.copts.MQTTVersion = 5;
        }
        self
    }

    /// Sets the maximum number of in-flight messages that can be
    /// simultaneously handled by this client.
    ///
    /// # Arguments
    ///
    /// `max_inflight` The maximum number of messages that can be in-flight
    ///                at any given time with this client.
    pub fn max_inflight(&mut self, max_inflight: i32) -> &mut Self {
        self.copts.maxInflight = max_inflight;
        self
    }

    /// Sets the LWT options for the connection.
    ///
    /// # Arguments
    ///
    /// `will` The LWT options for the connection.
    #[deprecated(note = "Pass in a message with `will_message` instead")]
    pub fn will_options(&mut self, will: WillOptions) -> &mut Self {
        self.data.will = Some(will);
        self
    }

    /// Sets the LWT message for the connection.
    ///
    /// # Arguments
    ///
    /// `will` The LWT options for the connection.
    pub fn will_message(&mut self, will: Message) -> &mut Self {
        self.data.will_props = Some(will.properties().clone());
        self.data.will = Some(WillOptions::from(will));
        self
    }

    /// Sets the SSL options for the connection.
    ///
    /// # Arguments
    ///
    /// `ssl` The SSL options for the connection.
    pub fn ssl_options(&mut self, ssl: SslOptions) -> &mut Self {
        self.data.ssl = Some(ssl);
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
        let user_name = CString::new(user_name.into()).unwrap();
        self.data.user_name = Some(user_name);
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
        let password = password.into();
        self.data.password = Some(password);
        self
    }

    /// Sets the time interval to allow the connect to complete.
    ///
    /// # Arguments
    ///
    /// `timeout` The time interval to allow the connect to
    ///           complete. This has a resolution of seconds.
    ///
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        let secs = timeout.as_secs();
        self.copts.connectTimeout = if secs == 0 { 1 } else { secs as i32 };
        self
    }

    /// Sets the retry interval.
    ///
    /// # Arguments
    ///
    /// `interval` The retry interval. This has a resolution of seconds.
    pub fn retry_interval(&mut self, interval: Duration) -> &mut Self {
        let secs = interval.as_secs();
        self.copts.retryInterval = if secs == 0 { 1 } else { secs as i32 };
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
        self.data.server_uris = StringCollection::new(server_uris);
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
        self.copts.automaticReconnect = 1; // true

        let mut secs = min_retry_interval.as_secs();
        self.copts.minRetryInterval = if secs == 0 { 1 } else { secs as i32 };

        secs = max_retry_interval.as_secs();
        self.copts.maxRetryInterval = if secs == 0 { 1 } else { secs as i32 };
        self
    }

    /// Sets the collection of properties for the connections.
    ///
    /// # Arguments
    ///
    /// `props` The collection of properties to include with the connect message.
    pub fn properties(&mut self, props: Properties) -> &mut Self {
        self.data.props = Some(props);

        if self.copts.MQTTVersion < 5 {
            self.copts.MQTTVersion = 5;
        }
        self
    }

    /// Sets the additional HTTP headers that will be sent in the
    /// WebSocket opening handshake.
    pub fn http_headers<N, V>(&mut self, coll: &[(N, V)]) -> &mut Self
    where
        N: AsRef<str>,
        V: AsRef<str>,
    {
        let coll = NameValueCollection::new(coll);
        self.data.http_headers = Some(coll);
        self
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
        let proxy = CString::new(proxy.into()).unwrap();
        self.data.http_proxy = Some(proxy);
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
        let proxy = CString::new(proxy.into()).unwrap();
        self.data.https_proxy = Some(proxy);
        self
    }

    /// Finalize the builder to create the connect options.
    pub fn finalize(&self) -> ConnectOptions {
        ConnectOptions::from_data(self.copts, self.data.clone())
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{message::MessageBuilder, properties::*, ssl_options::SslOptionsBuilder};
    use std::{ffi::CStr, os::raw::c_char, thread};

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
        let opts = ConnectOptions::new();

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
        let ssl_opts = SslOptionsBuilder::new()
            .trust_store(TRUST_STORE)
            .unwrap()
            .finalize();

        let opts = ConnectOptionsBuilder::new()
            .ssl_options(ssl_opts)
            .finalize();

        assert!(!opts.copts.ssl.is_null());

        let ssl_opts = opts.data.ssl.as_ref().unwrap();
        // TODO: Test that ssl_opts.get_trust_store() is TRUST_STORE?
        assert_eq!(&ssl_opts.copts as *const _, opts.copts.ssl);
        let ts = unsafe { CStr::from_ptr((*opts.copts.ssl).trustStore) };
        assert_eq!(TRUST_STORE, ts.to_str().unwrap());
    }

    #[test]
    fn test_user_name() {
        let opts = ConnectOptionsBuilder::new().user_name(NAME).finalize();

        assert!(!opts.copts.username.is_null());

        let user_name = opts.data.user_name.as_deref().unwrap();
        assert_eq!(NAME, user_name.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.username) };
        assert_eq!(NAME, s.to_str().unwrap());
    }

    #[test]
    fn test_password() {
        let opts = ConnectOptionsBuilder::new().password(PSWD).finalize();

        assert!(!opts.copts.binarypwd.data.is_null());
        assert_eq!(opts.copts.binarypwd.len, PSWD.len() as c_int);

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

        let opts = ConnectOptionsBuilder::new()
            .server_uris(&servers)
            .finalize();

        assert_eq!(servers.len() as i32, opts.copts.serverURIcount);

        // Compare the strings to the C-arrays in copts
        for (i, ref svr) in servers.iter().enumerate() {
            let s = unsafe { CStr::from_ptr(*opts.copts.serverURIs.add(i)) };
            assert_eq!(&svr[..], s.to_str().unwrap());
        }
    }

    #[test]
    fn test_mqtt_version() {
        let opts = ConnectOptionsBuilder::new().finalize();
        assert_eq!(MQTT_VERSION_DEFAULT as c_int, opts.copts.MQTTVersion);

        let opts = ConnectOptionsBuilder::new_v3().finalize();
        assert_eq!(MQTT_VERSION_DEFAULT as c_int, opts.copts.MQTTVersion);

        let opts = ConnectOptionsBuilder::new_v5().finalize();
        assert_eq!(MQTT_VERSION_5 as c_int, opts.copts.MQTTVersion);
    }

    // Test that we can set each of the HTTP and HTTPS proxy values, but also
    // make sure we don't mist them up with cut-and-paste errors.
    #[test]
    fn test_proxies() {
        const HTTP: &str = "http://some_server:80";
        const HTTPS: &str = "https://some_other_server:443";

        let opts = ConnectOptionsBuilder::new().http_proxy(HTTP).finalize();

        assert!(!opts.copts.httpProxy.is_null());
        assert!(opts.copts.httpsProxy.is_null());

        assert!(opts.data.https_proxy.is_none());

        let proxy = opts.data.http_proxy.as_deref().unwrap();
        assert_eq!(HTTP, proxy.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.httpProxy) };
        assert_eq!(HTTP, s.to_str().unwrap());

        let opts = ConnectOptionsBuilder::new().https_proxy(HTTPS).finalize();

        assert!(!opts.copts.httpsProxy.is_null());
        assert!(opts.copts.httpProxy.is_null());

        assert!(opts.data.http_proxy.is_none());

        let proxy = opts.data.https_proxy.as_deref().unwrap();
        assert_eq!(HTTPS, proxy.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.httpsProxy) };
        assert_eq!(HTTPS, s.to_str().unwrap());
    }

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

    #[test]
    fn test_connect_properties() {
        let mut props = Properties::new();
        props
            .push_int(PropertyCode::SessionExpiryInterval, 60)
            .unwrap();

        // Remember, you can only set properties on a v5 connection.
        let opts = ConnectOptionsBuilder::new_v5().properties(props).finalize();

        let props = opts.data.props.as_ref().unwrap();
        assert_eq!(1, props.len());
        assert_eq!(Some(60), props.get_int(PropertyCode::SessionExpiryInterval));

        assert!(!opts.copts.connectProperties.is_null());
        assert_eq!(
            &props.cprops as *const _ as *mut ffi::MQTTProperties,
            opts.copts.connectProperties
        );
    }

    #[test]
    fn test_will_properties() {
        let mut props = Properties::new();
        props.push_int(PropertyCode::WillDelayInterval, 60).unwrap();

        let lwt = MessageBuilder::new()
            .topic("event/failure")
            .properties(props)
            .finalize();

        // Remember, you can only set properties on a v5 connection.
        let opts = ConnectOptionsBuilder::new_v5().will_message(lwt).finalize();

        let will_props = opts.data.will_props.as_ref().unwrap();
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
        let _ = thr.join().unwrap();
    }
}
