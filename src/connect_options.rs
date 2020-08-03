// connect_options.rs
//
// The set of options for connecting to an MQTT client.
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2020 Frank Pagliughi <fpagliughi@mindspring.com>
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Frank Pagliughi - initial implementation and documentation
 *******************************************************************************/

//! Connect options for the Paho MQTT Rust client library.
//! This contains the structures to define the options for connecting to the
//! MQTT broker/server.

use std::{ffi::CString, os::raw::c_int, pin::Pin, ptr, time::Duration};

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
};

/////////////////////////////////////////////////////////////////////////////
// Connections

/// The collection of options for connecting to a broker.
/// This can be constructed using a
/// [ConnectOptionsBuilder](struct.ConnectOptionsBuilder.html).
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
    password: Option<CString>,
    server_uris: StringCollection,
    props: Option<Properties>,
    will_props: Option<Properties>,
    http_headers: Option<NameValueCollection>,
}

impl ConnectOptions {
    /// Creates a new, default set of connect options.
    pub fn new() -> Self {
        ConnectOptions::default()
    }

    // Fixes up the underlying C struct to point to our cached values.
    // This should be called any time a cached object is modified.
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

        copts.password = match data.password {
            Some(ref password) => password.as_ptr(),
            _ => ptr::null(),
        };

        let n = data.server_uris.len();

        if n != 0 {
            copts.serverURIs = data.server_uris.as_c_arr_mut_ptr();
            copts.serverURIcount = n as c_int;
        } else {
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

        Self { copts, data }
    }

    /// Gets the "clean session" setting in the options.
    pub fn clean_session(&self) -> bool {
        from_c_bool(self.copts.cleansession)
    }

    /// This sets the "clean session" behavior for connecting to the server.
    /// When set to true, this directs the server to throw away any state
    /// related to the client, as determined by the client identifier.
    /// When set to false, the server keeps the state information and
    /// resumes the previous session.
    pub fn set_clean_session(&mut self, clean: bool) {
        self.copts.cleansession = to_c_bool(clean);
    }

    /// Sets the token to ber used for connect completion callbacks.
    /// Note that we leak the token to give to the C lib. When we're
    /// done with it, we must recover and drop it (i.e. in the completion
    /// callback).
    pub fn set_token(&mut self, tok: ConnectToken) {
        let tok: Token = tok.into();

        if self.copts.MQTTVersion < ffi::MQTTVERSION_5 as i32 {
            self.copts.onSuccess = Some(TokenInner::on_success);
            self.copts.onFailure = Some(TokenInner::on_failure);
        } else {
            self.copts.onSuccess5 = Some(TokenInner::on_success5);
            self.copts.onFailure5 = Some(TokenInner::on_failure5);
        }
        self.copts.context = tok.into_raw();
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
        Self::from_data(self.copts.clone(), (&*self.data).clone())
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
    /// Creates a new `ConnectOptionsBuilder`
    pub fn new() -> Self {
        Self::default()
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
    /// This is for MQTT v3.x connections, only.
    ///
    /// # Arguments
    ///
    /// `clean` Whether the broker should remove any previously-stored
    ///         information for this client.
    pub fn clean_session(&mut self, clean: bool) -> &mut Self {
        self.copts.cleansession = to_c_bool(clean);
        self
    }

    /// Sets the 'clean start' flag to send to the broker.
    ///
    /// This is for MQTT v5 connections only.
    ///
    /// # Arguments
    ///
    /// `clean` Whether the broker should remove any previously-stored
    ///         information for this client.
    pub fn clean_start(&mut self, clean: bool) -> &mut Self {
        self.copts.cleanstart = to_c_bool(clean);
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
    pub fn password<S>(&mut self, password: S) -> &mut Self
    where
        S: Into<String>,
    {
        let password = CString::new(password.into()).unwrap();
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

    /// Sets the version of MQTT to use on the connect.
    ///
    /// # Arguments
    ///
    /// `ver` The version of MQTT to use when connecting to the broker.
    ///       * (0) try the latest version (3.1.1) and work backwards
    ///       * (3) only try v3.1
    ///       * (4) only try v3.1.1
    ///       * (5) only try v5
    ///
    pub fn mqtt_version(&mut self, ver: u32) -> &mut Self {
        self.copts.MQTTVersion = ver as i32;

        if ver < ffi::MQTTVERSION_5 {
            self.copts.cleanstart = 0;
        } else {
            self.copts.cleansession = 0;
        }
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

    /// Finalize the builder to create the connect options.
    pub fn finalize(&self) -> ConnectOptions {
        ConnectOptions::from_data(self.copts.clone(), self.data.clone())
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{message::MessageBuilder, properties::*, ssl_options::SslOptionsBuilder, types::*};
    use std::{ffi::CStr, os::raw::c_char, thread};

    // Identifier fo a C connect options struct
    const STRUCT_ID: [c_char; 4] = [
        b'M' as c_char,
        b'Q' as c_char,
        b'T' as c_char,
        b'C' as c_char,
    ];

    #[test]
    fn test_new() {
        let opts = ConnectOptions::new();

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(7, opts.copts.struct_version);
        assert!(opts.copts.will.is_null());

        assert!(opts.copts.username.is_null());
        assert!(opts.copts.password.is_null());
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
    fn test_ssl() {
        const TRUST_STORE: &str = "some_file.crt";
        let ssl_opts = SslOptionsBuilder::new()
            .trust_store(TRUST_STORE)
            .unwrap()
            .finalize();

        let opts = ConnectOptionsBuilder::new()
            .ssl_options(ssl_opts)
            .finalize();

        assert!(!opts.copts.ssl.is_null());

        if let Some(ref ssl_opts) = opts.data.ssl {
            // TODO: Test that ssl_opts.get_trust_store() is TRUST_STORE?
            assert!(true);
            assert_eq!(&ssl_opts.copts as *const _, opts.copts.ssl);
            let ts = unsafe { CStr::from_ptr((*opts.copts.ssl).trustStore) };
            assert_eq!(TRUST_STORE, ts.to_str().unwrap());
        } else {
            // The SSL option should be set
            assert!(false);
        };
    }

    #[test]
    fn test_user_name() {
        const NAME: &str = "some-random-name";

        let opts = ConnectOptionsBuilder::new().user_name(NAME).finalize();

        assert!(!opts.copts.username.is_null());

        if let Some(ref user_name) = opts.data.user_name {
            assert_eq!(NAME, user_name.to_str().unwrap());

            let s = unsafe { CStr::from_ptr(opts.copts.username) };
            assert_eq!(NAME, s.to_str().unwrap());
        } else {
            assert!(false);
        };
    }

    #[test]
    fn test_password() {
        const PSWD: &str = "some-random-password";

        let opts = ConnectOptionsBuilder::new().password(PSWD).finalize();

        assert!(!opts.copts.password.is_null());

        if let Some(ref password) = opts.data.password {
            assert_eq!(PSWD, password.to_str().unwrap());

            let s = unsafe { CStr::from_ptr(opts.copts.password) };
            assert_eq!(PSWD, s.to_str().unwrap());
        } else {
            assert!(false);
        };
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
            let s = unsafe { CStr::from_ptr(*opts.copts.serverURIs.offset(i as isize)) };
            assert_eq!(&svr[..], s.to_str().unwrap());
        }
    }

    #[test]
    fn test_mqtt_version() {
        const VER: u32 = MQTT_VERSION_3_1_1;

        let opts = ConnectOptionsBuilder::new().mqtt_version(VER).finalize();
        assert_eq!(VER as i32, opts.copts.MQTTVersion);
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

        assert_eq!(KEEP_ALIVE_SECS as i32, opts.copts.keepAliveInterval);
        assert_eq!(0, opts.copts.cleansession);
        assert_eq!(MAX_INFLIGHT, opts.copts.maxInflight);

        assert_eq!(
            USER_NAME,
            opts.data.user_name.as_ref().unwrap().to_str().unwrap()
        );
        assert_eq!(
            PASSWORD,
            opts.data.password.as_ref().unwrap().to_str().unwrap()
        );

        if let Some(ref user_name) = opts.data.user_name {
            assert_eq!(user_name.as_ptr(), opts.copts.username)
        } else {
            assert!(false)
        };
        if let Some(ref password) = opts.data.password {
            assert_eq!(password.as_ptr(), opts.copts.password)
        } else {
            assert!(false)
        };

        assert_eq!(CONNECT_TIMEOUT_SECS as i32, opts.copts.connectTimeout);
    }

    #[test]
    fn test_connect_properties() {
        let mut props = Properties::new();
        props
            .push_int(PropertyCode::SessionExpiryInterval, 60)
            .unwrap();

        // Remember, you can only set properties on a v5 connection.
        let opts = ConnectOptionsBuilder::new()
            .properties(props) // Note: Order shouldn't matter when
            .mqtt_version(MQTT_VERSION_5) // building options
            .finalize();

        if let Some(ref props) = opts.data.props {
            assert_eq!(1, props.len());
            assert_eq!(Some(60), props.get_int(PropertyCode::SessionExpiryInterval));

            assert!(!opts.copts.connectProperties.is_null());
            assert_eq!(
                &props.cprops as *const _ as *mut ffi::MQTTProperties,
                opts.copts.connectProperties
            );
        } else {
            assert!(false)
        };
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
        let opts = ConnectOptionsBuilder::new()
            .mqtt_version(MQTT_VERSION_5)
            .will_message(lwt)
            .finalize();

        if let Some(ref will_props) = opts.data.will_props {
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
        } else {
            assert!(false)
        };
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
