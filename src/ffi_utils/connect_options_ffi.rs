// ffi_utils/connect_options_ffi.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// FFI representation of the connect options for the Paho C library.
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

//! FFI representation of the connect options for the Paho C library.

use std::{
    ffi::{c_int, c_void, CString},
    pin::Pin,
    ptr,
};

use crate::{
    ffi,
    ffi_utils::{NameValueCollectionFfi, SslOptionsFfi, StringCollectionFfi},
    to_c_bool,
    token::TokenInner,
    types::*,
    ffi_utils::WillOptionsFfi,
    ConnectOptions, ConnectToken, PropertiesFfi, Token,
};

/// The collection of options for connecting to a broker through the Paho C
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
    pub(crate) data: Pin<Box<ConnectOptionsFfiData>>,
}

/// Cached data for the connect options.
#[derive(Debug, Default)]
pub(crate) struct ConnectOptionsFfiData {
    pub(crate) will: Option<WillOptionsFfi>,
    pub(crate) ssl: Option<SslOptionsFfi>,
    pub(crate) user_name: Option<CString>,
    pub(crate) password: Option<Vec<u8>>,
    pub(crate) server_uris: StringCollectionFfi,
    pub(crate) props: PropertiesFfi,
    pub(crate) will_props: PropertiesFfi,
    pub(crate) http_headers: NameValueCollectionFfi,
    pub(crate) http_proxy: CString,
    pub(crate) https_proxy: CString,
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

        if let Some(retry) = opts.retry_interval {
            secs = retry.as_secs();
            copts.retryInterval = if secs == 0 { 1 } else { secs as i32 };
        }

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

impl From<&ConnectOptions> for ConnectOptionsFfiData {
    fn from(opts: &ConnectOptions) -> Self {
        let mut data = ConnectOptionsFfiData::default();

        if let Some(msg) = &opts.will {
            data.will = Some(WillOptionsFfi::from(msg.clone()));
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
            data.server_uris = StringCollectionFfi::from(opts.server_uris.as_slice());
        }
        data.props = PropertiesFfi::from(&opts.props);

        if !opts.http_headers.is_empty() {
            data.http_headers = NameValueCollectionFfi::from(opts.http_headers.clone());
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
    use crate::{
        connect_options::ConnectOptionsBuilder, message::MessageBuilder, properties::*,
        ssl_options::SslOptionsBuilder,
    };
    use std::ffi::CStr;

    const NAME: &str = "some-random-name";
    const TRUST_STORE: &str = "some_file.crt";

    #[test]
    fn test_new_ffi() {
        let opts = ConnectOptionsFfi::from(&ConnectOptions::new());
        let default_copts = ffi::MQTTAsync_connectOptions::default();

        assert_eq!(default_copts.struct_id, opts.copts.struct_id);
        assert_eq!(default_copts.struct_version, opts.copts.struct_version);
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
    fn test_ssl() {
        let ssl_opts = SslOptionsBuilder::new().trust_store(TRUST_STORE).finalize();

        let opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new()
                .ssl_options(ssl_opts)
                .finalize(),
        );

        assert!(!opts.copts.ssl.is_null());

        let ssl_opts = opts.data.ssl.as_ref().unwrap();
        assert_eq!(&ssl_opts.copts as *const _, opts.copts.ssl);
        let ts = unsafe { CStr::from_ptr((*opts.copts.ssl).trustStore) };
        assert_eq!(TRUST_STORE, ts.to_str().unwrap());
    }

    #[test]
    fn test_user_name() {
        let opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new().user_name(NAME).finalize(),
        );
        assert!(!opts.copts.username.is_null());

        let user_name = opts.data.user_name.as_deref().unwrap();
        assert_eq!(NAME, user_name.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.username) };
        assert_eq!(NAME, s.to_str().unwrap());
    }

    #[test]
    fn test_password() {
        const PSWD: &str = "some-random-password";

        let opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new().password(PSWD).finalize(),
        );
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
        const BIN_PSWD: &[u8] = b"some-random-binary-password";

        let opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new().password(BIN_PSWD).finalize(),
        );

        assert!(!opts.copts.binarypwd.data.is_null());
        assert_eq!(opts.copts.binarypwd.len, BIN_PSWD.len() as c_int);

        let s = unsafe {
            std::slice::from_raw_parts(
                opts.copts.binarypwd.data as *const u8,
                opts.copts.binarypwd.len as usize,
            )
        };
        assert_eq!(BIN_PSWD, s);
    }

    #[test]
    fn test_server_uris() {
        let servers = ["tcp://server1:1883", "ssl://server2:1885"];

        let opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new()
                .server_uris(&servers)
                .finalize(),
        );

        assert_eq!(servers.len() as i32, opts.copts.serverURIcount);

        for (i, svr) in servers.iter().enumerate() {
            let s = unsafe { CStr::from_ptr(*opts.copts.serverURIs.add(i)) };
            assert_eq!(&svr[..], s.to_str().unwrap());
        }
    }

    #[test]
    fn test_mqtt_version() {
        let opts = ConnectOptionsFfi::from(&ConnectOptionsBuilder::new().finalize());
        assert_eq!(MQTT_VERSION_DEFAULT as c_int, opts.copts.MQTTVersion);

        let opts = ConnectOptionsFfi::from(&ConnectOptionsBuilder::new_v3().finalize());
        assert_eq!(MQTT_VERSION_DEFAULT as c_int, opts.copts.MQTTVersion);

        let opts = ConnectOptionsFfi::from(&ConnectOptionsBuilder::new_v5().finalize());
        assert_eq!(MQTT_VERSION_5 as c_int, opts.copts.MQTTVersion);
    }

    #[test]
    fn test_proxies() {
        const HTTP: &str = "http://some_server:80";
        const HTTPS: &str = "https://some_other_server:443";

        let opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new().http_proxy(HTTP).finalize(),
        );
        assert!(!opts.copts.httpProxy.is_null());
        assert!(opts.copts.httpsProxy.is_null());

        assert!(opts.data.https_proxy.is_empty());
        assert_eq!(HTTP, opts.data.http_proxy.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.httpProxy) };
        assert_eq!(HTTP, s.to_str().unwrap());

        let opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new().https_proxy(HTTPS).finalize(),
        );

        assert!(!opts.copts.httpsProxy.is_null());
        assert!(opts.copts.httpProxy.is_null());

        assert!(opts.data.http_proxy.is_empty());
        assert_eq!(HTTPS, opts.data.https_proxy.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.httpsProxy) };
        assert_eq!(HTTPS, s.to_str().unwrap());
    }

    #[test]
    fn test_connect_properties() {
        let props = Properties::with_property(Property::SessionExpiryInterval(60));

        let _opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new_v5()
                .properties(props.clone())
                .finalize(),
        );

        assert_eq!(1, props.len());
        assert_eq!(
            Some(60),
            props.get_uint(PropertyCode::SessionExpiryInterval)
        );
    }

    #[test]
    fn test_will_properties() {
        let props = Properties::with_property(Property::WillDelayInterval(60));

        let lwt = MessageBuilder::new()
            .topic("event/failure")
            .properties(props)
            .finalize();

        let opts = ConnectOptionsFfi::from(
            &ConnectOptionsBuilder::new_v5().will_message(lwt).finalize(),
        );

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
}
