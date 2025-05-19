// ssl_options.rs
//
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

use std::{
    ffi::CString,
    fmt, io,
    os::raw::{c_char, c_int, c_uchar, c_uint},
    path::{Path, PathBuf},
    pin::Pin,
    ptr,
    sync::Arc,
};

use crate::{ffi, to_c_bool, AsyncClient, Error, Result, SslErrorCallback};

/// Convert a path name to a CString
pub fn path_to_cstring<P: AsRef<Path>>(path: P) -> Result<CString> {
    let s = CString::new(
        path.as_ref()
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Path string error"))?,
    )?;
    Ok(s)
}

/////////////////////////////////////////////////////////////////////////////

/// The SSL/TLS versions that can be requested.
#[repr(u32)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SslVersion {
    /// The default library SSL/TLS version
    #[default]
    Default = ffi::MQTT_SSL_VERSION_DEFAULT,
    /// TLS version 1.0
    Tls_1_0 = ffi::MQTT_SSL_VERSION_TLS_1_0,
    /// TLS version 1.1
    Tls_1_1 = ffi::MQTT_SSL_VERSION_TLS_1_1,
    /// TLS version 1.2
    Tls_1_2 = ffi::MQTT_SSL_VERSION_TLS_1_2,
}

/////////////////////////////////////////////////////////////////////////////
//                              SslOptions
/////////////////////////////////////////////////////////////////////////////

/// The options for SSL socket connections to the broker.
#[derive(Default, Clone)] //Debug, Default, Clone)]
pub struct SslOptions {
    /// File name of the trust store.
    pub trust_store: PathBuf,
    /// File name of the clien't public key store.
    pub key_store: PathBuf,
    /// File name for the private key, if not in key store.
    pub private_key: PathBuf,
    /// Password for the private key, if any.
    pub private_key_password: String,
    /// The list of cipher quites that the client presents to the server.
    pub enabled_cipher_suites: String,
    /// Whether to enable verification of the server certificate **/
    pub enable_server_cert_auth: bool,
    /// The SSL/TLS version to use.
    pub ssl_version: SslVersion,
    /// Whether to carry out post-connect checks, including that a certificate
    /// matches the given host name.
    pub verify: bool,
    /// The path to the CA certificates, if specified.
    pub ca_path: PathBuf,
    /// The SSL error callback.
    pub on_ssl_error: Option<Arc<SslErrorCallback>>,

    // TODO: PSK callback and context
    /// Don't load default SSL CA. Should be used together with PSK to make
    /// sure regular servers with certificate in place is not accepted.
    pub disable_default_trust_store: bool,
    /// The list of ALPN protocols available to be negotiated.
    pub protos: Vec<String>,
}

impl SslOptions {
    /// Creates a new, default, set of SSL/TLS options.
    pub fn new() -> Self {
        Self::default()
    }
}

// We manually define a Debug implementation to get around
// the callback function fields.
impl fmt::Debug for SslOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SslOptions")
            .field("trust_store", &self.trust_store)
            .field("key_store", &self.key_store)
            .field("private_key", &self.private_key)
            .field("private_key_password", &self.private_key_password)
            .field("enabled_cipher_suites", &self.enabled_cipher_suites)
            .field("enable_server_cert_auth", &self.enable_server_cert_auth)
            .field("ssl_version", &self.ssl_version)
            .field("verify", &self.verify)
            .field("ca_path", &self.ca_path)
            .field(
                "disable_default_trust_store",
                &self.disable_default_trust_store,
            )
            .field("protos", &self.protos)
            .finish()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder to create SSL Options.
#[derive(Default)]
pub struct SslOptionsBuilder(SslOptions);

impl SslOptionsBuilder {
    /// Creates a new builder with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the name of the file in PEM format containing the public
    /// digital certificates trusted by the client.
    pub fn trust_store<P>(&mut self, trust_store: P) -> &mut Self
    where
        P: AsRef<Path>,
    {
        self.0.trust_store = trust_store.as_ref().into();
        self
    }

    /// Set the name of the file in PEM format containing the public
    /// certificate chain of the client.
    ///
    /// It may also include the client's private key.
    pub fn key_store<P>(&mut self, key_store: P) -> &mut Self
    where
        P: AsRef<Path>,
    {
        self.0.key_store = key_store.as_ref().into();
        self
    }

    /// If not included in the Key Store, this setting points to the file
    /// in PEM format containing the client's private key.
    pub fn private_key<P>(&mut self, private_key: P) -> &mut Self
    where
        P: AsRef<Path>,
    {
        self.0.private_key = private_key.as_ref().into();
        self
    }

    /// The password to load the client's privateKey if it's encrypted.
    pub fn private_key_password<S>(&mut self, private_key_password: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.private_key_password = private_key_password.into();
        self
    }

    /// The list of cipher suites that the client will present to the server
    /// during the SSL handshake.
    ///
    /// For a full explanation of the cipher list format, please see the
    /// OpenSSL on-line documentation:
    /// <http://www.openssl.org/docs/apps/ciphers.html#CIPHER_LIST_FORMAT>
    ///
    /// If this setting is ommitted, its default value will be "ALL", that is,
    /// all the cipher suites -excluding those offering no encryption- will
    /// be considered.
    pub fn enabled_cipher_suites<S>(&mut self, enabled_cipher_suites: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.enabled_cipher_suites = enabled_cipher_suites.into();
        self
    }

    /// Enable or disable verification of the server certificate
    pub fn enable_server_cert_auth(&mut self, on: bool) -> &mut Self {
        self.0.enable_server_cert_auth = on;
        self
    }

    /// The SSL/TLS version to use.
    pub fn ssl_version(&mut self, ver: SslVersion) -> &mut Self {
        self.0.ssl_version = ver;
        self
    }

    /// Whether to carry out post-connect checks, including that a
    /// certificate matches the given host name.
    pub fn verify(&mut self, on: bool) -> &mut Self {
        self.0.verify = on;
        self
    }

    /// If set, this points to a directory containing CA certificates
    /// in PEM format.
    pub fn ca_path<P>(&mut self, ca_path: P) -> &mut Self
    where
        P: AsRef<Path>,
    {
        self.0.ca_path = ca_path.as_ref().into();
        self
    }

    /// Sets the SSL/TLS error callback.
    pub fn error_callback<F>(&mut self, cb: F) -> &mut Self
    where
        F: Fn(&AsyncClient, &str) + Send + 'static,
    {
        self.0.on_ssl_error = Some(Arc::new(cb));
        self
    }

    /// Don't load the default SSL CA.
    ///
    /// This should be used together with PSK to make sure
    /// regular servers with certificate in place is not accepted.
    pub fn disable_default_trust_store(&mut self, disable: bool) -> &mut Self {
        self.0.disable_default_trust_store = disable;
        self
    }

    /// If set, only these protocols are used during negotiation.
    pub fn alpn_protos(&mut self, protos: &[&str]) -> &mut Self {
        self.0.protos = protos.iter().map(|s| String::from(*s)).collect();
        self
    }

    /// Gets the FFI representation of the SSL/TLS options.
    /// This is a convenience function for the unit tests.
    #[cfg(test)]
    pub(crate) fn to_ffi(&self) -> Result<SslOptionsFfi> {
        SslOptionsFfi::try_from(&self.0)
    }

    /// Create the SSL options from the builder.
    pub fn finalize(&self) -> SslOptions {
        self.0.clone()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                          Crate FFI Options
/////////////////////////////////////////////////////////////////////////////

// Implementation note:
// The C library seems to require the SSL Options struct to provide valid
// strings for all the entries. Empty entries require a zero-length string
// and should not be left as NULL. Therefore, out ffi::MQTTAsync_SSLOptions
// struct needs to be fixed up to always point to the cached CString
// values.
// Caching the CStrings in the struct with the fixed-up pointers in the
// underlying C struct.
//

/// The options for SSL socket connections to the broker.
#[derive(Debug)]
pub(crate) struct SslOptionsFfi {
    /// The underlying C struct to pass to the library
    pub(crate) copts: ffi::MQTTAsync_SSLOptions,
    /// Cache of Rust values tied to the C struct
    #[allow(dead_code)]
    data: Pin<Box<SslOptionsFfiData>>,
}

#[derive(Debug, Default, Clone)]
struct SslOptionsFfiData {
    // File name of the trust store.
    trust_store: CString,
    // File name of the clien't public key store.
    key_store: CString,
    // File name for the private key, if not in key store.
    private_key: CString,
    // Password for the private key, if any.
    private_key_password: CString,
    // The list of cipher quites that the client presents to the server.
    enabled_cipher_suites: CString,
    // The path to the CA certificates, if specified.
    ca_path: CString,
    // The list of ALPN protocols available to be negotiated.
    protos: Vec<c_uchar>,
}

impl SslOptionsFfi {
    // The C library expects unset values in the SSL options struct to be
    // NULL rather than empty string.
    #[inline]
    fn c_str(s: &CString) -> *const c_char {
        match &s {
            s if !s.to_bytes().is_empty() => s.as_ptr(),
            _ => ptr::null(),
        }
    }

    // Updates the underlying C structure to match the cached data.
    fn from_data(mut copts: ffi::MQTTAsync_SSLOptions, data: SslOptionsFfiData) -> Self {
        let data = Box::pin(data);

        copts.trustStore = Self::c_str(&data.trust_store);
        copts.keyStore = Self::c_str(&data.key_store);
        copts.privateKey = Self::c_str(&data.private_key);
        copts.privateKeyPassword = Self::c_str(&data.private_key_password);
        copts.enabledCipherSuites = Self::c_str(&data.enabled_cipher_suites);
        copts.CApath = Self::c_str(&data.ca_path);

        copts.protos = match &data.protos {
            protos if !protos.is_empty() => protos.as_ptr(),
            _ => ptr::null(),
        };
        copts.protos_len = data.protos.len() as c_uint;

        Self { copts, data }
    }

    /// Gets the list of ALPN protocols available to be negotiated.
    /// This is in the wire format of the data.
    #[cfg(test)]
    pub fn alpn_proto_vec(&self) -> &[c_uchar] {
        &self.data.protos
    }
}

impl Default for SslOptionsFfi {
    fn default() -> Self {
        Self::from_data(
            ffi::MQTTAsync_SSLOptions::default(),
            SslOptionsFfiData::default(),
        )
    }
}

unsafe impl Send for SslOptionsFfi {}
unsafe impl Sync for SslOptionsFfi {}

impl TryFrom<&SslOptions> for SslOptionsFfi {
    type Error = Error;

    fn try_from(opts: &SslOptions) -> Result<Self> {
        let data = SslOptionsFfiData::try_from(opts)?;

        let copts = ffi::MQTTAsync_SSLOptions {
            sslVersion: opts.ssl_version as c_int,
            verify: to_c_bool(opts.verify),
            disableDefaultTrustStore: to_c_bool(opts.disable_default_trust_store),
            ..ffi::MQTTAsync_SSLOptions::default()
        };

        Ok(Self::from_data(copts, data))
    }
}

impl TryFrom<SslOptions> for SslOptionsFfi {
    type Error = Error;

    fn try_from(opts: SslOptions) -> Result<Self> {
        SslOptionsFfi::try_from(&opts)
    }
}

impl SslOptionsFfiData {
    // Converts the list of ALPN protocol strings into the wire format for
    // openssl.
    //
    // The wire format is a single array of non-empty byte string, with a
    // length byte prefix for each. It's described here:
    // https://www.openssl.org/docs/man1.1.0/man3/SSL_CTX_set_alpn_protos.html
    fn proto_vec<S: AsRef<str>>(protos: &[S]) -> Vec<c_uchar> {
        let protos: Vec<c_uchar> = protos
            .iter()
            .flat_map(|s| {
                let mut p: Vec<c_uchar> =
                    s.as_ref().bytes().map(|c| c as c_uchar).collect();
                p.insert(0, p.len() as c_uchar);
                p
            })
            .collect();
        protos
    }
}

impl TryFrom<&SslOptions> for SslOptionsFfiData {
    type Error = Error;

    fn try_from(opts: &SslOptions) -> Result<Self> {
        Ok(SslOptionsFfiData {
            trust_store: path_to_cstring(&opts.trust_store)?,
            key_store: path_to_cstring(&opts.key_store)?,
            private_key: path_to_cstring(&opts.private_key)?,
            private_key_password: CString::new(opts.private_key_password.clone())?,
            enabled_cipher_suites: CString::new(opts.enabled_cipher_suites.clone())?,
            ca_path: path_to_cstring(&opts.ca_path)?,
            protos: SslOptionsFfiData::proto_vec(opts.protos.as_slice()),
        })
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from_c_bool;
    use std::ffi::CStr;

    // Identifier for the C SSL options structure
    const STRUCT_ID: [c_char; 4] = [
        b'M' as c_char,
        b'Q' as c_char,
        b'T' as c_char,
        b'S' as c_char,
    ];

    // The currently supported SSL struct version
    const STRUCT_VERSION: i32 = ffi::SSL_OPTIONS_STRUCT_VERSION;

    #[test]
    fn test_new() {
        let opts = SslOptionsFfi::try_from(SslOptions::new()).unwrap();
        //let copts = ffi::MQTTAsync_SSLOptions::default();

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);
        assert!(opts.copts.trustStore.is_null());
        // TODO: Check the other strings
    }

    #[test]
    fn test_builder_dflt() {
        let opts = SslOptionsBuilder::new().to_ffi().unwrap();

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);
        assert_eq!(ptr::null(), opts.copts.trustStore);
        // TODO: Check the other strings
    }

    #[test]
    fn test_builder_trust_store() {
        const TRUST_STORE: &str = "some_file.crt";
        let opts = SslOptionsBuilder::new()
            .trust_store(TRUST_STORE)
            .to_ffi()
            .unwrap();

        assert_eq!(TRUST_STORE, opts.data.trust_store.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.trustStore) };
        assert_eq!(TRUST_STORE, s.to_str().unwrap());
    }

    #[test]
    fn test_builder_key_store() {
        const KEY_STORE: &str = "some_file.crt";
        let opts = SslOptionsBuilder::new()
            .key_store(KEY_STORE)
            .to_ffi()
            .unwrap();

        assert_eq!(KEY_STORE, opts.data.key_store.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.keyStore) };
        assert_eq!(KEY_STORE, s.to_str().unwrap());
    }

    #[test]
    fn test_verify() {
        let opts = SslOptionsBuilder::new().verify(true).to_ffi().unwrap();
        assert!(from_c_bool(opts.copts.verify));
    }

    #[test]
    fn test_disable_default_trust_store() {
        let opts = SslOptionsBuilder::new()
            .disable_default_trust_store(true)
            .to_ffi()
            .unwrap();
        assert!(from_c_bool(opts.copts.disableDefaultTrustStore));
    }

    #[test]
    fn test_protos() {
        let protos = &["spdy/1", "http/1.1"];

        let v: Vec<c_uchar> = vec![
            6, b's', b'p', b'd', b'y', b'/', b'1', 8, b'h', b't', b't', b'p', b'/', b'1',
            b'.', b'1',
        ];

        assert_eq!(v, SslOptionsFfiData::proto_vec(protos));

        let opts = SslOptionsBuilder::new()
            .alpn_protos(protos)
            .to_ffi()
            .unwrap();

        assert_eq!(v, opts.alpn_proto_vec());
    }

    // TODO: Test the other builder initializers
    /*
        #[test]
        fn test_move() {
            const TRUST_STORE: &str = "some_file.crt";
            let org_opts = SslOptionsBuilder::new()
                .trust_store(TRUST_STORE)
                .finalize();

            let opts = org_opts;

            assert_eq!(TRUST_STORE, opts.data.trust_store.to_str().unwrap());

            let s = unsafe { CStr::from_ptr(opts.copts.trustStore) };
            assert_eq!(TRUST_STORE, s.to_str().unwrap());
        }

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

            assert_eq!(TRUST_STORE, opts.data.trust_store.to_str().unwrap());

            let s = unsafe { CStr::from_ptr(opts.copts.trustStore) };
            assert_eq!(TRUST_STORE, s.to_str().unwrap());
        }
    */
}
