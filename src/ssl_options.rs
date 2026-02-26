// ssl_options.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2022 Frank Pagliughi <fpagliughi@mindspring.com>
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
    io,
    os::raw::{c_char, c_uchar, c_uint},
    path::{Path, PathBuf},
    pin::Pin,
    ptr,
};

use crate::{errors::Result, ffi, from_c_bool, to_c_bool};

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
pub struct SslOptions {
    /// The underlying C struct to pass to the library
    pub(crate) copts: ffi::MQTTAsync_SSLOptions,
    /// Cache of Rust values tied to the C struct
    data: Pin<Box<SslOptionsData>>,
}

#[derive(Debug, Default, Clone)]
struct SslOptionsData {
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

/// The SSL/TLS versions that can be requested.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslVersion {
    /// The default library SSL/TLS version
    Default = ffi::MQTT_SSL_VERSION_DEFAULT,
    /// TLS version 1.0
    Tls_1_0 = ffi::MQTT_SSL_VERSION_TLS_1_0,
    /// TLS version 1.1
    Tls_1_1 = ffi::MQTT_SSL_VERSION_TLS_1_1,
    /// TLS version 1.2
    Tls_1_2 = ffi::MQTT_SSL_VERSION_TLS_1_2,
}

impl SslOptions {
    /// Creates a new set of default SSL options
    pub fn new() -> Self {
        Self::default()
    }

    // The C library expects unset values in the SSL options struct to be
    // NULL rather than empty string.
    fn c_str(str: &CString) -> *const c_char {
        if str.to_bytes().is_empty() {
            ptr::null()
        }
        else {
            str.as_ptr()
        }
    }

    // Converts the list of ALPN protocol strings into the wire format for
    // openssl.
    //
    // The wire format is a single array of non-empty byte string, with a
    // length byte prefix for each. It's described here:
    // https://www.openssl.org/docs/man1.1.0/man3/SSL_CTX_set_alpn_protos.html
    fn proto_vec(protos: &[&str]) -> Vec<c_uchar> {
        let protos: Vec<c_uchar> = protos
            .iter()
            .flat_map(|p| {
                let mut p: Vec<c_uchar> = p.bytes().map(|c| c as c_uchar).collect();
                p.insert(0, p.len() as c_uchar);
                p
            })
            .collect();
        protos
    }

    // Updates the underlying C structure to match the cached data.
    fn from_data(mut copts: ffi::MQTTAsync_SSLOptions, data: SslOptionsData) -> Self {
        let data = Box::pin(data);

        copts.trustStore = Self::c_str(&data.trust_store);
        copts.keyStore = Self::c_str(&data.key_store);
        copts.privateKey = Self::c_str(&data.private_key);
        copts.privateKeyPassword = Self::c_str(&data.private_key_password);
        copts.enabledCipherSuites = Self::c_str(&data.enabled_cipher_suites);
        copts.CApath = Self::c_str(&data.ca_path);

        copts.protos = if data.protos.is_empty() {
            ptr::null()
        }
        else {
            data.protos.as_ptr()
        };
        copts.protos_len = data.protos.len() as c_uint;

        Self { copts, data }
    }

    /// Set the name of the PEM file containing the public
    /// digital certificates trusted by the client.
    pub fn trust_store(&self) -> PathBuf {
        PathBuf::from(&self.data.trust_store.to_str().unwrap())
    }

    /// Get the name of the PEM file containing the public
    /// certificate chain of the client.
    pub fn key_store(&self) -> PathBuf {
        PathBuf::from(&self.data.key_store.to_str().unwrap())
    }

    /// The PEM file containing the client's private key, if not included
    /// in the Key Store.
    pub fn private_key(&self) -> PathBuf {
        PathBuf::from(&self.data.private_key.to_str().unwrap())
    }

    /// Gets the list of cipher suites that the client will present to
    /// the server during the SSL handshake.
    pub fn enabled_cipher_suites(&self) -> String {
        self.data
            .enabled_cipher_suites
            .to_str()
            .unwrap()
            .to_string()
    }

    /// Determine if the client will verify the server certificate.
    pub fn enable_server_cert_auth(&self) -> bool {
        from_c_bool(self.copts.enableServerCertAuth)
    }

    /// Gets the directory containing CA certificates in PEM format,
    /// if set.
    pub fn ca_path(&self) -> PathBuf {
        PathBuf::from(&self.data.ca_path.to_str().unwrap())
    }

    /// Determines if the default trust store should not be loaded.
    pub fn is_default_trust_store_disabled(&self) -> bool {
        self.copts.disableDefaultTrustStore != 0
    }

    /// Gets the list of ALPN protocols available to be negotiated.
    /// This is in the wire format of the data.
    pub fn alpn_proto_vec(&self) -> &[c_uchar] {
        &self.data.protos
    }
}

impl Default for SslOptions {
    fn default() -> Self {
        Self::from_data(
            ffi::MQTTAsync_SSLOptions::default(),
            SslOptionsData::default(),
        )
    }
}

impl Clone for SslOptions {
    fn clone(&self) -> Self {
        Self::from_data(self.copts, (*self.data).clone())
    }
}

unsafe impl Send for SslOptions {}
unsafe impl Sync for SslOptions {}

/////////////////////////////////////////////////////////////////////////////
//                              Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder to create SSL Options.
#[derive(Default)]
pub struct SslOptionsBuilder {
    copts: ffi::MQTTAsync_SSLOptions,
    data: SslOptionsData,
}

impl SslOptionsBuilder {
    /// Creates a new builder with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the name of the file in PEM format containing the public
    /// digital certificates trusted by the client.
    pub fn trust_store<P>(&mut self, trust_store: P) -> Result<&mut Self>
    where
        P: AsRef<Path>,
    {
        self.data.trust_store = CString::new(
            trust_store
                .as_ref()
                .to_str()
                .ok_or_else(|| io::Error::other("Path string error"))?,
        )?;
        Ok(self)
    }

    /// Set the name of the file in PEM format containing the public
    /// certificate chain of the client.
    ///
    /// It may also include the client's private key.
    pub fn key_store<P>(&mut self, key_store: P) -> Result<&mut Self>
    where
        P: AsRef<Path>,
    {
        self.data.key_store = CString::new(
            key_store
                .as_ref()
                .to_str()
                .ok_or_else(|| io::Error::other("Path string error"))?,
        )?;
        Ok(self)
    }

    /// If not included in the Key Store, this setting points to the file
    /// in PEM format containing the client's private key.
    pub fn private_key<P>(&mut self, private_key: P) -> Result<&mut Self>
    where
        P: AsRef<Path>,
    {
        self.data.private_key = CString::new(
            private_key
                .as_ref()
                .to_str()
                .ok_or_else(|| io::Error::other("Path string error"))?,
        )?;
        Ok(self)
    }

    /// The password to load the client's privateKey if it's encrypted.
    pub fn private_key_password<S>(&mut self, private_key_password: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.data.private_key_password = CString::new(private_key_password.into()).unwrap();
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
        self.data.enabled_cipher_suites = CString::new(enabled_cipher_suites.into()).unwrap();
        self
    }

    /// Enable or disable verification of the server certificate
    pub fn enable_server_cert_auth(&mut self, on: bool) -> &mut Self {
        self.copts.enableServerCertAuth = to_c_bool(on);
        self
    }

    /// The SSL/TLS version to use.
    pub fn ssl_version(&mut self, ver: SslVersion) -> &mut Self {
        self.copts.sslVersion = ver as i32;
        self
    }

    /// Whether to carry out post-connect checks, including that a
    /// certificate matches the given host name.
    pub fn verify(&mut self, on: bool) -> &mut Self {
        self.copts.verify = to_c_bool(on);
        self
    }

    /// If set, this points to a directory containing CA certificates
    /// in PEM format.
    pub fn ca_path<P>(&mut self, ca_path: P) -> Result<&mut Self>
    where
        P: AsRef<Path>,
    {
        self.data.ca_path = CString::new(
            ca_path
                .as_ref()
                .to_str()
                .ok_or_else(|| io::Error::other("Path string error"))?,
        )?;
        Ok(self)
    }

    /// Don't load the default SSL CA.
    ///
    /// This should be used together with PSK to make sure
    /// regular servers with certificate in place is not accepted.
    pub fn disable_default_trust_store(&mut self, disable: bool) -> &mut Self {
        self.copts.disableDefaultTrustStore = to_c_bool(disable);
        self
    }

    /// If set, only these protocols are used during negotiation.
    pub fn alpn_protos(&mut self, protos: &[&str]) -> &mut Self {
        self.data.protos = SslOptions::proto_vec(protos);
        self
    }

    /// Create the SSL options from the builder.
    pub fn finalize(&self) -> SslOptions {
        SslOptions::from_data(self.copts, self.data.clone())
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
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
        let opts = SslOptions::new();
        //let copts = ffi::MQTTAsync_SSLOptions::default();

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);
        assert!(opts.copts.trustStore.is_null());
        // TODO: Check the other strings
    }

    #[test]
    fn test_builder_dflt() {
        let opts = SslOptionsBuilder::new().finalize();

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
            .unwrap()
            .finalize();

        assert_eq!(TRUST_STORE, opts.data.trust_store.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.trustStore) };
        assert_eq!(TRUST_STORE, s.to_str().unwrap());
    }

    #[test]
    fn test_builder_key_store() {
        const KEY_STORE: &str = "some_file.crt";
        let opts = SslOptionsBuilder::new()
            .key_store(KEY_STORE)
            .unwrap()
            .finalize();

        assert_eq!(KEY_STORE, opts.data.key_store.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.keyStore) };
        assert_eq!(KEY_STORE, s.to_str().unwrap());
    }

    #[test]
    fn test_verify() {
        let opts = SslOptionsBuilder::new().verify(true).finalize();
        assert!(from_c_bool(opts.copts.verify));
    }

    #[test]
    fn test_disable_default_trust_store() {
        let opts = SslOptionsBuilder::new()
            .disable_default_trust_store(true)
            .finalize();
        assert!(from_c_bool(opts.copts.disableDefaultTrustStore));
    }

    #[test]
    fn test_protos() {
        let protos = &["spdy/1", "http/1.1"];

        let v: Vec<c_uchar> = vec![
            6, b's', b'p', b'd', b'y', b'/', b'1', 8, b'h', b't', b't', b'p', b'/', b'1', b'.',
            b'1',
        ];

        assert_eq!(v, SslOptions::proto_vec(protos));

        let opts = SslOptionsBuilder::new().alpn_protos(protos).finalize();

        assert_eq!(v, opts.alpn_proto_vec());
    }

    // TODO: Test the other builder initializers

    #[test]
    fn test_move() {
        const TRUST_STORE: &str = "some_file.crt";
        let org_opts = SslOptionsBuilder::new()
            .trust_store(TRUST_STORE)
            .unwrap()
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
                .unwrap()
                .finalize();

            org_opts.clone()
        };

        assert_eq!(TRUST_STORE, opts.data.trust_store.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.trustStore) };
        assert_eq!(TRUST_STORE, s.to_str().unwrap());
    }
}
