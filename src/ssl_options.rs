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
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{AsyncClient, SslErrorCallback};

/// The SSL/TLS versions that can be requested.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SslVersion {
    /// The default library SSL/TLS version
    #[default]
    Default,
    /// TLS version 1.0
    Tls1_0,
    /// TLS version 1.1
    Tls1_1,
    /// TLS version 1.2
    Tls1_2,
}

/////////////////////////////////////////////////////////////////////////////
//                              SslOptions
/////////////////////////////////////////////////////////////////////////////

/// The options for SSL socket connections to the broker.
#[derive(Default, Clone)] //Debug, Default, Clone)]
pub struct SslOptions {
    /// File name of the trust store.
    pub trust_store: PathBuf,
    /// File name of the client't public key store.
    pub key_store: PathBuf,
    /// File name for the private key, if not in key store.
    pub private_key: PathBuf,
    /// Password for the private key, if any.
    pub private_key_password: String,
    /// The list of cipher quotes that the client presents to the server.
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
    /// If this setting is omitted, its default value will be "ALL", that is,
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
        F: Fn(&AsyncClient, &str) + Send + Sync + 'static,
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

    /// Create the SSL options from the builder.
    pub fn finalize(&self) -> SslOptions {
        self.0.clone()
    }
}
