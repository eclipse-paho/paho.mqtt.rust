// tls_options.rs
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

use crate::{AsyncClient, TlsErrorCallback};

/// The TLS versions that can be requested.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersion {
    /// The default library TLS version
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
//                              TlsOptions
/////////////////////////////////////////////////////////////////////////////

/// The options for TLS socket connections to the broker.
#[derive(Default, Clone)]
pub struct TlsOptions {
    /// File name of the trust store.
    pub trust_store: PathBuf,
    /// File name of the client's public key store.
    pub key_store: PathBuf,
    /// File name for the private key, if not in key store.
    pub private_key: PathBuf,
    /// Password for the private key, if any.
    pub private_key_password: String,
    /// The list of cipher suites that the client presents to the server.
    pub enabled_cipher_suites: String,
    /// Whether to enable verification of the server certificate.
    pub enable_server_cert_auth: bool,
    /// The TLS version to use.
    pub version: TlsVersion,
    /// Whether the broker's certificate must name the host connected to.
    ///
    /// Independent of [`enable_server_cert_auth`](Self::enable_server_cert_auth),
    /// which decides whether the certificate chain is checked at all. With
    /// the chain checked and this off, the certificate must still come from
    /// a trusted CA and be in date, but may name some other host — for a
    /// broker reached by IP address, through a tunnel, or on a test rig.
    ///
    /// Defaults to `true`. Turning it off is a real weakening: any host
    /// holding any certificate from a trusted CA is then accepted.
    pub verify: bool,
    /// The path to the CA certificates, if specified.
    pub ca_path: PathBuf,
    /// The TLS error callback.
    pub on_tls_error: Option<Arc<TlsErrorCallback>>,

    /// Don't load the default TLS CA certificates.
    ///
    /// Leaves the trust store holding only what
    /// [`trust_store`](Self::trust_store) and [`ca_path`](Self::ca_path)
    /// put there, so a broker presenting a certificate from a public CA is
    /// not accepted just because the system trusts that CA.
    //
    // Pre-shared keys (the v1.x `ssl_psk` callback) are not supported.
    // rustls, the default backend, does not expose external PSKs at all;
    // implementing them for OpenSSL alone would mean an option that silently
    // does nothing on the backend most builds use. Deferred past 2.0.
    pub disable_default_trust_store: bool,
    /// The list of ALPN protocols available to be negotiated.
    pub protos: Vec<String>,
}

impl TlsOptions {
    /// Creates a new, default, set of TLS options.
    pub fn new() -> Self {
        Self::default()
    }
}

// We manually define a Debug implementation to get around
// the callback function fields.
impl fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsOptions")
            .field("trust_store", &self.trust_store)
            .field("key_store", &self.key_store)
            .field("private_key", &self.private_key)
            .field("private_key_password", &self.private_key_password)
            .field("enabled_cipher_suites", &self.enabled_cipher_suites)
            .field("enable_server_cert_auth", &self.enable_server_cert_auth)
            .field("version", &self.version)
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

/// Builder to create TLS options.
#[derive(Default)]
pub struct TlsOptionsBuilder(TlsOptions);

impl TlsOptionsBuilder {
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

    /// The password to load the client's private key if it's encrypted.
    pub fn private_key_password<S>(&mut self, private_key_password: S) -> &mut Self
    where
        S: Into<String>,
    {
        self.0.private_key_password = private_key_password.into();
        self
    }

    /// The list of cipher suites that the client will present to the server
    /// during the TLS handshake.
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

    /// Enable or disable verification of the server certificate.
    pub fn enable_server_cert_auth(&mut self, on: bool) -> &mut Self {
        self.0.enable_server_cert_auth = on;
        self
    }

    /// The TLS version to use.
    pub fn version(&mut self, ver: TlsVersion) -> &mut Self {
        self.0.version = ver;
        self
    }

    /// Whether the broker's certificate must name the host connected to.
    ///
    /// See [`TlsOptions::verify`]; defaults to `true`, and turning it off
    /// keeps chain verification but drops the identity check.
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

    /// Sets the TLS error callback.
    pub fn error_callback<F>(&mut self, cb: F) -> &mut Self
    where
        F: Fn(&AsyncClient, &str) + Send + Sync + 'static,
    {
        self.0.on_tls_error = Some(Arc::new(cb));
        self
    }

    /// Don't load the default TLS CA certificates.
    ///
    /// Use this when the broker's certificate comes from a private CA given
    /// with [`trust_store`](Self::trust_store) or [`ca_path`](Self::ca_path),
    /// so that a certificate from any public CA is not accepted as well.
    pub fn disable_default_trust_store(&mut self, disable: bool) -> &mut Self {
        self.0.disable_default_trust_store = disable;
        self
    }

    /// If set, only these protocols are used during negotiation.
    pub fn alpn_protos(&mut self, protos: &[&str]) -> &mut Self {
        self.0.protos = protos.iter().map(|s| String::from(*s)).collect();
        self
    }

    /// Create the TLS options from the builder.
    pub fn finalize(&self) -> TlsOptions {
        self.0.clone()
    }
}
