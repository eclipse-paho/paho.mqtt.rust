// paho-mqtt/src/runtime/tls_config.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2026 Frank Pagliughi <fpagliughi@mindspring.com>
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
//! Translation of [`TlsOptions`](crate::TlsOptions) into TLS library
//! configuration.
//!
//! Nothing here touches an async runtime: the same `make_tls_config` serves
//! both the tokio (`tls`) and smol (`smol-tls`) rustls transports, and
//! `make_openssl_config` serves the tokio OpenSSL transport. The `*_rt`
//! modules wrap the resulting configuration in their runtime's connector.
//!
//! The rustls items are compiled for `tls` or `smol-tls`; the OpenSSL item
//! for `openssl` without `tls` (the tokio backend prefers rustls when both
//! are enabled, matching `tokio_rt`).

#[cfg(all(feature = "openssl", not(feature = "tls")))]
use openssl::ssl::{
    SslConnector, SslFiletype, SslMethod, SslVerifyMode, SslVersion as OpenSslProtoVersion,
};
#[cfg(any(feature = "tls", feature = "smol-tls"))]
use std::sync::Arc as StdArc;

use crate::{Error, Result};

#[cfg(any(feature = "tls", feature = "smol-tls"))]
type TlsResult<T> = std::result::Result<T, rustls::Error>;

/// A [`rustls::client::danger::ServerCertVerifier`] that accepts any certificate.
///
/// Used when `TlsOptions::enable_server_cert_auth` is `false`.
#[cfg(any(feature = "tls", feature = "smol-tls"))]
#[derive(Debug)]
struct NoCertVerifier;

#[cfg(any(feature = "tls", feature = "smol-tls"))]
impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> TlsResult<rustls::client::danger::ServerCertVerified> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> TlsResult<rustls::client::danger::HandshakeSignatureValid> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> TlsResult<rustls::client::danger::HandshakeSignatureValid> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA1,
            ECDSA_SHA1_Legacy,
            RSA_PKCS1_SHA256,
            ECDSA_NISTP256_SHA256,
            RSA_PKCS1_SHA384,
            ECDSA_NISTP384_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            ED25519,
            ED448,
        ]
    }
}

/// A verifier that checks the certificate chain in full but tolerates a
/// name that does not match the host connected to.
///
/// Used when `TlsOptions::verify` is `false` while
/// `enable_server_cert_auth` is `true`: the certificate must still be issued
/// by a trusted CA, be in date, and be valid for its purpose — only the
/// identity check is waived. That is the case for a broker reached by an
/// address its certificate does not name: an IP address, a tunnel, a test
/// rig whose certificate says `localhost`.
///
/// It is a real weakening — any host holding *any* certificate from a
/// trusted CA is accepted — so it is worth less than it looks and should be
/// a deliberate choice.
#[cfg(any(feature = "tls", feature = "smol-tls"))]
#[derive(Debug)]
struct NoHostnameVerifier(StdArc<rustls::client::WebPkiServerVerifier>);

#[cfg(any(feature = "tls", feature = "smol-tls"))]
impl rustls::client::danger::ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> TlsResult<rustls::client::danger::ServerCertVerified> {
        use rustls::CertificateError::{NotValidForName, NotValidForNameContext};
        use rustls::Error::InvalidCertificate;

        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            // The one failure this verifier exists to tolerate.
            Err(InvalidCertificate(NotValidForName | NotValidForNameContext { .. })) => {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> TlsResult<rustls::client::danger::HandshakeSignatureValid> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> TlsResult<rustls::client::danger::HandshakeSignatureValid> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// Filters the ring provider's default cipher suites to those named in `spec`.
///
/// `spec` is a colon- or comma-separated list of rustls suite names, e.g.
/// `"TLS13_AES_256_GCM_SHA384:TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"`.
/// Matching is case-insensitive.  Returns an error if no names in `spec` match
/// any suite offered by the ring provider.
#[cfg(any(feature = "tls", feature = "smol-tls"))]
fn filter_cipher_suites(spec: &str) -> Result<Vec<rustls::SupportedCipherSuite>> {
    let provider = rustls::crypto::ring::default_provider();
    let requested: std::collections::HashSet<String> = spec
        .split([':', ','])
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    let filtered: Vec<rustls::SupportedCipherSuite> = provider
        .cipher_suites
        .iter()
        .filter(|cs| {
            let name = format!("{:?}", cs.suite()).to_ascii_uppercase();
            requested.contains(&name)
        })
        .copied()
        .collect();

    if filtered.is_empty() {
        return Err(Error::GeneralString(format!(
            "no recognized cipher suites in '{spec}'; \
            use rustls names such as TLS13_AES_256_GCM_SHA384"
        )));
    }
    Ok(filtered)
}

/// Builds a [`rustls::ClientConfig`] from [`TlsOptions`](crate::TlsOptions).
///
/// Implements all `TlsOptions` fields that are applicable to the rustls
/// backend:
///
/// * `trust_store` — PEM file of trusted CA certificates.
/// * `ca_path` — directory of PEM CA certificate files (all files loaded).
/// * `key_store` / `private_key` — client certificate and private key.
/// * `private_key_password` — **not supported** by rustls; returns an error
///   if set.
/// * `enable_server_cert_auth` — when `false`, installs [`NoCertVerifier`].
/// * `version` — selects allowed TLS protocol versions; TLS 1.0 and 1.1
///   are not supported by rustls and fall back to TLS 1.2 with a warning.
/// * `enabled_cipher_suites` — colon/comma list of rustls suite names;
///   empty string keeps all defaults.
/// * `protos` — ALPN protocol list.
#[cfg(any(feature = "tls", feature = "smol-tls"))]
pub(crate) fn make_tls_config(tls: &crate::TlsOptions) -> Result<StdArc<rustls::ClientConfig>> {
    use crate::TlsVersion;
    use rustls::RootCertStore;
    use std::io::BufReader;

    // Encrypted private keys require out-of-band decryption not supported by
    // the rustls backend.
    if !tls.private_key_password.is_empty() {
        return Err(Error::General(
            "encrypted private keys are not supported with the rustls TLS backend",
        ));
    }

    // ---- Root certificate store ----
    let mut root_store = RootCertStore::empty();

    if !tls.disable_default_trust_store {
        let native_certs = rustls_native_certs::load_native_certs().map_err(Error::Io)?;
        for cert in native_certs {
            let _ = root_store.add(cert);
        }
    }

    if !tls.trust_store.as_os_str().is_empty() {
        let f = std::fs::File::open(&tls.trust_store).map_err(Error::Io)?;
        let mut r = BufReader::new(f);
        for cert in rustls_pemfile::certs(&mut r) {
            root_store
                .add(cert.map_err(Error::Io)?)
                .map_err(|e| Error::GeneralString(e.to_string()))?;
        }
    }

    // ca_path: load every file in the directory as PEM; silently skip
    // non-PEM / unreadable files so mixed-content directories work.
    if !tls.ca_path.as_os_str().is_empty() {
        for entry in std::fs::read_dir(&tls.ca_path).map_err(Error::Io)? {
            let path = entry.map_err(Error::Io)?.path();
            if path.is_file()
                && let Ok(f) = std::fs::File::open(&path)
            {
                let mut r = BufReader::new(f);
                for cert in rustls_pemfile::certs(&mut r).flatten() {
                    let _ = root_store.add(cert);
                }
            }
        }
    }

    // ---- Crypto provider (cipher suites) ----
    let mut provider = rustls::crypto::ring::default_provider();
    if !tls.enabled_cipher_suites.is_empty() {
        provider.cipher_suites = filter_cipher_suites(&tls.enabled_cipher_suites)?;
    }

    // ---- Protocol versions ----
    let versions: &[&'static rustls::SupportedProtocolVersion] = match tls.version {
        TlsVersion::Default | TlsVersion::Tls1_2 => {
            &[&rustls::version::TLS12, &rustls::version::TLS13]
        }
        TlsVersion::Tls1_0 | TlsVersion::Tls1_1 => {
            log::warn!(
                "TLS 1.0/1.1 are not supported by the rustls backend; \
                 using TLS 1.2 as the minimum version"
            );
            &[&rustls::version::TLS12, &rustls::version::TLS13]
        }
    };

    // ---- Config builder ----
    let provider_arc = StdArc::new(provider);
    let builder = rustls::ClientConfig::builder_with_provider(provider_arc.clone())
        .with_protocol_versions(versions)
        .map_err(|e| Error::GeneralString(e.to_string()))?;

    // ---- Certificate verification ----
    //
    // Two independent switches, as in the Paho C library:
    //   enable_server_cert_auth — check the chain at all
    //   verify                  — check that the name matches the host
    // Turning the first off makes the second irrelevant.
    let wants_client = match (tls.enable_server_cert_auth, tls.verify) {
        (false, _) => builder
            .dangerous()
            .with_custom_certificate_verifier(StdArc::new(NoCertVerifier)),
        (true, true) => builder.with_root_certificates(root_store),
        (true, false) => {
            let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
                StdArc::new(root_store),
                provider_arc.clone(),
            )
            .build()
            .map_err(|e| Error::GeneralString(e.to_string()))?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(StdArc::new(NoHostnameVerifier(inner)))
        }
    };

    // ---- Client authentication ----
    let mut config = if !tls.key_store.as_os_str().is_empty() {
        let cert_f = std::fs::File::open(&tls.key_store).map_err(Error::Io)?;
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(cert_f))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Error::Io)?;

        let key_path = if !tls.private_key.as_os_str().is_empty() {
            &tls.private_key
        } else {
            &tls.key_store
        };
        let key_f = std::fs::File::open(key_path).map_err(Error::Io)?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_f))
            .map_err(Error::Io)?
            .ok_or_else(|| Error::General("no private key found in key store"))?;

        wants_client
            .with_client_auth_cert(certs, key)
            .map_err(|e| Error::GeneralString(e.to_string()))?
    } else {
        wants_client.with_no_client_auth()
    };

    // ---- ALPN protocols ----
    if !tls.protos.is_empty() {
        config.alpn_protocols = tls.protos.iter().map(|p| p.as_bytes().to_vec()).collect();
    }

    Ok(StdArc::new(config))
}

/// Builds an OpenSSL [`SslConnector`] from [`TlsOptions`](crate::TlsOptions).
///
/// Maps all applicable `TlsOptions` fields to the OpenSSL API:
///
/// * `trust_store` — CA certificate file (PEM).
/// * `ca_path` — directory of CA certificate files.
/// * `key_store` / `private_key` — client certificate and private key (PEM).
/// * `private_key_password` — password for an encrypted private key.
/// * `enable_server_cert_auth` — when `false`, disables peer verification.
/// * `version` — minimum TLS protocol version.
/// * `enabled_cipher_suites` — OpenSSL cipher list string (TLS 1.2 and below).
/// * `protos` — ALPN protocol list.
/// * `disable_default_trust_store` — when `true`, skips loading system CA.
#[cfg(all(feature = "openssl", not(feature = "tls")))]
pub(crate) fn make_openssl_config(tls: &crate::TlsOptions) -> Result<openssl::ssl::SslConnector> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())
        .map_err(|e| Error::GeneralString(e.to_string()))?;

    // Server certificate verification
    builder.set_verify(if tls.enable_server_cert_auth {
        SslVerifyMode::PEER
    } else {
        SslVerifyMode::NONE
    });

    // System CA certificates
    if !tls.disable_default_trust_store {
        builder
            .set_default_verify_paths()
            .map_err(|e| Error::GeneralString(e.to_string()))?;
    }

    // Explicit CA certificate file
    if !tls.trust_store.as_os_str().is_empty() {
        builder
            .set_ca_file(&tls.trust_store)
            .map_err(|e| Error::GeneralString(e.to_string()))?;
    }

    // CA certificate directory — load each PEM file; silently skip non-PEM files.
    if !tls.ca_path.as_os_str().is_empty() {
        for entry in std::fs::read_dir(&tls.ca_path).map_err(Error::Io)? {
            let path = entry.map_err(Error::Io)?.path();
            if path.is_file()
                && let Ok(pem) = std::fs::read(&path)
                && let Ok(cert) = openssl::x509::X509::from_pem(&pem)
            {
                let _ = builder.cert_store_mut().add_cert(cert);
            }
        }
    }

    // Cipher suite list (OpenSSL format, applies to TLS 1.2 and below)
    if !tls.enabled_cipher_suites.is_empty() {
        builder
            .set_cipher_list(&tls.enabled_cipher_suites)
            .map_err(|e| Error::GeneralString(e.to_string()))?;
    }

    // Minimum TLS protocol version
    let min_ver = match tls.version {
        crate::TlsVersion::Default | crate::TlsVersion::Tls1_2 => Some(OpenSslProtoVersion::TLS1_2),
        crate::TlsVersion::Tls1_0 => Some(OpenSslProtoVersion::TLS1),
        crate::TlsVersion::Tls1_1 => Some(OpenSslProtoVersion::TLS1_1),
    };
    builder
        .set_min_proto_version(min_ver)
        .map_err(|e| Error::GeneralString(e.to_string()))?;

    // Client certificate and private key
    if !tls.key_store.as_os_str().is_empty() {
        builder
            .set_certificate_file(&tls.key_store, SslFiletype::PEM)
            .map_err(|e| Error::GeneralString(e.to_string()))?;
        let key_path = if !tls.private_key.as_os_str().is_empty() {
            &tls.private_key
        } else {
            &tls.key_store
        };
        if !tls.private_key_password.is_empty() {
            // Load the PEM and decrypt it explicitly (openssl-rs has no sync passwd callback).
            let pem = std::fs::read(key_path).map_err(Error::Io)?;
            let key = openssl::pkey::PKey::private_key_from_pem_passphrase(
                &pem,
                tls.private_key_password.as_bytes(),
            )
            .map_err(|e| Error::GeneralString(e.to_string()))?;
            builder
                .set_private_key(&key)
                .map_err(|e| Error::GeneralString(e.to_string()))?;
        } else {
            builder
                .set_private_key_file(key_path, SslFiletype::PEM)
                .map_err(|e| Error::GeneralString(e.to_string()))?;
        }
    }

    // ALPN protocol negotiation (wire-format: length-prefixed byte strings)
    if !tls.protos.is_empty() {
        let mut wire = Vec::new();
        for proto in &tls.protos {
            wire.push(proto.len() as u8);
            wire.extend_from_slice(proto.as_bytes());
        }
        builder
            .set_alpn_protos(&wire)
            .map_err(|e| Error::GeneralString(e.to_string()))?;
    }

    Ok(builder.build())
}

/// Prepares the per-connection OpenSSL state for a connection to `host`.
///
/// OpenSSL checks the host name separately from the chain, and only per
/// connection, so `TlsOptions::verify` is applied here rather than on the
/// connector: with it off the chain is still verified but a certificate
/// naming some other host is accepted.
#[cfg(all(feature = "openssl", not(feature = "tls")))]
pub(crate) fn openssl_ssl_for(
    connector: &SslConnector,
    tls: &crate::TlsOptions,
    host: &str,
) -> Result<openssl::ssl::Ssl> {
    let mut config = connector
        .configure()
        .map_err(|e| Error::GeneralString(e.to_string()))?;
    config.set_verify_hostname(tls.enable_server_cert_auth && tls.verify);
    config
        .into_ssl(host)
        .map_err(|e| Error::GeneralString(e.to_string()))
}
