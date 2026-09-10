// paho-mqtt/src/runtime/smol_conn.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2024-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Smol-based connection types for the MQTT client.
//!
//! Provides transport implementations of the shared [`Connection`] trait:
//! [`TcpConnection`], [`UnixConnection`], [`TlsConnection`], and
//! [`WsConnection`], together with the TLS configuration helper functions.
//!
//! TLS support requires the `smol-tls` feature (`futures-rustls`).
//! WebSocket support requires the `smol-websocket` feature (`async-tungstenite`).

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "smol-tls")]
use std::sync::Arc as StdArc;
#[cfg(unix)]
use smol::net::unix::UnixStream;
use smol::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use super::connection::Connection;
#[cfg(any(feature = "smol-tls", feature = "smol-websocket"))]
use crate::{Error, Result};

/////////////////////////////////////////////////////////////////////////////

/// A plain TCP transport backed by [`smol::net::TcpStream`].
pub(crate) struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
    /// Opens a TCP connection to `host` on `port`.
    pub(crate) async fn connect(host: &str, port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }
}

/// Implements `AsyncRead`, `AsyncWrite`, and `Connection` for a newtype struct
/// whose only field is named `stream` and already implements those traits
/// via the `futures-io` traits.
macro_rules! impl_connection {
    ($ty:ty) => {
        impl AsyncRead for $ty {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.stream).poll_read(cx, buf)
            }
        }

        impl AsyncWrite for $ty {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.stream).poll_write(cx, buf)
            }

            fn poll_flush(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_flush(cx)
            }

            fn poll_close(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_close(cx)
            }
        }

        impl Connection for $ty {}
    };
}

impl_connection!(TcpConnection);

/////////////////////////////////////////////////////////////////////////////

/// A Unix-domain socket transport backed by [`smol::net::unix::UnixStream`].
#[cfg(unix)]
pub(crate) struct UnixConnection {
    stream: UnixStream,
}

#[cfg(unix)]
impl UnixConnection {
    /// Opens a Unix-domain socket connection to `path`.
    pub(crate) async fn connect(path: &str) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self { stream })
    }
}

#[cfg(unix)]
impl_connection!(UnixConnection);

/////////////////////////////////////////////////////////////////////////////

/// A TLS transport backed by [`futures_rustls`] (requires the `smol-tls` feature).
#[cfg(feature = "smol-tls")]
pub(crate) struct TlsConnection {
    stream: futures_rustls::client::TlsStream<TcpStream>,
}

#[cfg(feature = "smol-tls")]
impl TlsConnection {
    pub(super) fn new(stream: futures_rustls::client::TlsStream<TcpStream>) -> Self {
        Self { stream }
    }
}

#[cfg(feature = "smol-tls")]
impl_connection!(TlsConnection);

#[cfg(feature = "smol-tls")]
type TlsResult<T> = std::result::Result<T, rustls::Error>;

/////////////////////////////////////////////////////////////////////////////

/// A WebSocket transport wrapping an [`async_tungstenite::WebSocketStream`].
///
/// Presents a byte-stream interface (`AsyncRead` + `AsyncWrite`) over a
/// WebSocket connection.  Each flushed write becomes one binary WebSocket
/// frame; reads reassemble frames back into a byte stream.  Requires the
/// `smol-websocket` feature.
#[cfg(feature = "smol-websocket")]
pub(super) struct WsConnection<S> {
    stream: async_tungstenite::WebSocketStream<S>,
    /// Overflow bytes from the last received frame not yet consumed by the reader.
    read_buf: Vec<u8>,
    /// Position within `read_buf` of the next unread byte.
    read_pos: usize,
    /// Bytes accumulated via `poll_write`; sent as one binary frame on flush.
    write_buf: Vec<u8>,
    /// Non-`None` while waiting for the sink to be ready before `start_send`.
    pending_msg: Option<Vec<u8>>,
}

#[cfg(feature = "smol-websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> WsConnection<S> {
    pub(super) fn new(stream: async_tungstenite::WebSocketStream<S>) -> Self {
        Self {
            stream,
            read_buf: Vec::new(),
            read_pos: 0,
            write_buf: Vec::new(),
            pending_msg: None,
        }
    }
}

#[cfg(feature = "smol-websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WsConnection<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        use async_tungstenite::tungstenite::Message as WsMessage;
        use futures::Stream;

        let this = self.get_mut();

        // Serve buffered bytes from a previous frame first.
        if this.read_pos < this.read_buf.len() {
            let available = &this.read_buf[this.read_pos..];
            let to_copy = buf.len().min(available.len());
            buf[..to_copy].copy_from_slice(&available[..to_copy]);
            this.read_pos += to_copy;
            if this.read_pos >= this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return Poll::Ready(Ok(to_copy));
        }

        // Poll the WebSocket stream for the next message.
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(0)),
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                }
                Poll::Ready(Some(Ok(msg))) => match msg {
                    WsMessage::Binary(data) => {
                        if data.is_empty() {
                            continue;
                        }
                        let to_copy = buf.len().min(data.len());
                        buf[..to_copy].copy_from_slice(&data[..to_copy]);
                        if to_copy < data.len() {
                            this.read_buf = data;
                            this.read_pos = to_copy;
                        }
                        return Poll::Ready(Ok(to_copy));
                    }
                    WsMessage::Text(text) => {
                        let data = text.into_bytes();
                        if data.is_empty() {
                            continue;
                        }
                        let to_copy = buf.len().min(data.len());
                        buf[..to_copy].copy_from_slice(&data[..to_copy]);
                        if to_copy < data.len() {
                            this.read_buf = data;
                            this.read_pos = to_copy;
                        }
                        return Poll::Ready(Ok(to_copy));
                    }
                    WsMessage::Close(_) => return Poll::Ready(Ok(0)),
                    // Control frames are handled internally by tungstenite.
                    WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_) => {
                        continue;
                    }
                },
            }
        }
    }
}

#[cfg(feature = "smol-websocket")]
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WsConnection<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().write_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use async_tungstenite::tungstenite::Message as WsMessage;
        use futures::Sink;

        let this = self.get_mut();

        // Move the accumulated write buffer into `pending_msg` for sending.
        if this.pending_msg.is_none() && !this.write_buf.is_empty() {
            this.pending_msg = Some(std::mem::take(&mut this.write_buf));
        }

        // Drive the pending message through the Sink.
        if let Some(data) = this.pending_msg.take() {
            match Pin::new(&mut this.stream).poll_ready(cx) {
                Poll::Pending => {
                    this.pending_msg = Some(data);
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)));
                }
                Poll::Ready(Ok(())) => {
                    if let Err(e) =
                        Pin::new(&mut this.stream).start_send(WsMessage::Binary(data))
                    {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            e,
                        )));
                    }
                }
            }
        }

        // Flush the sink to push the frame onto the wire.
        Pin::new(&mut this.stream)
            .poll_flush(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use futures::Sink;
        Pin::new(&mut self.get_mut().stream)
            .poll_close(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
    }
}

#[cfg(feature = "smol-websocket")]
impl<S: AsyncRead + AsyncWrite + Send + Unpin + 'static> Connection for WsConnection<S> {}

/// Builds an HTTP upgrade request for a WebSocket MQTT connection.
#[cfg(feature = "smol-websocket")]
pub(super) fn build_ws_request(
    url: &str,
    http_headers: &std::collections::HashMap<String, String>,
) -> Result<async_tungstenite::tungstenite::handshake::client::Request> {
    use async_tungstenite::tungstenite::{
        client::IntoClientRequest,
        http::header::{HeaderName, HeaderValue},
    };

    let mut request = url
        .into_client_request()
        .map_err(|e| Error::GeneralString(e.to_string()))?;

    {
        let headers = request.headers_mut();
        headers.insert(
            HeaderName::from_static("sec-websocket-protocol"),
            HeaderValue::from_static("mqtt"),
        );
        for (name, value) in http_headers {
            if let (Ok(h_name), Ok(h_val)) =
                (name.parse::<HeaderName>(), value.parse::<HeaderValue>())
            {
                headers.insert(h_name, h_val);
            }
        }
    }

    Ok(request)
}

/////////////////////////////////////////////////////////////////////////////

/// A [`rustls::client::ServerCertVerifier`] that accepts any certificate.
///
/// Used when `TlsOptions::enable_server_cert_auth` is `false`.
#[cfg(feature = "smol-tls")]
#[derive(Debug)]
struct NoCertVerifier;

#[cfg(feature = "smol-tls")]
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

/// Filters the ring provider's cipher suites to those named in `spec`.
///
/// `spec` is a colon- or comma-separated list of rustls suite names.
/// Matching is case-insensitive.  Returns an error if no names match.
#[cfg(feature = "smol-tls")]
fn filter_cipher_suites(spec: &str) -> Result<Vec<rustls::SupportedCipherSuite>> {
    let provider = rustls::crypto::ring::default_provider();
    let requested: std::collections::HashSet<String> = spec
        .split(|c: char| c == ':' || c == ',')
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

/// Builds a [`rustls::ClientConfig`] from [`TlsOptions`].
///
/// Identical logic to the tokio backend's `make_tls_config`; only the
/// async wrapper crate differs (`futures-rustls` vs `tokio-rustls`).
#[cfg(feature = "smol-tls")]
pub(super) fn make_tls_config(tls: &crate::TlsOptions) -> Result<StdArc<rustls::ClientConfig>> {
    use crate::TlsVersion;
    use rustls::RootCertStore;
    use std::io::BufReader;

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

    if !tls.ca_path.as_os_str().is_empty() {
        for entry in std::fs::read_dir(&tls.ca_path).map_err(Error::Io)? {
            let path = entry.map_err(Error::Io)?.path();
            if path.is_file() {
                if let Ok(f) = std::fs::File::open(&path) {
                    let mut r = BufReader::new(f);
                    for cert in rustls_pemfile::certs(&mut r).flatten() {
                        let _ = root_store.add(cert);
                    }
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
    let builder = rustls::ClientConfig::builder_with_provider(StdArc::new(provider))
        .with_protocol_versions(versions)
        .map_err(|e| Error::GeneralString(e.to_string()))?;

    // ---- Certificate verification ----
    let wants_client = if tls.enable_server_cert_auth {
        builder.with_root_certificates(root_store)
    }
    else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(StdArc::new(NoCertVerifier))
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
        }
        else {
            &tls.key_store
        };
        let key_f = std::fs::File::open(key_path).map_err(Error::Io)?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_f))
            .map_err(Error::Io)?
            .ok_or_else(|| Error::General("no private key found in key store"))?;

        wants_client
            .with_client_auth_cert(certs, key)
            .map_err(|e| Error::GeneralString(e.to_string()))?
    }
    else {
        wants_client.with_no_client_auth()
    };

    // ---- ALPN protocols ----
    if !tls.protos.is_empty() {
        config.alpn_protocols = tls.protos.iter().map(|p| p.as_bytes().to_vec()).collect();
    }

    Ok(StdArc::new(config))
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_connection<C: Connection>() {}

    #[test]
    fn test_tcp_connection_implements_connection() {
        assert_connection::<TcpConnection>();
    }
}
