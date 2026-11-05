// paho-mqtt/src/runtime/proxy.rs
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
//! HTTP `CONNECT` tunnelling, for reaching a broker through a proxy.
//!
//! A WebSocket connection to a broker is often the only one a corporate
//! network will carry, and it usually has to go through an HTTP proxy. The
//! client opens a TCP connection to the proxy, asks it with `CONNECT` for a
//! tunnel to the broker, and from then on the socket behaves as if it were
//! connected to the broker directly — TLS and the WebSocket upgrade included,
//! both of which happen inside the tunnel.
//!
//! This module is the protocol half: parsing the proxy URI, building the
//! request, reading the reply. The socket work belongs to whichever backend
//! is in use.

use crate::{Error, Result};

/// A proxy to tunnel through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Proxy {
    /// Host name or address of the proxy itself.
    pub host: String,
    /// Port of the proxy; 3128 if the URI did not say.
    pub port: u16,
    /// `Proxy-Authorization` header value, when the URI carried credentials.
    pub auth: Option<String>,
}

impl Proxy {
    /// Parses a proxy URI.
    ///
    /// Accepts what the environment variables of the same name usually hold:
    /// `http://proxy:3128`, `proxy:3128`, `http://user:pass@proxy:3128`. The
    /// scheme, if any, is ignored — the connection to the proxy is plain TCP
    /// either way, and `https_proxy` names the proxy for a TLS *destination*,
    /// not a TLS proxy.
    pub fn parse(uri: &str) -> Result<Self> {
        let rest = uri
            .split_once("://")
            .map_or(uri, |(_scheme, rest)| rest)
            .trim_end_matches('/');

        let (auth, authority) = match rest.rsplit_once('@') {
            Some((creds, authority)) => (
                Some(format!("Basic {}", base64(creds.as_bytes()))),
                authority,
            ),
            None => (None, rest),
        };

        if authority.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "proxy URI has no host: {uri}"
            )));
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (
                h,
                p.parse::<u16>().map_err(|_| {
                    Error::InvalidArgument(format!("invalid port in proxy URI: {uri}"))
                })?,
            ),
            None => (authority, 3128),
        };

        Ok(Self {
            host: host.to_string(),
            port,
            auth,
        })
    }

    /// Builds the `CONNECT` request asking this proxy for a tunnel to
    /// `host:port`.
    pub fn connect_request(&self, host: &str, port: u16) -> Vec<u8> {
        let mut req = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n");
        if let Some(auth) = &self.auth {
            req.push_str(&format!("Proxy-Authorization: {auth}\r\n"));
        }
        req.push_str("\r\n");
        req.into_bytes()
    }
}

// --------------------------------------------------------------------------

/// Examines the bytes read back from the proxy.
///
/// Returns `Ok(None)` while the response headers are incomplete, so the
/// caller can read more, and `Ok(Some(n))` once the tunnel is open, where
/// `n` is the length of the response — anything after it was sent by the
/// broker and belongs to the caller.
///
/// A status other than 2xx means the proxy refused, and its reason is worth
/// repeating: `407` in particular means the credentials were missing or
/// wrong, which is otherwise a puzzling failure.
pub(crate) fn check_response(buf: &[u8]) -> Result<Option<usize>> {
    let Some(end) = find_header_end(buf) else {
        // A proxy that says nothing at all is more likely broken than slow.
        if buf.len() > 8192 {
            return Err(Error::Proxy("the response is over-long".into()));
        }
        return Ok(None);
    };

    let head = core::str::from_utf8(&buf[..end])
        .map_err(|_| Error::Proxy("the response is not valid UTF-8".into()))?;
    let status_line = head.lines().next().unwrap_or_default();

    // "HTTP/1.1 200 Connection established"
    let mut parts = status_line.split_whitespace();
    let _version = parts.next();
    let code = parts
        .next()
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| Error::Proxy(format!("malformed status line: {}", status_line.trim())))?;

    if (200..300).contains(&code) {
        Ok(Some(end))
    } else {
        Err(Error::ProxyRefused(code, status_line.trim().to_string()))
    }
}

/// Offset just past the blank line that ends the response headers.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

/// Minimal base64, for the one header that needs it.
fn base64(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);

    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[((n >> (18 - 6 * i)) & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64() {
        // The padding cases, which are where a hand-rolled encoder goes wrong.
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foob"), "Zm9vYg==");
        assert_eq!(base64(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64(b"user:pass"), "dXNlcjpwYXNz");
    }

    #[test]
    fn test_parse_proxy_uri() {
        let p = Proxy::parse("http://proxy.example:8080").unwrap();
        assert_eq!(p.host, "proxy.example");
        assert_eq!(p.port, 8080);
        assert_eq!(p.auth, None);

        // No scheme, and the default port.
        let p = Proxy::parse("proxy.example").unwrap();
        assert_eq!(p.host, "proxy.example");
        assert_eq!(p.port, 3128);

        // Credentials become a Basic header.
        let p = Proxy::parse("http://user:pass@proxy.example:3128").unwrap();
        assert_eq!(p.host, "proxy.example");
        assert_eq!(p.auth.as_deref(), Some("Basic dXNlcjpwYXNz"));

        assert!(matches!(
            Proxy::parse("http://proxy:notaport"),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            Proxy::parse("http://"),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn test_connect_request() {
        let p = Proxy::parse("proxy:3128").unwrap();
        let req = String::from_utf8(p.connect_request("broker.example", 8883)).unwrap();
        assert_eq!(
            req,
            "CONNECT broker.example:8883 HTTP/1.1\r\nHost: broker.example:8883\r\n\r\n"
        );

        let p = Proxy::parse("user:pass@proxy").unwrap();
        let req = String::from_utf8(p.connect_request("b", 1883)).unwrap();
        assert!(req.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"));
        assert!(req.ends_with("\r\n\r\n"));
    }

    #[test]
    fn test_check_response() {
        // Incomplete headers: keep reading.
        assert_eq!(check_response(b"HTTP/1.1 200 OK\r\n").unwrap(), None);

        // Complete and successful; the offset excludes anything that follows.
        let resp = b"HTTP/1.1 200 Connection established\r\n\r\nMQTT-BYTES";
        let n = check_response(resp).unwrap().unwrap();
        assert_eq!(&resp[n..], b"MQTT-BYTES");

        // Any 2xx is a tunnel.
        assert!(
            check_response(b"HTTP/1.0 201 Created\r\n\r\n")
                .unwrap()
                .is_some()
        );

        // A refusal carries the status code and the proxy's own words.
        let err =
            check_response(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n").unwrap_err();
        assert!(
            matches!(&err, Error::ProxyRefused(407, line) if line.ends_with("Required")),
            "{err:?}"
        );

        assert!(matches!(
            check_response(b"garbage\r\n\r\n"),
            Err(Error::Proxy(_))
        ));
    }
}
