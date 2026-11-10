// paho-mqtt/src/runtime/ws_util.rs
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
//! WebSocket handshake helpers shared by the tokio and smol transports.
//!
//! `tokio-tungstenite` and `async-tungstenite` both re-export the same
//! `tungstenite` crate, so the request builder is written once against that
//! re-export and each transport hands the result to its own `client_async`.

#[cfg(all(feature = "smol-websocket", not(feature = "websocket")))]
use async_tungstenite::tungstenite;
#[cfg(feature = "websocket")]
use tokio_tungstenite::tungstenite;

use crate::{Error, Result};

/// Builds an HTTP upgrade request for a WebSocket MQTT connection.
///
/// Uses tungstenite's [`IntoClientRequest`] to derive the request from the
/// URL — which generates a valid `Sec-WebSocket-Key` automatically — then
/// appends the mandatory `Sec-WebSocket-Protocol: mqtt` header and any
/// application-supplied headers from `http_headers`.
///
/// [`IntoClientRequest`]: tungstenite::client::IntoClientRequest
pub(crate) fn build_ws_request(
    url: &str,
    http_headers: &std::collections::HashMap<String, String>,
) -> Result<tungstenite::handshake::client::Request> {
    use tungstenite::{
        client::IntoClientRequest,
        http::header::{HeaderName, HeaderValue},
    };

    let mut request = url
        .into_client_request()
        .map_err(|e| Error::WebSocket(e.to_string()))?;

    {
        let headers = request.headers_mut();
        headers.insert(
            HeaderName::from_static("sec-websocket-protocol"),
            HeaderValue::from_static("mqtt"),
        );
        for (name, value) in http_headers {
            let (h_name, h_val) = parse_header(name, value)?;
            headers.insert(h_name, h_val);
        }
    }

    Ok(request)
}

/// Checks that every application-supplied header is valid HTTP, so that
/// `connect()` can refuse one that is not rather than each connection
/// attempt failing on it.
pub(crate) fn check_headers(
    http_headers: &std::collections::HashMap<String, String>,
) -> Result<()> {
    for (name, value) in http_headers {
        parse_header(name, value)?;
    }
    Ok(())
}

/// One header as `tungstenite` takes it. A name or value that HTTP does not
/// allow (RFC 9110 §5) is an error naming the header — not quoting its
/// value, which may be a credential. It used to be dropped without a word.
fn parse_header(
    name: &str,
    value: &str,
) -> Result<(
    tungstenite::http::header::HeaderName,
    tungstenite::http::header::HeaderValue,
)> {
    use tungstenite::http::header::{HeaderName, HeaderValue};
    let h_name = name
        .parse::<HeaderName>()
        .map_err(|_| Error::InvalidArgument(format!("invalid HTTP header name: {name:?}")))?;
    let h_val = value
        .parse::<HeaderValue>()
        .map_err(|_| Error::InvalidArgument(format!("invalid value for HTTP header {name:?}")))?;
    Ok((h_name, h_val))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Application headers go into the request; one that HTTP does not
    /// allow is an error, from `check_headers` as from the request builder,
    /// naming the header but not quoting its value. It used to be dropped.
    #[test]
    fn test_headers_are_checked() {
        let headers = |pairs: &[(&str, &str)]| -> std::collections::HashMap<String, String> {
            pairs
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect()
        };
        let good = headers(&[("X-Client", "paho"), ("Authorization", "Bearer abc")]);
        assert!(check_headers(&good).is_ok());
        let req = build_ws_request("ws://broker/mqtt", &good).unwrap();
        assert_eq!(req.headers()["x-client"], "paho");

        for (bad, what) in [
            (headers(&[("Bad Name", "x")]), "name"),
            (headers(&[("", "x")]), "name"),
            (headers(&[("X-Secret", "line\r\nbreak")]), "value"),
        ] {
            for res in [
                check_headers(&bad).err(),
                build_ws_request("ws://broker/mqtt", &bad).err(),
            ] {
                let Some(Error::InvalidArgument(msg)) = res else {
                    panic!("{bad:?}: {res:?}")
                };
                assert!(msg.contains(what), "{msg}");
                assert!(!msg.contains("break"), "the value is not quoted: {msg}");
            }
        }
    }

    /// The upgrade request for an IPv6 broker names it in brackets, in the
    /// URI and the `Host` header.
    #[test]
    fn test_ws_request_for_ipv6() {
        let url = format!(
            "ws://{}/mqtt",
            crate::runtime::commands::host_port("::1", 9001)
        );
        let req = build_ws_request(&url, &Default::default()).unwrap();
        assert_eq!(req.uri().host(), Some("[::1]"));
        assert_eq!(req.uri().port_u16(), Some(9001));
        assert_eq!(req.headers()["host"], "[::1]:9001");
    }
}
