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
            if let (Ok(h_name), Ok(h_val)) =
                (name.parse::<HeaderName>(), value.parse::<HeaderValue>())
            {
                headers.insert(h_name, h_val);
            }
        }
    }

    Ok(request)
}
