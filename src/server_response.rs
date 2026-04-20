// paho-mqtt/src/server_response.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2018-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Internal server-response types used by the C-library callback layer.
//!
//! These types bridge the raw C callback data to the typed [`responses`]
//! module. They are not part of the public API.

use crate::{Properties, ReasonCode, ffi, from_c_bool};
use std::ffi::CStr;

/////////////////////////////////////////////////////////////////////////////
// ServerRequest

/// The server requests that expect a response.
///
/// Required because the `alt` union of the C `MQTTAsync_successData` struct
/// does not indicate which field is valid.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ServerRequest {
    /// No response expected from the server
    #[default]
    None,
    /// Connecting to the server
    Connect,
    /// A subscription request for a single topic
    Subscribe,
    /// A subscription request for multiple topics
    SubscribeMany(usize),
    /// An unsubscribe request for a single topic
    Unsubscribe,
    /// An unsubscribe request for multiple topics
    UnsubscribeMany(usize),
    /// Disconnecting from the server
    Disconnect,
}

/////////////////////////////////////////////////////////////////////////////
// RequestResponse

/// Request-specific data extracted from the C success callback.
#[derive(Debug, Default, Clone)]
pub enum RequestResponse {
    /// No request-specific data
    #[default]
    None,
    /// CONNACK data: server URI, MQTT version, session-present flag
    Connect(ConnAckData),
    /// Granted QoS for a single-topic subscribe (v3.1.1 or v5 single)
    Subscribe(i32),
    /// Granted QoS / reason codes for a multi-topic subscribe
    SubscribeMany(Vec<i32>),
    /// Reason code for a single-topic unsubscribe (v5 only)
    Unsubscribe(i32),
    /// Reason codes for a multi-topic unsubscribe (v5 only)
    UnsubscribeMany(Vec<i32>),
}

/// Internal CONNACK payload extracted from the C callback.
///
/// This is an implementation detail used to build the public
/// [`ConnectResponse`](crate::ConnectResponse).
#[derive(Clone, Default, Debug)]
pub struct ConnAckData {
    /// The URI of the server.
    pub server_uri: String,
    /// The MQTT protocol version of the connection.
    pub mqtt_version: u32,
    /// Whether the broker already held a session for this client ID.
    pub session_present: bool,
}

/////////////////////////////////////////////////////////////////////////////
// ServerResponse

/// Raw response from the C library, used internally by the token layer.
///
/// The typed public responses ([`ConnectResponse`](crate::ConnectResponse),
/// [`SubscribeResponse`](crate::SubscribeResponse), etc.) are produced from
/// this by the convert functions in [`responses`](crate::responses).
#[derive(Clone, Default, Debug)]
pub struct ServerResponse {
    /// Request-specific payload
    rsp: RequestResponse,
    /// MQTT v5 Properties
    props: Properties,
    /// MQTT v5 Reason Code
    reason_code: ReasonCode,
}

impl ServerResponse {
    /// Creates a new, empty server response.
    pub fn new() -> Self {
        ServerResponse::default()
    }

    /// Constructs a response from the v3.1.1 C success callback.
    ///
    /// # Safety
    ///
    /// Must only be called from the C `MQTTAsync_onSuccess` callback with a
    /// valid, non-null `rsp` pointer.
    pub unsafe fn from_success(
        req: ServerRequest,
        rsp: &ffi::MQTTAsync_successData,
    ) -> Self {
        use ServerRequest::*;

        let rsp = unsafe {
            match req {
                Connect => {
                    let data = ConnAckData {
                        server_uri: CStr::from_ptr(rsp.alt.connect.serverURI)
                            .to_string_lossy()
                            .to_string(),
                        mqtt_version: rsp.alt.connect.MQTTVersion as u32,
                        session_present: from_c_bool(rsp.alt.connect.sessionPresent),
                    };
                    RequestResponse::Connect(data)
                }
                Subscribe => RequestResponse::Subscribe(rsp.alt.qos),
                SubscribeMany(n) => {
                    let mut qosv = Vec::new();
                    if n == 1 {
                        qosv.push(rsp.alt.qos);
                    }
                    else if !rsp.alt.qosList.is_null() {
                        for i in 0..n {
                            qosv.push(*rsp.alt.qosList.add(i));
                        }
                    }
                    debug!("Subscribed to {} topics w/ QoS: {:?}", qosv.len(), qosv);
                    RequestResponse::SubscribeMany(qosv)
                }
                _ => RequestResponse::None,
            }
        };
        Self {
            rsp,
            props: Properties::new(),
            reason_code: ReasonCode::default(),
        }
    }

    /// Constructs a response from the MQTT v5 C success callback.
    ///
    /// # Safety
    ///
    /// Must only be called from the C `MQTTAsync_onSuccess5` callback with a
    /// valid, non-null `rsp` pointer.
    pub unsafe fn from_success5(
        req: ServerRequest,
        rsp: &ffi::MQTTAsync_successData5,
    ) -> Self {
        use ServerRequest::*;

        let props = Properties::from(&rsp.properties);
        let reason_code = ReasonCode::from(rsp.reasonCode);

        let rsp = unsafe {
            match req {
                Connect => {
                    let data = ConnAckData {
                        server_uri: CStr::from_ptr(rsp.alt.connect.serverURI)
                            .to_string_lossy()
                            .to_string(),
                        mqtt_version: rsp.alt.connect.MQTTVersion as u32,
                        session_present: from_c_bool(rsp.alt.connect.sessionPresent),
                    };
                    RequestResponse::Connect(data)
                }
                Subscribe => RequestResponse::Subscribe(rsp.reasonCode as i32),
                SubscribeMany(n) => {
                    let ncode = rsp.alt.sub.reasonCodeCount as usize;
                    debug_assert!(n == ncode);
                    let n = std::cmp::min(n, ncode);

                    let mut qosv = Vec::new();
                    if n == 1 {
                        qosv.push(rsp.reasonCode as i32);
                    }
                    else if !rsp.alt.sub.reasonCodes.is_null() {
                        for i in 0..n {
                            qosv.push(*rsp.alt.sub.reasonCodes.add(i) as i32);
                        }
                    }
                    debug!("Subscribed to {} topics w/ QoS: {:?}", qosv.len(), qosv);
                    RequestResponse::SubscribeMany(qosv)
                }
                Unsubscribe => RequestResponse::Unsubscribe(rsp.reasonCode as i32),
                UnsubscribeMany(n) => {
                    let ncode = rsp.alt.unsub.reasonCodeCount as usize;
                    debug!("Server returned {} unsubscribe codes", ncode);
                    debug_assert!(n == ncode);
                    let n = std::cmp::min(n, ncode);

                    let mut qosv = Vec::new();
                    if n == 1 {
                        qosv.push(rsp.reasonCode as i32);
                    }
                    else if !rsp.alt.unsub.reasonCodes.is_null() {
                        for i in 0..n {
                            qosv.push(*rsp.alt.unsub.reasonCodes.add(i) as i32);
                        }
                    }
                    debug!(
                        "Unsubscribed from {} topics w/ codes: {:?}",
                        qosv.len(),
                        qosv
                    );
                    RequestResponse::UnsubscribeMany(qosv)
                }
                _ => RequestResponse::None,
            }
        };
        Self {
            rsp,
            props,
            reason_code,
        }
    }

    /// Constructs a response from the MQTT v5 C failure callback.
    ///
    /// # Safety
    ///
    /// Must only be called from the C `MQTTAsync_onFailure5` callback with a
    /// valid, non-null `rsp` pointer.
    pub unsafe fn from_failure5(rsp: &ffi::MQTTAsync_failureData5) -> Self {
        Self {
            rsp: RequestResponse::default(),
            props: Properties::from(&rsp.properties),
            reason_code: rsp.reasonCode.into(),
        }
    }

    /// Returns the request-specific response payload.
    pub fn request_response(&self) -> &RequestResponse {
        &self.rsp
    }

    /// Returns the CONNACK data if this response is from a CONNECT operation.
    pub fn connect_data(&self) -> Option<ConnAckData> {
        match &self.rsp {
            RequestResponse::Connect(data) => Some(data.clone()),
            _ => None,
        }
    }

    /// Returns the MQTT v5 properties.
    pub fn properties(&self) -> &Properties {
        &self.props
    }

    /// Returns the MQTT v5 reason code.
    pub fn reason_code(&self) -> ReasonCode {
        self.reason_code
    }
}
