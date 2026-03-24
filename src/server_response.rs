// paho-mqtt/src/server_response.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2018-2024 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! The Token module for the Paho MQTT Rust client library.
//!
//! Asynchronous operations return a `Token` that is a type of future. It
//! can be used to determine if an operation has completed, block and wait
//! for the operation to complete, and obtain the final result.
//! For example, you can start a connection, do something else, and then
//! wait for the connection to complete.
//!
//! The Token object implements the Future trait, and thus can be used and
//! combined with any other Rust futures.
//!

use crate::{ffi, from_c_bool, properties::Properties, reason_code::ReasonCode};
use std::ffi::CStr;

/////////////////////////////////////////////////////////////////////////////
// ServerRequest

/// The server requests that expect a response.
/// This is required because the `alt` union of the MQTTAsync_successData
/// struct from C library doesn't indicate which field is valid.
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
}

/////////////////////////////////////////////////////////////////////////////
// RequestResponse

/// The possible responses that may come back from the server, depending on
/// the type of request.
#[derive(Debug, Default, Clone)]
pub enum RequestResponse {
    /// No response from the server
    #[default]
    None,
    /// The server URI, MQTT version, and whether the session is present
    Connect(ConnectResponse),
    /// The granted QoS of the subscription
    Subscribe(i32),
    /// The granted QoS of all the subscriptions
    SubscribeMany(Vec<i32>),
    /// The granted QoS of the subscription
    Unsubscribe(i32),
    /// The granted QoS of all the subscriptions
    UnsubscribeMany(Vec<i32>),
}

/// The response from the server on a connect request.
#[derive(Clone, Default, Debug)]
pub struct ConnectResponse {
    /// The URI of the server.
    pub server_uri: String,
    /// The version of MQTT granted by the server.
    pub mqtt_version: u32,
    /// Whether the client session is already present on the server.
    pub session_present: bool,
}

/////////////////////////////////////////////////////////////////////////////
// ServerResponse

/// Responses coming back from the server from client requests.
#[derive(Clone, Default, Debug)]
pub struct ServerResponse {
    /// The request-specific response
    rsp: RequestResponse,
    /// MQTT v5 Properties
    props: Properties,
    /// MQTT v5 Reason Code
    reason_code: ReasonCode,
}

impl ServerResponse {
    /// Creates a new, empty, server response.
    pub fn new() -> Self {
        ServerResponse::default()
    }

    /// Creates the response object from the v3 "success" data structure
    /// sent by the C lib on completion of the operation.
    ///
    /// # Safety
    ///
    /// This function runs in the context of a C callback indicating a
    /// successful action. It directly accesses the pointers and unions
    /// in the C `MQTTAsync_successData` struct.
    ///
    pub unsafe fn from_success(req: ServerRequest, rsp: &ffi::MQTTAsync_successData) -> Self {
        use ServerRequest::*;

        let rsp = match req {
            Connect => {
                let conn_rsp = ConnectResponse {
                    server_uri: CStr::from_ptr(rsp.alt.connect.serverURI)
                        .to_string_lossy()
                        .to_string(),
                    mqtt_version: rsp.alt.connect.MQTTVersion as u32,
                    session_present: from_c_bool(rsp.alt.connect.sessionPresent),
                };
                RequestResponse::Connect(conn_rsp)
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
        };
        Self {
            rsp,
            props: Properties::new(),
            reason_code: ReasonCode::default(),
        }
    }

    /// Creates the response object from the v5 "success" data structure
    /// sent by the C lib on completion of the operation.
    ///
    /// # Safety
    ///
    /// This function runs in the context of a C callback indicating a
    /// successful v5 action. It directly accesses the pointers and unions
    /// in the C `MQTTAsync_successData5` struct.
    ///
    pub unsafe fn from_success5(req: ServerRequest, rsp: &ffi::MQTTAsync_successData5) -> Self {
        use ServerRequest::*;

        let props = Properties::from_c_struct(&rsp.properties);
        //debug!("Properties: {:?}", props);
        let reason_code = ReasonCode::from(rsp.reasonCode);

        let rsp = match req {
            Connect => {
                let conn_rsp = ConnectResponse {
                    server_uri: CStr::from_ptr(rsp.alt.connect.serverURI)
                        .to_string_lossy()
                        .to_string(),
                    mqtt_version: rsp.alt.connect.MQTTVersion as u32,
                    session_present: from_c_bool(rsp.alt.connect.sessionPresent),
                };
                RequestResponse::Connect(conn_rsp)
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
                else if !rsp.alt.sub.reasonCodes.is_null() {
                    for i in 0..n {
                        qosv.push(*rsp.alt.unsub.reasonCodes.add(i) as i32);
                    }
                }
                debug!("Subscribed to {} topics w/ Qos: {:?}", qosv.len(), qosv);
                RequestResponse::SubscribeMany(qosv)
            }
            _ => RequestResponse::None,
        };
        Self {
            rsp,
            props,
            reason_code,
        }
    }

    /// Gets the response for the specific type of request.
    pub fn request_response(&self) -> &RequestResponse {
        &self.rsp
    }

    /// Gets the response for a connection request
    pub fn connect_response(&self) -> Option<ConnectResponse> {
        match &self.rsp {
            RequestResponse::Connect(conn_rsp) => Some(conn_rsp.clone()),
            _ => None,
        }
    }

    /// Gets the response for a subscription request.
    pub fn subscribe_response(&self) -> Option<i32> {
        match &self.rsp {
            RequestResponse::Subscribe(qos) => Some(*qos),
            _ => None,
        }
    }

    /// Gets the response for a multi-topic subscription request.
    pub fn subscribe_many_response(&self) -> Option<Vec<i32>> {
        match &self.rsp {
            RequestResponse::SubscribeMany(qosv) => Some(qosv.clone()),
            _ => None,
        }
    }

    /// Gets the response for an unsubscribe request.
    pub fn unsubscribe_response(&self) -> Option<i32> {
        match &self.rsp {
            RequestResponse::Unsubscribe(qos) => Some(*qos),
            _ => None,
        }
    }

    /// Gets the response for a multi-topic unsubscribe request.
    pub fn unsubscribe_many_response(&self) -> Option<Vec<i32>> {
        match &self.rsp {
            RequestResponse::UnsubscribeMany(qosv) => Some(qosv.clone()),
            _ => None,
        }
    }

    /// Gets the properties returned from the server.
    pub fn properties(&self) -> &Properties {
        &self.props
    }

    /// Gets the reason code returned from the server.
    pub fn reason_code(&self) -> ReasonCode {
        self.reason_code
    }
}
