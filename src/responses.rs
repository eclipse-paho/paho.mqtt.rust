// paho-mqtt/src/responses.rs
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

//! Typed response objects for each MQTT client operation.
//!
//! Each async operation returns a [`Token<T>`](crate::Token) where `T` is one of
//! the response types defined here.

use crate::{
    Properties, ReasonCode, Result,
    server_response::{RequestResponse, ServerResponse},
};

/// Response data from a CONNECT → CONNACK exchange.
#[derive(Clone, Default, Debug)]
pub struct ConnectResponse {
    /// The URI of the server.
    pub server_uri: String,
    /// The MQTT protocol version granted by the server.
    pub mqtt_version: u32,
    /// Whether the client session is already present on the server.
    pub session_present: bool,
    /// Reason code from the CONNACK (MQTT v5 only). Always `Success` on v3.1.1.
    pub reason_code: ReasonCode,
    /// MQTT v5 properties from the CONNACK. Empty on v3.1.1.
    pub properties: Properties,
}

/// Response data for a completed PUBLISH operation.
///
/// For QoS 0 the token resolves immediately with defaults. For QoS 1 the data
/// comes from the PUBACK packet; for QoS 2 from the PUBCOMP.
#[derive(Clone, Default, Debug)]
pub struct PublishResponse {
    /// Reason code from PUBACK or PUBCOMP (MQTT v5 only). `Success` on v3.1.1.
    pub reason_code: ReasonCode,
    /// MQTT v5 properties. Empty on v3.1.1.
    pub properties: Properties,
}

/// Response data from a SUBSCRIBE → SUBACK exchange.
#[derive(Clone, Default, Debug)]
pub struct SubscribeResponse {
    /// Per-topic reason codes from the SUBACK.
    ///
    /// For MQTT v3.1.1 these carry the granted QoS value cast to `ReasonCode`;
    /// for MQTT v5 these are the actual v5 reason codes.
    pub reason_codes: Vec<ReasonCode>,
    /// MQTT v5 properties from the SUBACK. Empty on v3.1.1.
    pub properties: Properties,
}

/// Response data from an UNSUBSCRIBE → UNSUBACK exchange.
#[derive(Clone, Default, Debug)]
pub struct UnsubscribeResponse {
    /// Per-topic reason codes from the UNSUBACK (MQTT v5 only). Empty on v3.1.1.
    pub reason_codes: Vec<ReasonCode>,
    /// MQTT v5 properties from the UNSUBACK. Empty on v3.1.1.
    pub properties: Properties,
}

// ---- convert functions -------------------------------------------------------

/// Converts a raw [`ServerResponse`] to a typed [`ConnectResponse`].
pub(crate) fn connect_convert(rsp: ServerResponse) -> Result<ConnectResponse> {
    let data = rsp.connect_data().unwrap_or_default();
    Ok(ConnectResponse {
        server_uri: data.server_uri,
        mqtt_version: data.mqtt_version,
        session_present: data.session_present,
        reason_code: rsp.reason_code(),
        properties: rsp.properties().clone(),
    })
}

/// Converts a raw [`ServerResponse`] to a typed [`PublishResponse`].
pub(crate) fn publish_convert(rsp: ServerResponse) -> Result<PublishResponse> {
    Ok(PublishResponse {
        reason_code: rsp.reason_code(),
        properties: rsp.properties().clone(),
    })
}

/// Converts a raw [`ServerResponse`] to a typed [`SubscribeResponse`].
pub(crate) fn subscribe_convert(rsp: ServerResponse) -> Result<SubscribeResponse> {
    let reason_codes = match rsp.request_response() {
        RequestResponse::Subscribe(qos) => vec![ReasonCode::from(*qos as u8)],
        RequestResponse::SubscribeMany(qosv) => {
            qosv.iter().map(|&q| ReasonCode::from(q as u8)).collect()
        }
        _ => vec![rsp.reason_code()],
    };
    Ok(SubscribeResponse {
        reason_codes,
        properties: rsp.properties().clone(),
    })
}

/// Converts a raw [`ServerResponse`] to a typed [`UnsubscribeResponse`].
pub(crate) fn unsubscribe_convert(rsp: ServerResponse) -> Result<UnsubscribeResponse> {
    let reason_codes = match rsp.request_response() {
        RequestResponse::Unsubscribe(qos) => vec![ReasonCode::from(*qos as u8)],
        RequestResponse::UnsubscribeMany(qosv) => {
            qosv.iter().map(|&q| ReasonCode::from(q as u8)).collect()
        }
        _ => vec![],
    };
    Ok(UnsubscribeResponse {
        reason_codes,
        properties: rsp.properties().clone(),
    })
}

/// Discards the response value for operations that return `Token<()>`.
pub(crate) fn unit_convert(_rsp: ServerResponse) -> Result<()> {
    Ok(())
}
