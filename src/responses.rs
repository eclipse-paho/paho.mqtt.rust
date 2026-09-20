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

use crate::{Properties, ReasonCode};

/// Response data from a CONNECT → CONNACK exchange.
#[derive(Clone, Default, Debug)]
pub struct ConnectResponse {
    /// The URI of the server.
    pub server_uri: String,
    /// The MQTT protocol version granted by the server.
    pub mqtt_version: u32,
    /// Whether the client session is already present on the server.
    pub session_present: bool,
    /// Reason code from the CONNACK (MQTT v5 only). Always `Success` on v3.
    pub reason_code: ReasonCode,
    /// MQTT v5 properties from the CONNACK. Empty on v3.
    pub properties: Properties,
}

/// Response data for a completed PUBLISH operation.
///
/// For QoS 0 the token resolves immediately with defaults. For QoS 1 the data
/// comes from the PUBACK packet; for QoS 2 from the PUBCOMP.
///
/// A response is only produced for a *successful* acknowledgement. When the
/// broker answers with an error reason code (0x80 and above — `NotAuthorized`,
/// `QuotaExceeded`, `TopicNameInvalid`, …) the token resolves to
/// [`Error::ReasonCode`](crate::Error::ReasonCode) carrying that code and the
/// acknowledgement's properties, so `reason_code` here is always a success
/// code (`Success` or `NoMatchingSubscribers`).
#[derive(Clone, Default, Debug)]
pub struct PublishResponse {
    /// Reason code from PUBACK or PUBCOMP (MQTT v5 only). `Success` on v3.
    pub reason_code: ReasonCode,
    /// MQTT v5 properties. Empty on v3.
    pub properties: Properties,
}

impl PublishResponse {
    /// Create a new response
    pub fn new(reason_code: ReasonCode, properties: Properties) -> Self {
        Self {
            reason_code,
            properties,
        }
    }
}

/// Response data from a SUBSCRIBE → SUBACK exchange.
///
/// How an error reason code is reported depends on the call that produced
/// the token. [`subscribe`](crate::AsyncClient::subscribe) has exactly one
/// filter, so an error code fails the token with
/// [`Error::ReasonCode`](crate::Error::ReasonCode) and this response is only
/// seen on success. [`subscribe_many`](crate::AsyncClient::subscribe_many)
/// may succeed for some filters and fail for others, so it always resolves
/// `Ok` and `reason_codes` must be inspected per filter — check each code
/// with `ReasonCode::is_err()`.
#[derive(Clone, Default, Debug)]
pub struct SubscribeResponse {
    /// Per-topic reason codes from the SUBACK.
    ///
    /// For MQTT v3 these carry the granted QoS value cast to `ReasonCode`;
    /// for MQTT v5 these are the actual v5 reason codes.
    pub reason_codes: Vec<ReasonCode>,
    /// MQTT v5 properties from the SUBACK. Empty on v3.
    pub properties: Properties,
}

impl SubscribeResponse {
    /// Create a new response
    pub fn new(reason_codes: Vec<ReasonCode>, properties: Properties) -> Self {
        Self {
            reason_codes,
            properties,
        }
    }
}

/// Response data from an UNSUBSCRIBE → UNSUBACK exchange.
///
/// Reported like [`SubscribeResponse`]: an error code fails an
/// [`unsubscribe`](crate::AsyncClient::unsubscribe) token, while
/// [`unsubscribe_many`](crate::AsyncClient::unsubscribe_many) always
/// resolves `Ok` with the per-filter codes. On v3 there are no codes, so an
/// unsubscribe never fails once acknowledged.
#[derive(Clone, Default, Debug)]
pub struct UnsubscribeResponse {
    /// Per-topic reason codes from the UNSUBACK (MQTT v5 only). Empty on v3.
    pub reason_codes: Vec<ReasonCode>,
    /// MQTT v5 properties from the UNSUBACK. Empty on v3.
    pub properties: Properties,
}

impl UnsubscribeResponse {
    /// Create a new response
    pub fn new(reason_codes: Vec<ReasonCode>, properties: Properties) -> Self {
        Self {
            reason_codes,
            properties,
        }
    }
}
