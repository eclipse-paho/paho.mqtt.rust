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

use std::time::Duration;

use crate::{Properties, PropertyId, PropertyValue, QoS, ReasonCode};

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

impl ConnectResponse {
    /// The client identifier the server assigned, when the client connected
    /// with an empty one (v5 `AssignedClientIdentifier`).
    ///
    /// [`AsyncClient::client_id`](crate::AsyncClient::client_id) already
    /// reports it, so this is only needed to tell an assigned identifier
    /// from a configured one.
    pub fn assigned_client_id(&self) -> Option<&str> {
        match self.properties.get(PropertyId::AssignedClientIdentifier) {
            Some(PropertyValue::String(s)) => Some(s),
            _ => None,
        }
    }

    /// The keep-alive the server imposed, if it overrode the requested one
    /// (v5 `ServerKeepAlive`). The session uses this value.
    pub fn server_keep_alive(&self) -> Option<Duration> {
        self.u16_prop(PropertyId::ServerKeepAlive)
            .map(|secs| Duration::from_secs(secs as u64))
    }

    /// How many QoS 1/2 publishes the server will accept unacknowledged at
    /// once (v5 `ReceiveMaximum`, default 65 535). The client caps its own
    /// in-flight window at this, narrowed further by `max_inflight`.
    pub fn receive_maximum(&self) -> u16 {
        self.u16_prop(PropertyId::ReceiveMaximum)
            .unwrap_or(u16::MAX)
    }

    /// The largest packet the server will accept (v5 `MaximumPacketSize`).
    /// `None` means no limit beyond the protocol maximum. A publish that
    /// would exceed it fails locally rather than being sent.
    pub fn maximum_packet_size(&self) -> Option<u32> {
        match self.properties.get(PropertyId::MaximumPacketSize) {
            Some(PropertyValue::U32(n)) => Some(*n),
            _ => None,
        }
    }

    /// The highest topic alias the server accepts (v5 `TopicAliasMaximum`).
    /// Zero — the default — means topic aliases may not be used.
    pub fn topic_alias_maximum(&self) -> u16 {
        self.u16_prop(PropertyId::TopicAliasMaximum).unwrap_or(0)
    }

    /// The highest QoS the server supports (v5 `MaximumQoS`, default
    /// [`QoS::ExactlyOnce`]). Publishing above it is refused by the server.
    pub fn maximum_qos(&self) -> QoS {
        match self.properties.get(PropertyId::MaximumQoS) {
            Some(PropertyValue::Byte(b)) => QoS::try_from(*b).unwrap_or(QoS::ExactlyOnce),
            _ => QoS::ExactlyOnce,
        }
    }

    /// Whether the server supports retained messages (v5 `RetainAvailable`,
    /// default `true`).
    pub fn retain_available(&self) -> bool {
        self.flag_prop(PropertyId::RetainAvailable)
    }

    /// Whether the server supports wildcard subscriptions (v5
    /// `WildcardSubscriptionAvailable`, default `true`).
    pub fn wildcard_subscriptions_available(&self) -> bool {
        self.flag_prop(PropertyId::WildcardSubscriptionAvailable)
    }

    /// Whether the server supports subscription identifiers (v5
    /// `SubscriptionIdentifierAvailable`, default `true`).
    pub fn subscription_identifiers_available(&self) -> bool {
        self.flag_prop(PropertyId::SubscriptionIdentifierAvailable)
    }

    /// Whether the server supports shared subscriptions (v5
    /// `SharedSubscriptionAvailable`, default `true`).
    pub fn shared_subscriptions_available(&self) -> bool {
        self.flag_prop(PropertyId::SharedSubscriptionAvailable)
    }

    /// Another server the client should use instead (v5 `ServerReference`),
    /// typically alongside a `UseAnotherServer` or `ServerMoved` reason code.
    pub fn server_reference(&self) -> Option<&str> {
        match self.properties.get(PropertyId::ServerReference) {
            Some(PropertyValue::String(s)) => Some(s),
            _ => None,
        }
    }

    /// A human-readable note from the server about the outcome (v5
    /// `ReasonString`). Diagnostic only — never for programmatic decisions.
    pub fn reason_string(&self) -> Option<&str> {
        match self.properties.get(PropertyId::ReasonString) {
            Some(PropertyValue::String(s)) => Some(s),
            _ => None,
        }
    }

    /// A one-byte "available" flag, which defaults to true when absent.
    fn flag_prop(&self, id: PropertyId) -> bool {
        !matches!(self.properties.get(id), Some(PropertyValue::Byte(0)))
    }

    fn u16_prop(&self, id: PropertyId) -> Option<u16> {
        match self.properties.get(id) {
            Some(PropertyValue::U16(n)) => Some(*n),
            _ => None,
        }
    }
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

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Property;

    fn with(props: Vec<Property>) -> ConnectResponse {
        ConnectResponse {
            properties: props.into_iter().collect(),
            ..ConnectResponse::default()
        }
    }

    /// An absent property means the specification's default, not "none".
    #[test]
    fn test_connack_capability_defaults() {
        let rsp = with(vec![]);
        assert_eq!(rsp.receive_maximum(), u16::MAX);
        assert_eq!(rsp.topic_alias_maximum(), 0);
        assert_eq!(rsp.maximum_qos(), QoS::ExactlyOnce);
        assert!(rsp.retain_available());
        assert!(rsp.wildcard_subscriptions_available());
        assert!(rsp.subscription_identifiers_available());
        assert!(rsp.shared_subscriptions_available());
        assert_eq!(rsp.maximum_packet_size(), None);
        assert_eq!(rsp.server_keep_alive(), None);
        assert_eq!(rsp.assigned_client_id(), None);
        assert_eq!(rsp.server_reference(), None);
        assert_eq!(rsp.reason_string(), None);
    }

    #[test]
    fn test_connack_capabilities_read_properties() {
        let rsp = with(vec![
            Property::new(PropertyId::ReceiveMaximum, PropertyValue::U16(10)),
            Property::new(PropertyId::TopicAliasMaximum, PropertyValue::U16(5)),
            Property::new(PropertyId::MaximumQoS, PropertyValue::Byte(1)),
            Property::new(PropertyId::RetainAvailable, PropertyValue::Byte(0)),
            Property::new(PropertyId::MaximumPacketSize, PropertyValue::U32(4096)),
            Property::new(PropertyId::ServerKeepAlive, PropertyValue::U16(30)),
            Property::new(
                PropertyId::AssignedClientIdentifier,
                PropertyValue::String("auto-1".into()),
            ),
            Property::new(
                PropertyId::SharedSubscriptionAvailable,
                PropertyValue::Byte(0),
            ),
        ]);
        assert_eq!(rsp.receive_maximum(), 10);
        assert_eq!(rsp.topic_alias_maximum(), 5);
        assert_eq!(rsp.maximum_qos(), QoS::AtLeastOnce);
        assert!(!rsp.retain_available());
        assert_eq!(rsp.maximum_packet_size(), Some(4096));
        assert_eq!(rsp.server_keep_alive(), Some(Duration::from_secs(30)));
        assert_eq!(rsp.assigned_client_id(), Some("auto-1"));
        assert!(!rsp.shared_subscriptions_available());
        // Untouched flags keep their defaults.
        assert!(rsp.wildcard_subscriptions_available());
    }
}
