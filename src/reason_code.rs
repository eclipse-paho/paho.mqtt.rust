// paho-mqtt/src/reason_code.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2019-2024 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! The Reason Code module for the Paho MQTT Rust client library.

use std::{ffi::CStr, fmt};

/// MQTT v5 single-byte reason codes.
#[repr(u8)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReasonCode {
    /// The operation completed successfully.
    /// Also used as `NormalDisconnection` and `GrantedQos0`.
    #[default]
    Success = 0x00,
    /// The subscription was accepted and the maximum QoS sent will be 1.
    GrantedQos1 = 0x01,
    /// The subscription was accepted and the maximum QoS sent will be 2.
    GrantedQos2 = 0x02,
    /// The client wishes to disconnect but requires that the server also
    /// publishes its Will message.
    DisconnectWithWillMessage = 0x04,
    /// The message was accepted but there are no subscribers.
    /// Only sent by the broker if the broker was configured to send
    /// this when there are no matching subscribers.
    NoMatchingSubscribers = 0x10,
    /// No matching topic filter is being used by the client.
    /// Only sent by the broker in response to an UNSUBSCRIBE packet.
    NoSubscriptionFound = 0x11,
    /// Continue the authentication with another step.
    ContinueAuthentication = 0x18,
    /// Initiate re-authentication.
    ReAuthenticate = 0x19,

    /// The server does not wish to reveal the reason for the failure,
    /// or none of the other reason codes apply.
    UnspecifiedError = 0x80,
    /// Data within the packet could not be correctly parsed.
    MalformedPacket = 0x81,
    /// Data in the packet does not conform to the MQTT specification.
    ProtocolError = 0x82,
    /// An operation is not accepted and the server is not willing to
    /// reveal the reason.
    ImplementationSpecificError = 0x83,
    /// The server does not support the version of the MQTT protocol
    /// requested by the client.
    UnsupportedProtocolVersion = 0x84,
    /// The client identifier is a valid string but is not allowed by the
    /// server.
    ClientIdentifierNotValid = 0x85,
    /// The server does not accept the user name or password specified
    /// by the client.
    BadUserNameOrPassword = 0x86,
    /// The request is not authorized.
    NotAuthorized = 0x87,
    /// The MQTT server is not available.
    ServerUnavailable = 0x88,
    /// The server is busy. Try again later.
    ServerBusy = 0x89,
    /// This client has been banned by administrative action. Contact the
    /// server operator.
    Banned = 0x8A,
    /// The server is shutting down.
    ServerShuttingDown = 0x8B,
    /// The authentication method is not supported or does not match the
    /// authentication method currently in use.
    BadAuthenticationMethod = 0x8C,
    /// The connection is closed because no packet has been received for
    /// 1.5x the keep-alive time.
    KeepAliveTimeout = 0x8D,
    /// Another connection using the same client ID has connected, causing
    /// this connection to be closed.
    SessionTakenOver = 0x8E,
    /// The topic filter format is not allowed by the server.
    TopicFilterInvalid = 0x8F,
    /// The topic name is not accepted by the client or server.
    TopicNameInvalid = 0x90,
    /// The packet identifier is already in use. This might indicate a
    /// mismatch in the session state between the client and server.
    PacketIdentifierInUse = 0x91,
    /// The packet identifier is not known. This is not an error during
    /// recovery; it is a sign of mismatch between the session state on
    /// the client and server.
    PacketIdentifierNotFound = 0x92,
    /// The client or server has received more than the receive maximum
    /// it sent in the CONNECT or CONNACK packet.
    ReceiveMaximumExceeded = 0x93,
    /// The topic alias was greater than the maximum topic alias sent in
    /// the CONNECT or CONNACK packet.
    TopicAliasInvalid = 0x94,
    /// The packet size is greater than the maximum packet size for this
    /// client or server.
    PacketTooLarge = 0x95,
    /// The received data rate is too high.
    MessageRateTooHigh = 0x96,
    /// An implementation or administrative imposed limit has been exceeded.
    QuotaExceeded = 0x97,
    /// The connection is closed due to an administrative action.
    AdministrativeAction = 0x98,
    /// The payload format does not match the one specified in the
    /// payload format indicator.
    PayloadFormatInvalid = 0x99,
    /// The server does not support retained messages.
    RetainNotSupported = 0x9A,
    /// The client specified a QoS greater than the QoS specified in a
    /// maximum QoS in the CONNACK.
    QosNotSupported = 0x9B,
    /// The client should temporarily use another server.
    UseAnotherServer = 0x9C,
    /// The client should permanently use another server.
    ServerMoved = 0x9D,
    /// The server does not support shared subscriptions.
    SharedSubscriptionsNotSupported = 0x9E,
    /// The connection rate limit has been exceeded.
    ConnectionRateExceeded = 0x9F,
    /// The maximum connection time authorized for this connection has
    /// been exceeded.
    MaximumConnectTime = 0xA0,
    /// The server does not support subscription identifiers.
    /// The subscription is not accepted.
    SubscriptionIdentifiersNotSupported = 0xA1,
    /// The server does not support wildcard subscriptions; the subscription
    /// is not accepted.
    WildcardSubscriptionsNotSupported = 0xA2,
    /// Not a protocol-defined reason code. Used internally by the Paho C
    /// library for MQTT v3 error codes.
    MqttppV3Code = 0xFF,
}

// Some aliased ReasonCode values
impl ReasonCode {
    /// Reason code for a normal disconnect
    #[allow(non_upper_case_globals)]
    pub const NormalDisconnection: ReasonCode = ReasonCode::Success;

    /// Reason code for QoS 0 granted
    #[allow(non_upper_case_globals)]
    pub const GrantedQos0: ReasonCode = ReasonCode::Success;
}

impl ReasonCode {
    /// Reason codes less than 0x80 indicate a successful operation.
    pub fn is_ok(&self) -> bool {
        (*self as u32) < 0x80
    }

    /// Reason codes of 0x80 or greater indicate failure.
    pub fn is_err(&self) -> bool {
        (*self as u32) >= 0x80
    }
}

impl From<ffi::MQTTReasonCodes> for ReasonCode {
    fn from(code: ffi::MQTTReasonCodes) -> Self {
        use ReasonCode::*;
        match code {
            0x00 => Success, // also: NormalDisconnection & GrantedQos0
            0x01 => GrantedQos1,
            0x02 => GrantedQos2,
            0x04 => DisconnectWithWillMessage,
            0x10 => NoMatchingSubscribers,
            0x11 => NoSubscriptionFound,
            0x18 => ContinueAuthentication,
            0x19 => ReAuthenticate,

            0x80 => UnspecifiedError,
            0x81 => MalformedPacket,
            0x82 => ProtocolError,
            0x83 => ImplementationSpecificError,
            0x84 => UnsupportedProtocolVersion,
            0x85 => ClientIdentifierNotValid,
            0x86 => BadUserNameOrPassword,
            0x87 => NotAuthorized,
            0x88 => ServerUnavailable,
            0x89 => ServerBusy,
            0x8A => Banned,
            0x8B => ServerShuttingDown,
            0x8C => BadAuthenticationMethod,
            0x8D => KeepAliveTimeout,
            0x8E => SessionTakenOver,
            0x8F => TopicFilterInvalid,
            0x90 => TopicNameInvalid,
            0x91 => PacketIdentifierInUse,
            0x92 => PacketIdentifierNotFound,
            0x93 => ReceiveMaximumExceeded,
            0x94 => TopicAliasInvalid,
            0x95 => PacketTooLarge,
            0x96 => MessageRateTooHigh,
            0x97 => QuotaExceeded,
            0x98 => AdministrativeAction,
            0x99 => PayloadFormatInvalid,
            0x9A => RetainNotSupported,
            0x9B => QosNotSupported,
            0x9C => UseAnotherServer,
            0x9D => ServerMoved,
            0x9E => SharedSubscriptionsNotSupported,
            0x9F => ConnectionRateExceeded,
            0xA0 => MaximumConnectTime,
            0xA1 => SubscriptionIdentifiersNotSupported,
            0xA2 => WildcardSubscriptionsNotSupported,
            _ => MqttppV3Code, // This is not a protocol code; used internally by the library
        }
    }
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe {
            let p = ffi::MQTTReasonCode_toString(*self as ffi::MQTTReasonCodes);

            if p.is_null() {
                write!(f, "Unknown")
            }
            else {
                let s = CStr::from_ptr(p).to_string_lossy();
                write!(f, "{}", s)
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as() {
        assert_eq!(
            ReasonCode::Success as ffi::MQTTReasonCodes,
            ffi::MQTTReasonCodes_MQTTREASONCODE_SUCCESS
        );

        assert_eq!(
            ReasonCode::DisconnectWithWillMessage as ffi::MQTTReasonCodes,
            ffi::MQTTReasonCodes_MQTTREASONCODE_DISCONNECT_WITH_WILL_MESSAGE
        );

        assert_eq!(
            ReasonCode::UnspecifiedError as ffi::MQTTReasonCodes,
            ffi::MQTTReasonCodes_MQTTREASONCODE_UNSPECIFIED_ERROR
        );

        assert_eq!(
            ReasonCode::MaximumConnectTime as ffi::MQTTReasonCodes,
            ffi::MQTTReasonCodes_MQTTREASONCODE_MAXIMUM_CONNECT_TIME
        );
    }

    #[test]
    fn test_from() {
        assert_eq!(
            ReasonCode::Success,
            ReasonCode::from(ffi::MQTTReasonCodes_MQTTREASONCODE_SUCCESS)
        );

        assert_eq!(
            ReasonCode::DisconnectWithWillMessage,
            ReasonCode::from(ffi::MQTTReasonCodes_MQTTREASONCODE_DISCONNECT_WITH_WILL_MESSAGE)
        );

        assert_eq!(
            ReasonCode::UnspecifiedError,
            ReasonCode::from(ffi::MQTTReasonCodes_MQTTREASONCODE_UNSPECIFIED_ERROR)
        );

        assert_eq!(
            ReasonCode::MaximumConnectTime,
            ReasonCode::from(ffi::MQTTReasonCodes_MQTTREASONCODE_MAXIMUM_CONNECT_TIME)
        );
    }

    #[test]
    fn test_is_ok() {
        assert!(ReasonCode::Success.is_ok());
        assert!(ReasonCode::ReAuthenticate.is_ok());

        assert!(!ReasonCode::UnspecifiedError.is_ok());
        assert!(!ReasonCode::ServerMoved.is_ok());
    }

    #[test]
    fn test_is_err() {
        assert!(!ReasonCode::Success.is_err());
        assert!(!ReasonCode::ReAuthenticate.is_err());

        assert!(ReasonCode::UnspecifiedError.is_err());
        assert!(ReasonCode::ServerMoved.is_err());
    }

    // Note: These strings are from the Paho C library in MQTTReasonCodes.c
    // They may change between versions, but we mainly want to see that
    // the Display trait is working.
    #[test]
    fn test_display() {
        let s = format!("{}", ReasonCode::GrantedQos2);
        assert_eq!(&s, "Granted QoS 2");

        let s = format!("{}", ReasonCode::UnspecifiedError);
        assert_eq!(&s, "Unspecified error");

        let s = format!("{}", ReasonCode::Banned);
        assert_eq!(&s, "Banned");
    }
}
