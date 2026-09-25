// paho-mqtt-codec/src/reason_code.rs
//
// This file is part of the Eclipse Paho MQTT Rust Codec library.

/*******************************************************************************
 * Copyright (c) 2019-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! MQTT v5 reason codes.

use crate::{PacketType, error::Error};
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// MQTT v5 single-byte reason codes.
///
/// These are used in MQTT v5 packets to indicate the result of an operation.
/// Codes below 0x80 indicate success; 0x80 and above indicate an error.
/// In MQTT v3, the connect return code is mapped to the nearest equivalent
/// reason code.
#[repr(u8)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
    /// The client identifier is a valid string but is not allowed by the server.
    ClientIdentifierNotValid = 0x85,
    /// The server does not accept the user name or password specified by the client.
    BadUserNameOrPassword = 0x86,
    /// The request is not authorized.
    NotAuthorized = 0x87,
    /// The MQTT server is not available.
    ServerUnavailable = 0x88,
    /// The server is busy. Try again later.
    ServerBusy = 0x89,
    /// This client has been banned by administrative action.
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
    /// The packet identifier is already in use.
    PacketIdentifierInUse = 0x91,
    /// The packet identifier is not known.
    PacketIdentifierNotFound = 0x92,
    /// The client or server has received more than the receive maximum
    /// it sent in the CONNECT or CONNACK packet.
    ReceiveMaximumExceeded = 0x93,
    /// The topic alias was greater than the maximum topic alias sent in
    /// the CONNECT or CONNACK packet.
    TopicAliasInvalid = 0x94,
    /// The packet size is greater than the maximum packet size for this client or server.
    PacketTooLarge = 0x95,
    /// The received data rate is too high.
    MessageRateTooHigh = 0x96,
    /// An implementation or administrative imposed limit has been exceeded.
    QuotaExceeded = 0x97,
    /// The connection is closed due to an administrative action.
    AdministrativeAction = 0x98,
    /// The payload format does not match the one specified in the payload format indicator.
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
    /// The maximum connection time authorized for this connection has been exceeded.
    MaximumConnectTime = 0xA0,
    /// The server does not support subscription identifiers.
    SubscriptionIdentifiersNotSupported = 0xA1,
    /// The server does not support wildcard subscriptions.
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl ReasonCode {
    /// Reason code for a normal disconnect (alias for [`Success`](Self::Success)).
    #[allow(non_upper_case_globals)]
    pub const NormalDisconnection: ReasonCode = ReasonCode::Success;

    /// Reason code for QoS 0 granted (alias for [`Success`](Self::Success)).
    #[allow(non_upper_case_globals)]
    pub const GrantedQos0: ReasonCode = ReasonCode::Success;

    /// Returns `true` for codes below 0x80, which indicate success.
    pub fn is_ok(&self) -> bool {
        (*self as u8) < 0x80
    }

    /// Returns `true` for codes of 0x80 or greater, which indicate failure.
    pub fn is_err(&self) -> bool {
        (*self as u8) >= 0x80
    }

    /// Returns `true` if the specification lists this code for packets of
    /// type `ty`.
    ///
    /// Each packet type that carries a reason code has its own allowed set
    /// (MQTT v5 §2.4 and the per-packet tables): a PUBACK cannot say
    /// `SessionTakenOver`, a DISCONNECT cannot say `GrantedQos1`, and so on.
    /// Packet types that never carry a reason code return `false` for every
    /// code.
    ///
    /// The sets are compile-time bitsets; this is one load and one shift.
    #[inline]
    pub fn is_allowed_in(self, ty: PacketType) -> bool {
        let code = self as u8;
        let set = &ALLOWED[ty as usize];
        (set[(code >> 6) as usize] >> (code & 63)) & 1 != 0
    }
}

/// A 256-bit set of reason-code values.
type CodeSet = [u64; 4];

const fn code_set(codes: &[ReasonCode]) -> CodeSet {
    let mut set = [0u64; 4];
    let mut i = 0;
    while i < codes.len() {
        let c = codes[i] as u8;
        set[(c >> 6) as usize] |= 1 << (c & 63);
        i += 1;
    }
    set
}

/// Allowed reason codes per packet type, indexed by `PacketType as usize`
/// (index 0 unused). MQTT v5 §3.2.2.2, 3.4.2.1, 3.5.2.1, 3.6.2.1, 3.7.2.1,
/// 3.9.3, 3.11.3, 3.14.2.1, 3.15.2.1.
const ALLOWED: [CodeSet; 16] = {
    use ReasonCode::*;
    let mut t = [[0u64; 4]; 16];
    t[PacketType::ConnAck as usize] = code_set(&[
        Success,
        UnspecifiedError,
        MalformedPacket,
        ProtocolError,
        ImplementationSpecificError,
        UnsupportedProtocolVersion,
        ClientIdentifierNotValid,
        BadUserNameOrPassword,
        NotAuthorized,
        ServerUnavailable,
        ServerBusy,
        Banned,
        BadAuthenticationMethod,
        TopicNameInvalid,
        PacketTooLarge,
        QuotaExceeded,
        PayloadFormatInvalid,
        RetainNotSupported,
        QosNotSupported,
        UseAnotherServer,
        ServerMoved,
        ConnectionRateExceeded,
    ]);
    let pub_ack = code_set(&[
        Success,
        NoMatchingSubscribers,
        UnspecifiedError,
        ImplementationSpecificError,
        NotAuthorized,
        TopicNameInvalid,
        PacketIdentifierInUse,
        QuotaExceeded,
        PayloadFormatInvalid,
    ]);
    t[PacketType::PubAck as usize] = pub_ack;
    t[PacketType::PubRec as usize] = pub_ack;
    let pub_rel = code_set(&[Success, PacketIdentifierNotFound]);
    t[PacketType::PubRel as usize] = pub_rel;
    t[PacketType::PubComp as usize] = pub_rel;
    t[PacketType::SubAck as usize] = code_set(&[
        Success,
        GrantedQos1,
        GrantedQos2,
        UnspecifiedError,
        ImplementationSpecificError,
        NotAuthorized,
        TopicFilterInvalid,
        PacketIdentifierInUse,
        QuotaExceeded,
        SharedSubscriptionsNotSupported,
        SubscriptionIdentifiersNotSupported,
        WildcardSubscriptionsNotSupported,
    ]);
    t[PacketType::UnsubAck as usize] = code_set(&[
        Success,
        NoSubscriptionFound,
        UnspecifiedError,
        ImplementationSpecificError,
        NotAuthorized,
        TopicFilterInvalid,
        PacketIdentifierInUse,
    ]);
    t[PacketType::Disconnect as usize] = code_set(&[
        Success,
        DisconnectWithWillMessage,
        UnspecifiedError,
        MalformedPacket,
        ProtocolError,
        ImplementationSpecificError,
        NotAuthorized,
        ServerBusy,
        ServerShuttingDown,
        KeepAliveTimeout,
        SessionTakenOver,
        TopicFilterInvalid,
        TopicNameInvalid,
        ReceiveMaximumExceeded,
        TopicAliasInvalid,
        PacketTooLarge,
        MessageRateTooHigh,
        QuotaExceeded,
        AdministrativeAction,
        PayloadFormatInvalid,
        RetainNotSupported,
        QosNotSupported,
        UseAnotherServer,
        ServerMoved,
        SharedSubscriptionsNotSupported,
        ConnectionRateExceeded,
        MaximumConnectTime,
        SubscriptionIdentifiersNotSupported,
        WildcardSubscriptionsNotSupported,
    ]);
    t[PacketType::Auth as usize] = code_set(&[Success, ContinueAuthentication, ReAuthenticate]);
    t
};

impl TryFrom<u8> for ReasonCode {
    type Error = Error;

    /// Converts a wire byte into a reason code.
    ///
    /// Returns [`Error::ProtocolError`] for any value not defined by the
    /// MQTT v5 specification.
    fn try_from(code: u8) -> Result<Self, Error> {
        use ReasonCode::*;
        let rc = match code {
            0x00 => Success,
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
            _ => return Err(Error::ProtocolError),
        };
        Ok(rc)
    }
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ReasonCode::*;
        let s = match self {
            Success => "Success",
            GrantedQos1 => "Granted QoS 1",
            GrantedQos2 => "Granted QoS 2",
            DisconnectWithWillMessage => "Disconnect with Will Message",
            NoMatchingSubscribers => "No matching subscribers",
            NoSubscriptionFound => "No subscription found",
            ContinueAuthentication => "Continue authentication",
            ReAuthenticate => "Re-authenticate",
            UnspecifiedError => "Unspecified error",
            MalformedPacket => "Malformed Packet",
            ProtocolError => "Protocol error",
            ImplementationSpecificError => "Implementation specific error",
            UnsupportedProtocolVersion => "Unsupported Protocol Version",
            ClientIdentifierNotValid => "Client Identifier not valid",
            BadUserNameOrPassword => "Bad User Name or Password",
            NotAuthorized => "Not authorized",
            ServerUnavailable => "Server unavailable",
            ServerBusy => "Server busy",
            Banned => "Banned",
            ServerShuttingDown => "Server shutting down",
            BadAuthenticationMethod => "Bad authentication method",
            KeepAliveTimeout => "Keep Alive timeout",
            SessionTakenOver => "Session taken over",
            TopicFilterInvalid => "Topic filter invalid",
            TopicNameInvalid => "Topic name invalid",
            PacketIdentifierInUse => "Packet Identifier in use",
            PacketIdentifierNotFound => "Packet Identifier not found",
            ReceiveMaximumExceeded => "Receive Maximum exceeded",
            TopicAliasInvalid => "Topic Alias invalid",
            PacketTooLarge => "Packet too large",
            MessageRateTooHigh => "Message rate too high",
            QuotaExceeded => "Quota exceeded",
            AdministrativeAction => "Administrative action",
            PayloadFormatInvalid => "Payload format invalid",
            RetainNotSupported => "Retain not supported",
            QosNotSupported => "QoS not supported",
            UseAnotherServer => "Use another server",
            ServerMoved => "Server moved",
            SharedSubscriptionsNotSupported => "Shared subscriptions not supported",
            ConnectionRateExceeded => "Connection rate exceeded",
            MaximumConnectTime => "Maximum connect time",
            SubscriptionIdentifiersNotSupported => "Subscription Identifiers not supported",
            WildcardSubscriptionsNotSupported => "Wildcard Subscriptions not supported",
        };
        write!(f, "{}", s)
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn test_try_from_u8_known() {
        assert_eq!(ReasonCode::try_from(0x00_u8), Ok(ReasonCode::Success));
        assert_eq!(
            ReasonCode::try_from(0x04_u8),
            Ok(ReasonCode::DisconnectWithWillMessage)
        );
        assert_eq!(
            ReasonCode::try_from(0x80_u8),
            Ok(ReasonCode::UnspecifiedError)
        );
        assert_eq!(
            ReasonCode::try_from(0xA0_u8),
            Ok(ReasonCode::MaximumConnectTime)
        );
        assert_eq!(
            ReasonCode::try_from(0xA2_u8),
            Ok(ReasonCode::WildcardSubscriptionsNotSupported)
        );
    }

    #[test]
    fn test_try_from_u8_unknown_is_err() {
        // Gaps inside the success range must not be reinterpreted as errors.
        for b in [0x03_u8, 0x05, 0x0F, 0x12, 0x17, 0x1A, 0x7F] {
            assert_eq!(ReasonCode::try_from(b), Err(Error::ProtocolError), "{b:#x}");
        }
        // Undefined values above the last defined code.
        for b in [0xA3_u8, 0xC0, 0xFF] {
            assert_eq!(ReasonCode::try_from(b), Err(Error::ProtocolError), "{b:#x}");
        }
    }

    #[test]
    fn test_try_from_u8_round_trips_every_variant() {
        // Every defined code must survive `as u8` -> try_from.
        for b in 0..=u8::MAX {
            if let Ok(rc) = ReasonCode::try_from(b) {
                assert_eq!(rc as u8, b);
            }
        }
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

    #[test]
    fn test_display() {
        assert_eq!(ReasonCode::GrantedQos2.to_string(), "Granted QoS 2");
        assert_eq!(
            ReasonCode::UnspecifiedError.to_string(),
            "Unspecified error"
        );
        assert_eq!(ReasonCode::Banned.to_string(), "Banned");
    }
}
