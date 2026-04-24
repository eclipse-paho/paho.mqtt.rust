// error.rs
//
// Error and Result types for the Paho MQTT Rust library.
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

use crate::{Message, Properties, ReasonCode};
use std::{fmt, io, result, str};
use thiserror::Error;

/// Connect Return Code
///
/// This is the 2nd byte of the variable header of the CONNACK packet
/// which indicates whether the server accepted the connection, and if not,
/// contains some information about why it rejected the request.
///
/// These are defined in MQTT v3.x. In v5, these are replaced with reason
/// codes which expand the number of reasons why the connection was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum ConnectReturnCode {
    /// The connection was accepted.
    Accepted = 0,
    /// The Server does not support the level of the MQTT protocol
    /// requested by the Client.
    UnacceptableProtocolVersion = 1,
    /// The Client identifier is correct UTF-8 but not allowed by the Server
    IdentifierRejected = 2,
    /// The Network Connection has been made but the MQTT service
    /// is unavailable
    ServerUnavailable = 3,
    /// The data in the user name or password is malformed
    BadUserNameOrPassword = 4,
    /// The Client is not authorized to connect
    NotAuthorized = 5,
}

impl fmt::Display for ConnectReturnCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ConnectReturnCode::*;
        let msg = match *self {
            Accepted => "Accepted",
            UnacceptableProtocolVersion => "Unacceptable Protocol Version",
            IdentifierRejected => "Client Identifier Rejected",
            ServerUnavailable => "Server Unavailable",
            BadUserNameOrPassword => "Bad User Name or Password",
            NotAuthorized => "Not Authorized",
        };
        write!(f, "{}", msg)
    }
}

impl TryFrom<u8> for ConnectReturnCode {
    type Error = Error;

    fn try_from(rc: u8) -> Result<Self> {
        use ConnectReturnCode::*;
        match rc {
            0 => Ok(Accepted),
            1 => Ok(UnacceptableProtocolVersion),
            2 => Ok(IdentifierRejected),
            3 => Ok(ServerUnavailable),
            4 => Ok(BadUserNameOrPassword),
            5 => Ok(NotAuthorized),
            _ => Err(Error::Failure),
        }
    }
}

/// The errors from an MQTT operation.
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum Error {
    // ---- From Paho failure return codes ----
    /// A generic error code indicating the failure of an MQTT client operation.
    #[error("General failure")]
    Failure,
    /// Persistence error
    #[error("Persistence error")]
    PersistenceError,
    /// The client is disconnected.
    #[error("Client disconnected")]
    Disconnected,
    /// The maximum number of messages allowed to be simultaneously
    /// in-flight has been reached.
    #[error("Maximum inflight messages")]
    MaxMessagesInflight,
    /// An invalid UTF-8 string has been detected.
    #[error("Bad UTF8 string")]
    BadUtf8String,
    /// A NULL parameter has been supplied to the underlying C library
    /// when this is invalid.
    #[error("NULL Parameter")]
    NullParameter,
    /// The topic has been truncated (the topic string includes embedded NULL
    /// characters). String functions will not access the full topic.
    /// Use the topic length value to access the full topic.
    #[error("Topic name truncated")]
    TopicNameTruncated,
    /// A structure parameter passed down to the C library  does not have the
    /// correct eyecatcher and version number.
    #[error("Bad structure")]
    BadStructure,
    /// An invalid QoS value was used not 0, 1, or 2)
    #[error("Bad QoS")]
    BadQos,
    /// All 64k MQTT message ID are currently in use
    #[error("No more message IDs")]
    NoMoreMsgIds,
    /// The request is being discarded when not complete
    #[error("Operation incomplete")]
    OperationIncomplete,
    /// The limit on the maximum number of message buffers has been reached.
    #[error("Max buffered messages")]
    MaxBufferedMessages,
    /// Attempting SSL connection using non-SSL version of library.
    #[error("SSL not supported by library")]
    SslNotSupported,
    /// A bad URL protocol/schema was requested/
    /// Note that the TLS enabled prefixes (ssl, mqtts, wss) are only
    /// valid if the TLS version of the library is linked int the app.
    #[error("Bad protocol")]
    BadProtocol,
    /// Don't use options for another version of MQTT
    #[error("Bad MQTT option")]
    BadMqttOption,
    /// Call not applicable to the client's version of MQTT
    #[error("Wrong MQTT version")]
    WrongMqttVersion,
    /// The LWT topic can not be zero length
    #[error("Zero length Will Topic")]
    ZeroLenWillTopic,
    /// Connect or disconnect command ignored because there is already a
    /// connect or disconnect command at the head of the list waiting to be
    /// processed. Use the onSuccess/onFailure callbacks to wait
    /// for the previous connect or disconnect command to be complete.
    #[error("Command Ignored")]
    CommandIgnored,
    /// The max number of buffered messages can not be zero.
    /// The library needs at least one slot for the outbound message.
    #[error("Max buffered is zero")]
    MaxBufferedZero,

    // ----- From Paho return code w/ string message -----
    /// The TCP connection timed out
    #[error("TCP connect timeout")]
    TcpConnectTimeout,
    /// The TCP connection failed to complete
    #[error("TCP connect completion failure")]
    TcpConnectCompletionFailure,
    /// TCP/TLS connection failure
    #[error("TCP/TLS connect failure")]
    TcpTlsConnectFailure,
    /// A socket error occurred
    #[error("Socket error")]
    SocketError(i32),
    // An MQTT v3 connect return (failure) code
    #[error("(0)")]
    ConnectReturn(ConnectReturnCode),
    #[error("Received disconnect {0}")]
    ReceivedDisconnect(i32),

    // ----- Errors from the Rust layer & libraries -----
    /// An error from the MQTT codec.
    #[error("{0}")]
    Protocol(#[from] paho_mqtt_codec::Error),
    /// A synchronous error when publishing creating or queuing the message.
    #[error("{}", error_message(*.0))]
    Publish(i32, Message),
    /// An MQTT v5 error from a reason code.
    /// Any reason code >= 0x80 indicates an error.
    #[error("{0}")]
    ReasonCode(ReasonCode, Properties),
    /// A bad topic filter
    #[error("Bad topic filter")]
    BadTopicFilter,
    /// An low-level I/O error
    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
    /// An error parsing a UTF-8 string
    #[error("String UTF-8 Error")]
    Utf8(#[from] str::Utf8Error),
    /// Missing SSL/TLS options
    #[error("Missing SSL/TLS options")]
    MissingSslOptions,
    /// Bad property, typically from improper value type for the code
    #[error("Bad v5 property")]
    BadProperty,
    /// Conversion error between types
    #[error("Conversion Error")]
    Conversion,
    /// A timeout, particularly from a synchronous operation.
    #[error("Timeout")]
    Timeout,
    /// A general error with description
    #[error("{0}")]
    General(&'static str),
    /// A general error with description
    #[error("{0}")]
    GeneralString(String),
}

impl Error {
    /// Create an error from a Paho C return code.
    ///
    /// These should not be confused with Connect Return Codes (v3.x) or
    /// Reason Codes (v5), although the C lib is sometimes vague about
    /// which integer value it is returning.
    pub fn from_return_code(rc: i32) -> Self {
        use Error::*;
        match rc {
            -2 => PersistenceError,
            -3 => Disconnected,
            -4 => MaxMessagesInflight,
            -5 => BadUtf8String,
            -6 => NullParameter,
            -7 => TopicNameTruncated,
            -8 => BadStructure,
            -9 => BadQos,
            -10 => NoMoreMsgIds,
            -11 => OperationIncomplete,
            -12 => MaxBufferedMessages,
            -13 => SslNotSupported,
            -14 => BadProtocol,
            -15 => BadMqttOption,
            -16 => WrongMqttVersion,
            -17 => ZeroLenWillTopic,
            -18 => CommandIgnored,
            -19 => MaxBufferedZero,
            _ => Failure,
        }
    }

    /// Create an error from an MQTT v3 Connect Return Code
    pub fn from_connect_return_code(code: u8) -> Self {
        match ConnectReturnCode::try_from(code) {
            Ok(cr) => Error::ConnectReturn(cr),
            Err(err) => err,
        }
    }

    /// Create an error from an MQTT v5 Reason Code and properties
    pub fn from_reason_code(code: u8, props: Properties) -> Self {
        Error::ReasonCode(ReasonCode::from(code), props)
    }
}

impl From<i32> for Error {
    /// Create an error from a Paho C return code.
    fn from(rc: i32) -> Error {
        Self::from_return_code(rc)
    }
}

impl From<(i32, &str)> for Error {
    /// Convert a return code with reason string to an Error.
    fn from((rc, msg): (i32, &str)) -> Self {
        use Error::*;
        match (rc, msg) {
            (-1, "TCP connect timeout") => TcpConnectTimeout,
            (-1, "TCP connect completion failure") => TcpConnectCompletionFailure,
            (-1, "TCP/TLS connect failure") => TcpTlsConnectFailure,
            (-3, _) => Disconnected,
            (rc, "Received disconnect") => ReceivedDisconnect(rc),
            (rc, "CONNACK return code") => match ConnectReturnCode::try_from(rc as u8) {
                Ok(ret) => ConnectReturn(ret),
                Err(err) => err,
            },
            (rc, "socket error") => SocketError(rc),
            _ => Failure,
        }
    }
}

impl From<(i32, Option<String>)> for Error {
    fn from((rc, msg): (i32, Option<String>)) -> Self {
        match msg {
            Some(msg) => Self::from((rc, msg.as_str())),
            None => Self::from(rc),
        }
    }
}

impl From<&'static str> for Error {
    /// Create a general error from a string.
    fn from(descr: &'static str) -> Error {
        Error::General(descr)
    }
}

impl From<String> for Error {
    /// Create a general error from a string.
    fn from(descr: String) -> Error {
        Error::GeneralString(descr)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        match err {
            Error::Io(e) => e,
            Error::Timeout => io::Error::new(io::ErrorKind::TimedOut, err),
            _ => io::Error::other(err),
        }
    }
}

/// The result type for MQTT operations.
pub type Result<T> = result::Result<T, Error>;

/// Gets the error message string for a numeric error code.
pub fn error_message(rc: i32) -> &'static str {
    match rc {
        -1 => "General failure",
        -2 => "Persistence error",
        -3 => "Client disconnected",
        -4 => "Maximum inflight messages",
        -5 => "Bad UTF8 string",
        -6 => "NULL Parameter",
        -7 => "Topic name truncated",
        -8 => "Bad structure",
        -9 => "Bad QoS",
        -10 => "No more message ID's",
        -11 => "Operation incomplete",
        -12 => "Max buffered messages",
        -13 => "SSL not supported",
        -14 => "Bad protocol",
        -15 => "Bad MQTT option",
        -16 => "Wrong MQTT version",
        -17 => "Zero length Will Topic",
        -18 => "Command Ignored",
        -19 => "Max buffered is zero",
        _ => "Unknown Error",
    }
}

/// Gets an error message from an error code with optional message.
pub fn error_message_opt<M>(rc: i32, msg: M) -> String
where
    M: Into<Option<String>>,
{
    match msg.into() {
        Some(msg) => format!("{} ({})", error_message(rc), msg),
        None => error_message(rc).to_string(),
    }
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_from() {
        assert!(matches!(Error::from(-1), Error::Failure));
        assert!(matches!(Error::from(-9), Error::BadQos));

        // Out of range is considered a general failure.
        assert!(matches!(Error::from(127), Error::Failure));
    }

    #[test]
    fn test_error_from_tuple() {
        assert!(matches!(
            Error::from((-1, "unspecified error")),
            Error::Failure
        ));
        assert!(matches!(
            Error::from((-1, "TCP connect timeout")),
            Error::TcpConnectTimeout
        ));
        assert!(matches!(
            Error::from((-34, "socket error")),
            Error::SocketError(-34)
        ));
    }

    #[test]
    fn test_error_from_reason_code() {
        use crate::Properties;
        use ReasonCode::*;

        assert!(matches!(
            Error::from_reason_code(0x80, Properties::default()),
            Error::ReasonCode(UnspecifiedError, _)
        ));
        assert!(matches!(
            Error::from_reason_code(0x87, Properties::default()),
            Error::ReasonCode(NotAuthorized, _)
        ));
    }

    #[test]
    fn test_error_from_v3_connect_return() {
        use ConnectReturnCode::*;

        // Unacceptable Protocol Version (1)
        assert!(matches!(
            Error::from_connect_return_code(1),
            Error::ConnectReturn(UnacceptableProtocolVersion)
        ));

        // Server Unavailable (3)
        assert!(matches!(
            Error::from_connect_return_code(3),
            Error::ConnectReturn(ServerUnavailable)
        ));
    }
}
