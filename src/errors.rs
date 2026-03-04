// errors.rs
//
// Error and Result types for the Paho MQTT Rust library.
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2023 Frank Pagliughi <fpagliughi@mindspring.com>
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

use crate::{ffi, message::Message, reason_code::ReasonCode};
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
    /// An invaid QoS value was used not 0, 1, or 2)
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

    // ----- From Paho string messages -----
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
    /// A synchronous error when publishing creating or queuing the message.
    #[error("{}", error_message(*.0))]
    Publish(i32, Message),
    /// An MQTT v5 error from a reason code.
    #[error("{0}")]
    ReasonCode(ReasonCode),
    /// A bad topic filter
    #[error("Bad topic filter")]
    BadTopicFilter,
    /// An low-level I/O error
    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
    /// An error parsing a UTF-8 string
    #[error("String UTF-8 Error")]
    Utf8(#[from] str::Utf8Error),
    /// A string NUL error
    #[error("String NUL Error")]
    Nul(#[from] std::ffi::NulError),
    /// Missing SSL/TLS options
    #[error("Missing SSL/TLS options")]
    MissingSslOptions,
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

impl From<i32> for Error {
    /// Create an error from a Paho C return code.
    fn from(rc: i32) -> Error {
        use Error::*;
        match rc {
            ffi::MQTTASYNC_PERSISTENCE_ERROR => PersistenceError,
            ffi::MQTTASYNC_DISCONNECTED => Disconnected,
            ffi::MQTTASYNC_MAX_MESSAGES_INFLIGHT => MaxMessagesInflight,
            ffi::MQTTASYNC_BAD_UTF8_STRING => BadUtf8String,
            ffi::MQTTASYNC_NULL_PARAMETER => NullParameter,
            ffi::MQTTASYNC_TOPICNAME_TRUNCATED => TopicNameTruncated,
            ffi::MQTTASYNC_BAD_STRUCTURE => BadStructure,
            ffi::MQTTASYNC_BAD_QOS => BadQos,
            ffi::MQTTASYNC_NO_MORE_MSGIDS => NoMoreMsgIds,
            ffi::MQTTASYNC_OPERATION_INCOMPLETE => OperationIncomplete,
            ffi::MQTTASYNC_MAX_BUFFERED_MESSAGES => MaxBufferedMessages,
            ffi::MQTTASYNC_SSL_NOT_SUPPORTED => SslNotSupported,
            ffi::MQTTASYNC_BAD_PROTOCOL => BadProtocol,
            ffi::MQTTASYNC_BAD_MQTT_OPTION => BadMqttOption,
            ffi::MQTTASYNC_WRONG_MQTT_VERSION => WrongMqttVersion,
            ffi::MQTTASYNC_0_LEN_WILL_TOPIC => ZeroLenWillTopic,
            ffi::MQTTASYNC_COMMAND_IGNORED => CommandIgnored,
            ffi::MQTTASYNC_MAX_BUFFERED => MaxBufferedZero,
            _ => Failure,
        }
    }
}

impl From<(i32, &str)> for Error {
    // The Paho C library passes up error description strings that are
    // supposed to be for "additional" information, but they actually
    // describe specific errors that should have actually been enumerated.
    // This is somewhat of a hack to use the string to enumerate errors.
    // These strings are
    fn from((rc, msg): (i32, &str)) -> Self {
        use Error::*;
        match (rc, msg) {
            (ffi::MQTTASYNC_FAILURE, "TCP connect timeout") => TcpConnectTimeout,
            (ffi::MQTTASYNC_FAILURE, "TCP connect completion failure") => {
                TcpConnectCompletionFailure
            }
            (ffi::MQTTASYNC_FAILURE, "TCP/TLS connect failure") => TcpTlsConnectFailure,
            (ffi::MQTTASYNC_DISCONNECTED, _) => Disconnected,
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

impl From<std::ffi::IntoStringError> for Error {
    fn from(_: std::ffi::IntoStringError) -> Self {
        Error::BadUtf8String
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

/// Gets the string associated with the error code from the C lib.
pub fn error_message(rc: i32) -> &'static str {
    match rc {
        ffi::MQTTASYNC_FAILURE => "General failure",
        ffi::MQTTASYNC_PERSISTENCE_ERROR => "Persistence error",
        ffi::MQTTASYNC_DISCONNECTED => "Client disconnected",
        ffi::MQTTASYNC_MAX_MESSAGES_INFLIGHT => "Maximum inflight messages",
        ffi::MQTTASYNC_BAD_UTF8_STRING => "Bad UTF8 string",
        ffi::MQTTASYNC_NULL_PARAMETER => "NULL Parameter",
        ffi::MQTTASYNC_TOPICNAME_TRUNCATED => "Topic name truncated",
        ffi::MQTTASYNC_BAD_STRUCTURE => "Bad structure",
        ffi::MQTTASYNC_BAD_QOS => "Bad QoS",
        ffi::MQTTASYNC_NO_MORE_MSGIDS => "No more message ID's",
        ffi::MQTTASYNC_OPERATION_INCOMPLETE => "Operation incomplete",
        ffi::MQTTASYNC_MAX_BUFFERED_MESSAGES => "Max buffered messages",
        ffi::MQTTASYNC_SSL_NOT_SUPPORTED => "SSL not supported by Paho C library",
        ffi::MQTTASYNC_BAD_PROTOCOL => "Bad protocol",
        ffi::MQTTASYNC_BAD_MQTT_OPTION => "Bad MQTT option",
        ffi::MQTTASYNC_WRONG_MQTT_VERSION => "Wrong MQTT version",
        ffi::MQTTASYNC_0_LEN_WILL_TOPIC => "Zero length Will Topic",
        ffi::MQTTASYNC_COMMAND_IGNORED => "Command Ignored",
        ffi::MQTTASYNC_MAX_BUFFERED => "Max buffered is zero",
        _ => "Unknown Error",
    }
}

/// Gets an error message from an error code with optional
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
    fn test_error_from_rc() {
        let err = Error::from(ffi::MQTTASYNC_BAD_QOS);
        assert!(matches!(err, Error::BadQos));
    }

    #[test]
    fn test_error_from_msg() {
        let err = Error::from((ffi::MQTTASYNC_FAILURE, "TCP connect timeout"));
        assert!(matches!(err, Error::TcpConnectTimeout));
    }
}
