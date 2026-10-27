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
use std::{io, result, str};
use thiserror::Error;

/// The errors from an MQTT operation.
///
/// `#[non_exhaustive]`: a `match` on this from outside the crate needs a
/// wildcard arm, so that a variant added in a later release is a change an
/// application can absorb rather than a compilation failure.
#[derive(Error, Debug)]
#[allow(missing_docs)]
#[non_exhaustive]
pub enum Error {
    /// A generic error code indicating the failure of an MQTT client operation.
    #[error("General failure")]
    Failure,
    /// The client is disconnected.
    #[error("Client disconnected")]
    Disconnected,
    /// A QoS 1 or QoS 2 publish was abandoned because the server did not
    /// resume the session it belonged to.
    ///
    /// When the connection drops with a publish still unacknowledged and
    /// the next connection does not continue the session — the server
    /// reports Session Present = 0, or the client asked for a clean start —
    /// the client discards its half of the session, as the specification
    /// requires. Whether the message reached the server is unknown, so its
    /// token fails with this rather than being re-sent into a session the
    /// server says no longer exists. Publish it again if it must be
    /// delivered.
    #[error("The server did not resume the session; the publish was abandoned")]
    SessionNotResumed,
    /// Attempting a TLS connection when the TLS feature is not enabled.
    #[error("TLS not supported by library")]
    TlsNotSupported,
    /// Attempting a WebSocket connection (`ws://` or `wss://`) when the
    /// WebSocket feature is not enabled.
    #[error("WebSocket not supported by library")]
    WebSocketNotSupported,
    /// A TLS handshake failure.
    #[error("TLS error: {0}")]
    Tls(String),
    /// Call not applicable to the client's version of MQTT.
    #[error("Wrong MQTT version")]
    WrongMqttVersion,

    // ----- Errors from the Rust layer & libraries -----
    /// An error from the MQTT codec.
    #[error("{0}")]
    Protocol(#[from] paho_mqtt_codec::Error),
    /// A synchronous failure when publishing — creating or queuing the
    /// message — carrying the cause and the message back to the caller.
    ///
    /// Returned only by [`try_publish`](crate::AsyncClient::try_publish) and
    /// the calls that forward to it, so that a message which never reached
    /// the broker can be held on to and sent again. `Display` shows the
    /// cause, so a message that is only logged reads the same as before.
    ///
    /// [`publish`](crate::AsyncClient::publish) reports the cause alone on
    /// its token, since it has taken ownership of the message.
    ///
    /// Both halves are boxed to keep `Error` small enough to return by value
    /// cheaply.
    #[error("{0}")]
    Publish(Box<Error>, Box<Message>),
    /// An MQTT v5 error from a reason code.
    /// Any reason code >= 0x80 indicates an error.
    #[error("{0}")]
    ReasonCode(ReasonCode, Properties),
    /// A persistence store failed.
    ///
    /// Carries whatever the store reported. A [`ClientPersistence`]
    /// implementation should return this for a failure of its own that is
    /// not an [`Io`](Self::Io) error; it replaces v1.x's
    /// `Error::PersistenceError`, which carried nothing.
    ///
    /// A failure to store an outbound message does not fail the publish: the
    /// message has already been queued for the broker, so the operation
    /// proceeds and the error is logged. What is lost is the ability to
    /// re-send that message after a restart.
    ///
    /// [`ClientPersistence`]: crate::ClientPersistence
    #[error("Persistence error: {0}")]
    Persistence(String),
    /// A bad topic filter.
    #[error("Bad topic filter")]
    BadTopicFilter,
    /// A low-level I/O error.
    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
    /// An error parsing a UTF-8 string.
    #[error("String UTF-8 Error")]
    Utf8(#[from] str::Utf8Error),
    /// Missing TLS options.
    #[error("Missing TLS options")]
    MissingTlsOptions,
    /// Bad property, typically from an improper value type for the code.
    #[error("Bad v5 property")]
    BadProperty,
    /// Conversion error between types.
    #[error("Conversion Error")]
    Conversion,
    /// A timeout, particularly from a synchronous operation.
    #[error("Timeout")]
    Timeout,
    /// A general error with description.
    #[error("{0}")]
    General(&'static str),
    /// A general error with an owned description.
    #[error("{0}")]
    GeneralString(String),
}

/// `io::Error` is not `Clone`, so the I/O variant is cloned by re-creating an
/// error of the same kind carrying the original's message. The `source()`
/// chain of the original is not preserved. Every other variant clones its
/// payload directly. Cloning exists so that a [`Token`](crate::Token) result
/// can be observed by more than one holder of the token.
impl Clone for Error {
    fn clone(&self) -> Self {
        use Error::*;
        match self {
            Failure => Failure,
            Disconnected => Disconnected,
            SessionNotResumed => SessionNotResumed,
            TlsNotSupported => TlsNotSupported,
            WebSocketNotSupported => WebSocketNotSupported,
            Tls(s) => Tls(s.clone()),
            WrongMqttVersion => WrongMqttVersion,
            Protocol(e) => Protocol(*e),
            Publish(s, m) => Publish(s.clone(), m.clone()),
            ReasonCode(rc, props) => ReasonCode(*rc, props.clone()),
            Persistence(s) => Persistence(s.clone()),
            BadTopicFilter => BadTopicFilter,
            Io(e) => Io(io::Error::new(e.kind(), e.to_string())),
            Utf8(e) => Utf8(*e),
            MissingTlsOptions => MissingTlsOptions,
            BadProperty => BadProperty,
            Conversion => Conversion,
            Timeout => Timeout,
            General(s) => General(s),
            GeneralString(s) => GeneralString(s.clone()),
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

/// A `TryInto<QoS>` (and the like) from a type that cannot fail converts with
/// `Error = Infallible`. Accepting it here lets one bound,
/// `Q::Error: Into<Error>`, cover both a plain `QoS` and an integer that has
/// to be range-checked.
impl From<core::convert::Infallible> for Error {
    fn from(never: core::convert::Infallible) -> Self {
        match never {}
    }
}

/// The result type for MQTT operations.
pub type Result<T> = result::Result<T, Error>;

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_from_str() {
        assert!(matches!(Error::from("oops"), Error::General("oops")));
    }

    #[test]
    fn test_error_from_string() {
        let s = String::from("oops");
        assert!(matches!(Error::from(s), Error::GeneralString(_)));
    }

    #[test]
    fn test_io_error_clone_keeps_kind_and_message() {
        let e = Error::Io(io::Error::new(io::ErrorKind::ConnectionRefused, "nope"));
        let c = e.clone();
        match (&e, &c) {
            (Error::Io(a), Error::Io(b)) => {
                assert_eq!(a.kind(), b.kind());
                assert_eq!(a.to_string(), b.to_string());
            }
            _ => panic!("clone changed variant"),
        }
    }

    // The replacement for v1.x's payload-free `PersistenceError`.
    #[test]
    fn test_session_not_resumed() {
        let err = Error::SessionNotResumed;
        assert!(matches!(err.clone(), Error::SessionNotResumed));
        assert!(err.to_string().contains("did not resume the session"));
    }

    #[test]
    fn test_persistence_error() {
        let err = Error::Persistence("disk full".into());
        assert_eq!("Persistence error: disk full", err.to_string());
        assert!(matches!(err.clone(), Error::Persistence(s) if s == "disk full"));
    }
}
