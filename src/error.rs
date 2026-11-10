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
    ///
    /// Kept from v1.x so that code naming it still compiles; this library
    /// does not return it.
    #[error("General failure")]
    Failure,
    /// The client is disconnected.
    #[error("Client disconnected")]
    Disconnected,
    /// A `connect()` was made while the client was already connected.
    ///
    /// The existing connection is unaffected. To connect with other options,
    /// disconnect first.
    #[error("Client already connected")]
    AlreadyConnected,
    /// A `connect()` was made while another was still in progress.
    ///
    /// The connect already under way is unaffected; its token reports how it
    /// ends. This includes a `connect()` made during a `disconnect()` when
    /// an earlier `connect()` is already waiting for the disconnect to
    /// finish.
    #[error("A connect is already in progress")]
    ConnectInProgress,
    /// A QoS 1 or QoS 2 publish was abandoned because the server did not
    /// resume the session it belonged to.
    ///
    /// When the connection drops with a publish still unacknowledged and
    /// the next connection does not continue the session — the server
    /// reports Session Present = 0, or the client asked for a clean start —
    /// the client discards its half of the session, as the specification
    /// requires. It does so as the connection closes, without waiting for
    /// another, when the session ends with it: a v3 clean session, or a v5
    /// session whose Session Expiry Interval is 0. Whether the message reached the server is unknown, so its
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
    /// A TLS failure: the handshake was refused, or the [`TlsOptions`] could
    /// not be turned into a TLS configuration. [`TlsError::kind`] says which,
    /// in the same terms for every TLS backend.
    ///
    /// A connect that fails with this also reports it to the
    /// `TlsOptions::on_tls_error` callback. A certificate or key file that
    /// cannot be opened or read is an [`Io`](Self::Io) error instead.
    ///
    /// [`TlsOptions`]: crate::TlsOptions
    #[error("TLS error: {0}")]
    Tls(#[from] TlsError),
    /// The WebSocket upgrade of a `ws://` or `wss://` connection failed, or
    /// its request could not be built from the server URI and headers.
    #[error("WebSocket error: {0}")]
    WebSocket(String),
    /// An HTTP proxy refused to open the tunnel, or the exchange with it
    /// broke down; the [`ProxyError`] says which.
    ///
    /// A proxy URI that cannot be parsed is an
    /// [`InvalidArgument`](Self::InvalidArgument) error instead.
    #[error("HTTP proxy error: {0}")]
    Proxy(#[from] ProxyError),
    /// Call not applicable to the client's version of MQTT.
    #[error("Wrong MQTT version")]
    WrongMqttVersion,

    // ----- Errors from the Rust layer & libraries -----
    /// An error from the MQTT codec.
    #[error("{0}")]
    Protocol(#[from] paho_mqtt_codec::Error),
    /// The server broke the MQTT protocol: a packet that is well-formed but not
    /// allowed where it came, such as something other than CONNACK in
    /// answer to CONNECT, or an AUTH challenge the client did not offer
    /// authentication for.
    #[error("Protocol violation by the server: {0}")]
    ProtocolViolation(&'static str),
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
    /// An argument or option the client cannot use: an unparsable server or
    /// proxy URI, connect options that contradict each other, lists of
    /// topics and QoS of different lengths, an out-of-range value.
    ///
    /// Reported by the call that was given the bad value where it can be,
    /// rather than left for the connection or the broker to fail on.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
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
    ///
    /// Kept from v1.x so that code naming it still compiles; this library
    /// does not return it. A property value the specification does not
    /// allow is reported by `Properties::try_push`, or by the encode.
    #[error("Bad v5 property")]
    BadProperty,
    /// Conversion error between types.
    ///
    /// Kept from v1.x so that code naming it still compiles; this library
    /// does not return it.
    #[error("Conversion Error")]
    Conversion,
    /// A timeout, particularly from a synchronous operation.
    #[error("Timeout")]
    Timeout,
    /// A general error with description.
    ///
    /// Not returned by this library, which has a variant for each failure it
    /// reports. It is here for application code — an `AuthHandler`, a
    /// `ClientPersistence` implementation — with a failure of its own to
    /// return.
    #[error("{0}")]
    General(&'static str),
    /// A general error with an owned description.
    ///
    /// Not returned by this library; see [`General`](Self::General).
    #[error("{0}")]
    GeneralString(String),
}

/// A TLS failure, classified the same way for every TLS backend, with the
/// backend's own description of it.
///
/// Carried by [`Error::Tls`] and given to the `TlsOptions::on_tls_error`
/// callback. It displays as the backend's message.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("{message}")]
pub struct TlsError {
    kind: TlsErrorKind,
    message: String,
}

impl TlsError {
    /// Creates an error of the given kind with a description of it.
    pub fn new(kind: TlsErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// The category of the failure.
    pub fn kind(&self) -> TlsErrorKind {
        self.kind
    }

    /// The TLS backend's description of the failure.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// The category of a [`TlsError`].
///
/// `#[non_exhaustive]`, as [`enum@Error`] is: a `match` on this from outside
/// the crate needs a wildcard arm, and a category may be split later.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TlsErrorKind {
    /// The server's certificate chain does not lead to a trusted CA.
    UntrustedCertificate,
    /// The server's certificate does not name the host connected to.
    HostnameMismatch,
    /// The server's certificate has expired.
    CertificateExpired,
    /// The server's certificate is not valid yet.
    CertificateNotYetValid,
    /// Any other handshake failure, such as no common protocol version or
    /// cipher suite, or the server refusing the client's certificate.
    HandshakeFailed,
    /// The host is not a valid TLS server name.
    InvalidServerName,
    /// The `TlsOptions` could not be turned into a configuration: a
    /// certificate or key the backend rejects, no recognized cipher suite, or
    /// a missing private key.
    Config,
    /// An option the TLS backend does not support, such as an encrypted
    /// private key with rustls.
    Unsupported,
}

impl fmt::Display for TlsErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TlsErrorKind::*;
        f.pad(match self {
            UntrustedCertificate => "untrusted certificate",
            HostnameMismatch => "hostname mismatch",
            CertificateExpired => "certificate expired",
            CertificateNotYetValid => "certificate not yet valid",
            HandshakeFailed => "handshake failed",
            InvalidServerName => "invalid server name",
            Config => "invalid TLS configuration",
            Unsupported => "unsupported TLS option",
        })
    }
}

/// The ways that opening a tunnel through an HTTP proxy can fail.
///
/// Carried by [`Error::Proxy`]. `#[non_exhaustive]`, as [`enum@Error`] is: a
/// `match` on this from outside the crate needs a wildcard arm.
#[non_exhaustive]
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ProxyError {
    /// The proxy refused to open the tunnel, with this HTTP status code and
    /// status line.
    ///
    /// `407` means the proxy wants credentials, or did not accept the ones
    /// given in the proxy URI.
    #[error("the proxy refused the tunnel: {status_line}")]
    Refused {
        /// The HTTP status code.
        status: u16,
        /// The whole status line, in the proxy's own words.
        status_line: String,
    },
    /// The proxy closed the connection before the tunnel was open.
    #[error("the proxy closed the connection")]
    Closed,
    /// Data arrived behind the proxy's response, before the tunnel was open.
    #[error("data arrived before the tunnel was open")]
    EarlyData,
    /// The response headers went on for longer than any proxy's should.
    #[error("the response is over-long")]
    OverlongResponse,
    /// The response headers are not valid UTF-8.
    #[error("the response is not valid UTF-8")]
    NotUtf8,
    /// The response does not start with an HTTP status line. This holds the
    /// line that was received.
    #[error("malformed status line: {0}")]
    MalformedStatusLine(String),
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
            AlreadyConnected => AlreadyConnected,
            ConnectInProgress => ConnectInProgress,
            SessionNotResumed => SessionNotResumed,
            TlsNotSupported => TlsNotSupported,
            WebSocketNotSupported => WebSocketNotSupported,
            Tls(e) => Tls(e.clone()),
            WebSocket(s) => WebSocket(s.clone()),
            Proxy(e) => Proxy(e.clone()),
            WrongMqttVersion => WrongMqttVersion,
            Protocol(e) => Protocol(*e),
            ProtocolViolation(s) => ProtocolViolation(s),
            Publish(s, m) => Publish(s.clone(), m.clone()),
            ReasonCode(rc, props) => ReasonCode(*rc, props.clone()),
            Persistence(s) => Persistence(s.clone()),
            BadTopicFilter => BadTopicFilter,
            InvalidArgument(s) => InvalidArgument(s.clone()),
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

    #[test]
    fn test_tls_error() {
        let tls = TlsError::new(TlsErrorKind::CertificateExpired, "certificate expired");
        assert_eq!(tls.kind(), TlsErrorKind::CertificateExpired);
        assert_eq!(tls.message(), "certificate expired");
        assert_eq!(tls.to_string(), "certificate expired");

        let err = Error::from(tls.clone());
        assert_eq!(err.to_string(), "TLS error: certificate expired");
        assert!(matches!(err.clone(), Error::Tls(e) if e == tls));
    }

    #[test]
    fn test_tls_error_kind_display() {
        assert_eq!(
            TlsErrorKind::CertificateNotYetValid.to_string(),
            "certificate not yet valid"
        );
        assert_eq!(
            format!("{:>20}", TlsErrorKind::HostnameMismatch),
            "   hostname mismatch"
        );
    }

    #[test]
    fn test_new_variants_clone_and_display() {
        let errs = [
            Error::WebSocket("upgrade failed".into()),
            Error::Proxy(ProxyError::Closed),
            Error::Proxy(ProxyError::Refused {
                status: 407,
                status_line: "HTTP/1.1 407 Proxy Authentication Required".into(),
            }),
            Error::ProtocolViolation("expected CONNACK"),
            Error::InvalidArgument("topics/qos length mismatch".into()),
        ];
        let shown = [
            "WebSocket error: upgrade failed",
            "HTTP proxy error: the proxy closed the connection",
            "HTTP proxy error: the proxy refused the tunnel: \
             HTTP/1.1 407 Proxy Authentication Required",
            "Protocol violation by the server: expected CONNACK",
            "Invalid argument: topics/qos length mismatch",
        ];
        for (err, shown) in errs.iter().zip(shown) {
            assert_eq!(err.to_string(), shown);
            assert_eq!(err.clone().to_string(), shown);
        }
        assert!(matches!(
            errs[2].clone(),
            Error::Proxy(ProxyError::Refused { status: 407, .. })
        ));
    }

    #[test]
    fn test_connect_state_errors() {
        assert!(matches!(
            Error::AlreadyConnected.clone(),
            Error::AlreadyConnected
        ));
        assert!(matches!(
            Error::ConnectInProgress.clone(),
            Error::ConnectInProgress
        ));
        assert_eq!(
            Error::AlreadyConnected.to_string(),
            "Client already connected"
        );
        assert_eq!(
            Error::ConnectInProgress.to_string(),
            "A connect is already in progress"
        );
    }

    #[test]
    fn test_session_not_resumed() {
        let err = Error::SessionNotResumed;
        assert!(matches!(err.clone(), Error::SessionNotResumed));
        assert!(err.to_string().contains("did not resume the session"));
    }

    // The replacement for v1.x's payload-free `PersistenceError`.
    #[test]
    fn test_persistence_error() {
        let err = Error::Persistence("disk full".into());
        assert_eq!("Persistence error: disk full", err.to_string());
        assert!(matches!(err.clone(), Error::Persistence(s) if s == "disk full"));
    }
}
