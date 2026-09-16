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
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum Error {
    /// A generic error code indicating the failure of an MQTT client operation.
    #[error("General failure")]
    Failure,
    /// The client is disconnected.
    #[error("Client disconnected")]
    Disconnected,
    /// Attempting a TLS connection when the TLS feature is not enabled.
    #[error("TLS not supported by library")]
    TlsNotSupported,
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
    /// A synchronous error when publishing: creating or queuing the message.
    ///
    /// The message is boxed to keep `Error` small enough to return by value
    /// cheaply.
    #[error("{0}")]
    Publish(String, Box<Message>),
    /// An MQTT v5 error from a reason code.
    /// Any reason code >= 0x80 indicates an error.
    #[error("{0}")]
    ReasonCode(ReasonCode, Properties),
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
}
