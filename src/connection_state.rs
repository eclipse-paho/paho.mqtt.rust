// paho-mqtt/src/connection_state.rs
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

//! The state of a client's connection to the broker.

/// Where a client's connection to the broker stands.
///
/// Returned by `AsyncClient::state()` and `Client::state()`. It is kept by
/// the client's event loop, which changes it as the connection comes and
/// goes, so it describes what the client is doing rather than what the
/// application last asked for: just after a `connect()` call returns, the
/// state may still be [`Disconnected`](Self::Disconnected) for a moment
/// before the event loop picks the request up. What is guaranteed is the
/// order against everything the application can observe: the state has
/// already changed when the token of the operation that changed it
/// resolves, and when the callback or event announcing the change runs.
///
/// `#[non_exhaustive]`: a `match` on this from outside the crate needs a
/// wildcard arm, so that a state added in a later release is a change an
/// application can absorb rather than a compilation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum ConnectionState {
    /// Not connected, and not trying to be: the client has never connected,
    /// was disconnected by the application, or lost its connection without
    /// automatic reconnect, or its first connect failed.
    Disconnected = 0,
    /// A `connect()` or `reconnect()` is in progress: opening the transport,
    /// sending CONNECT and waiting for the CONNACK, trying each configured
    /// server in turn.
    Connecting = 1,
    /// Connected to the broker.
    Connected = 2,
    /// A `disconnect()` is in progress: in-flight operations are given the
    /// requested time to complete, then a DISCONNECT is sent. New
    /// operations are not started on the connection meanwhile.
    Disconnecting = 3,
    /// The connection was lost and automatic reconnect is waiting out its
    /// back-off delay before the next attempt.
    Retrying = 4,
    /// The connection was lost and automatic reconnect is attempting to
    /// connect again.
    Reconnecting = 5,
}

impl ConnectionState {
    /// The state stored as `n` by [`as_u8`](Self::as_u8).
    ///
    /// Only ever given a value this crate stored, so an unknown one is a bug
    /// here; it reads as [`Disconnected`](Self::Disconnected), the state that
    /// promises nothing.
    pub(crate) fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::Connecting,
            2 => Self::Connected,
            3 => Self::Disconnecting,
            4 => Self::Retrying,
            5 => Self::Reconnecting,
            _ => Self::Disconnected,
        }
    }

    /// The state as stored in an atomic.
    pub(crate) fn as_u8(self) -> u8 {
        self as u8
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        use ConnectionState::*;
        for state in [
            Disconnected,
            Connecting,
            Connected,
            Disconnecting,
            Retrying,
            Reconnecting,
        ] {
            assert_eq!(ConnectionState::from_u8(state.as_u8()), state);
        }
    }
}
