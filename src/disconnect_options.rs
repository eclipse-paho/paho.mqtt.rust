// diconnect_options.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2025 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Disconnect options for the Paho MQTT Rust client library.
//!
//! This contains the structures to define the options for disconnecting from
//! the MQTT broker/server.

use crate::{Properties, ReasonCode};
use std::time::Duration;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// The collection of options for disconnecting from the client.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[derive(Debug, Default, Clone)]
pub struct DisconnectOptions {
    /// A delay to disconnect to allow any in-flight messages to complete.
    /// This has a resolution of milliseconds.
    pub timeout: Duration,
    /// MQTT v5 disconnect properties
    pub props: Properties,
    /// MQTT v5 reason code for disconnect
    pub reason_code: ReasonCode,
}

impl DisconnectOptions {
    /// Create disconnect options
    pub fn new() -> Self {
        Self::default()
    }

    /// Create disconnect options with the specified timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            ..Self::default()
        }
    }

    /// Gets the properties used to disconnect.
    pub fn properties(&self) -> &Properties {
        &self.props
    }

    /// Gets the reason code sent with the disconnect.
    ///
    /// The fields of this struct are public, so new code can simply read
    /// [`reason_code`](Self::reason_code). The accessor is kept for
    /// compatibility with the v1.x API.
    pub fn reason_code(&self) -> ReasonCode {
        self.reason_code
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder to create the options for disconnecting from an MQTT server.
#[derive(Debug, Default, Clone)]
pub struct DisconnectOptionsBuilder(DisconnectOptions);

impl DisconnectOptionsBuilder {
    /// Create a new `DisconnectOptionsBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the time interval to allow the disconnect to complete.
    /// This specifies the time to allow in-flight messages to complete.
    ///
    /// # Arguments
    ///
    /// * `timeout` The time interval to allow the disconnect to
    ///   complete. This has a resolution of milliseconds.
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.0.timeout = timeout;
        self
    }

    /// Set the reason for the disconnect.
    ///
    /// The valid disconnect reasons are listed here in the spec:
    /// <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208>
    pub fn reason_code(&mut self, reason_code: ReasonCode) -> &mut Self {
        self.0.reason_code = reason_code;
        self
    }

    /// Tell the server to publish the will message on this disconnect.
    ///
    /// This sets the reason code in the options to 0x04:
    ///   "Disconnect with Will Message"
    pub fn publish_will_message(&mut self) -> &mut Self {
        self.0.reason_code = ReasonCode::DisconnectWithWillMessage;
        self
    }

    /// Sets the collection of properties for the disconnect.
    ///
    /// # Arguments
    ///
    /// * `props` The collection of properties to include with the
    ///   disconnect message.
    pub fn properties<P>(&mut self, props: P) -> &mut Self
    where
        P: Into<Option<Properties>>,
    {
        self.0.props = props.into().unwrap_or_default();
        self
    }

    /// Finalize the builder to create the disconnect options.
    pub fn finalize(&self) -> DisconnectOptions {
        self.0.clone()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Properties, ReasonCode};

    #[test]
    fn test_reason_code() {
        let opts = DisconnectOptionsBuilder::new().finalize();
        assert_eq!(opts.reason_code, ReasonCode::NormalDisconnection);

        let opts = DisconnectOptionsBuilder::new()
            .reason_code(ReasonCode::DisconnectWithWillMessage)
            .finalize();
        assert_eq!(opts.reason_code, ReasonCode::DisconnectWithWillMessage);

        let opts = DisconnectOptionsBuilder::new()
            .publish_will_message()
            .finalize();
        assert_eq!(opts.reason_code, ReasonCode::DisconnectWithWillMessage);
    }

    #[test]
    fn test_properties() {
        use crate::{Property, PropertyId};

        let opts = DisconnectOptions::new();
        assert!(opts.properties().is_empty());

        let opts = DisconnectOptionsBuilder::new().finalize();
        assert!(opts.properties().is_empty());

        let mut props = Properties::new();
        props.push(Property::SessionExpiryInterval(1000));
        props.push(Property::ReasonString("causeIwanna".into()));

        let opts = DisconnectOptionsBuilder::new().properties(props).finalize();
        let props = opts.properties();

        assert!(!props.is_empty());

        assert_eq!(
            props.get(PropertyId::SessionExpiryInterval),
            Some(&Property::SessionExpiryInterval(1000))
        );
        assert_eq!(
            props.get(PropertyId::ReasonString),
            Some(&Property::ReasonString("causeIwanna".into()))
        );

        assert_eq!(props.get(PropertyId::ContentType), None);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_round_trip() {
        let opts = DisconnectOptionsBuilder::new()
            .timeout(Duration::from_millis(1500))
            .reason_code(ReasonCode::DisconnectWithWillMessage)
            .finalize();
        let back: DisconnectOptions =
            serde_json::from_str(&serde_json::to_string(&opts).unwrap()).unwrap();
        assert_eq!(back.timeout, Duration::from_millis(1500));
        assert_eq!(back.reason_code, ReasonCode::DisconnectWithWillMessage);

        // Missing fields come from the default.
        let empty: DisconnectOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(empty.reason_code, DisconnectOptions::default().reason_code);
    }

    // The accessor removed when the field became public.
    #[test]
    fn test_reason_code_accessor() {
        let opts = DisconnectOptionsBuilder::new()
            .reason_code(ReasonCode::DisconnectWithWillMessage)
            .finalize();
        assert_eq!(ReasonCode::DisconnectWithWillMessage, opts.reason_code());
        assert_eq!(
            ReasonCode::NormalDisconnection,
            DisconnectOptions::new().reason_code()
        );
    }
}
