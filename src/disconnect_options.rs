// diconnect_options.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2020 Frank Pagliughi <fpagliughi@mindspring.com>
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

#![deny(missing_docs)]

//! Disconnect options for the Paho MQTT Rust client library.
//! This contains the structures to define the options for disconnecting from
//! the MQTT broker/server.

use crate::{ffi, properties::Properties, reason_code::ReasonCode};
use std::{os::raw::c_int, time::Duration};

/// The collection of options for disconnecting from the client.
#[derive(Debug, Default, Clone)]
pub struct DisconnectOptions {
    /// A delay to disconnect to allow any in-flight messages to complete.
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

    /// Gets an FFI C struct for the disconnect options
    pub(crate) fn c_struct(&self) -> ffi::MQTTAsync_disconnectOptions {
        ffi::MQTTAsync_disconnectOptions {
            timeout: self.timeout.as_secs() as c_int,
            properties: self.props.cprops,
            reasonCode: self.reason_code as ffi::MQTTReasonCodes,
            ..ffi::MQTTAsync_disconnectOptions::default()
        }
    }

    /// Gets the properties used o disconnect
    pub fn properties(&self) -> &Properties {
        &self.props
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
    pub fn properties(&mut self, props: Properties) -> &mut Self {
        self.0.props = props;
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
    use crate::{properties::PropertyCode, reason_code::ReasonCode};
    use std::{os::raw::c_char, thread};

    // Identifier fo a C disconnect options struct
    const STRUCT_ID: [c_char; 4] = [
        b'M' as c_char,
        b'Q' as c_char,
        b'T' as c_char,
        b'D' as c_char,
    ];

    const STRUCT_VERSION: i32 = ffi::DISCONNECT_OPTIONS_STRUCT_VERSION;

    #[test]
    fn test_new() {
        let opts = DisconnectOptions::new();
        let copts = opts.c_struct();

        assert_eq!(STRUCT_ID, copts.struct_id);
        assert_eq!(STRUCT_VERSION, copts.struct_version);
    }

    #[test]
    fn test_send() {
        let opts = DisconnectOptions::new();

        let thr = thread::spawn(move || {
            let copts = opts.c_struct();
            assert_eq!(STRUCT_ID, copts.struct_id);
            assert_eq!(STRUCT_VERSION, copts.struct_version);
        });
        thr.join().unwrap();
    }

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
        let opts = DisconnectOptions::new();
        assert!(opts.properties().is_empty());
        assert_eq!(opts.properties().len(), 0);

        let opts = DisconnectOptionsBuilder::new().finalize();
        assert!(opts.properties().is_empty());
        assert_eq!(opts.properties().len(), 0);

        let mut props = Properties::new();
        props
            .push_int(PropertyCode::SessionExpiryInterval, 1000)
            .unwrap();
        props
            .push_val(PropertyCode::ReasonString, "causeIwanna")
            .unwrap();

        let opts = DisconnectOptionsBuilder::new().properties(props).finalize();
        let copts = opts.c_struct();

        let props = opts.properties();

        assert!(!props.is_empty());
        assert_eq!(props.len(), 2);

        assert_eq!(
            props.get_int(PropertyCode::SessionExpiryInterval),
            Some(1000)
        );
        assert_eq!(
            props.get_val::<String>(PropertyCode::ReasonString),
            Some("causeIwanna".to_string())
        );

        assert_eq!(props.get_int(PropertyCode::ContentType), None);

        let props = Properties::from_c_struct(&copts.properties);
        assert_eq!(props.len(), 2);

        assert_eq!(
            props.get_int(PropertyCode::SessionExpiryInterval),
            Some(1000)
        );
        assert_eq!(
            props.get_val::<String>(PropertyCode::ReasonString),
            Some("causeIwanna".to_string())
        );
    }
}
