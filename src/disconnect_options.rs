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

use std::time::Duration;

use crate::{
    ffi,
    properties::Properties,
    reason_code::ReasonCode,
    token::{Token, TokenInner},
};

/// The collection of options for disconnecting from the client.
#[derive(Debug)]
pub struct DisconnectOptions {
    /// The underlying C disconnect options
    pub(crate) copts: ffi::MQTTAsync_disconnectOptions,
    props: Properties,
}

impl DisconnectOptions {
    /// Create a new `DisconnectOptions`
    pub fn new() -> Self {
        Self::default()
    }

    // Ensures that the underlying C struct points to cached values
    fn from_data(mut copts: ffi::MQTTAsync_disconnectOptions, props: Properties) -> Self {
        copts.properties = props.cprops;
        Self { copts, props }
    }

    /// Sets the token to ber used for connect completion callbacks.
    /// Note that we leak the token to give to the C lib. When we're
    /// done with it, we must recover and drop it (i.e. in the completion
    /// callback).
    pub fn set_token(&mut self, tok: Token) {
        self.copts.onSuccess = Some(TokenInner::on_success);
        self.copts.onFailure = Some(TokenInner::on_failure);
        self.copts.context = tok.into_raw();
    }

    /// Gets the reason code in the options.
    pub fn reason_code(&self) -> ReasonCode {
        ReasonCode::from(self.copts.reasonCode)
    }

    /// Gets the properties in the message
    pub fn properties(&self) -> &Properties {
        &self.props
    }
}

impl Default for DisconnectOptions {
    fn default() -> Self {
        Self::from_data(
            ffi::MQTTAsync_disconnectOptions::default(),
            Properties::default(),
        )
    }
}

impl Clone for DisconnectOptions {
    fn clone(&self) -> Self {
        Self::from_data(self.copts, self.props.clone())
    }
}

unsafe impl Send for DisconnectOptions {}
unsafe impl Sync for DisconnectOptions {}

/////////////////////////////////////////////////////////////////////////////
//                              Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder to create the options for disconnecting from an MQTT server.
#[derive(Debug, Default)]
pub struct DisconnectOptionsBuilder {
    copts: ffi::MQTTAsync_disconnectOptions,
    props: Properties,
}

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
    /// `timeout` The time interval to allow the disconnect to
    ///           complete. This has a resolution of milliseconds.
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        let millis = timeout.as_millis();
        self.copts.timeout = if millis == 0 { 1 } else { millis as i32 };
        self
    }

    /// Set the reason for the disconnect.
    ///
    /// The valid disconnect reasons are listed here in the spec:
    /// <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208>
    pub fn reason_code(&mut self, reason_code: ReasonCode) -> &mut Self {
        self.copts.reasonCode = reason_code as ffi::MQTTReasonCodes;
        self
    }

    /// Tell the server to publish the will message on this disconnect.
    ///
    /// This sets the reason code in the options to 0x04:
    ///   "Disconnect with Will Message"
    pub fn publish_will_message(&mut self) -> &mut Self {
        self.reason_code(ReasonCode::DisconnectWithWillMessage)
    }

    /// Sets the collection of properties for the disconnect.
    ///
    /// # Arguments
    ///
    /// `props` The collection of properties to include with the
    ///     disconnect message.
    pub fn properties(&mut self, props: Properties) -> &mut Self {
        self.props = props;
        self
    }

    /// Finalize the builder to create the disconnect options.
    pub fn finalize(&self) -> DisconnectOptions {
        DisconnectOptions::from_data(self.copts, self.props.clone())
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

        assert_eq!(STRUCT_ID, opts.copts.struct_id);
        assert_eq!(STRUCT_VERSION, opts.copts.struct_version);
    }

    #[test]
    fn test_send() {
        let opts = DisconnectOptions::new();

        let thr = thread::spawn(move || {
            assert_eq!(STRUCT_ID, opts.copts.struct_id);
            assert_eq!(STRUCT_VERSION, opts.copts.struct_version);
        });
        thr.join().unwrap();
    }

    #[test]
    fn test_reason_code() {
        let opts = DisconnectOptionsBuilder::new().finalize();
        assert_eq!(opts.reason_code(), ReasonCode::NormalDisconnection);

        let opts = DisconnectOptionsBuilder::new()
            .reason_code(ReasonCode::DisconnectWithWillMessage)
            .finalize();
        assert_eq!(opts.reason_code(), ReasonCode::DisconnectWithWillMessage);

        let opts = DisconnectOptionsBuilder::new()
            .publish_will_message()
            .finalize();
        assert_eq!(opts.reason_code(), ReasonCode::DisconnectWithWillMessage);
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

        let props = Properties::from_c_struct(&opts.copts.properties);
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
