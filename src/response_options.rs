// response_options.rs
//
// The set of options for responses coming back to an MQTT client.
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2019-2020 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Response options for the Paho MQTT Rust client library.
//!
//! Originally this presented a way for the application to specify callbacks
//! to the C library to receive responses to various requests. With MQTT v5,
//! the C lib extended this struct as a way to specify v5 options (like
//! properties, etc) in the requests. So this is now sometimes referred to
//! as the "call options".
//!

use std::{os::raw::c_int, pin::Pin, ptr};

use crate::{
    ffi,
    properties::Properties,
    subscribe_options::SubscribeOptions,
    token::{Token, TokenInner},
    types::*,
};

/// The collection of options for responses coming back to the client.
#[derive(Debug)]
pub struct ResponseOptions {
    /// The underlying C options struct
    pub(crate) copts: ffi::MQTTAsync_responseOptions,
    /// The cached Rust data for the struct
    data: Pin<Box<ResponseOptionsData>>,
}

/// Cached data for the response options
#[derive(Debug, Default, Clone)]
struct ResponseOptionsData {
    /// The MQTT v5 properties
    props: Properties,
    /// The collection of MQTT v5 subscription options
    /// If used, there should be one per subscription topic
    sub_opts: Option<Vec<ffi::MQTTSubscribe_options>>,
}

impl ResponseOptions {
    /// Creates a `ResponseOptions` intance for the provided token.
    /// The option's `copts` can be passed to one of the C library's
    /// asynchronous functions to set the callbacks to complete the token.
    ///
    /// Note: We leak an Arc clone of the token into the internal C
    /// structure to act as the context pointer for the callback. It is
    /// up to the callback (or calling function) to recapture and release
    /// this token.
    pub(crate) fn new<V, T>(ver: V, tok: T) -> Self
    where
        V: Into<MqttVersion>,
        T: Into<Token>,
    {
        let mut copts = Self::c_options(ver.into());
        copts.context = tok.into().into_raw();

        Self::from_data(copts, ResponseOptionsData::default())
    }

    // Creates a C response options struct for the specified MQTT version
    fn c_options(ver: MqttVersion) -> ffi::MQTTAsync_responseOptions {
        if ver < MqttVersion::V5 {
            ffi::MQTTAsync_responseOptions {
                onSuccess: Some(TokenInner::on_success),
                onFailure: Some(TokenInner::on_failure),
                ..ffi::MQTTAsync_responseOptions::default()
            }
        }
        else {
            ffi::MQTTAsync_responseOptions {
                onSuccess5: Some(TokenInner::on_success5),
                onFailure5: Some(TokenInner::on_failure5),
                ..ffi::MQTTAsync_responseOptions::default()
            }
        }
    }

    // Creates a set of options from a C struct and cached values.
    // Fixes up the underlying C struct to point to the cached values,
    // then returns a new options object with them combined.
    fn from_data(mut copts: ffi::MQTTAsync_responseOptions, data: ResponseOptionsData) -> Self {
        let mut data = Box::pin(data);

        copts.properties = data.props.cprops;

        let (p, n) = match data.sub_opts {
            Some(ref mut sub_opts) => (sub_opts.as_mut_ptr(), sub_opts.len() as c_int),
            _ => (ptr::null_mut(), 0),
        };

        copts.subscribeOptionsList = p;
        copts.subscribeOptionsCount = n;

        Self { copts, data }
    }

    /// Gets the MQTT v5 properties in the response, if any.
    pub fn properties(&self) -> &Properties {
        &self.data.props
    }
}

impl Default for ResponseOptions {
    fn default() -> Self {
        Self::from_data(
            ffi::MQTTAsync_responseOptions::default(),
            ResponseOptionsData::default(),
        )
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Builder to create MQTT v5 response options.
#[derive(Default)]
pub struct ResponseOptionsBuilder {
    /// The underlying C options struct
    copts: ffi::MQTTAsync_responseOptions,
    /// The cached Rust data for the struct
    data: ResponseOptionsData,
}

impl ResponseOptionsBuilder {
    /// Creates a new builder starting with default options.
    pub fn new() -> Self {
        Self {
            copts: ResponseOptions::c_options(MqttVersion::V5),
            data: ResponseOptionsData::default(),
        }
    }

    /// Sets the token for the response.
    pub fn token<T>(&mut self, tok: T) -> &mut Self
    where
        T: Into<Token>,
    {
        self.copts.context = tok.into().into_raw();
        self
    }

    /// Sets the collection of properties for the request.
    pub fn properties(&mut self, props: Properties) -> &mut Self {
        self.data.props = props;
        self
    }

    /// Sets a single set of subscribe options - for a call to subscribe()
    pub fn subscribe_options(&mut self, opts: SubscribeOptions) -> &mut Self {
        self.copts.subscribeOptions = opts.copts;
        self
    }

    /// Sets a single set of subscribe options - for a call to subscribe_many()
    pub fn subscribe_many_options(&mut self, opts: &[SubscribeOptions]) -> &mut Self {
        match opts {
            [] => {}
            // This is necessary, as the `MQTTAsync_subscribeMany` paho.mqtt.c function uses `opts` over `optlist` when `count <= 1`
            [opts] => self.copts.subscribeOptions = opts.copts,
            _ => self.data.sub_opts = Some(opts.iter().map(|opt| opt.copts).collect()),
        }
        self
    }

    /// Create the response options from the builder.
    pub fn finalize(&self) -> ResponseOptions {
        ResponseOptions::from_data(self.copts, self.data.clone())
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::Token;

    // The currently supported response options struct version
    //const STRUCT_VERSION: i32 = ffi::RESPONSE_OPTIONS_STRUCT_VERSION;

    #[test]
    fn test_new_v3() {
        let tok = Token::new();
        let opts = ResponseOptions::new(MQTT_VERSION_3_1_1, tok.clone());

        let inner = Token::into_raw(tok);

        assert!(opts.copts.onSuccess.is_some());
        assert!(opts.copts.onFailure.is_some());
        // Check that the context is pointing to the right Token
        assert_eq!(inner, opts.copts.context);
        assert!(opts.copts.onSuccess5.is_none());
        assert!(opts.copts.onFailure5.is_none());

        unsafe { Token::from_raw(inner) };
    }

    #[test]
    fn test_new_v5() {
        let tok = Token::new();
        let opts = ResponseOptions::new(MQTT_VERSION_5, tok.clone());

        let inner = Token::into_raw(tok);

        assert!(opts.copts.onSuccess.is_none());
        assert!(opts.copts.onFailure.is_none());
        // Check that the context is pointing to the right Token
        assert_eq!(inner, opts.copts.context);
        assert!(opts.copts.onSuccess5.is_some());
        assert!(opts.copts.onFailure5.is_some());

        unsafe { Token::from_raw(inner) };
    }

    #[test]
    fn test_sub_opts() {
        let tok = Token::new();
        let sub_opts = SubscribeOptions::with_no_local();
        let opts = ResponseOptionsBuilder::new()
            .token(tok.clone())
            .subscribe_options(sub_opts)
            .finalize();

        let inner = Token::into_raw(tok);

        assert!(opts.copts.onSuccess.is_none());
        assert!(opts.copts.onFailure.is_none());
        // Check that the context is pointing to the right Token
        assert_eq!(inner, opts.copts.context);
        assert!(opts.copts.onSuccess5.is_some());
        assert!(opts.copts.onFailure5.is_some());

        assert!(opts.copts.subscribeOptions.noLocal != 0);

        unsafe { Token::from_raw(inner) };
    }

    #[test]
    fn test_sub_many_opts() {
        let tok = Token::new();
        let sub_opts = vec![SubscribeOptions::with_no_local(); 4];
        let opts = ResponseOptionsBuilder::new()
            .token(tok.clone())
            .subscribe_many_options(&sub_opts)
            .finalize();

        let inner = Token::into_raw(tok);

        assert!(opts.copts.onSuccess.is_none());
        assert!(opts.copts.onFailure.is_none());
        // Check that the context is pointing to the right Token
        assert_eq!(inner, opts.copts.context);
        assert!(opts.copts.onSuccess5.is_some());
        assert!(opts.copts.onFailure5.is_some());

        assert_eq!(0, opts.copts.subscribeOptions.noLocal);

        assert_eq!(4, opts.copts.subscribeOptionsCount);
        assert!(!opts.copts.subscribeOptionsList.is_null());

        unsafe {
            let sub_opts_list = std::slice::from_raw_parts(opts.copts.subscribeOptionsList, 4);
            assert!(sub_opts_list[0].noLocal != 0);
            assert!(sub_opts_list[1].noLocal != 0);
            assert!(sub_opts_list[2].noLocal != 0);
            assert!(sub_opts_list[3].noLocal != 0);
        }

        unsafe { Token::from_raw(inner) };
    }
}
