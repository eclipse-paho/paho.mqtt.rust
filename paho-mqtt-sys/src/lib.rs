// lib.rs
//
// The main library file for `paho-mqtt-sys`.
// This is the Paho MQTT Rust library low-level C wrapper.
//

/*******************************************************************************
 * Copyright (c) 2017-2019 Frank Pagliughi <fpagliughi@mindspring.com>
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

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// Weirdness in the bindgen bindings
#![allow(deref_nullptr)]
// Temporary
#![allow(dead_code)]

use std::fmt;
use std::os::raw::{c_char, c_int};
use std::ptr;

#[cfg(feature = "ssl")]
mod __make_openssl_linkage_work {
    #[allow(unused_imports)]
    use openssl_sys::*;
}

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

// The following 'Default' trait implementations contain initializations
// for the structures from the Paho C library. Each of those structs
// contains an initializer macro in MQTTAsync.h.
// By convention, these default initializers match those macros from the
// C library.

/////////////////////////////////////////////////////////////////////////////
// Client creation

/// The currently supported version of the C create options struct.
pub const CREATE_OPTIONS_STRUCT_VERSION: i32 = 2;

impl Default for MQTTAsync_createOptions {
    /// Creates a client that can connect using MQTT v3.x or v5
    fn default() -> Self {
        Self {
            struct_id: [
                b'M' as c_char,
                b'Q' as c_char,
                b'C' as c_char,
                b'O' as c_char,
            ],
            struct_version: CREATE_OPTIONS_STRUCT_VERSION,
            sendWhileDisconnected: 0,
            maxBufferedMessages: 100,
            MQTTVersion: MQTTVERSION_5 as c_int,
            allowDisconnectedSendAtAnyTime: 0,
            deleteOldestMessages: 0,
            restoreMessages: 1,
            persistQoS0: 1,
        }
    }
}

impl MQTTAsync_createOptions {
    /// Creates a client that can connect using MQTT v3.x or v5
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a client that can only connect using MQTT v3.x
    pub fn new_v3() -> Self {
        Self {
            MQTTVersion: MQTTVERSION_DEFAULT as c_int,
            ..Self::default()
        }
    }

    /// Creates a client that can connect using MQTT v3.x or v5
    pub fn new_v5() -> Self {
        Self::default()
    }
}

/////////////////////////////////////////////////////////////////////////////
// Connecting

// Note that this sets up the defaults for MQTT 3.1.1 or earlier.
// The application must specifically set the version to 5 for MQTT v5, and
// disable clean sessions (at a minimum).

/// The currently supported version of the C connect options struct.
pub const CONNECT_OPTIONS_STRUCT_VERSION: i32 = 8;

impl Default for MQTTAsync_connectOptions {
    fn default() -> Self {
        Self {
            struct_id: [
                b'M' as c_char,
                b'Q' as c_char,
                b'T' as c_char,
                b'C' as c_char,
            ],
            struct_version: CONNECT_OPTIONS_STRUCT_VERSION,
            keepAliveInterval: 60,
            cleansession: 1,
            maxInflight: 65535,
            will: ptr::null_mut(),
            username: ptr::null(),
            password: ptr::null(),
            connectTimeout: 30,
            retryInterval: 0,
            ssl: ptr::null_mut(),
            onSuccess: None,
            onFailure: None,
            context: ptr::null_mut(),
            serverURIcount: 0,
            serverURIs: ptr::null(),
            MQTTVersion: 0,
            automaticReconnect: 0,
            minRetryInterval: 1,
            maxRetryInterval: 60,
            binarypwd: MQTTAsync_connectOptions__bindgen_ty_1 {
                len: 0,
                data: ptr::null(),
            },
            cleanstart: 0,
            connectProperties: ptr::null_mut(),
            willProperties: ptr::null_mut(),
            onSuccess5: None,
            onFailure5: None,
            httpHeaders: ptr::null(),
            httpProxy: ptr::null(),
            httpsProxy: ptr::null(),
        }
    }
}

impl MQTTAsync_connectOptions {
    /// Creates default connect options for v3.x
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates default connect options specifically for v3.x
    pub fn new_v3() -> Self {
        Self::default()
    }

    /// Creates default connect options for v5
    pub fn new_v5() -> Self {
        Self {
            cleansession: 0,
            cleanstart: 1,
            MQTTVersion: MQTTVERSION_5 as i32,
            ..Self::default()
        }
    }

    /// Creates default connect options for v3.x over websockets
    pub fn new_ws() -> Self {
        Self {
            keepAliveInterval: 45,
            ..Self::default()
        }
    }

    /// Creates default connect options for v3.x over websockets
    pub fn new_ws_v5() -> Self {
        Self {
            keepAliveInterval: 45,
            ..Self::new_v5()
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Options

/// The currently supported version of the C will options struct.
pub const WILL_OPTIONS_STRUCT_VERSION: i32 = 1;

impl Default for MQTTAsync_willOptions {
    fn default() -> MQTTAsync_willOptions {
        MQTTAsync_willOptions {
            struct_id: [
                b'M' as c_char,
                b'Q' as c_char,
                b'T' as c_char,
                b'W' as c_char,
            ],
            struct_version: WILL_OPTIONS_STRUCT_VERSION,
            topicName: ptr::null(),
            message: ptr::null(),
            retained: 0,
            qos: 0,
            payload: MQTTAsync_willOptions__bindgen_ty_1 {
                len: 0,
                data: ptr::null(),
            },
        }
    }
}

/// The currently supported version of the C SSL options struct.
pub const SSL_OPTIONS_STRUCT_VERSION: i32 = 5;

impl Default for MQTTAsync_SSLOptions {
    fn default() -> MQTTAsync_SSLOptions {
        MQTTAsync_SSLOptions {
            struct_id: [
                b'M' as c_char,
                b'Q' as c_char,
                b'T' as c_char,
                b'S' as c_char,
            ],
            struct_version: SSL_OPTIONS_STRUCT_VERSION,
            trustStore: ptr::null(),
            keyStore: ptr::null(),
            privateKey: ptr::null(),
            privateKeyPassword: ptr::null(),
            enabledCipherSuites: ptr::null(),
            enableServerCertAuth: 1,
            sslVersion: MQTT_SSL_VERSION_DEFAULT as i32,
            verify: 0,
            CApath: ptr::null(),
            ssl_error_cb: None,
            ssl_error_context: ptr::null_mut(),
            ssl_psk_cb: None,
            ssl_psk_context: ptr::null_mut(),
            disableDefaultTrustStore: 0,
            protos: ptr::null(),
            protos_len: 0,
        }
    }
}

/// The currently supported version of the C subscribe options struct.
pub const SUBSCRIBE_OPTIONS_STRUCT_VERSION: i32 = 0;

// New for MQTT v5
impl Default for MQTTSubscribe_options {
    fn default() -> MQTTSubscribe_options {
        MQTTSubscribe_options {
            struct_id: [
                b'M' as c_char,
                b'Q' as c_char,
                b'S' as c_char,
                b'O' as c_char,
            ],
            struct_version: SUBSCRIBE_OPTIONS_STRUCT_VERSION,
            noLocal: 0,
            retainAsPublished: 0,
            retainHandling: 0,
        }
    }
}

/// The currently suppoted version of the C response options struct.
pub const RESPONSE_OPTIONS_STRUCT_VERSION: i32 = 1;

impl Default for MQTTAsync_responseOptions {
    fn default() -> MQTTAsync_responseOptions {
        MQTTAsync_responseOptions {
            struct_id: [
                b'M' as c_char,
                b'Q' as c_char,
                b'T' as c_char,
                b'R' as c_char,
            ],
            struct_version: RESPONSE_OPTIONS_STRUCT_VERSION,
            onSuccess: None,
            onFailure: None,
            context: ptr::null_mut(),
            token: 0,
            onSuccess5: None,
            onFailure5: None,
            properties: MQTTProperties::default(),
            subscribeOptions: MQTTSubscribe_options::default(),
            subscribeOptionsCount: 0,
            subscribeOptionsList: ptr::null_mut(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// MQTTProperties (new for v5)

impl fmt::Debug for MQTTProperty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Implement this
        write!(f, "[]")
    }
}

impl Default for MQTTProperties {
    fn default() -> MQTTProperties {
        MQTTProperties {
            count: 0,
            max_count: 0,
            length: 0,
            array: ptr::null_mut(),
        }
    }
}

impl Default for MQTTLenString {
    fn default() -> MQTTLenString {
        MQTTLenString {
            len: 0,
            data: ptr::null_mut(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Messages

/// The currently suppoted version of the C message struct.
pub const MESSAGE_STRUCT_VERSION: i32 = 1;

impl Default for MQTTAsync_message {
    fn default() -> MQTTAsync_message {
        MQTTAsync_message {
            struct_id: [
                b'M' as c_char,
                b'Q' as c_char,
                b'T' as c_char,
                b'M' as c_char,
            ],
            struct_version: MESSAGE_STRUCT_VERSION,
            payloadlen: 0,
            payload: ptr::null_mut(),
            qos: 0,
            retained: 0,
            dup: 0,
            msgid: 0,
            properties: MQTTProperties::default(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Disconnecting

/// The currently suppoted version of the C disconnect options struct.
pub const DISCONNECT_OPTIONS_STRUCT_VERSION: i32 = 1;

impl Default for MQTTAsync_disconnectOptions {
    fn default() -> MQTTAsync_disconnectOptions {
        MQTTAsync_disconnectOptions {
            struct_id: [
                b'M' as c_char,
                b'Q' as c_char,
                b'T' as c_char,
                b'D' as c_char,
            ],
            struct_version: DISCONNECT_OPTIONS_STRUCT_VERSION,
            timeout: 0,
            onSuccess: None,
            onFailure: None,
            context: ptr::null_mut(),
            properties: MQTTProperties::default(),
            reasonCode: 0,
            onSuccess5: None,
            onFailure5: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Persistence

impl Default for MQTTClient_persistence {
    fn default() -> Self {
        MQTTClient_persistence {
            context: ptr::null_mut(),
            popen: None,
            pclose: None,
            pput: None,
            pget: None,
            premove: None,
            pkeys: None,
            pclear: None,
            pcontainskey: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Misc Data Structures

impl MQTTAsync_nameValue {
    pub fn new(name: *const c_char, value: *const c_char) -> Self {
        Self { name, value }
    }
}

impl Default for MQTTAsync_nameValue {
    fn default() -> Self {
        Self {
            name: ptr::null(),
            value: ptr::null(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Unit Tests

#[cfg(test)]
mod tests {}
