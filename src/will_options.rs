// will_options.rs
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

//! Last Will and Testament (LWT) options for the Paho MQTT Rust client library.
//!

use std::{borrow::Cow, os::raw::c_void, pin::Pin, ptr};

use crate::{
    ffi,
    ffi_utils::{MessageFfi, MessageFfiData},
    message::Message,
};

/// The options for the Last Will and Testament (LWT).
/// This defines a message that is registered with the the server at the time
/// of connection. Then if the connection is lost unexpectedly, the message
/// is published by the server.
///
/// The will options are somewhat redundant in that they simply represent a
/// message, albeit a special one, that is included in the connect options.
/// This structure may eventually be phased out, and therefore users are
/// encouraged to just create a `Message` object and use it when building
/// `ConnectOptions`:
/// ```
/// use paho_mqtt as mqtt;
///
/// let lwt = mqtt::Message::new("lwt", "disconnected", 1);
/// let opts = mqtt::ConnectOptionsBuilder::new().will_message(lwt).finalize();
/// ```
///
#[derive(Debug)]
pub struct WillOptions {
    /// The underlying C options struct
    pub(crate) copts: ffi::MQTTAsync_willOptions,
    /// The local data cache for the C options
    data: Pin<Box<MessageFfiData>>,
}

impl WillOptions {
    /// Creates a new WillOptions message.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic on which the LWT message is to be published.
    /// * `payload` The binary payload of the LWT message
    /// * `qos` The quality of service for message delivery (0, 1, or 2)
    pub fn new<S, V>(topic: S, payload: V, qos: i32) -> WillOptions
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let copts = ffi::MQTTAsync_willOptions {
            qos,
            ..ffi::MQTTAsync_willOptions::default()
        };
        Self::from_data(copts, MessageFfiData::new(topic, payload))
    }

    /// Creates a new WillOptions message with the 'retain' flag set.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic on which the LWT message is to be published.
    /// * `payload` The binary payload of the LWT message
    /// * `qos` The quality of service for message delivery (0, 1, or 2)
    pub fn new_retained<S, V>(topic: S, payload: V, qos: i32) -> WillOptions
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut opts = Self::new(topic, payload, qos);
        opts.copts.retained = 1;
        opts
    }

    // Updates the C struct from the cached topic and payload vars
    fn from_data(copts: ffi::MQTTAsync_willOptions, data: MessageFfiData) -> Self {
        Self::from_pinned_data(copts, Box::pin(data))
    }

    // Updates the C struct from the cached topic and payload vars
    fn from_pinned_data(
        mut copts: ffi::MQTTAsync_willOptions,
        data: Pin<Box<MessageFfiData>>,
    ) -> Self {
        copts.topicName = if !data.topic.as_bytes().is_empty() {
            data.topic.as_ptr()
        }
        else {
            ptr::null()
        };
        copts.payload.len = data.payload.len() as i32;
        copts.payload.data = if copts.payload.len != 0 {
            data.payload.as_ptr() as *const c_void
        }
        else {
            ptr::null()
        };
        // Note: For some reason, properties aren't in the will options
        //   They're in the connect options
        Self { copts, data }
    }

    /// Gets the topic string for the LWT
    pub fn topic(&self) -> &str {
        self.data.topic.to_str().unwrap()
    }

    /// Gets the payload of the LWT
    pub fn payload(&self) -> &[u8] {
        &self.data.payload
    }

    /// Gets the payload of the message as a string.
    /// Note that this clones the payload.
    pub fn payload_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.data.payload)
    }

    /// Returns the Quality of Service (QOS) for the message.
    pub fn qos(&self) -> i32 {
        self.copts.qos
    }

    /// Gets the 'retained' flag for the message.
    pub fn retained(&self) -> bool {
        self.copts.retained != 0
    }
}

impl Default for WillOptions {
    /// Creates a WillOptions struct with default options
    fn default() -> Self {
        Self::from_data(
            ffi::MQTTAsync_willOptions::default(),
            MessageFfiData::default(),
        )
    }
}

impl Clone for WillOptions {
    /// Creates a clone of the WillOptions struct.
    /// This clones the cached values and updates the C struct to refer
    /// to them.
    fn clone(&self) -> Self {
        Self::from_data(self.copts, (*self.data).clone())
    }
}

unsafe impl Send for WillOptions {}
unsafe impl Sync for WillOptions {}

impl From<Message> for WillOptions {
    /// Create `WillOptions` from a `Message`
    fn from(msg: Message) -> Self {
        // TODO: Convert to try_from()
        let msg = MessageFfi::try_from(msg).unwrap();

        let copts = ffi::MQTTAsync_willOptions {
            qos: msg.cmsg.qos,
            retained: msg.cmsg.retained,
            ..ffi::MQTTAsync_willOptions::default()
        };
        Self::from_pinned_data(copts, msg.data)
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MessageBuilder;

    // These should differ from defaults.
    const TOPIC: &str = "test";
    const PAYLOAD: &[u8] = b"Hello world";
    const QOS: i32 = 2;
    const RETAINED: bool = true;

    // By convention our defaults should match the defaults of the C library
    #[test]
    fn test_default() {
        let opts = WillOptions::default();
        // Get default C options for comparison
        let copts = ffi::MQTTAsync_willOptions::default();

        assert_eq!(ptr::null(), copts.message);

        assert_eq!(copts.struct_id, opts.copts.struct_id);
        assert_eq!(copts.struct_version, opts.copts.struct_version);
        assert_eq!(copts.topicName, opts.copts.topicName);
        assert_eq!(copts.retained, opts.copts.retained);
        assert_eq!(copts.qos, opts.copts.qos);
        assert_eq!(copts.payload.len, opts.copts.payload.len);
        assert_eq!(copts.payload.data, opts.copts.payload.data);

        assert_eq!(&[] as &[u8], opts.data.topic.as_bytes());
        assert_eq!(&[] as &[u8], opts.data.payload.as_slice());
    }

    #[test]
    fn test_new() {
        let msg = WillOptions::new(TOPIC, PAYLOAD, QOS);
        let default_copts = ffi::MQTTAsync_willOptions::default();

        assert_eq!(default_copts.struct_id, msg.copts.struct_id);
        assert_eq!(default_copts.struct_version, msg.copts.struct_version);

        assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, msg.data.payload.as_slice());

        assert_eq!(msg.data.payload.len() as i32, msg.copts.payload.len);
        assert_eq!(
            msg.data.payload.as_ptr() as *const c_void,
            msg.copts.payload.data
        );

        assert_eq!(QOS, msg.copts.qos);
        assert!(msg.copts.retained == 0);
    }

    #[test]
    fn test_new_retained() {
        let msg = WillOptions::new_retained(TOPIC, PAYLOAD, QOS);
        let default_copts = ffi::MQTTAsync_willOptions::default();

        assert_eq!(default_copts.struct_id, msg.copts.struct_id);
        assert_eq!(default_copts.struct_version, msg.copts.struct_version);

        assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, msg.data.payload.as_slice());

        assert_eq!(msg.data.payload.len() as i32, msg.copts.payload.len);
        assert_eq!(
            msg.data.payload.as_ptr() as *const c_void,
            msg.copts.payload.data
        );

        assert_eq!(QOS, msg.copts.qos);
        assert!(msg.copts.retained != 0);
    }

    // Test creating will options from a message
    #[test]
    fn test_from_message() {
        let msg = MessageBuilder::new()
            .topic(TOPIC)
            .payload(PAYLOAD)
            .qos(QOS)
            .retained(RETAINED)
            .finalize();

        let opts = WillOptions::from(msg);

        assert_eq!(TOPIC, opts.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, opts.data.payload.as_slice());

        assert_eq!(opts.data.payload.len() as i32, opts.copts.payload.len);
        assert_eq!(
            opts.data.payload.as_ptr() as *const c_void,
            opts.copts.payload.data
        );

        assert_eq!(QOS, opts.copts.qos);
        assert!(opts.copts.retained != 0);
    }

    // Make sure assignment works properly
    // This primarily ensures that C pointers stay fixed to cached values,
    // and/or that the cached buffers don't move due to assignment.
    #[test]
    fn test_assign() {
        let msg = MessageBuilder::new()
            .topic(TOPIC)
            .payload(PAYLOAD)
            .qos(QOS)
            .retained(RETAINED)
            .finalize();

        let org_opts = WillOptions::from(msg);
        let opts = org_opts;

        assert_eq!(TOPIC, opts.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, opts.data.payload.as_slice());

        assert_eq!(opts.data.payload.len() as i32, opts.copts.payload.len);
        assert_eq!(
            opts.data.payload.as_ptr() as *const c_void,
            opts.copts.payload.data
        );

        assert_eq!(QOS, opts.copts.qos);
        assert!(opts.copts.retained != 0);
    }

    // Test that a clone works properly.
    // This ensures that the cached values are cloned and that the C pointers
    // in the new object point to those clones.
    #[test]
    fn test_clone() {
        let opts = {
            let msg = MessageBuilder::new()
                .topic(TOPIC)
                .payload(PAYLOAD)
                .qos(QOS)
                .retained(RETAINED)
                .finalize();

            let org_opts = WillOptions::from(msg);
            org_opts.clone()
        };

        assert_eq!(TOPIC, opts.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, opts.data.payload.as_slice());

        assert_eq!(opts.data.payload.len() as i32, opts.copts.payload.len);
        assert_eq!(
            opts.data.payload.as_ptr() as *const c_void,
            opts.copts.payload.data
        );

        assert_eq!(QOS, opts.copts.qos);
        assert!(opts.copts.retained != 0);
    }
}
