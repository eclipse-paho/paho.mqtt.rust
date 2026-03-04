// message.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2023 Frank Pagliughi <fpagliughi@mindspring.com>
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

use std::{
    borrow::Cow,
    convert::From,
    ffi::CString,
    fmt,
    os::raw::{c_int, c_void},
    pin::Pin,
    slice,
};

use crate::{ffi, properties::Properties, to_c_bool, QoS};

/// A `Message` represents all the information passed in an MQTT PUBLISH
/// packet.
/// This is the primary data transfer mechanism.
#[derive(Debug)]
pub struct Message {
    pub(crate) cmsg: ffi::MQTTAsync_message,
    pub(crate) data: Pin<Box<MessageData>>,
}

/// Cache of data values that the C msg struct point to.
#[derive(Debug, Default, Clone)]
pub(crate) struct MessageData {
    pub(crate) topic: CString,
    pub(crate) payload: Vec<u8>,
    pub(crate) props: Properties,
}

impl MessageData {
    /// Creates new message data from the topic and payload.
    pub(crate) fn new<S, V>(topic: S, payload: V) -> Self
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        Self {
            topic: CString::new(topic.into()).unwrap(),
            payload: payload.into(),
            props: Properties::default(),
        }
    }
}

impl Message {
    /// Creates a new message.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic on which the message is published.
    /// * `payload` The binary payload of the message
    /// * `qos` The quality of service for message delivery (0, 1, or 2)
    pub fn new<S, V, Q>(topic: S, payload: V, qos: Q) -> Self
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
        Q: Into<QoS>,
    {
        let cmsg = ffi::MQTTAsync_message {
            qos: qos.into() as c_int,
            ..ffi::MQTTAsync_message::default()
        };
        let data = MessageData::new(topic, payload);
        Self::from_data(cmsg, data)
    }

    /// Creates a new message that will be retained by the broker.
    /// This creates a message with the 'retained' flag set.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic on which the message is published.
    /// * `payload` The binary payload of the message
    /// * `qos` The quality of service for message delivery (0, 1, or 2)
    ///
    pub fn new_retained<S, V, Q>(topic: S, payload: V, qos: Q) -> Self
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
        Q: Into<QoS>,
    {
        let cmsg = ffi::MQTTAsync_message {
            qos: qos.into() as c_int,
            retained: 1,
            ..ffi::MQTTAsync_message::default()
        };
        let data = MessageData::new(topic, payload);
        Self::from_data(cmsg, data)
    }

    /// Creates a message from the underlying parts.
    ///
    /// This fixes up the pointers in the C msg struct to point to the
    /// the individual components in the data, all of which get pinned.
    fn from_data(mut cmsg: ffi::MQTTAsync_message, data: MessageData) -> Self {
        let data = Box::pin(data);
        cmsg.payload = data.payload.as_ptr() as *const _ as *mut c_void;
        cmsg.payloadlen = data.payload.len() as i32;
        cmsg.properties = data.props.cprops;
        Self { cmsg, data }
    }

    /// Creates a new message from C language components.
    ///
    /// # Arguments
    ///
    /// * `topic` The topic on which the message is published.
    /// * `msg` The message struct from the C library
    ///
    /// # Safety
    ///
    /// Caller must make sure that cmsg is valid
    pub(crate) unsafe fn from_c_parts(topic: CString, cmsg: &ffi::MQTTAsync_message) -> Self {
        let len = cmsg.payloadlen as usize;

        let payload = if cmsg.payload.is_null() {
            Vec::new()
        }
        else {
            slice::from_raw_parts(cmsg.payload as *mut u8, len).to_vec()
        };

        let data = MessageData {
            topic,
            payload,
            props: Properties::from_c_struct(&cmsg.properties),
        };

        Self::from_data(*cmsg, data)
    }

    /// Gets the topic for the message.
    pub fn topic(&self) -> &str {
        self.data
            .topic
            .to_str()
            .expect("paho.mqtt.c already validated utf8")
    }

    /// Gets the payload of the message.
    /// This returns the payload as a slice.
    pub fn payload(&self) -> &[u8] {
        self.data.payload.as_slice()
    }

    /// Gets the payload of the message as a string.
    ///
    /// This utilizes the "lossy" style of conversion from the std library.
    /// If the contents of the CStr are valid UTF-8 data, this function will
    /// return a `Cow::Borrowed(&str)` with the the corresponding `&str` slice.
    /// Otherwise, it will replace any invalid UTF-8 sequences with U+FFFD
    /// REPLACEMENT CHARACTER and return a `Cow::Owned(String)` with the result.
    pub fn payload_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.data.payload)
    }

    /// Returns the Quality of Service (QOS) for the message.
    pub fn qos(&self) -> QoS {
        QoS::from(self.cmsg.qos)
    }

    /// Gets the 'retain' flag for the message.
    pub fn retained(&self) -> bool {
        self.cmsg.retained != 0
    }

    /// Gets the properties in the message
    pub fn properties(&self) -> &Properties {
        &self.data.props
    }
}

impl Default for Message {
    /// Creates a default, empty message
    fn default() -> Message {
        Self::from_data(ffi::MQTTAsync_message::default(), MessageData::default())
    }
}

impl Clone for Message {
    /// Create a clone of the message
    fn clone(&self) -> Self {
        Self::from_data(self.cmsg, (*self.data).clone())
    }
}

unsafe impl Send for Message {}
unsafe impl Sync for Message {}

impl<'a, 'b> From<(&'a str, &'b [u8])> for Message {
    fn from((topic, payload): (&'a str, &'b [u8])) -> Self {
        Self::from_data(
            ffi::MQTTAsync_message::default(),
            MessageData::new(topic, payload),
        )
    }
}

impl<'a, 'b> From<(&'a str, &'b [u8], i32, bool)> for Message {
    fn from((topic, payload, qos, retained): (&'a str, &'b [u8], i32, bool)) -> Self {
        let cmsg = ffi::MQTTAsync_message {
            qos,
            retained: to_c_bool(retained),
            ..ffi::MQTTAsync_message::default()
        };
        Self::from_data(cmsg, MessageData::new(topic, payload))
    }
}

impl fmt::Display for Message {
    /// Formats the message for display
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topic = match self.data.topic.as_c_str().to_str() {
            Ok(s) => s,
            Err(_) => return Err(fmt::Error),
        };
        let payload = self.payload_str();
        write!(f, "{}: {}", topic, payload)
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Builder to create a new Message
#[derive(Debug)]
pub struct MessageBuilder {
    topic: String,
    payload: Vec<u8>,
    qos: QoS,
    retained: bool,
    props: Properties,
}

impl MessageBuilder {
    /// Create a new message builder.
    pub fn new() -> Self {
        Self {
            topic: String::new(),
            payload: Vec::new(),
            qos: QoS::default(),
            retained: false,
            props: Properties::default(),
        }
    }

    /// Sets the topic for the message
    ///
    /// # Arguments
    ///
    /// `topic` The topic on which the message should be published.
    pub fn topic<T>(mut self, topic: T) -> Self
    where
        T: Into<String>,
    {
        self.topic = topic.into();
        self
    }

    /// Sets the payload for the message
    ///
    /// # Arguments
    ///
    /// `payload` The binary payload of the message
    pub fn payload<V>(mut self, payload: V) -> Self
    where
        V: Into<Vec<u8>>,
    {
        self.payload = payload.into();
        self
    }

    /// Sets the Quality of Service for the message.
    ///
    /// # Arguments
    ///
    /// `qos` The quality of service for the message.
    pub fn qos<Q: Into<QoS>>(mut self, qos: Q) -> Self {
        self.qos = qos.into();
        self
    }

    /// Sets whether or not the published message should be retained by the
    /// broker.
    ///
    /// # Arguments
    ///
    /// `retained` Set true if the message should be retained by the broker,
    ///            false if not.
    pub fn retained(mut self, retained: bool) -> Self {
        self.retained = retained;
        self
    }

    /// Sets the collection of properties for the message.
    ///
    /// # Arguments
    ///
    /// `props` The collection of properties to include with the message.
    pub fn properties(mut self, props: Properties) -> Self {
        self.props = props;
        self
    }

    /// Finalize the builder to create the message.
    pub fn finalize(self) -> Message {
        let cmsg = ffi::MQTTAsync_message {
            qos: self.qos as c_int,
            retained: to_c_bool(self.retained),
            ..ffi::MQTTAsync_message::default()
        };
        let data = MessageData {
            topic: CString::new(self.topic).unwrap(),
            payload: self.payload,
            props: self.props,
        };
        Message::from_data(cmsg, data)
    }
}

impl Default for MessageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use std::{os::raw::c_char, thread};

    const STRUCT_ID: [c_char; 4] = [
        b'M' as c_char,
        b'Q' as c_char,
        b'T' as c_char,
        b'M' as c_char,
    ];

    const STRUCT_VERSION: i32 = ffi::MESSAGE_STRUCT_VERSION;

    // These should differ from defaults
    const TOPIC: &str = "test";
    const PAYLOAD: &[u8] = b"Hello world";
    const QOS: QoS = QoS::ExactlyOnce;
    const RETAINED: bool = true;

    // By convention our defaults should match the defaults of the C library
    #[test]
    fn test_default() {}

    #[test]
    fn test_new() {
        let msg = Message::new(TOPIC, PAYLOAD, QOS);

        assert_eq!(STRUCT_ID, msg.cmsg.struct_id);
        assert_eq!(STRUCT_VERSION, msg.cmsg.struct_version);

        assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, msg.data.payload.as_slice());

        assert_eq!(msg.data.payload.len() as i32, msg.cmsg.payloadlen);
        assert_eq!(msg.data.payload.as_ptr() as *mut c_void, msg.cmsg.payload);

        assert_eq!(QOS as c_int, msg.cmsg.qos);
        assert!(msg.cmsg.retained == 0);
    }

    #[test]
    fn test_from_2_tuple() {
        let msg = Message::from((TOPIC, PAYLOAD));

        // The topic is only kept in the Rust struct as a CString
        assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, msg.data.payload.as_slice());

        assert_eq!(msg.data.payload.len() as i32, msg.cmsg.payloadlen);
        assert_eq!(msg.data.payload.as_ptr() as *mut c_void, msg.cmsg.payload);
    }

    #[test]
    fn test_from_4_tuple() {
        let msg = Message::from((TOPIC, PAYLOAD, QOS as i32, RETAINED));

        // The topic is only kept in the Rust struct as a CString
        assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, msg.data.payload.as_slice());

        assert_eq!(msg.data.payload.len() as i32, msg.cmsg.payloadlen);
        assert_eq!(msg.data.payload.as_ptr() as *mut c_void, msg.cmsg.payload);

        assert_eq!(QOS as c_int, msg.cmsg.qos);
        assert!(msg.cmsg.retained != 0);
    }

    #[test]
    fn test_builder_default() {
        let msg = MessageBuilder::new().finalize();
        let cmsg = ffi::MQTTAsync_message::default();

        assert_eq!(STRUCT_ID, cmsg.struct_id);
        assert_eq!(STRUCT_VERSION, cmsg.struct_version);

        assert_eq!(cmsg.struct_id, msg.cmsg.struct_id);
        assert_eq!(cmsg.struct_version, msg.cmsg.struct_version);

        assert_eq!(0, msg.data.topic.as_bytes().len());
        assert_eq!(&[] as &[u8], msg.data.topic.as_bytes());
        assert_eq!(&[] as &[u8], msg.data.payload.as_slice());
    }

    #[test]
    fn test_builder_topic() {
        const TOPIC: &str = "test";

        let msg = MessageBuilder::new().topic(TOPIC).finalize();

        // The topic is only kept in the Rust struct as a CString
        assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
        assert_eq!(TOPIC, msg.topic());
    }

    #[test]
    fn test_builder_payload() {
        const PAYLOAD: &[u8] = b"Hello world";

        let msg = MessageBuilder::new().payload(PAYLOAD).finalize();

        assert_eq!(PAYLOAD, msg.data.payload.as_slice());
        assert_eq!(PAYLOAD, msg.payload());

        assert_eq!(msg.data.payload.len() as i32, msg.cmsg.payloadlen);
        assert_eq!(msg.data.payload.as_ptr() as *mut c_void, msg.cmsg.payload);
    }

    #[test]
    fn test_builder_qos() {
        let msg = MessageBuilder::new().qos(QOS).finalize();

        assert_eq!(QOS as c_int, msg.cmsg.qos);
        assert_eq!(QOS, msg.qos());

        let qos = 1;
        let msg = MessageBuilder::new().qos(qos).finalize();

        assert_eq!(qos, msg.cmsg.qos);
        assert_eq!(QoS::from(qos), msg.qos());
    }

    #[test]
    fn test_builder_retained() {
        let msg = MessageBuilder::new().retained(false).finalize();
        assert!(msg.cmsg.retained == 0);

        let msg = MessageBuilder::new().retained(true).finalize();
        assert!(msg.cmsg.retained != 0);
        assert!(msg.retained());
    }

    // Make sure assignment works properly
    // This primarily ensures that C pointers stay fixed to cached values,
    // and/or that the cached buffers don't move due to assignment.
    #[test]
    fn test_assign() {
        let org_msg = MessageBuilder::new()
            .topic(TOPIC)
            .payload(PAYLOAD)
            .qos(QOS)
            .retained(RETAINED)
            .finalize();

        let msg = org_msg;

        // The topic is only kept in the Rust struct as a CString
        assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, msg.data.payload.as_slice());

        assert_eq!(msg.data.payload.len() as i32, msg.cmsg.payloadlen);
        assert_eq!(msg.data.payload.as_ptr() as *mut c_void, msg.cmsg.payload);

        assert_eq!(QOS as c_int, msg.cmsg.qos);
        assert!(msg.cmsg.retained != 0);
    }

    // Test that a clone works properly.
    // This ensures that the cached values are cloned and that the C pointers
    // in the new object point to those clones.
    #[test]
    fn test_clone() {
        const TOPIC: &str = "test";
        const PAYLOAD: &[u8] = b"Hello world";
        const QOS: QoS = QoS::ExactlyOnce;
        const RETAINED: bool = true;

        let msg = {
            // Make sure the original goes out of scope before testing
            let org_msg = MessageBuilder::new()
                .topic(TOPIC)
                .payload(PAYLOAD)
                .qos(QOS)
                .retained(RETAINED)
                .finalize();
            org_msg.clone()
        };

        assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
        assert_eq!(PAYLOAD, msg.data.payload.as_slice());

        assert_eq!(msg.data.payload.len() as i32, msg.cmsg.payloadlen);
        assert_eq!(msg.data.payload.as_ptr() as *mut c_void, msg.cmsg.payload);

        assert_eq!(QOS as c_int, msg.cmsg.qos);
        assert!(msg.cmsg.retained != 0);
    }

    // Determine that a message can be sent across threads.
    // As long as it compiles, this indicates that Message implements
    // the Send trait.
    #[test]
    fn test_message_send() {
        let msg = Message::new(TOPIC, PAYLOAD, QOS);

        let thr = thread::spawn(move || {
            assert_eq!(TOPIC, msg.data.topic.to_str().unwrap());
            assert_eq!(PAYLOAD, msg.data.payload.as_slice());
            assert_eq!(QOS, msg.qos());
        });
        thr.join().unwrap();
    }
}
