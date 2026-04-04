// message.rs
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

use std::{
    borrow::Cow,
    ffi::{c_char, c_int, c_void, CString},
    fmt,
    pin::Pin,
    slice,
};

use crate::{
    ffi,
    properties::{Properties, PropertiesFfi},
    Error, QoS, Result,
};

/// An "outbound" MQTT message.
///
/// This is the form of an MQTT message created by the application for
/// publication. It consists of standard Rust fields.
#[derive(Debug, Default, Clone)]
pub struct OutMessage {
    /// The message topic
    pub topic: String,
    /// The message payload
    pub payload: Vec<u8>,
    /// The message quality of service
    pub qos: QoS,
    /// Whether this is a retain message
    pub retain: bool,
    /// The message properties
    pub props: Properties,
    /// Whether this is a duplicate
    /// Not to be set by the application
    pub dup: bool,
    /// The message identifier.
    pub msgid: Option<u16>,
}

impl OutMessage {
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
        Self {
            topic: topic.into(),
            payload: payload.into(),
            qos: qos.into(),
            ..Self::default()
        }
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
        Self {
            retain: true,
            ..Self::new(topic, payload, qos)
        }
    }
}

// --------------------------------------------------------------------------

/// An "inbound" MQTT message.
///
/// This is a message received from the remote broker. Internally, it's kept
/// in the received format to prevent unnecessary copying of the topic or
/// payload.
#[derive(Debug)]
pub struct InMessage {
    /// Pointer to the message topic from the C lib.
    /// The underlying library promises that this is UTF-8
    topic_ptr: *const c_char,
    /// The topic as a slice
    topic: &'static str,
    /// The C message structure
    cmsg: &'static ffi::MQTTAsync_message,
    /// The payload as a slice
    payload: &'static [u8],
    /// The properties in Rust format
    props: Properties,
}

impl InMessage {
    //// Creates a new inbound message from the C parts.
    pub(crate) fn new(
        topic_ptr: *const c_char,
        topic_len: usize,
        cmsg: *const ffi::MQTTAsync_message,
    ) -> Self {
        // TODO: Handle UTF-8 error(s) in the topic?
        unsafe {
            let topic = str::from_utf8_unchecked(slice::from_raw_parts(
                topic_ptr as *const _ as *const u8,
                topic_len,
            ));

            let cmsg = cmsg.as_ref().unwrap();

            let payload =
                slice::from_raw_parts(cmsg.payload as *const u8, cmsg.payloadlen as usize);

            let props = (&cmsg.properties).into();

            Self {
                topic_ptr,
                topic,
                cmsg,
                payload,
                props,
            }
        }
    }

    /// Gets the message topic.
    pub fn topic(&self) -> &str {
        self.topic
    }

    /// Gets the message payload
    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    /// Gets the payload of the message as a string.
    ///
    /// This utilizes the "lossy" style of conversion from the std library.
    /// If the contents of the CStr are valid UTF-8 data, this function will
    /// return a `Cow::Borrowed(&str)` with the the corresponding `&str` slice.
    /// Otherwise, it will replace any invalid UTF-8 sequences with U+FFFD
    /// REPLACEMENT CHARACTER and return a `Cow::Owned(String)` with the result.
    pub fn payload_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.payload)
    }

    /// Gets the QoS of the message.
    pub fn qos(&self) -> QoS {
        self.cmsg.qos.into()
    }

    /// Gets the retain flag of the message
    pub fn retain(&self) -> bool {
        crate::from_c_bool(self.cmsg.retained)
    }

    /// Gets the properties in the message
    pub fn properties(&self) -> &Properties {
        &self.props
    }

    /// Gets the message identifier.
    pub fn msgid(&self) -> Option<u16> {
        let msgid = self.cmsg.msgid as u16;
        (msgid != 0).then_some(msgid)
    }
}

unsafe impl Send for InMessage {}
unsafe impl Sync for InMessage {}

impl Drop for InMessage {
    // Deallocate the memory from the Paho C lib.
    fn drop(&mut self) {
        let mut cmsg = self.cmsg as *const _ as *mut ffi::MQTTAsync_message;
        unsafe {
            ffi::MQTTAsync_freeMessage(&mut cmsg);
            ffi::MQTTAsync_free(self.topic_ptr as *mut c_void);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

/// An MQTT Message.
/// This represents all the information passed in an MQTT PUBLISH packet.
/// It's the primary data transfer mechanism for the protocol.
#[derive(Debug)]
pub enum Message {
    /// An out-bound message, created by the app for publication .
    Out(OutMessage),
    /// An in-bound message received from the broker
    In(InMessage),
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
        Self::Out(OutMessage::new(topic, payload, qos))
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
        Self::Out(OutMessage::new_retained(topic, payload, qos))
    }

    /// Creates a message from the C parts
    pub(crate) fn from_c_parts(
        topic: *const c_char,
        topic_len: usize,
        cmsg: *const ffi::MQTTAsync_message,
    ) -> Self {
        Self::In(InMessage::new(topic, topic_len, cmsg))
    }

    /// Gets the message topic.
    pub fn topic(&self) -> &str {
        match self {
            Self::Out(msg) => &msg.topic,
            Self::In(msg) => msg.topic(),
        }
    }

    /// Gets the message payload
    pub fn payload(&self) -> &[u8] {
        match self {
            Self::Out(msg) => &msg.payload,
            Self::In(msg) => msg.payload(),
        }
    }

    /// Gets the payload of the message as a string.
    ///
    /// This utilizes the "lossy" style of conversion from the std library.
    /// If the contents of the CStr are valid UTF-8 data, this function will
    /// return a `Cow::Borrowed(&str)` with the the corresponding `&str` slice.
    /// Otherwise, it will replace any invalid UTF-8 sequences with U+FFFD
    /// REPLACEMENT CHARACTER and return a `Cow::Owned(String)` with the result.
    pub fn payload_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.payload())
    }

    /// Gets the QoS of the message.
    pub fn qos(&self) -> QoS {
        match self {
            Self::Out(msg) => msg.qos,
            Self::In(msg) => msg.qos(),
        }
    }

    /// Gets the retain flag of the message
    pub fn retain(&self) -> bool {
        match self {
            Self::Out(msg) => msg.retain,
            Self::In(msg) => msg.retain(),
        }
    }

    // TODO: Mark as deprecated?
    /// Gets the retain flag of the message
    #[inline]
    pub fn retained(&self) -> bool {
        self.retain()
    }

    /// Gets the properties in the message
    pub fn properties(&self) -> &Properties {
        match self {
            Self::Out(msg) => &msg.props,
            Self::In(msg) => msg.properties(),
        }
    }

    /// Gets the message identifier.
    pub fn msgid(&self) -> Option<u16> {
        match self {
            Self::Out(msg) => msg.msgid,
            Self::In(msg) => msg.msgid(),
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self::Out(OutMessage::default())
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        match self {
            Self::Out(msg) => Self::Out(msg.clone()),
            Self::In(msg) => {
                let topic = msg.topic().to_string();
                let payload = msg.payload().to_vec();
                let qos = msg.qos();
                let retain = msg.retain();
                let props = msg.properties().clone();

                Self::Out(OutMessage {
                    topic,
                    payload,
                    qos,
                    retain,
                    props,
                    // TODO: Implement these?
                    ..OutMessage::default()
                })
            }
        }
    }
}

impl fmt::Display for Message {
    /// Formats the message for display
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.topic(), self.payload_str())
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Builder to create a new Message
#[derive(Debug, Default, Clone)]
pub struct MessageBuilder(OutMessage);

impl MessageBuilder {
    /// Create a new message builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the topic for the message
    ///
    /// # Arguments
    ///
    /// `topic` The topic on which the message should be published.
    pub fn topic<T: Into<String>>(mut self, topic: T) -> Self {
        self.0.topic = topic.into();
        self
    }

    /// Sets the payload for the message
    ///
    /// # Arguments
    ///
    /// `payload` The binary payload of the message
    pub fn payload<V: Into<Vec<u8>>>(mut self, payload: V) -> Self {
        self.0.payload = payload.into();
        self
    }

    /// Sets the Quality of Service for the message.
    ///
    /// # Arguments
    ///
    /// `qos` The quality of service for the message.
    pub fn qos<Q: Into<QoS>>(mut self, qos: Q) -> Self {
        self.0.qos = qos.into();
        self
    }

    /// Sets whether or not the published message should be retained by the
    /// broker.
    ///
    /// # Arguments
    ///
    /// `retained` Set true if the message should be retained by the broker,
    ///            false if not.
    pub fn retain(mut self, retain: bool) -> Self {
        self.0.retain = retain;
        self
    }

    // TODO: Mark deprecated?
    /// Obsolete method for `retain`
    pub fn retained(self, retain: bool) -> Self {
        self.retain(retain)
    }

    /// Sets the collection of properties for the message.
    ///
    /// # Arguments
    ///
    /// `props` The collection of properties to include with the message.
    pub fn properties(mut self, props: Properties) -> Self {
        self.0.props = props;
        self
    }

    /// Finalize the builder to create the message.
    pub fn finalize(self) -> Message {
        Message::Out(self.0)
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A `Message` represents all the information passed in an MQTT PUBLISH
/// packet.
/// This is the primary data transfer mechanism.
#[derive(Debug)]
pub(crate) struct MessageFfi {
    pub(crate) cmsg: ffi::MQTTAsync_message,
    pub(crate) data: Pin<Box<MessageFfiData>>,
}

/// Cache of data values that the C msg struct point to.
#[derive(Debug, Default, Clone)]
pub(crate) struct MessageFfiData {
    pub(crate) topic: CString,
    pub(crate) payload: Vec<u8>,
    pub(crate) props: PropertiesFfi,
}

impl MessageFfiData {
    /// Creates new message data from the topic and payload.
    pub(crate) fn new<S, V>(topic: S, payload: V) -> Self
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        Self {
            topic: CString::new(topic.into()).unwrap(),
            payload: payload.into(),
            props: PropertiesFfi::default(),
        }
    }
}

impl MessageFfi {
    /// Gets the C struct to pass to the underlying library
    pub fn c_struct(&self) -> &ffi::MQTTAsync_message {
        &self.cmsg
    }

    /// Creates a message from the underlying parts.
    ///
    /// This fixes up the pointers in the C msg struct to point to the
    /// the individual components in the data, all of which get pinned.
    fn from_data(mut cmsg: ffi::MQTTAsync_message, data: MessageFfiData) -> Self {
        let data = Box::pin(data);
        cmsg.payload = data.payload.as_ptr() as *mut c_void;
        cmsg.payloadlen = data.payload.len() as i32;
        cmsg.properties = data.props.cprops;
        Self { cmsg, data }
    }

    /// Gets the topic for the message.
    pub fn topic(&self) -> &str {
        self.data
            .topic
            .to_str()
            .expect("paho.mqtt.c already validated utf8")
    }
}

impl TryFrom<Message> for MessageFfi {
    type Error = Error;

    fn try_from(msg: Message) -> Result<Self> {
        let data = MessageFfiData {
            topic: CString::new(msg.topic())?,
            payload: msg.payload().to_vec(),
            props: msg.properties().into(),
        };
        let mut cmsg = ffi::MQTTAsync_message::default();
        cmsg.qos = msg.qos() as c_int;
        cmsg.retained = msg.retain() as c_int;
        if let Some(id) = msg.msgid() {
            cmsg.msgid = id as c_int;
        }
        Ok(Self::from_data(cmsg, data))
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
