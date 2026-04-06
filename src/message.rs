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
    ffi::{c_char, c_void},
    fmt, slice,
};

use crate::{QoS, ffi, properties::Properties};

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
            let topic = std::str::from_utf8_unchecked(slice::from_raw_parts(
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
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    const TOPIC: &str = "test";
    const PAYLOAD: &[u8] = b"Hello world";
    const QOS: QoS = QoS::ExactlyOnce;

    // Determine that a message can be sent across threads.
    #[test]
    fn test_message_send() {
        let msg = Message::new(TOPIC, PAYLOAD, QOS);
        let thr = thread::spawn(move || {
            assert_eq!(TOPIC, msg.topic());
            assert_eq!(PAYLOAD, msg.payload());
            assert_eq!(QOS, msg.qos());
        });
        thr.join().unwrap();
    }
}
