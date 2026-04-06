// ffi_utils/message_ffi.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// FFI representation of an MQTT message for the Paho C library.
//

/*******************************************************************************
 * Copyright (c) 2017-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! FFI representation of an MQTT message for the Paho C library.

use std::{
    ffi::{CString, c_int, c_void},
    pin::Pin,
};

use crate::{Error, Message, PropertiesFfi, Result, ffi};

/// A `Message` in FFI form, ready to pass to the Paho C library.
#[derive(Debug)]
pub(crate) struct MessageFfi {
    pub(crate) cmsg: ffi::MQTTAsync_message,
    pub(crate) data: Pin<Box<MessageFfiData>>,
}

/// Cache of data values that the C msg struct points to.
#[derive(Debug, Default, Clone)]
pub(crate) struct MessageFfiData {
    pub(crate) topic: CString,
    pub(crate) payload: Vec<u8>,
    pub(crate) props: PropertiesFfi,
}

impl MessageFfi {
    /// Gets the C struct to pass to the underlying library.
    pub fn c_struct(&self) -> &ffi::MQTTAsync_message {
        &self.cmsg
    }

    /// Creates a message from the underlying parts.
    ///
    /// Fixes up the pointers in the C msg struct to point to the
    /// individual components in the data, all of which get pinned.
    pub(crate) fn from_data(
        mut cmsg: ffi::MQTTAsync_message,
        data: MessageFfiData,
    ) -> Self {
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
        let mut cmsg = ffi::MQTTAsync_message {
            qos: msg.qos() as c_int,
            retained: msg.retain() as c_int,
            ..ffi::MQTTAsync_message::default()
        };
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
    use crate::{
        QoS,
        message::{Message, MessageBuilder},
    };
    use std::os::raw::{c_int, c_void};

    // These should differ from defaults
    const TOPIC: &str = "test";
    const PAYLOAD: &[u8] = b"Hello world";
    const QOS: QoS = QoS::ExactlyOnce;
    const RETAINED: bool = true;

    #[test]
    fn test_new() {
        let msg = Message::new(TOPIC, PAYLOAD, QOS);

        assert_eq!(TOPIC, msg.topic());
        assert_eq!(PAYLOAD, msg.payload());
        assert_eq!(QOS, msg.qos());
        assert!(!msg.retained());

        let ffi = MessageFfi::try_from(msg).unwrap();
        let default_cmsg = ffi::MQTTAsync_message::default();
        assert_eq!(default_cmsg.struct_id, ffi.cmsg.struct_id);
        assert_eq!(default_cmsg.struct_version, ffi.cmsg.struct_version);
        assert_eq!(ffi.data.payload.len() as i32, ffi.cmsg.payloadlen);
        assert_eq!(ffi.data.payload.as_ptr() as *mut c_void, ffi.cmsg.payload);
        assert_eq!(QOS as c_int, ffi.cmsg.qos);
        assert!(ffi.cmsg.retained == 0);
    }

    #[test]
    fn test_builder_default() {
        let msg = MessageBuilder::new().finalize();
        let default_cmsg = ffi::MQTTAsync_message::default();

        let ffi = MessageFfi::try_from(msg).unwrap();
        assert_eq!(default_cmsg.struct_id, ffi.cmsg.struct_id);
        assert_eq!(default_cmsg.struct_version, ffi.cmsg.struct_version);

        assert_eq!(0, ffi.data.topic.as_bytes().len());
        assert_eq!(&[] as &[u8], ffi.data.topic.as_bytes());
        assert_eq!(&[] as &[u8], ffi.data.payload.as_slice());
    }

    #[test]
    fn test_builder_topic() {
        let msg = MessageBuilder::new().topic(TOPIC).finalize();
        assert_eq!(TOPIC, msg.topic());

        let ffi = MessageFfi::try_from(msg).unwrap();
        assert_eq!(TOPIC, ffi.data.topic.to_str().unwrap());
    }

    #[test]
    fn test_builder_payload() {
        let msg = MessageBuilder::new().payload(PAYLOAD).finalize();
        assert_eq!(PAYLOAD, msg.payload());

        let ffi = MessageFfi::try_from(msg).unwrap();
        assert_eq!(PAYLOAD, ffi.data.payload.as_slice());
        assert_eq!(ffi.data.payload.len() as i32, ffi.cmsg.payloadlen);
        assert_eq!(ffi.data.payload.as_ptr() as *mut c_void, ffi.cmsg.payload);
    }

    #[test]
    fn test_builder_qos() {
        let msg = MessageBuilder::new().qos(QOS).finalize();
        assert_eq!(QOS, msg.qos());

        let ffi = MessageFfi::try_from(msg).unwrap();
        assert_eq!(QOS as c_int, ffi.cmsg.qos);

        let qos = 1;
        let msg = MessageBuilder::new().qos(qos).finalize();
        assert_eq!(QoS::from(qos), msg.qos());

        let ffi = MessageFfi::try_from(msg).unwrap();
        assert_eq!(qos, ffi.cmsg.qos);
    }

    #[test]
    fn test_builder_retained() {
        let msg = MessageBuilder::new().retained(false).finalize();
        assert!(!msg.retained());
        let ffi = MessageFfi::try_from(msg).unwrap();
        assert!(ffi.cmsg.retained == 0);

        let msg = MessageBuilder::new().retained(true).finalize();
        assert!(msg.retained());
        let ffi = MessageFfi::try_from(msg).unwrap();
        assert!(ffi.cmsg.retained != 0);
    }

    #[test]
    fn test_assign() {
        let org_msg = MessageBuilder::new()
            .topic(TOPIC)
            .payload(PAYLOAD)
            .qos(QOS)
            .retained(RETAINED)
            .finalize();

        let msg = org_msg;
        assert_eq!(TOPIC, msg.topic());
        assert_eq!(PAYLOAD, msg.payload());
        assert_eq!(QOS, msg.qos());
        assert!(msg.retained());

        let ffi = MessageFfi::try_from(msg).unwrap();
        assert_eq!(ffi.data.payload.len() as i32, ffi.cmsg.payloadlen);
        assert_eq!(ffi.data.payload.as_ptr() as *mut c_void, ffi.cmsg.payload);
        assert_eq!(QOS as c_int, ffi.cmsg.qos);
        assert!(ffi.cmsg.retained != 0);
    }

    #[test]
    fn test_clone() {
        let msg = {
            let org_msg = MessageBuilder::new()
                .topic(TOPIC)
                .payload(PAYLOAD)
                .qos(QOS)
                .retained(RETAINED)
                .finalize();
            org_msg.clone()
        };

        assert_eq!(TOPIC, msg.topic());
        assert_eq!(PAYLOAD, msg.payload());
        assert_eq!(QOS, msg.qos());
        assert!(msg.retained());

        let ffi = MessageFfi::try_from(msg).unwrap();
        assert_eq!(ffi.data.payload.len() as i32, ffi.cmsg.payloadlen);
        assert_eq!(ffi.data.payload.as_ptr() as *mut c_void, ffi.cmsg.payload);
        assert_eq!(QOS as c_int, ffi.cmsg.qos);
        assert!(ffi.cmsg.retained != 0);
    }
}
