// will_options_ffi.rs
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

//! FFI representation of the Last Will and Testament (LWT) options for the
//! Paho C library.

use std::{os::raw::c_void, pin::Pin, ptr};

use crate::{
    ffi,
    ffi_utils::{MessageFfi, MessageFfiData},
    Message,
};

/// The FFI representation of the Last Will and Testament (LWT) options.
#[derive(Debug)]
pub(crate) struct WillOptionsFfi {
    /// The underlying C options struct
    pub(crate) copts: ffi::MQTTAsync_willOptions,
    /// The local data cache for the C options
    data: Pin<Box<MessageFfiData>>,
}

impl WillOptionsFfi {
    // Creates options from a C struct and pinned data cache.
    // Fixes up the C struct pointers to point into the pinned data.
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
}

impl Default for WillOptionsFfi {
    fn default() -> Self {
        let data = Box::pin(MessageFfiData::default());
        Self::from_pinned_data(ffi::MQTTAsync_willOptions::default(), data)
    }
}

impl Clone for WillOptionsFfi {
    /// Creates a clone of the WillOptionsFfi struct.
    /// This clones the cached values and updates the C struct to refer
    /// to them.
    fn clone(&self) -> Self {
        let data = Box::pin((*self.data).clone());
        Self::from_pinned_data(self.copts, data)
    }
}

unsafe impl Send for WillOptionsFfi {}
unsafe impl Sync for WillOptionsFfi {}

impl From<Message> for WillOptionsFfi {
    /// Create `WillOptionsFfi` from a `Message`
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
        let opts = WillOptionsFfi::default();
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

    // Test creating will options from a message
    #[test]
    fn test_from_message() {
        let msg = MessageBuilder::new()
            .topic(TOPIC)
            .payload(PAYLOAD)
            .qos(QOS)
            .retained(RETAINED)
            .finalize();

        let opts = WillOptionsFfi::from(msg);

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
    #[test]
    fn test_assign() {
        let msg = MessageBuilder::new()
            .topic(TOPIC)
            .payload(PAYLOAD)
            .qos(QOS)
            .retained(RETAINED)
            .finalize();

        let org_opts = WillOptionsFfi::from(msg);
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
    #[test]
    fn test_clone() {
        let opts = {
            let msg = MessageBuilder::new()
                .topic(TOPIC)
                .payload(PAYLOAD)
                .qos(QOS)
                .retained(RETAINED)
                .finalize();

            let org_opts = WillOptionsFfi::from(msg);
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
