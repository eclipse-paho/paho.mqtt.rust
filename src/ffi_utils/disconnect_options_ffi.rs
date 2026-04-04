// ffi_utils/disconnect_options_ffi.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// FFI representation of the disconnect options for the Paho C library.
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

//! FFI representation of the disconnect options for the Paho C library.

use std::os::raw::c_int;

use crate::{disconnect_options::DisconnectOptions, ffi, properties::PropertiesFfi};

/// The FFI representation of disconnect options.
///
/// This keeps the [`PropertiesFfi`] alive alongside the C struct so that
/// `copts.properties.array` remains valid for the duration of the FFI call.
pub(crate) struct DisconnectOptionsFfi {
    /// The underlying C options struct.
    pub(crate) copts: ffi::MQTTAsync_disconnectOptions,
    /// The properties data cache. Must outlive `copts`.
    #[allow(dead_code)]
    props: PropertiesFfi,
}

impl From<&DisconnectOptions> for DisconnectOptionsFfi {
    fn from(opts: &DisconnectOptions) -> Self {
        let props = PropertiesFfi::from(&opts.props);
        let copts = ffi::MQTTAsync_disconnectOptions {
            timeout: opts.timeout.as_millis() as c_int,
            properties: props.cprops,
            reasonCode: opts.reason_code as ffi::MQTTReasonCodes,
            ..ffi::MQTTAsync_disconnectOptions::default()
        };
        Self { copts, props }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disconnect_options::DisconnectOptions;
    use std::thread;

    #[test]
    fn test_new() {
        let ffi = DisconnectOptionsFfi::from(&DisconnectOptions::new());
        let default_copts = ffi::MQTTAsync_disconnectOptions::default();

        assert_eq!(default_copts.struct_id, ffi.copts.struct_id);
        assert_eq!(default_copts.struct_version, ffi.copts.struct_version);
    }

    #[test]
    fn test_send() {
        let opts = DisconnectOptions::new();

        let thr = thread::spawn(move || {
            let ffi = DisconnectOptionsFfi::from(&opts);
            let default_copts = ffi::MQTTAsync_disconnectOptions::default();
            assert_eq!(default_copts.struct_id, ffi.copts.struct_id);
            assert_eq!(default_copts.struct_version, ffi.copts.struct_version);
        });
        thr.join().unwrap();
    }
}
