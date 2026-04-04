// ffi_utils/subscribe_options_ffi.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// FFI representation of the subscribe options for the Paho C library.
//

/*******************************************************************************
 * Copyright (c) 2019-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! FFI representation of the MQTT v5 subscribe options for the Paho C library.

use crate::{ffi, subscribe_options::SubscribeOptions};

/// The MQTT v5 subscribe options in FFI form.
#[derive(Debug, Default, Copy, Clone)]
pub(crate) struct SubscribeOptionsFfi {
    pub copts: ffi::MQTTSubscribe_options,
}

impl From<SubscribeOptions> for SubscribeOptionsFfi {
    fn from(opts: SubscribeOptions) -> Self {
        Self {
            copts: ffi::MQTTSubscribe_options {
                noLocal: opts.no_local as u8,
                retainAsPublished: opts.retain_as_published as u8,
                retainHandling: opts.retain_handling as u8,
                ..ffi::MQTTSubscribe_options::default()
            },
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscribe_options::{RetainHandling, SubscribeOptions};

    #[test]
    fn test_default() {
        let opts = SubscribeOptions::default();

        assert!(!opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());

        let ffi = SubscribeOptionsFfi::from(opts);
        assert!(ffi.copts.noLocal == 0);
        assert!(ffi.copts.retainAsPublished == 0);
        assert!(ffi.copts.retainHandling == 0);
    }

    #[test]
    fn test_new() {
        let opts = SubscribeOptions::new(false, false, None);

        assert!(!opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());

        let ffi = SubscribeOptionsFfi::from(opts);
        assert!(ffi.copts.noLocal == 0);
        assert!(ffi.copts.retainAsPublished == 0);
        assert!(ffi.copts.retainHandling == 0);

        let opts = SubscribeOptions::new(true, false, None);

        assert!(opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());

        let ffi = SubscribeOptionsFfi::from(opts);
        assert!(ffi.copts.noLocal != 0);
        assert!(ffi.copts.retainAsPublished == 0);
        assert!(ffi.copts.retainHandling == 0);

        let opts = SubscribeOptions::new(true, true, None);

        assert!(opts.no_local);
        assert!(opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());

        let ffi = SubscribeOptionsFfi::from(opts);
        assert!(ffi.copts.noLocal != 0);
        assert!(ffi.copts.retainAsPublished != 0);
        assert!(ffi.copts.retainHandling == 0);

        let opts = SubscribeOptions::new(true, true, RetainHandling::SendRetainedOnNew);

        assert!(opts.no_local);
        assert!(opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::SendRetainedOnNew);

        let ffi = SubscribeOptionsFfi::from(opts);
        assert!(ffi.copts.noLocal != 0);
        assert!(ffi.copts.retainAsPublished != 0);
        assert!(ffi.copts.retainHandling == RetainHandling::SendRetainedOnNew as u8);
    }
}
