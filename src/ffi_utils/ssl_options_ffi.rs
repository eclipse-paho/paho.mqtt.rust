// ffi_utils/ssl_options_ffi.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// FFI representation of the SSL/TLS options for the Paho C library.
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

//! FFI representation of the SSL/TLS options for the Paho C library.

use std::{
    ffi::CString,
    os::raw::{c_char, c_int, c_uchar, c_uint},
    pin::Pin,
    ptr,
};

use crate::{ffi, ssl_options::path_to_cstring, to_c_bool, Error, Result, SslOptions};

/// The options for SSL socket connections to the broker.
// Implementation note:
// The C library seems to require the SSL Options struct to provide valid
// strings for all the entries. Empty entries require a zero-length string
// and should not be left as NULL. Therefore, our ffi::MQTTAsync_SSLOptions
// struct needs to be fixed up to always point to the cached CString values.
#[derive(Debug)]
pub(crate) struct SslOptionsFfi {
    /// The underlying C struct to pass to the library
    pub(crate) copts: ffi::MQTTAsync_SSLOptions,
    /// Cache of Rust values tied to the C struct
    #[allow(dead_code)]
    pub(crate) data: Pin<Box<SslOptionsFfiData>>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct SslOptionsFfiData {
    pub(crate) trust_store: CString,
    pub(crate) key_store: CString,
    pub(crate) private_key: CString,
    pub(crate) private_key_password: CString,
    pub(crate) enabled_cipher_suites: CString,
    pub(crate) ca_path: CString,
    pub(crate) protos: Vec<c_uchar>,
}

impl SslOptionsFfi {
    // The C library expects unset values in the SSL options struct to be
    // NULL rather than empty string.
    #[inline]
    fn c_str(s: &CString) -> *const c_char {
        match &s {
            s if !s.to_bytes().is_empty() => s.as_ptr(),
            _ => ptr::null(),
        }
    }

    // Updates the underlying C structure to match the cached data.
    fn from_data(mut copts: ffi::MQTTAsync_SSLOptions, data: SslOptionsFfiData) -> Self {
        let data = Box::pin(data);

        copts.trustStore = Self::c_str(&data.trust_store);
        copts.keyStore = Self::c_str(&data.key_store);
        copts.privateKey = Self::c_str(&data.private_key);
        copts.privateKeyPassword = Self::c_str(&data.private_key_password);
        copts.enabledCipherSuites = Self::c_str(&data.enabled_cipher_suites);
        copts.CApath = Self::c_str(&data.ca_path);

        copts.protos = match &data.protos {
            protos if !protos.is_empty() => protos.as_ptr(),
            _ => ptr::null(),
        };
        copts.protos_len = data.protos.len() as c_uint;

        Self { copts, data }
    }

    /// Gets the list of ALPN protocols available to be negotiated.
    /// This is in the wire format of the data.
    #[cfg(test)]
    pub fn alpn_proto_vec(&self) -> &[c_uchar] {
        &self.data.protos
    }
}

impl Default for SslOptionsFfi {
    fn default() -> Self {
        Self::from_data(
            ffi::MQTTAsync_SSLOptions::default(),
            SslOptionsFfiData::default(),
        )
    }
}

unsafe impl Send for SslOptionsFfi {}
unsafe impl Sync for SslOptionsFfi {}

impl TryFrom<&SslOptions> for SslOptionsFfi {
    type Error = Error;

    fn try_from(opts: &SslOptions) -> Result<Self> {
        let data = SslOptionsFfiData::try_from(opts)?;

        let copts = ffi::MQTTAsync_SSLOptions {
            sslVersion: opts.ssl_version as c_int,
            verify: to_c_bool(opts.verify),
            disableDefaultTrustStore: to_c_bool(opts.disable_default_trust_store),
            ..ffi::MQTTAsync_SSLOptions::default()
        };

        Ok(Self::from_data(copts, data))
    }
}

impl TryFrom<SslOptions> for SslOptionsFfi {
    type Error = Error;

    fn try_from(opts: SslOptions) -> Result<Self> {
        SslOptionsFfi::try_from(&opts)
    }
}

impl SslOptionsFfiData {
    // Converts the list of ALPN protocol strings into the wire format for
    // openssl.
    //
    // The wire format is a single array of non-empty byte string, with a
    // length byte prefix for each. It's described here:
    // https://www.openssl.org/docs/man1.1.0/man3/SSL_CTX_set_alpn_protos.html
    pub(crate) fn proto_vec<S: AsRef<str>>(protos: &[S]) -> Vec<c_uchar> {
        protos
            .iter()
            .flat_map(|s| {
                let mut p: Vec<c_uchar> =
                    s.as_ref().bytes().map(|c| c as c_uchar).collect();
                p.insert(0, p.len() as c_uchar);
                p
            })
            .collect()
    }
}

impl TryFrom<&SslOptions> for SslOptionsFfiData {
    type Error = Error;

    fn try_from(opts: &SslOptions) -> Result<Self> {
        Ok(SslOptionsFfiData {
            trust_store: path_to_cstring(&opts.trust_store)?,
            key_store: path_to_cstring(&opts.key_store)?,
            private_key: path_to_cstring(&opts.private_key)?,
            private_key_password: CString::new(opts.private_key_password.clone())?,
            enabled_cipher_suites: CString::new(opts.enabled_cipher_suites.clone())?,
            ca_path: path_to_cstring(&opts.ca_path)?,
            protos: SslOptionsFfiData::proto_vec(opts.protos.as_slice()),
        })
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{from_c_bool, ssl_options::SslOptionsBuilder};
    use std::{ffi::CStr, os::raw::c_char};

    #[test]
    fn test_new() {
        let opts = SslOptionsFfi::try_from(SslOptions::new()).unwrap();
        let default_copts = ffi::MQTTAsync_SSLOptions::default();

        assert_eq!(default_copts.struct_id, opts.copts.struct_id);
        assert_eq!(default_copts.struct_version, opts.copts.struct_version);
        assert!(opts.copts.trustStore.is_null());
    }

    #[test]
    fn test_builder_dflt() {
        let opts = SslOptionsFfi::try_from(SslOptionsBuilder::new().finalize()).unwrap();
        let default_copts = ffi::MQTTAsync_SSLOptions::default();

        assert_eq!(default_copts.struct_id, opts.copts.struct_id);
        assert_eq!(default_copts.struct_version, opts.copts.struct_version);
        assert_eq!(ptr::null() as *const c_char, opts.copts.trustStore);
    }

    #[test]
    fn test_builder_trust_store() {
        const TRUST_STORE: &str = "some_file.crt";
        let opts = SslOptionsFfi::try_from(
            SslOptionsBuilder::new().trust_store(TRUST_STORE).finalize(),
        )
        .unwrap();

        assert_eq!(TRUST_STORE, opts.data.trust_store.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.trustStore) };
        assert_eq!(TRUST_STORE, s.to_str().unwrap());
    }

    #[test]
    fn test_builder_key_store() {
        const KEY_STORE: &str = "some_file.crt";
        let opts = SslOptionsFfi::try_from(
            SslOptionsBuilder::new().key_store(KEY_STORE).finalize(),
        )
        .unwrap();

        assert_eq!(KEY_STORE, opts.data.key_store.to_str().unwrap());

        let s = unsafe { CStr::from_ptr(opts.copts.keyStore) };
        assert_eq!(KEY_STORE, s.to_str().unwrap());
    }

    #[test]
    fn test_verify() {
        let opts =
            SslOptionsFfi::try_from(SslOptionsBuilder::new().verify(true).finalize())
                .unwrap();
        assert!(from_c_bool(opts.copts.verify));
    }

    #[test]
    fn test_disable_default_trust_store() {
        let opts = SslOptionsFfi::try_from(
            SslOptionsBuilder::new()
                .disable_default_trust_store(true)
                .finalize(),
        )
        .unwrap();
        assert!(from_c_bool(opts.copts.disableDefaultTrustStore));
    }

    #[test]
    fn test_protos() {
        let protos = &["spdy/1", "http/1.1"];

        let v: Vec<c_uchar> = vec![
            6, b's', b'p', b'd', b'y', b'/', b'1', 8, b'h', b't', b't', b'p', b'/', b'1',
            b'.', b'1',
        ];

        assert_eq!(v, SslOptionsFfiData::proto_vec(protos));

        let opts = SslOptionsFfi::try_from(
            SslOptionsBuilder::new().alpn_protos(protos).finalize(),
        )
        .unwrap();

        assert_eq!(v, opts.alpn_proto_vec());
    }
}
