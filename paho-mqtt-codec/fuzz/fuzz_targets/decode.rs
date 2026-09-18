// paho-mqtt-codec/fuzz/fuzz_targets/decode.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Fuzz target for the untrusted-input surface of the codec: framing
//! (`RawPacket::decode`) followed by parsing (`Packet::from_raw`).
//!
//! The only property checked is that no input panics. Returning an error is
//! fine — that is the decoder doing its job. Anything that decodes
//! successfully is additionally re-encoded and decoded again, which must
//! reproduce the same packet; that catches encode/decode asymmetries as well.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use paho_mqtt_codec::{DecodeOptions, MqttVersion, Packet, RawPacket};

fuzz_target!(|data: &[u8]| {
    for ver in [MqttVersion::V3_1, MqttVersion::V3_1_1, MqttVersion::V5] {
        let mut buf = BytesMut::from(data);

        // A read buffer may hold several packets back to back.
        while let Ok(Some(raw)) = RawPacket::decode(&mut buf) {
            // Strict decoding must be at least as strict as the default, and
            // must agree with it whenever both succeed.
            let strict = Packet::from_raw_with(raw.clone(), ver, DecodeOptions::STRICT);
            let Ok(pkt) = Packet::from_raw(raw, ver)
            else {
                assert!(strict.is_err(), "strict accepted what lenient rejected");
                break;
            };
            if let Ok(strict_pkt) = strict {
                assert_eq!(strict_pkt, pkt, "strict and lenient decode differ");
            }

            // Whatever decoded must survive a round trip unchanged.
            let mut wire = BytesMut::new();
            pkt.encode(&mut wire, ver)
                .expect("a decoded packet must always re-encode");
            let raw = RawPacket::decode(&mut wire)
                .expect("re-encoded bytes must frame")
                .expect("re-encoded bytes must be a complete packet");
            assert!(wire.is_empty(), "re-encode left trailing bytes");
            let again = Packet::from_raw(raw, ver).expect("re-encoded packet must decode");
            assert_eq!(pkt, again, "packet changed across a round trip");
        }
    }
});
