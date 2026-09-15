// paho-mqtt/examples/legacy_async_publish.rs
//
// This is a Paho MQTT Rust client, sample application.
//
//! This is a simple MQTT asynchronous message publisher using the
//! Paho Rust library.
//!
//! This uses the older legacy .wait() calls on individual futures. Newer
//! applications would likely want to consider the async/await style.
//! See: async_publish.rs
//!
//! This sample demonstrates:
//!   - Connecting to an MQTT broker
//!   - Publishing a message asynchronously
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

use paho_mqtt as mqtt;
use std::{env, process};

/////////////////////////////////////////////////////////////////////////////

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    // Command-line option(s)
    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    // Create a client to the specified host
    // Since we don't use a client ID there's no persistence

    let cli = mqtt::AsyncClient::new(host).unwrap_or_else(|err| {
        println!("Error creating the client: {}", err);
        process::exit(1);
    });

    // Connect and wait for it to complete or fail
    if let Err(err) = cli.connect(None).wait() {
        println!("Unable to connect: {}", err);
        process::exit(1);
    }

    // Create a message and publish it
    println!("Publishing a message on the 'test' topic");
    let msg = mqtt::Message::new("test", "Hello Rust MQTT world!", mqtt::QOS_0);
    let tok = cli.publish(msg);

    if let Err(e) = tok.wait() {
        println!("Error sending message: {:?}", e);
    }

    // Disconnect from the broker
    let tok = cli.disconnect(None);
    tok.wait().unwrap();
}
