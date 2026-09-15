// paho-mqtt/examples/sync_publish.rs
//
// This is a Paho MQTT Rust client sample application.
//
//! This application is a simple MQTT publisher using the
//! synchronous/blocking client interface.
//!
//! The sample demonstrates:
//!   - Use of the synchronous/blocking API
//!   - Connecting to an MQTT broker
//!   - Publishing messages
//!

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
use std::{env, process, time::Duration};

/////////////////////////////////////////////////////////////////////////////

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    // Create a client & define connect options
    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    let mut cli = mqtt::Client::new(host).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    // Use 5sec timeouts for sync calls.
    cli.set_timeout(Duration::from_secs(5));

    // Connect and wait for it to complete or fail.
    // The default connection uses MQTT v3.x
    if let Err(e) = cli.connect(None) {
        println!("Unable to connect: {:?}", e);
        process::exit(1);
    }

    // Create a message and publish it
    let msg = mqtt::MessageBuilder::new()
        .topic("test")
        .payload("Hello synchronous world!")
        .qos(mqtt::QOS_1)
        .finalize();

    if let Err(e) = cli.publish(msg) {
        println!("Error sending message: {:?}", e);
    }

    // Disconnect from the broker
    cli.disconnect(None).unwrap();
}
