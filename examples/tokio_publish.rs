// paho-mqtt/examples/tokio_publish.rs
//
// Example application for Paho MQTT Rust library.
//
//! This is a simple MQTT asynchronous message publisher using the
//! Paho Rust library.
//!
//! This sample demonstrates:
//!   - Using the tokio runtime
//!   - Connecting to an MQTT broker
//!   - Publishing a message asynchronously
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

use paho_mqtt as mqtt;
use std::{env, process};

/////////////////////////////////////////////////////////////////////////////

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    // Command-line option(s)
    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    println!("Connecting to the MQTT server at '{}'", host);

    // We just use an async block to capture and print errors.
    let res: mqtt::Result<()> = async {
        // Create the client
        let cli = mqtt::AsyncClient::new(host)?;

        // Connect with default options and wait for it to complete or fail
        // The default is an MQTT v3.x connection.
        cli.connect(None).await?;

        // Create and publish a message
        println!("Publishing a message on the topic 'test'");
        let msg = mqtt::Message::new("test", "Hello Rust MQTT world!", mqtt::QOS_1);
        cli.publish(msg).await?;

        // Disconnect from the broker
        println!("Disconnecting");
        cli.disconnect(None).await?;

        Ok(())
    }
    .await;

    if let Err(err) = res {
        eprintln!("{}", err);
        process::exit(1);
    }
}
