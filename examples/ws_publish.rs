// paho-mqtt/examples/ws_publish.rs
//
//! This is a simple MQTT message publisher using a websocket connection with
//! the Paho Rust library.
//!
//! This sample demonstrates:
//!   - Connecting to an MQTT broker using websockets.
//!   - Publishing a message asynchronously
//!
//! Usage:
//! ```text
//! $ ws_publish [server_uri [proxy]]
//! ```
//

/*******************************************************************************
 * Copyright (c) 2017-20124 Frank Pagliughi <fpagliughi@mindspring.com>
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
    let mut args = env::args().skip(1);

    // Get the server URI, if any, or default
    let host = args
        .next()
        .unwrap_or_else(|| "ws://localhost:8080".to_string());

    // Get the proxy, if any, or default
    let proxy = args.next().unwrap_or_default();

    // Create the client
    let cli = mqtt::AsyncClient::new(host).unwrap_or_else(|err| {
        println!("Error creating the client: {}", err);
        process::exit(1);
    });

    // Build the connect options for a WebSocket connection.
    let mut conn_builder = mqtt::ConnectOptionsBuilder::new_ws();

    if !proxy.is_empty() {
        conn_builder.http_proxy(proxy);
    }

    let conn_opts = conn_builder.finalize();

    // Connect and wait for it to complete or fail
    if let Err(err) = cli.connect(conn_opts).wait() {
        println!("Unable to connect: {}", err);
        process::exit(1);
    }

    // Create a message and publish it
    println!("Publishing a message on the 'test' topic");
    let msg = mqtt::Message::new("test", "Hello Rust Websocket MQTT world!", 0);
    let tok = cli.publish(msg);

    if let Err(e) = tok.wait() {
        println!("Error sending message: {:?}", e);
    }

    // Disconnect from the broker
    let tok = cli.disconnect(None);
    tok.wait().unwrap();
}
