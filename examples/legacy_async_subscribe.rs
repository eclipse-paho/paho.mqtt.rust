// paho-mqtt/examples/async_subscribe.rs
//
// This is a Paho MQTT Rust client, sample application.
//
//! This application is an MQTT subscriber using the asynchronous client
//! interface of the Paho Rust client library, employing callbacks to
//! receive messages and status updates. It also monitors for disconnects
//! and performs manual re-connections.
//!
//! The sample demonstrates:
//!   - Connecting to an MQTT server/broker.
//!   - Subscribing to a topic
//!   - Receiving messages through the callback API
//!   - Receiving network disconnect updates and attempting manual reconnects.
//!   - Using a "clean session" and manually re-subscribing to topics on
//!     reconnect.
//!   - Last will and testament
//!   - Automatic reconnects
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

use std::{env, process, thread, time::Duration};

// The topics to which we subscribe.
const TOPICS: &[&str] = &["test", "hello"];
const QOS: &[i32] = &[1, 1];

// Min & max reconnect backoff times (2x each time till max).
const MIN_RECONNECT: Duration = Duration::from_secs(1);
const MAX_RECONNECT: Duration = Duration::from_secs(30);

/////////////////////////////////////////////////////////////////////////////

// Callback for a successful initial connect to the broker.
// This is the callback given to the connect() function, so is only called
// once after the initial connect succeeds (not after reconnects).
// We subscribe to the topic(s) we want here.
fn on_connect_success(cli: &mqtt::AsyncClient, _msgid: u16) {
    println!(
        "Initial connection succeeded. Subscribing to topics: {:?}",
        TOPICS
    );
    // Subscribe to the desired topic(s).
    cli.subscribe_many(TOPICS, QOS);
    // TODO: This doesn't yet handle a failed subscription.
}

// Callback for a failed initialattempt to connect to the server.
// We report the failure and exit.
fn on_connect_failure(_cli: &mqtt::AsyncClient, _msgid: u16, rc: i32) {
    println!("Connection attempt failed with error code {}.", rc);
    process::exit(1);
}

/////////////////////////////////////////////////////////////////////////////

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    // Create the client. Use an ID for a persistent session.
    // A real system should try harder to use a unique ID.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id("rust_async_subscribe")
        .finalize();

    // Create the client connection
    let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    // Set a closure to be called whenever the client connection is established.
    // This is called after the initial connection and also after successful
    // reconnections.
    cli.set_connected_callback(|_| {
        println!("Connected.");
    });

    // Set a closure to be called whenever the client loses the connection.
    // It just reports the state to the user, and lets the library try to
    // reconnect.
    cli.set_connection_lost_callback(|_| {
        println!("Connection lost. Attempting reconnect...");
    });

    // Attach a closure to the client to receive callback
    // on incoming messages.
    cli.set_message_callback(|_cli, msg| {
        if let Some(msg) = msg {
            let topic = msg.topic();
            let payload_str = msg.payload_str();
            println!("{} - {}", topic, payload_str);
        }
    });

    // Define the set of options for the connection
    let lwt = mqtt::Message::new("test/lwt", "[LWT] Async subscriber lost connection", 1);

    let conn_opts = mqtt::ConnectOptionsBuilder::new_v3()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(false)
        .automatic_reconnect(MIN_RECONNECT, MAX_RECONNECT)
        .will_message(lwt)
        .finalize();

    // Make the connection to the broker
    println!("Connecting to the MQTT server...");
    cli.connect_with_callbacks(conn_opts, on_connect_success, on_connect_failure);

    // Just wait for incoming messages.
    loop {
        thread::sleep(Duration::from_millis(1000));
    }

    // Hitting ^C will exit the app and cause the broker to publish the
    // LWT message since we're not disconnecting cleanly.
}
