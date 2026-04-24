// paho-mqtt/examples/dyn_subscribe.rs
//
// This is a Paho MQTT Rust client, sample application.
//
//! This application is an MQTT subscriber using the asynchronous client
//! interface of the Paho Rust client library. It maintains a dynamic
//! set of subscription topics in the client's user data, and employs
//! callbacks to receive messages and status updates. It also monitors
//! for disconnects and performs manual re-connections.
//!
//! The MQTT client lets an application keep a single "User Data" item as a
//! boxed `Any`. It is set when the client is created and can be accessed
//! from the various callbacks. It must adhere to Rust's usual concurrency
//! and safety rules, so if it is to be updated by the application and
//! callbacks, then it must be thread protected. In this example, we use a
//! `RwLock` to provide thread safety.
//!
//! This sample demonstrates:
//!   - Connecting to an MQTT server/broker.
//!   - Client user data
//!   - Subscribing to multiple topics simultaneously
//!   - Receiving messages through the callback API
//!   - Receiving network disconnect updates and attempting manual reconnects.
//!   - Using a "clean session" and manually re-subscribing to topics on
//!     reconnect.
//!   - Last will and testament
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

use std::{env, process, sync::RwLock, thread, time::Duration};

// The topics to which we subscribe.
const DFLT_TOPICS: &[&str] = &["requests/subscription/add", "test", "hello"];
const QOS: i32 = 1;

// The type we'll use to keep our dynamic list of topics inside the
// MQTT client. Since we want to update it after creating the client,
// we need to wrap the data in a lock, like a Mutex or RwLock.
type UserTopics = RwLock<Vec<String>>;

/////////////////////////////////////////////////////////////////////////////

fn subscribe_topics(cli: &mqtt::AsyncClient) {
    let data = cli.user_data().unwrap();
    if let Some(lock) = data.downcast_ref::<UserTopics>() {
        let topics = lock.read().unwrap();
        println!("Subscribing to topics: {:?}", topics);
        let qos = vec![QOS; topics.len()];
        cli.subscribe_many(&topics, &qos);
    }
}

/////////////////////////////////////////////////////////////////////////////

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    let topics: Vec<String> = DFLT_TOPICS.iter().map(|s| s.to_string()).collect();

    // Create the client. Use an ID for a persistent session.
    // A real system should try harder to use a unique ID.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id("rust_dyn_subscribe")
        .user_data(Box::new(RwLock::new(topics)))
        .finalize();

    // Create the client connection
    let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    // Subscribe to topics on every successful connection (initial + reconnects).
    cli.set_connected_callback(|cli: &mqtt::AsyncClient, _rsp| {
        println!("Connected.");
        subscribe_topics(cli);
    });

    // Set a closure to be called whenever the client loses the connection.
    cli.set_connection_lost_callback(|_| {
        println!("Connection lost. Attempting reconnect.");
    });

    // Attach a closure to the client to receive callback
    // on incoming messages.
    cli.set_message_callback(|cli, msg| {
        if let Some(msg) = msg {
            let topic = msg.topic();
            let payload_str = msg.payload_str();

            if topic == "requests/subscription/add" {
                let data = cli.user_data().unwrap();
                if let Some(lock) = data.downcast_ref::<UserTopics>() {
                    let mut topics = lock.write().unwrap();
                    let new_topic = payload_str.into_owned();
                    println!("Adding topic: {}", new_topic);
                    cli.subscribe(&new_topic, QOS);
                    topics.push(new_topic);
                }
                else {
                    println!("Failed to add topic: {}", payload_str);
                }
            }
            else {
                println!("{} - {}", topic, payload_str);
            }
        }
    });

    // Define the set of options for the connection
    let lwt = mqtt::Message::new("test/lwt", "[LWT] Dynamic subscriber lost connection", 1);

    // The connect options. Defaults to an MQTT v3.x connection.
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .will_message(lwt)
        .finalize();

    // Make the connection to the broker
    println!("Connecting to the MQTT server...");
    if let Err(e) = cli.connect(conn_opts).wait() {
        println!("Unable to connect: {}", e);
        process::exit(1);
    }

    // Just wait for incoming messages.
    loop {
        thread::sleep(Duration::from_millis(1000));
    }

    // Hitting ^C will exit the app and cause the broker to publish
    // the LWT message since we're not disconnecting cleanly.
}
