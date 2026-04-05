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

// Callback for a successful connection to the broker.
// We subscribe to the topic(s) we want here.
fn on_connect_success(cli: &mqtt::AsyncClient, _msgid: u16) {
    println!("Connection succeeded");
    let data = cli.user_data().unwrap();

    if let Some(lock) = data.downcast_ref::<UserTopics>() {
        let topics = lock.read().unwrap();
        println!("Subscribing to topics: {:?}", topics);

        // Create a QoS vector, same len as # topics
        let qos = vec![QOS; topics.len()];
        // Subscribe to the desired topic(s).
        cli.subscribe_many(&topics, &qos);
        // TODO: This doesn't yet handle a failed subscription.
    }
}

// Callback for a failed attempt to connect to the server.
// We simply sleep and then try again.
//
// Note that normally we don't want to do a blocking operation or sleep
// from  within a callback. But in this case, we know that the client is
// *not* connected, and thus not doing anything important. So we don't worry
// too much about stopping its callback thread.
fn on_connect_failure(cli: &mqtt::AsyncClient, _msgid: u16, rc: i32) {
    println!("Connection attempt failed with error code {}.\n", rc);
    thread::sleep(Duration::from_millis(2500));
    cli.reconnect_with_callbacks(on_connect_success, on_connect_failure);
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

    // Set a closure to be called whenever the client connection is established.
    cli.set_connected_callback(|_cli: &mqtt::AsyncClient| {
        println!("Connected.");
    });

    // Set a closure to be called whenever the client loses the connection.
    // It will attempt to reconnect, and set up function callbacks to keep
    // retrying until the connection is re-established.
    cli.set_connection_lost_callback(|cli: &mqtt::AsyncClient| {
        println!("Connection lost. Attempting reconnect.");
        thread::sleep(Duration::from_millis(2500));
        cli.reconnect_with_callbacks(on_connect_success, on_connect_failure);
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
    cli.connect_with_callbacks(conn_opts, on_connect_success, on_connect_failure);

    // Just wait for incoming messages.
    loop {
        thread::sleep(Duration::from_millis(1000));
    }

    // Hitting ^C will exit the app and cause the broker to publish
    // the LWT message since we're not disconnecting cleanly.
}
