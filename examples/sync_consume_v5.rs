// paho-mqtt/examples/sync_consume_v5.rs
// This is a Paho MQTT v5 Rust client, sample application.
//
//! This application is an MQTT v5 consumer/subscriber using the Rust
//! synchronous client interface, which uses the queuing API to
//! receive messages.
//!
//! It also uses MQTT v5 subscription identifiers to create an indexed table
//! for processing messages based on the subscribed topic.
//!
//! The sample demonstrates:
//!   - Connecting to an MQTT server/broker
//!   - Checking server responses
//!   - Subscribing to multiple topics
//!   - MQTT v5 subscription identifiers
//!   - Receiving messages through the queueing consumer API
//!   - Receiving and acting upon commands via MQTT topics
//!   - Manual reconnects
//!   - Using a persistent (non-clean) session
//!   - Last will and testament
//!   - Using ^C handler for a clean exit
//!

/*******************************************************************************
 * Copyright (c) 2020-2023 Frank Pagliughi <fpagliughi@mindspring.com>
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
use std::{env, thread, time::Duration};

// --------------------------------------------------------------------------
// Handlers for different types of incoming messages based on their
// Subscription  Identifiers

// Handler for data messages (i.e. topic "data/#")
// Subscription ID: 1
fn data_handler(msg: mqtt::Message) -> bool {
    println!("{}", msg);
    true
}

// Handler for command messages (i.e. topic "command")
// Return false to exit the application
// Subscription ID: 2
fn command_handler(msg: mqtt::Message) -> bool {
    let cmd = msg.payload_str();
    if cmd == "exit" {
        println!("Exit command received");
        false
    } else {
        println!("Received command: '{}'", cmd);
        true
    }
}

// --------------------------------------------------------------------------

// This will attempt to reconnect to the broker. It can be called after
// connection is lost. In this example, we try to reconnect several times,
// with a few second pause between each attempt. A real system might keep
// trying indefinitely, with a backoff, or something like that.

fn try_reconnect(cli: &mqtt::Client) -> bool {
    println!("Connection lost. Attempting to reconnect...");
    for _ in 0..60 {
        thread::sleep(Duration::from_secs(1));
        if cli.reconnect().is_ok() {
            println!("  Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}

// Create a set of properties with a single Subscription ID
fn sub_id(id: u32) -> mqtt::Properties {
    mqtt::properties![mqtt::Property::SubscriptionIdentifier(id)]
}

// --------------------------------------------------------------------------

fn main() -> mqtt::Result<()> {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    println!("Connecting to the MQTT broker at '{}'...", host);

    // Create the client. Use an ID for a persistent session.
    // A real system should try harder to use a unique ID.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id("rust_sync_consumer_v5")
        .finalize();

    let cli = mqtt::Client::new(create_opts)?;

    // Initialize the consumer before connecting
    let rx = cli.start_consuming();

    // Define the set of options for the connection
    let lwt = mqtt::MessageBuilder::new()
        .topic("lwt")
        .payload("Sync consumer v5 lost connection")
        .finalize();

    // Connect with MQTT v5 and a persistent server session (no clean start).
    // For a persistent v5 session, we must set the Session Expiry Interval
    // on the server. Here we set that requests will persist for an hour
    // (3600sec) if the service disconnects or restarts.
    let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
        .clean_start(false)
        .properties(mqtt::properties![mqtt::Property::SessionExpiryInterval(
            3600
        )])
        .will_message(lwt)
        .finalize();

    // A table of dispatch function for incoming messages by Subscription ID.
    // (actually sub_id-1 since we can't use zero for a subscription ID)
    let handler: Vec<fn(mqtt::Message) -> bool> = vec![data_handler, command_handler];

    // Make the connection to the broker
    let rsp = cli.connect(conn_opts)?;

    // We're connecting with a persistent session. So we check if
    // the server already knows about us and rembers our subscription(s).
    // If not, we subscribe for incoming requests.

    println!(
        "Connected to: '{}' with MQTT version {}",
        rsp.server_uri, rsp.mqtt_version
    );

    if rsp.session_present {
        println!("  w/ client session already present on broker.");
    } else {
        // Register subscriptions on the server, using Subscription ID's.
        println!(r#"Subscribing to topics ["data/#", "command"]..."#);
        cli.subscribe_with_options("data/#", mqtt::QOS_0, None, sub_id(1))?;
        cli.subscribe_with_options("command", mqtt::QOS_1, None, sub_id(2))?;
    }

    // ^C handler will stop the consumer, breaking us out of the loop, below
    let ctrlc_cli = cli.clone();
    ctrlc::set_handler(move || {
        ctrlc_cli.stop_consuming();
    })
    .expect("Error setting Ctrl-C handler");

    // Just loop on incoming messages.
    // If we get a None message, check if we got disconnected,
    // and then try a reconnect.
    println!("\nWaiting for messages...");
    for msg in rx.iter() {
        if let Some(msg) = msg {
            // In a real app you'd want to do a lot more error checking and
            // recovery, but this should give an idea about the basics.

            let sub_id = match msg
                .properties()
                .get(mqtt::PropertyId::SubscriptionIdentifier)
            {
                Some(mqtt::Property::SubscriptionIdentifier(n)) => *n as usize,
                _ => panic!("No Subscription ID"),
            };

            if !handler[sub_id - 1](msg) {
                break;
            }
        } else if cli.is_connected() || !try_reconnect(&cli) {
            break;
        }
    }

    // If we're still connected, then disconnect now,
    // otherwise we're already disconnected.
    if cli.is_connected() {
        println!("\nDisconnecting");
        cli.disconnect(None).unwrap();
    }
    println!("Exiting");

    Ok(())
}
