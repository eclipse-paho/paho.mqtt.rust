// paho-mqtt/examples/sync_consume.rs
//
// This is a Paho MQTT Rust client, sample application.
//
//! This application is an MQTT consumer/subscriber using the Rust
//! synchronous client interface, which uses the queuing API to
//! receive messages.
//!
//! The sample demonstrates:
//!   - Connecting to an MQTT server/broker
//!   - Checking server responses
//!   - Subscribing to multiple topics
//!   - Receiving messages through the queueing consumer API
//!   - Receiving and acting upon commands via MQTT topics
//!   - Manual reconnects
//!   - Using a persistent (non-clean) session
//!   - Last will and testament
//!   - Using ^C handler for a clean exit
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

/////////////////////////////////////////////////////////////////////////////

// This will attempt to reconnect to the broker. It can be called after
// connection is lost. In this example, we try to reconnect several times,
// with a few second pause between each attempt. A real system might keep
// trying indefinitely, with a backoff, or something like that.

fn try_reconnect(cli: &mqtt::Client) -> bool {
    println!("Connection lost. Reconnecting...");
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

// --------------------------------------------------------------------------

fn main() {
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
        .client_id("rust_sync_consumer")
        .finalize();

    let cli = mqtt::Client::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    // Initialize the consumer before connecting
    let rx = cli.start_consuming();

    // Define the set of options for the connection
    let lwt = mqtt::MessageBuilder::new()
        .topic("test")
        .payload("Sync consumer lost connection")
        .finalize();

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(false)
        .will_message(lwt)
        .finalize();

    let subscriptions = ["test", "hello"];
    let qos = mqtt::QOS_1;

    // Make the connection to the broker
    match cli.connect(conn_opts) {
        Ok(rsp) => {
            println!(
                "Connected to: '{}' with MQTT version {}",
                rsp.server_uri, rsp.mqtt_version
            );
            if rsp.session_present {
                // Since our persistent session is already on the broker
                // we don't need to subscribe to the topics.
                println!("  w/ client session already present on broker.");
            } else {
                // The server doesn't have a persistent session already
                // stored for us (1st connection?), so we need to subscribe
                // to the topics we want to receive.
                println!(
                    "Subscribing to topics {:?} with requested QoS: {:?}...",
                    subscriptions, qos
                );

                cli.subscribe_many_same_qos(&subscriptions, qos)
                    .map(|rsp| {
                        println!("QoS granted: {:?}", rsp.reason_codes);
                    })
                    .unwrap_or_else(|err| {
                        println!("Error subscribing to topics: {:?}", err);
                        cli.disconnect(None).unwrap();
                        process::exit(1);
                    });
            }
        }
        Err(e) => {
            println!("Error connecting to the broker: {:?}", e);
            process::exit(1);
        }
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
    println!("\nWaiting for messages on topics {:?}...", subscriptions);
    for msg in rx.iter() {
        if let Some(msg) = msg {
            println!("{}", msg);
        } else if cli.is_connected() || !try_reconnect(&cli) {
            break;
        }
    }

    // If we're still connected, then disconnect now,
    // otherwise we're already disconnected.
    if cli.is_connected() {
        println!("\nDisconnecting...");
        cli.disconnect(None).unwrap();
    }
    println!("Exiting");
}
