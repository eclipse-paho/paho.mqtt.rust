// paho-mqtt/examples/event_subscribe_v5.rs
//
// This is a Paho MQTT v5 Rust sample application.
//
//! This application is an MQTT subscriber using the asynchronous client
//! interface of the Paho Rust client library.
//!
//! It also monitors for disconnects and performs manual re-connections.
//!
//! The sample demonstrates:
//!   - An async/await subscriber
//!   - Connecting to an MQTT server/broker.
//!   - Subscribing to multiple topics
//!   - Using MQTT v5 subscribe options
//!   - Receiving events from an async stream.
//!   - Handling disconnects and attempting manual reconnects.
//!   - Using a "persistent" (non-clean) session so the broker keeps
//!     subscriptions and messages through reconnects.
//!   - Last will and testament
//!
//! Note that this example specifically does *not* handle a ^C, so breaking
//! out of the app will always result in an un-clean disconnect causing the
//! broker to emit the LWT message.

/*******************************************************************************
 * Copyright (c) 2017-2025 Frank Pagliughi <fpagliughi@mindspring.com>
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

use futures::executor::block_on;
use paho_mqtt::{self as mqtt, Event, MQTT_VERSION_5};
use std::{env, process, time::Duration};

// The topics to which we subscribe.
const TOPICS: &[&str] = &["test", "hello"];
const QOS: &[i32] = &[1, 1];

/////////////////////////////////////////////////////////////////////////////

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    println!("Connecting to the MQTT server at '{}'...", host);

    // Create the client. Use an ID for a persistent session.
    // A real system should try harder to use a unique ID.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id("rust_evt_sub_v5")
        .finalize();

    // Create the client connection
    let mut cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    if let Err(err) = block_on(async {
        // Create event stream before connecting.
        let strm = cli.get_event_stream(25);

        // Define the set of options for the connection
        let lwt = mqtt::Message::new(
            "test/lwt",
            "[LWT] Async subscriber v5 lost connection",
            mqtt::QOS_1,
        );

        // Connect with MQTT v5 and a persistent server session (no clean start).
        // For a persistent v5 session, we must set the Session Expiry Interval
        // on the server. Here we set that requests will persist for an hour
        // (3600sec) if the service disconnects or restarts.
        let conn_opts = mqtt::ConnectOptionsBuilder::with_mqtt_version(MQTT_VERSION_5)
            .clean_start(false)
            .properties(mqtt::properties![mqtt::PropertyCode::SessionExpiryInterval => 3600])
            .will_message(lwt)
            .finalize();

        // Make the connection to the broker
        cli.connect(conn_opts).await?;

        println!("Subscribing to topics: {:?}", TOPICS);
        let sub_opts = vec![mqtt::SubscribeOptions::with_retain_as_published(); TOPICS.len()];
        cli.subscribe_many_with_options(TOPICS, QOS, &sub_opts, None)
            .await?;

        // Just loop on incoming messages.
        println!("Waiting for messages...");

        // Note that we're not providing a way to cleanly shut down and
        // disconnect. Therefore, when you kill this app (with a ^C or
        // whatever) the server will get an unexpected drop and then
        // should emit the LWT message.

        while let Ok(evt) = strm.recv().await {
            match evt {
                Event::Connected => println!("Connected"),
                Event::Message(msg) => {
                    if msg.retained() {
                        print!("(R) ");
                    }
                    println!("{}", msg);
                }
                Event::ConnectionLost => {
                    println!("Lost connection. Attempting reconnect...");
                    while let Err(err) = cli.reconnect().await {
                        println!("  Error reconnecting: {}", err);
                        // For tokio use: tokio::time::delay_for()
                        smol::Timer::after(Duration::from_millis(1000)).await;
                    }
                }
                Event::Disconnected { reason_code, .. } => {
                    println!("Disconnected: {}\nExiting.", reason_code);
                    break;
                }
                Event::Shutdown => {
                    println!("Client shutdown.\nExiting.");
                    break;
                }
            }
        }

        // Explicit return type for the async block
        Ok::<(), mqtt::Error>(())
    }) {
        eprintln!("{}", err);
    }
}
