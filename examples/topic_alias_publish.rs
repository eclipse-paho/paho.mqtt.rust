// paho-mqtt/examples/topic_alias_publish.rs
//
// This is a Paho MQTT v5 Rust sample application.
//
//! This is an asynchronous publisher example that uses a topic object to
//! repeatedly publish messages on the same topic using an alias.
//!
//! This sample demonstrates:
//!   - Connecting to an MQTT broker
//!   - Publishing a message asynchronously
//!   - Using a 'Topic' object to publish multiple messages to the same topic.
//!   - Using MQTT v5 topic aliases.
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
use std::{env, process, time::Duration};

// This is our topic alias we'll use to publish.
// It must be a non-zero number.
const TOPIC_ALIAS: u16 = 1;

// This is the actual (long) topic name.
const TOPIC_NAME: &str = "test/very/long/topic_name/just/to_say_hello";

/////////////////////////////////////////////////////////////////////////////

fn main() -> mqtt::Result<()> {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    // Create a client to the specified host, no message persistence
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .finalize();

    let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|err| {
        println!("Error creating the client: {}", err);
        process::exit(1);
    });

    let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
        .connect_timeout(Duration::from_secs(5))
        .finalize();

    // Connect and wait for it to complete or fail

    let rsp = cli.connect(conn_opts).wait().unwrap_or_else(|err| {
        eprintln!("Unable to connect: {:?}", err);
        process::exit(1);
    });

    match rsp.properties.get(mqtt::PropertyId::TopicAliasMaximum) {
        None | Some(mqtt::PropertyValue::U16(0)) => {
            eprintln!("The server doesn't support Topic Aliases.");
            process::exit(2);
        }
        Some(mqtt::PropertyValue::U16(n)) => {
            println!("The server supports up to {} aliases.", n)
        }
        _ => {}
    }

    // Create a topic and publish to it
    println!("Publishing messages on the 'test' topic");
    let mut topic = mqtt::Topic::new(&cli, TOPIC_NAME, mqtt::QOS_1);

    // First, publish with the alias to set it on the server
    topic
        .publish_with_alias(TOPIC_ALIAS, "Hello. Here's an alias.")
        .wait()?;

    // These publishes now use the alias
    for _ in 0..4 {
        topic.publish("Hello there").wait()?;
    }

    // To remove an alias you could also publish with alias zero to get
    // rid of it, like this:
    //
    //  topic.publish_with_alias(0, "Hello. Removed the alias.")
    //
    topic.remove_alias();
    topic.publish("Hello. Removed the alias.").wait()?;

    // Subsequent publishes use the topic name string.
    topic.publish("No alias here").wait()?;

    // Disconnect from the broker
    cli.disconnect(None).wait()?;

    Ok(())
}
