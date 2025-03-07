// paho-mqtt/examples/mqttrs_chat.rs
//
// This is a Paho MQTT v5 C++ sample application.
//
//!
//! It's an example of how to create a client for performing remote procedure
//! calls using MQTT with the 'response topic' and 'correlation data'
//! properties.
//!
//! The sample demonstrates:
//!  - Connecting to an MQTT v5 server/broker
//!  - Using the "Topic" type to publish and subscribe
//!  - Using asynchronous tokens
//!  - Handling message and disconnect callbacks with closures
//!  - v5 properties, like `WillDelayInterval` and `SessionExpiraryInterval`
//

/*******************************************************************************
 * Copyright (c) 2019-2023 Frank Pagliughi <fpagliughi@mindspring.com>
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

use std::{env, io, process, time::Duration};

/////////////////////////////////////////////////////////////////////////////

fn main() -> mqtt::Result<()> {
    // Initialize the logger from the environment
    env_logger::init();

    // We use the broker on this host.
    let host = "localhost";

    // Command-line option(s)
    let args: Vec<String> = env::args().skip(1).collect();

    if args.len() != 2 {
        println!("USAGE: mqttrs_chat <user> <group>");
        process::exit(1);
    }

    let chat_user = args[0].to_string();
    let chat_group = args[1].to_string();
    let chat_topic = format!("chat/{}", chat_group);

    let client_id = format!("mqttrs_chat-{}-{}", chat_user, chat_group);

    const QOS: i32 = 1;
    const NO_LOCAL: bool = true;

    // The LWT is broadcast to the group if our connection is lost
    // But wait 30sec for reconnect before broadcasting it.

    let lwt_props = mqtt::properties![WillDelayInterval(10)];

    let lwt = mqtt::MessageBuilder::new()
        .topic(&chat_topic)
        .payload(format!("<<< {} left the group >>>", chat_user))
        .qos(QOS)
        .properties(lwt_props)
        .finalize();

    // Create a client to the specified host, no persistence
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id(client_id)
        .persistence(None)
        .finalize();

    let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|err| {
        eprintln!("Error creating the client: {}", err);
        process::exit(1);
    });

    // Session will exist for a day (86,400 sec) between connections.
    let props = mqtt::properties![SessionExpiryInterval(86400)];

    // Connect with a persistent sesstion

    // Connect with MQTT v5 and a persistent server session (no clean start).
    // For a persistent v5 session, we must set the Session Expiry Interval
    // on the server. Here we set that requests will persist for a day
    // (86,400sec) if the service disconnects or restarts.
    let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_start(false)
        .properties(props)
        .will_message(lwt)
        .finalize();

    // Set a closure to be called when the client loses the connection.
    // It will simply end the session.
    cli.set_connection_lost_callback(|_cli| {
        println!("*** Connection lost ***");
        process::exit(2);
    });

    // Attach a closure to the client to receive callbacks on incoming
    // messages. Just print them to the console.
    cli.set_message_callback(|_cli, msg| {
        if let Some(msg) = msg {
            println!("{}", msg.payload_str());
        }
    });

    // Since we publish and subscribe to a single topic,
    // a topic instance is helpful.

    let topic = mqtt::Topic::new(&cli, chat_topic, QOS);

    // Connect and wait for it to complete or fail

    if let Err(err) = cli.connect(conn_opts).wait() {
        eprintln!("Unable to connect: {}", err);
        process::exit(1);
    }

    // Subscribe to the group messages.

    println!("Joining the group '{}'...", chat_group);
    topic.subscribe_with_options(NO_LOCAL, None).wait()?;

    // Let everyone know that a new user joined the group

    topic
        .publish(format!("<<< {} joined the group >>>", chat_user))
        .wait()?;

    // Read messages from the console and publish them.
    // Quit when the use enters an empty line, or a read error occurs.

    loop {
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let msg = input.trim();
                if msg.is_empty() {
                    break;
                }

                // Publish payload as "<user>: <message>"
                let chat_msg = format!("{}: {}", chat_user, msg);
                if let Err(err) = topic.publish(chat_msg).wait() {
                    eprintln!("Error: {}", err);
                    break;
                }
            }
            Err(err) => println!("Error: {}", err),
        }
    }

    // If we're still connected, let everyone know that we left the group,
    // and then disconnect cleanly.

    if cli.is_connected() {
        println!("Leaving the group...");
        // Disconnect and tell the server to publish the LWT (after the expiry)
        let opts = mqtt::DisconnectOptionsBuilder::new()
            .publish_will_message()
            .finalize();
        cli.disconnect(opts).wait()?;
    }

    Ok(())
}
