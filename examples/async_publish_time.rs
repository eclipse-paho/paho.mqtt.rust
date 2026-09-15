// paho-mqtt/examples/async_publish_time.rs
//
// Example application for Paho MQTT Rust library.
//
//! This is a simple MQTT asynchronous message publisher using the Paho
//! Rust library. It shows a typical use case (although with contrived
//! "data"), where a client might poll a sensor and then publish the
//! reading when the data changes. In this case we use the system clock
//! in place of a sensor, looking at an integer time_t value in units
//! of 1/100 sec. Thus, it should publish the current time 100x per
//! second.
//!
//! This sample demonstrates:
//!   - Connecting to an MQTT broker
//!   - Repeatedly publishing message asynchronously
//

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
use std::{
    env, process, thread,
    time::{Duration, SystemTime},
};

/////////////////////////////////////////////////////////////////////////////

// Read the system time in units of 1/100 sec since epoch.
fn time_now_hundredths() -> u64 {
    (SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        / 10) as u64
}

// --------------------------------------------------------------------------

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    // Create a client with file persistence under a directory named,
    // "persist". Any string or Path can be used to specify the directory.
    // If the library can't find or create the path, client creation will
    // fail with a persistence error.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id("rust_async_pub_time")
        .persistence("persist")
        .finalize();

    let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|err| {
        println!("Error creating the client: {}", err);
        process::exit(1);
    });

    // Connect and wait for it to complete or fail
    if let Err(err) = cli.connect(None).wait() {
        println!("Unable to connect: {}", err);
        process::exit(1);
    }

    // Note that with MQTT v5, this would be a good place to use a topic
    // object with an alias. It might help reduce the size of the messages
    // if the topic string is long.

    let topic = "data/time";

    // Create messages and publish them
    println!("Publishing time on the topic '{}'", topic);

    loop {
        let t0 = time_now_hundredths();
        let mut t = t0;

        // Wait until the time reading changes
        while t == t0 {
            thread::sleep(Duration::from_millis(10));
            t = time_now_hundredths();
        }

        let tf = 0.01 * (t as f64);

        let msg = mqtt::Message::new(topic, format!("{:.3}", tf), mqtt::QOS_1);

        // We don't need to use `try_publish()` here since we just wait on
        // the token, but this shows how we could use it.
        match cli.try_publish(msg) {
            Err(err) => eprintln!("Error creating/queuing the message: {}", err),
            Ok(tok) => {
                if let Err(err) = tok.wait() {
                    eprintln!("Error sending message: {}", err);
                }
            }
        }
    }
}
