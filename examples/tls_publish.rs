// paho-mqtt/examples/tls_publish.rs
//
// Example application for Paho MQTT Rust library.
//
//! This is a simple asynchronous MQTT publisher using a TLS secured
//! connection via the Paho MQTT Rust Library.
//!
//! The sample demonstrates:
//!   - Connecting to an MQTT server/broker securely
//!   - Setting TLS options
//!   - Publishing messages asynchronously
//!   - Using using async/await
//!
//! We can test this using mosquitto configured with certificates in the
//! Paho C library. The C library has a TLS test suite, and we can use
//! that to test:
//!     $ cd paho.mqtt.c
//!     $ mosquitto -c test/tls-testing/mosquitto.conf
//!
//! Then use the files "test-root-ca.crt" and "client.pem" from the directory
//! (paho.mqtt.c/test/ssl) for the trust and key stores for this program.
//!
//! Note that this configuration also works with secure websocket connection.
//! Use the connection string:
//!     wss://localhost:18885
//!

/*******************************************************************************
 * Copyright (c) 2017-2024 Frank Pagliughi <fpagliughi@mindspring.com>
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
use paho_mqtt as mqtt;
use std::{env, process};

/////////////////////////////////////////////////////////////////////////////

fn main() -> mqtt::Result<()> {
    // Initialize the logger from the environment
    env_logger::init();

    // We use the trust store from the Paho C tls-testing/keys directory,
    // but we assume there's a copy in the current directory.
    const TRUST_STORE: &str = "test-root-ca.crt";
    const KEY_STORE: &str = "client.pem";

    // We assume that we are in a valid directory.
    let mut trust_store = env::current_dir()?;
    trust_store.push(TRUST_STORE);

    let mut key_store = env::current_dir()?;
    key_store.push(KEY_STORE);

    if !trust_store.exists() {
        println!("The trust store file does not exist: {:?}", trust_store);
        println!("  Get a copy from \"paho.mqtt.c/test/ssl/{}\"", TRUST_STORE);
        process::exit(1);
    }

    if !key_store.exists() {
        println!("The key store file does not exist: {:?}", key_store);
        println!("  Get a copy from \"paho.mqtt.c/test/ssl/{}\"", KEY_STORE);
        process::exit(1);
    }

    // Let the user override the host, but note the "mqtts://" protocol.
    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtts://localhost:18884".to_string());

    println!("Connecting to host: '{}'", host);

    // Run the client in an async block

    if let Err(err) = block_on(async {
        // Create a client & define connect options
        let cli = mqtt::CreateOptionsBuilder::new()
            .server_uri(&host)
            .client_id("tls_publish_rs")
            .max_buffered_messages(100)
            .create_async_client()?;

        let tls_opts = mqtt::TlsOptionsBuilder::new()
            .trust_store(trust_store)
            .key_store(key_store)
            .error_callback(|_, s| {
                eprintln!("TLS error: {}", s);
            })
            .finalize();

        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .tls_options(tls_opts)
            .user_name("testuser")
            .password("testpassword")
            .finalize();

        cli.connect(conn_opts).await?;

        let msg = mqtt::MessageBuilder::new()
            .topic("test")
            .payload("Hello Rust MQTT secure world!")
            .qos(1)
            .finalize();

        cli.publish(msg).await?;
        cli.disconnect(None).await?;

        Ok::<(), mqtt::Error>(())
    }) {
        eprintln!("{}", err);
    }
    Ok(())
}
