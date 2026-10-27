// paho-mqtt/examples/async_persist_publish.rs
//
// Example application for Paho MQTT Rust library.
//
//! This is an asynchronous MQTT publisher with user-defined, in-memory,
//! persistence using the Paho Rust client library.
//!
//! This example demonstrates:
//!   - Creating a client using CreateOptionsBuilder
//!   - User-defined persistence, in-memory, with a Rust HashMap
//!   - Connecting to an MQTT broker
//!   - Publishing messages asynchronously
//!   - Using the Rust logger to trace the persistence callbacks.
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

use log::trace;
use paho_mqtt as mqtt;
use std::{collections::HashMap, env, process};

// Use a non-zero QOS to exercise the persistence store
const QOS: mqtt::QoS = mqtt::QOS_1;

/////////////////////////////////////////////////////////////////////////////

/// The ClientPersistence maps pretty closely to a key/val store. We can use
/// a Rust HashMap to implement an in-memory persistence pretty easily.
/// The keys are strings, and the values are arbitrarily-sized byte buffers.
///
/// Note that this is an extremely silly example, because if you want to use
/// persistence, you probably want it to be out of process so that if the
/// client crashes and restarts, the persistence data still exists.
///
/// This is just here to show how the persistence API callbacks work.
///
#[derive(Default)]
struct MemPersistence {
    /// Name derived from the Client ID and Server URI
    /// This could be used to keep a separate persistence store for each
    /// client/server combination.
    name: String,
    /// We'll use a HashMap for a local in-memory store.
    map: HashMap<String, Vec<u8>>,
}

impl MemPersistence {
    /// Create a new/empty persistence store.
    fn new() -> Self {
        Self::default()
    }
}

impl mqtt::ClientPersistence for MemPersistence {
    // Open the persistence store.
    // We don't need to do anything here since the store is in memory.
    // We just capture the name for logging/debugging purposes.
    fn open(&mut self, client_id: &str, server_uri: &str) -> mqtt::Result<()> {
        self.name = format!("{}-{}", client_id, server_uri);
        trace!("Client persistence [{}]: open", self.name);
        Ok(())
    }

    // Close the persistence store.
    // We don't need to do anything.
    fn close(&mut self) -> mqtt::Result<()> {
        trace!("Client persistence [{}]: close", self.name);
        Ok(())
    }

    // Put data into the persistence store.
    // We get a vector of buffer references for the data to store, which we
    // can concatenate into a single byte buffer to place in the map.
    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> mqtt::Result<()> {
        trace!("Client persistence [{}]: put key '{}'", self.name, key);
        let buf: Vec<u8> = buffers.concat();
        self.map.insert(key.to_string(), buf);
        Ok(())
    }

    // Get (retrieve) data from the persistence store.
    // We look up and return any data corresponding to the specified key.
    fn get(&mut self, key: &str) -> mqtt::Result<Vec<u8>> {
        trace!("Client persistence [{}]: get key '{}'", self.name, key);
        match self.map.get(key) {
            Some(v) => Ok(v.to_vec()),
            None => Err(mqtt::Error::Persistence(format!("key not found: {key}"))),
        }
    }

    // Remove the key entry from the persistence store, if any.
    fn remove(&mut self, key: &str) -> mqtt::Result<()> {
        trace!("Client persistence [{}]: remove key '{}'", self.name, key);
        match self.map.remove(key) {
            Some(_) => Ok(()),
            None => Err(mqtt::Error::Persistence(format!("key not found: {key}"))),
        }
    }

    // Retrieve the complete set of keys in the persistence store.
    fn keys(&mut self) -> mqtt::Result<Vec<String>> {
        trace!("Client persistence [{}]: keys", self.name);
        let mut keys: Vec<String> = Vec::new();
        for key in self.map.keys() {
            keys.push(key.to_string());
        }
        if !keys.is_empty() {
            trace!("Found keys: {:?}", keys);
        }
        Ok(keys)
    }

    // Clears all the data from the persistence store.
    fn clear(&mut self) -> mqtt::Result<()> {
        trace!("Client persistence [{}]: clear", self.name);
        self.map.clear();
        Ok(())
    }

    // Determine if the persistence store contains the specified key.
    fn contains_key(&mut self, key: &str) -> bool {
        trace!("Client persistence [{}]: contains key '{}'", self.name, key);
        self.map.contains_key(key)
    }
}

// --------------------------------------------------------------------------

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://localhost:1883".to_string());

    // Create a client & define connect options.
    // Note that for pure publishers, you don't always need a Client ID. But
    // when using persistence the client library requires it so as to name
    // the local store to keep clients separate.
    println!("Creating the MQTT client.");

    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .user_persistence(MemPersistence::new())
        .client_id("rust_async_persist_pub")
        .finalize();

    let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    let conn_opts = mqtt::ConnectOptions::new();

    // Connect and wait for it to complete or fail
    println!("Connecting to MQTT broker.");
    if let Err(e) = cli.connect(conn_opts).wait() {
        println!("Unable to connect: {:?}", e);
        process::exit(1);
    }

    // Create a message and publish it
    println!("Publishing a message to 'test' topic");
    let msg = mqtt::Message::new("test", "Hello world!", QOS);
    let tok = cli.publish(msg);

    if let Err(e) = tok.wait() {
        println!("Error sending message: {:?}", e);
    }

    // Disconnect from the broker
    println!("Disconnecting from the broker.");

    let tok = cli.disconnect(None);
    tok.wait().unwrap();

    println!("Done");
}
