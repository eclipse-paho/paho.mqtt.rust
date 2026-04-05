// paho-mqtt/examples/rpc_math_srvr.rs
//
// This is a Paho MQTT v5 Rust sample application.
//
//!
//! It's an example of how to create an RPC server client for receiving and
//! executing remote procedure calls using MQTT with the 'response topic'
//! and 'correlation data' properties.
//!
//! The sample demonstrates:
//!  - Creating an RPC service for MQTT v5
//!  - Connecting to an MQTT v5 server/broker
//!  - Using MQTT v5 properties
//!  - Receiving RPC request messages, and sending replies.
//!  - Using asynchronous tokens
//!  - Subscribing to request topic
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

//#[macro_use]
//extern crate lazy_static;

use lazy_static::lazy_static;
use paho_mqtt as mqtt;
use serde_json::json;
use std::{
    collections::HashMap,
    process, thread,
    time::{Duration, Instant},
};

// The default QoS
const QOS: i32 = 1;

/////////////////////////////////////////////////////////////////////////////

// The RPC function implementations

fn add(args: &[f64]) -> f64 {
    args.iter().sum()
}
fn mult(args: &[f64]) -> f64 {
    args.iter().product()
}

// The math function signature.
type MathFn = fn(args: &[f64]) -> f64;

// A table of names to functions.
// The names are the supported public operations that come in on the
// request topics:
//     "requests/math/<operation>"
lazy_static! {
    static ref FUNC_TBL: HashMap<&'static str, MathFn> = {
        let mut tbl = HashMap::new();
        tbl.insert("add", add as MathFn);
        tbl.insert("mult", mult as MathFn);
        tbl
    };
}

// --------------------------------------------------------------------------
// This will attempt to reconnect to the broker. It can be called after
// connection is lost. In this example, we try to reconnect several times,
// with a few second pause between each attempt. A real system might keep
// trying indefinitely, with a backoff, or something like that.

fn try_reconnect(cli: &mqtt::AsyncClient, opts: mqtt::ConnectOptions) -> bool {
    println!("Connection lost. Waiting to retry connection");

    const PAUSE: Duration = Duration::from_millis(2500);
    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(60) - PAUSE {
        thread::sleep(PAUSE);
        if cli.connect(opts.clone()).wait().is_ok() {
            println!("Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}

// --------------------------------------------------------------------------
// Handle a single incoming request as encoded in an MQTT v5 message.
//
// The topic indicates the requested operation, in the form:
//     "requests/math/<operation>"
// where <operation> ("add", "mult", etc) tells us which processing function
// to use.
//
// The payload contains the parameters for the function as a JSON array
// of numbers, "[ 7, 12, 18]"
//
// The properties of the message should have the "reply to" address and
// Correlation ID for the response.
//

fn handle_request(cli: &mqtt::AsyncClient, msg: mqtt::Message) -> mqtt::Result<()> {
    // We need both a response topic and correlation data to respond.

    let reply_to = msg
        .properties()
        .get_string(mqtt::PropertyCode::ResponseTopic)
        .ok_or_else(|| mqtt::Error::from("No response topic provided."))?;

    let corr_id = msg
        .properties()
        .get_binary(mqtt::PropertyCode::CorrelationData)
        .ok_or_else(|| mqtt::Error::from("No correlation data provided."))?;

    println!(
        "\nRequest w/ Reply To: {}, Correlation ID: {:?}",
        reply_to, corr_id
    );

    // Get the name of the function from the topic

    let topic_arr: Vec<&str> = msg.topic().split('/').collect();

    if topic_arr.len() < 3 {
        return Err("Malformed request topic".into());
    }

    let fname = topic_arr[2];

    // Get the parameters from the payload

    let params: Vec<f64> = serde_json::from_str(&msg.payload_str()).unwrap();

    // Look up the function, by name, and call if found

    if let Some(func) = FUNC_TBL.get(fname) {
        println!("{}: {:?}", fname, params);
        let x = func(&params);
        println!("    Result: {}", x);

        // Form a reply message, using the correlation ID

        let props = mqtt::properties![CorrelationData(corr_id.into())];

        // Create the reply message and publish it on the response topic

        let payload = json!(x).to_string();

        let msg = mqtt::MessageBuilder::new()
            .topic(reply_to)
            .payload(payload)
            .qos(QOS)
            .properties(props)
            .finalize();

        let _ = cli.publish(msg).wait();
    }
    else {
        eprintln!("Unknown command: {}", fname);
    }
    Ok(())
}

// --------------------------------------------------------------------------

fn main() -> mqtt::Result<()> {
    // Initialize the logger from the environment
    env_logger::init();

    // We use the broker on this host.
    let host = "localhost";

    const REQ_TOPIC_HDR: &str = "requests/math/#";

    // Create a client to the specified host, no persistence
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id("rpc_math_srvr")
        .persistence(None)
        .finalize();

    let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|err| {
        eprintln!("Error creating the client: {}", err);
        process::exit(1);
    });

    // Initialize the consumer before connecting.
    // With a clean session/start, this order isn't important,
    // but it's still a good habit to start consuming first.
    let rx = cli.start_consuming();

    // Connect with default options for MQTT v5, and a short persistent
    // session of 60 sec. But we set a clean start
    let mut conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
        .clean_start(true)
        .properties(mqtt::properties![SessionExpiryInterval(60)])
        .finalize();

    // Connect and wait for it to complete or fail

    let rsp = cli.connect(conn_opts.clone()).wait().unwrap_or_else(|err| {
        eprintln!("Unable to connect: {:?}", err);
        process::exit(1);
    });

    // We're connecting with a persistent session. So we check if
    // the server already knows about us and rembers about our
    // subscription(s). If not, we subscribe for incoming requests.

    if let Some(mqtt::ConnectResponse {
        session_present, ..
    }) = rsp.connect_response()
    {
        if !session_present {
            println!("Subscribing to math requests");
            cli.subscribe(REQ_TOPIC_HDR, QOS).wait()?;
        }
    }

    // For any reconnects we maintain the session by turning off
    // the clean start option.
    conn_opts.set_clean_start(false);

    println!("Processing requests...");
    for msg in rx.iter() {
        if let Some(msg) = msg {
            if let Err(err) = handle_request(&cli, msg) {
                eprintln!("Error: {}", err);
            }
        }
        else if cli.is_connected() || !try_reconnect(&cli, conn_opts.clone()) {
            break;
        }
    }

    if cli.is_connected() {
        // Disconnect from the broker
        cli.disconnect(None).wait()?;
    }
    Ok(())
}
