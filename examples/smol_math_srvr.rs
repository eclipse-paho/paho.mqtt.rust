// paho-mqtt/examples/smol_math_srvr.rs
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
//!  - Using the smol runtime
//!  - Connecting to an MQTT v5 server/broker
//!  - Using MQTT v5 properties
//!  - Receiving RPC request messages, and sending replies.
//!  - Using asynchronous tokens
//!  - Subscribing to request topic
//

/*******************************************************************************
 * Copyright (c) 2019-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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
use serde_json::json;
use std::{
    collections::HashMap,
    process,
    sync::OnceLock,
    time::{Duration, Instant},
};

// The default QoS
const QOS: mqtt::QoS = mqtt::QOS_1;

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
static FUNC_TBL: OnceLock<HashMap<&'static str, MathFn>> = OnceLock::new();

fn func_tbl() -> &'static HashMap<&'static str, MathFn> {
    FUNC_TBL.get_or_init(|| {
        let mut tbl = HashMap::new();
        tbl.insert("add", add as MathFn);
        tbl.insert("mult", mult as MathFn);
        tbl
    })
}

// --------------------------------------------------------------------------
// This will attempt to reconnect to the broker. It can be called after
// connection is lost. In this example, we try to reconnect several times,
// with a few second pause between each attempt. A real system might keep
// trying indefinitely, with a backoff, or something like that.

async fn try_reconnect(cli: &mqtt::AsyncClient, opts: mqtt::ConnectOptions) -> bool {
    println!("Connection lost. Waiting to retry connection");

    const PAUSE: Duration = Duration::from_millis(2500);
    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(60) - PAUSE {
        smol::Timer::after(PAUSE).await;
        if cli.connect(opts.clone()).await.is_ok() {
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

async fn handle_request(cli: &mqtt::AsyncClient, msg: mqtt::Message) -> mqtt::Result<()> {
    // We need both a response topic and correlation data to respond.

    let reply_to = match msg.properties().get(mqtt::PropertyId::ResponseTopic) {
        Some(mqtt::PropertyValue::String(s)) => s.clone(),
        _ => return Err("No response topic provided.".into()),
    };

    let corr_id = match msg.properties().get(mqtt::PropertyId::CorrelationData) {
        Some(mqtt::PropertyValue::Binary(b)) => b.clone(),
        _ => return Err("No correlation data provided.".into()),
    };

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

    if let Some(func) = func_tbl().get(fname) {
        println!("{}: {:?}", fname, params);
        let x = func(&params);
        println!("    Result: {}", x);

        // Form a reply message, using the correlation ID

        let props = mqtt::properties![mqtt::Property::new(
            mqtt::PropertyId::CorrelationData,
            mqtt::PropertyValue::Binary(corr_id)
        )];

        // Create the reply message and publish it on the response topic

        let payload = json!(x).to_string();

        let msg = mqtt::MessageBuilder::new()
            .topic(reply_to)
            .payload(payload)
            .qos(QOS)
            .properties(props)
            .finalize();

        let _ = cli.publish(msg).await;
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

    smol::block_on(async {
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

        // Create event stream before connecting.
        let strm = cli.get_event_stream(25);

        // Connect with default options for MQTT v5, and a short persistent
        // session of 60 sec. But we set a clean start.
        let mut conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .clean_start(true)
            .properties(mqtt::properties![mqtt::Property::new(
                mqtt::PropertyId::SessionExpiryInterval,
                mqtt::PropertyValue::U32(60)
            )])
            .finalize();

        // Connect and wait for it to complete or fail

        let rsp = cli.connect(conn_opts.clone()).await.unwrap_or_else(|err| {
            eprintln!("Unable to connect: {:?}", err);
            process::exit(1);
        });

        // We're connecting with a persistent session. So we check if
        // the server already knows about us and remembers our
        // subscription(s). If not, we subscribe for incoming requests.

        if !rsp.session_present {
            println!("Subscribing to math requests");
            cli.subscribe(REQ_TOPIC_HDR, QOS).await?;
        }

        // For any reconnects we maintain the session by turning off
        // the clean start option.
        conn_opts.set_clean_start(false);

        println!("Processing requests...");

        // Drive the event stream, handling each incoming message.
        while let Ok(evt) = strm.recv().await {
            match evt {
                mqtt::Event::Message(msg) => {
                    if let Err(err) = handle_request(&cli, msg).await {
                        eprintln!("Error: {}", err);
                    }
                }
                mqtt::Event::ConnectionLost => {
                    if !try_reconnect(&cli, conn_opts.clone()).await {
                        break;
                    }
                }
                mqtt::Event::Disconnected { .. } => break,
                _ => {}
            }
        }

        if cli.is_connected() {
            cli.disconnect(None).await?;
        }
        Ok(())
    })
}
