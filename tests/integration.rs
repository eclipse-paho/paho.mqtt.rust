// tests/integration.rs
//
// Integration tests for the Eclipse Paho MQTT Rust client library.
//
// ## Broker requirements
//
// A Mosquitto broker must be running and reachable at the addresses below.
// The simplest configuration (mosquitto.conf) that satisfies all tests:
//
//   listener 1883
//   allow_anonymous true
//   socket_domain ipv4
//
//   # Unix-domain socket (unix tests)
//   listener 0
//   socket_domain unix
//   bind_address /tmp/mosquitto.sock
//
//   # WebSocket listener (ws_* tests; requires `websocket` feature)
//   listener 9001
//   protocol websockets
//   allow_anonymous true
//
// TLS tests (`tls_*`) are marked `#[ignore]` because they require additional
// broker configuration (listener on 8883, server cert/key) and a local CA
// certificate file.  Run them explicitly with:
//
//   cargo test --features tls tls_ -- --ignored
//
// or equivalently with the `openssl` feature.

use paho_mqtt as mqtt;
use std::time::Duration;

const HOST: &str = "tcp://localhost:1883";
#[cfg(unix)]
const UNIX_HOST: &str = "unix:///tmp/mosquitto.sock";
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
const WS_HOST: &str = "ws://localhost:9001";
#[cfg(any(feature = "tls", feature = "openssl"))]
const TLS_HOST: &str = "tls://localhost:8883";

const TIMEOUT: Duration = Duration::from_secs(5);

// Returns a unique topic string to avoid cross-test interference.
macro_rules! topic {
    ($name:expr) => {
        concat!("paho-rust-v2/test/", $name)
    };
}

/////////////////////////////////////////////////////////////////////////////
// Connect / disconnect
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_connect_disconnect_v3() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    let rsp = cli
        .connect(mqtt::ConnectOptions::new_v3())
        .wait()
        .expect("connect failed");
    assert_eq!(rsp.mqtt_version, 4); // v3.1.1 on wire
    assert!(!rsp.session_present);
    cli.disconnect(None).wait().expect("disconnect failed");
}

#[test]
fn test_connect_disconnect_v5() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    let opts = mqtt::ConnectOptions::new_v5();
    let rsp = cli.connect(opts).wait().expect("connect v5 failed");
    assert_eq!(rsp.mqtt_version, 5);
    assert!(!rsp.session_present);
    cli.disconnect(None).wait().expect("disconnect failed");
}

#[test]
fn test_is_connected() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    assert!(!cli.is_connected());
    cli.connect(None).wait().expect("connect failed");
    assert!(cli.is_connected());
    cli.disconnect(None).wait().expect("disconnect failed");
    assert!(!cli.is_connected());
}

/////////////////////////////////////////////////////////////////////////////
// Publish
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_publish_qos0() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(None).wait().expect("connect failed");

    let msg = mqtt::Message::new(topic!("pub_qos0"), "hello qos0", mqtt::QOS_0);
    cli.publish(msg).wait().expect("publish qos0 failed");

    cli.disconnect(None).wait().expect("disconnect failed");
}

#[test]
fn test_publish_qos1() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(None).wait().expect("connect failed");

    let msg = mqtt::Message::new(topic!("pub_qos1"), "hello qos1", mqtt::QOS_1);
    cli.publish(msg).wait().expect("publish qos1 failed");

    cli.disconnect(None).wait().expect("disconnect failed");
}

#[test]
fn test_publish_qos2() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(None).wait().expect("connect failed");

    let msg = mqtt::Message::new(topic!("pub_qos2"), "hello qos2", mqtt::QOS_2);
    cli.publish(msg).wait().expect("publish qos2 failed");

    cli.disconnect(None).wait().expect("disconnect failed");
}

/////////////////////////////////////////////////////////////////////////////
// Subscribe and receive
/////////////////////////////////////////////////////////////////////////////

fn subscribe_and_receive(qos: mqtt::QoS) {
    let topic = match qos {
        mqtt::QoS::QoS0 => topic!("sub_qos0"),
        mqtt::QoS::QoS1 => topic!("sub_qos1"),
        mqtt::QoS::QoS2 => topic!("sub_qos2"),
    };
    let payload = format!("payload-{:?}", qos);

    // Subscriber client
    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(None).wait().expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, qos).wait().expect("subscribe failed");

    // Publisher client
    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("pub connect failed");
    let msg = mqtt::Message::new(topic, payload.clone(), qos);
    pub_cli.publish(msg).wait().expect("publish failed");

    // Receive
    let received = rx.recv_timeout(TIMEOUT).expect("recv timed out");
    let received = received.expect("got None (connection lost) instead of message");

    assert_eq!(received.topic(), topic);
    assert_eq!(received.payload(), payload.as_bytes());
    assert_eq!(received.qos(), qos);

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

#[test]
fn test_subscribe_receive_qos0() {
    subscribe_and_receive(mqtt::QoS::QoS0);
}

#[test]
fn test_subscribe_receive_qos1() {
    subscribe_and_receive(mqtt::QoS::QoS1);
}

#[test]
fn test_subscribe_receive_qos2() {
    subscribe_and_receive(mqtt::QoS::QoS2);
}

// Payloads above the runtime's inline threshold are written from their own
// buffer rather than copied into the header buffer; make sure that path
// produces an intact frame at every QoS.
fn subscribe_and_receive_large(qos: mqtt::QoS) {
    let topic = match qos {
        mqtt::QoS::QoS0 => topic!("sub_large_qos0"),
        mqtt::QoS::QoS1 => topic!("sub_large_qos1"),
        mqtt::QoS::QoS2 => topic!("sub_large_qos2"),
    };
    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(None).wait().expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, qos).wait().expect("subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("pub connect failed");
    let msg = mqtt::Message::new(topic, payload.clone(), qos);
    pub_cli.publish(msg).wait().expect("publish failed");

    let received = rx.recv_timeout(TIMEOUT).expect("recv timed out");
    let received = received.expect("got None (connection lost) instead of message");

    assert_eq!(received.topic(), topic);
    assert_eq!(received.payload().len(), payload.len());
    assert_eq!(received.payload(), &payload[..]);
    assert_eq!(received.qos(), qos);

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

#[test]
fn test_subscribe_receive_large_qos0() {
    subscribe_and_receive_large(mqtt::QoS::QoS0);
}

#[test]
fn test_subscribe_receive_large_qos1() {
    subscribe_and_receive_large(mqtt::QoS::QoS1);
}

#[test]
fn test_subscribe_receive_large_qos2() {
    subscribe_and_receive_large(mqtt::QoS::QoS2);
}

/////////////////////////////////////////////////////////////////////////////
// Subscribe many
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_subscribe_many() {
    const TOPICS: &[&str] = &[topic!("many/a"), topic!("many/b"), topic!("many/c")];

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(None).wait().expect("connect failed");
    let rx = sub.start_consuming();

    let qos = vec![mqtt::QOS_1; TOPICS.len()];
    sub.subscribe_many(TOPICS, &qos)
        .wait()
        .expect("subscribe_many failed");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("pub connect failed");

    for topic in TOPICS {
        let msg = mqtt::Message::new(*topic, *topic, mqtt::QOS_1);
        pub_cli.publish(msg).wait().expect("publish failed");
    }

    let mut received_topics: Vec<String> = Vec::new();
    for _ in 0..TOPICS.len() {
        let msg = rx
            .recv_timeout(TIMEOUT)
            .expect("recv timed out")
            .expect("connection lost");
        received_topics.push(msg.topic().to_string());
    }
    received_topics.sort();

    let mut expected: Vec<String> = TOPICS.iter().map(|t| t.to_string()).collect();
    expected.sort();
    assert_eq!(received_topics, expected);

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Unsubscribe
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unsubscribe() {
    let topic = topic!("unsub");

    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(None).wait().expect("connect failed");
    let rx = cli.start_consuming();

    cli.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");
    cli.unsubscribe(topic).wait().expect("unsubscribe failed");

    // Publish after unsubscribe — should not arrive
    let msg = mqtt::Message::new(topic, "should not arrive", mqtt::QOS_1);
    cli.publish(msg).wait().expect("publish failed");

    // A short wait confirms silence on the channel
    match rx.recv_timeout(Duration::from_millis(300)) {
        Err(_) => {} // timeout — correct: nothing received
        Ok(Some(m)) => panic!("received message after unsubscribe: {:?}", m.topic()),
        Ok(None) => {} // connection-lost sentinel is OK here
    }

    cli.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Wildcard subscription
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_wildcard_subscribe() {
    let sub_filter = topic!("wild/#");
    let pub_topics = &[topic!("wild/a"), topic!("wild/b/c")];

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(None).wait().expect("connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(sub_filter, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("connect failed");
    for t in pub_topics {
        pub_cli
            .publish(mqtt::Message::new(*t, "x", mqtt::QOS_1))
            .wait()
            .expect("publish failed");
    }

    for _ in 0..pub_topics.len() {
        rx.recv_timeout(TIMEOUT)
            .expect("recv timed out")
            .expect("connection lost");
    }

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Retained message
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_retained_message() {
    let topic = topic!("retained");

    // Publish a retained message
    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("connect failed");
    let msg = mqtt::MessageBuilder::new()
        .topic(topic)
        .payload("retained-value")
        .qos(mqtt::QOS_1)
        .retained(true)
        .finalize();
    pub_cli
        .publish(msg)
        .wait()
        .expect("publish retained failed");
    pub_cli.disconnect(None).wait().ok();

    // New subscriber should receive the retained message immediately on subscribe
    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(None).wait().expect("connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("recv timed out for retained")
        .expect("connection lost");
    assert_eq!(msg.topic(), topic);
    assert_eq!(msg.payload_str(), "retained-value");
    assert!(msg.retain());

    // Clear the retained message
    let clear = mqtt::MessageBuilder::new()
        .topic(topic)
        .payload("")
        .qos(mqtt::QOS_1)
        .retained(true)
        .finalize();
    sub.publish(clear).wait().ok();
    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Last Will and Testament
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_lwt() {
    let lwt_topic = topic!("lwt");
    let lwt_payload = "client-gone";

    // Subscriber watches for the LWT
    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(None).wait().expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(lwt_topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    // LWT client connects with a will, then drops without clean disconnect
    {
        let lwt_msg = mqtt::Message::new(lwt_topic, lwt_payload, mqtt::QOS_1);
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .will_message(lwt_msg)
            .finalize();
        let lwt_cli = mqtt::AsyncClient::new(HOST).unwrap();
        lwt_cli
            .connect(conn_opts)
            .wait()
            .expect("lwt connect failed");
        // Drop without disconnect — broker should publish the will
    }

    let msg = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("LWT not received within timeout")
        .expect("connection lost");
    assert_eq!(msg.topic(), lwt_topic);
    assert_eq!(msg.payload_str(), lwt_payload);

    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Reconnect
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_reconnect() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(None).wait().expect("first connect failed");
    assert!(cli.is_connected());

    cli.disconnect(None).wait().expect("disconnect failed");

    // reconnect() uses the stored ConnectOptions
    cli.reconnect().wait().expect("reconnect failed");
    assert!(cli.is_connected());

    cli.disconnect(None).wait().ok();
}

#[test]
fn test_auto_reconnect() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    const CLIENT_ID: &str = "paho-rust-v2-auto-reconnect";

    let connect_count = Arc::new(AtomicUsize::new(0));
    let cc = Arc::clone(&connect_count);
    let (cb_tx, cb_rx) = mpsc::channel::<()>();

    let cli = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();

    cli.set_connected_callback(move |_, _rsp| {
        cc.fetch_add(1, Ordering::SeqCst);
        let _ = cb_tx.send(());
    });

    let opts = mqtt::ConnectOptionsBuilder::new()
        .automatic_reconnect(Duration::from_millis(200), Duration::from_secs(2))
        .finalize();

    cli.connect(opts).wait().expect("initial connect failed");
    assert!(cli.is_connected());

    cb_rx
        .recv_timeout(TIMEOUT)
        .expect("initial connected callback did not fire");
    assert_eq!(connect_count.load(Ordering::SeqCst), 1);

    // Force the broker to disconnect our client by connecting a second client
    // with the same client ID.  The broker must close the first session.
    let intruder = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    intruder
        .connect(None)
        .wait()
        .expect("intruder connect failed");

    cb_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("auto-reconnect did not fire connected callback within 10 s");

    assert!(
        cli.is_connected(),
        "auto-reconnect did not restore the session"
    );
    assert!(
        connect_count.load(Ordering::SeqCst) >= 2,
        "connected callback should have fired at least twice"
    );

    drop(intruder);
    cli.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// MQTT v5
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_v5_publish_subscribe() {
    let topic = topic!("v5/pub_sub");

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("v5 connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("v5 subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli
        .connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("v5 connect failed");
    pub_cli
        .publish(mqtt::Message::new(topic, "v5-payload", mqtt::QOS_1))
        .wait()
        .expect("v5 publish failed");

    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("v5 recv timed out")
        .expect("connection lost");
    assert_eq!(msg.payload_str(), "v5-payload");

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

// Verify user properties are preserved end-to-end through the broker.
#[test]
fn test_v5_user_properties() {
    use mqtt::{Properties, Property, PropertyId, PropertyValue};

    let topic = topic!("v5/user_props");

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("v5 connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli
        .connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("v5 connect failed");

    let mut props = Properties::new();
    props.push(Property::new(
        PropertyId::UserProperty,
        PropertyValue::StringPair("env".into(), "test".into()),
    ));
    let msg = mqtt::MessageBuilder::new()
        .topic(topic)
        .payload("with-props")
        .qos(mqtt::QOS_1)
        .properties(props)
        .finalize();
    pub_cli
        .publish(msg)
        .wait()
        .expect("publish with props failed");

    let received = rx
        .recv_timeout(TIMEOUT)
        .expect("recv timed out")
        .expect("connection lost");
    assert_eq!(received.payload_str(), "with-props");

    let user_prop = received
        .properties()
        .iter()
        .find(|p| p.id == PropertyId::UserProperty);
    assert!(
        user_prop.is_some(),
        "user property not present on received message"
    );
    if let Some(PropertyValue::StringPair(k, v)) = user_prop.map(|p| &p.value) {
        assert_eq!(k, "env");
        assert_eq!(v, "test");
    }
    else {
        panic!("user property has wrong type");
    }

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

// A message published with MessageExpiryInterval should still arrive promptly.
// This test verifies the property survives the broker round-trip (Mosquitto
// passes it through unchanged on MQTT v5 connections).
#[test]
fn test_v5_message_expiry_interval() {
    use mqtt::{Properties, Property, PropertyId, PropertyValue};

    let topic = topic!("v5/expiry");

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli
        .connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");

    let mut props = Properties::new();
    props.push(Property::new(
        PropertyId::MessageExpiryInterval,
        PropertyValue::U32(60),
    ));
    let msg = mqtt::MessageBuilder::new()
        .topic(topic)
        .payload("expiry-payload")
        .qos(mqtt::QOS_1)
        .properties(props)
        .finalize();
    pub_cli.publish(msg).wait().expect("publish failed");

    let received = rx
        .recv_timeout(TIMEOUT)
        .expect("recv timed out")
        .expect("connection lost");
    assert_eq!(received.payload_str(), "expiry-payload");

    // Broker passes the remaining expiry interval back in the PUBLISH
    let expiry_prop = received.properties().get(PropertyId::MessageExpiryInterval);
    assert!(
        expiry_prop.is_some(),
        "MessageExpiryInterval not present in received message"
    );

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

// A client that subscribes with no_local=true must NOT receive its own publishes
// on that topic.
#[test]
fn test_v5_no_local() {
    let topic = topic!("v5/no_local");

    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");
    let rx = cli.start_consuming();

    let opts = mqtt::SubscribeOptions::with_no_local();
    cli.subscribe_with_options(topic, mqtt::QOS_1, opts, None)
        .wait()
        .expect("subscribe failed");

    // Publish from the same client — must not be echoed back
    cli.publish(mqtt::Message::new(topic, "no-echo", mqtt::QOS_1))
        .wait()
        .expect("publish failed");

    // Allow up to 400 ms; receiving anything is a test failure
    match rx.recv_timeout(Duration::from_millis(400)) {
        Err(_) => {} // timeout — correct
        Ok(Some(m)) => panic!(
            "received own message with no_local=true: {:?}",
            m.payload_str()
        ),
        Ok(None) => {} // connection-lost sentinel is OK
    }

    cli.disconnect(None).wait().ok();
}

// Subscribing with RetainHandling::DontSendRetained must suppress the
// immediate re-delivery of a pre-existing retained message.
#[test]
fn test_v5_retain_handling_do_not_send() {
    use mqtt::RetainHandling;

    let topic = topic!("v5/retain_handling");

    // Seed a retained message
    let seed = mqtt::AsyncClient::new(HOST).unwrap();
    seed.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");
    let retained_msg = mqtt::MessageBuilder::new()
        .topic(topic)
        .payload("retained")
        .qos(mqtt::QOS_1)
        .retained(true)
        .finalize();
    seed.publish(retained_msg)
        .wait()
        .expect("publish retained failed");
    seed.disconnect(None).wait().ok();

    // Subscribe with DoNotSendRetained — retained message must not arrive
    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");
    let rx = sub.start_consuming();

    let opts =
        mqtt::SubscribeOptions::with_retain_handling(RetainHandling::DontSendRetained);
    sub.subscribe_with_options(topic, mqtt::QOS_1, opts, None)
        .wait()
        .expect("subscribe failed");

    match rx.recv_timeout(Duration::from_millis(400)) {
        Err(_) => {} // timeout — correct: retained message suppressed
        Ok(Some(m)) => panic!(
            "retained message arrived despite DontSendRetained: {:?}",
            m.payload_str()
        ),
        Ok(None) => {} // connection-lost sentinel is OK
    }

    // Clean up the retained message
    let clear = mqtt::MessageBuilder::new()
        .topic(topic)
        .payload("")
        .qos(mqtt::QOS_1)
        .retained(true)
        .finalize();
    sub.publish(clear).wait().ok();
    sub.disconnect(None).wait().ok();
}

// SUBACK reason codes confirm the broker granted the requested QoS level.
#[test]
fn test_v5_subscribe_reason_codes() {
    use mqtt::ReasonCode;

    let topic = topic!("v5/sub_reason");

    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");

    let rsp = cli
        .subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");
    assert_eq!(rsp.reason_codes.len(), 1);
    assert_eq!(rsp.reason_codes[0], ReasonCode::GrantedQos1);

    cli.disconnect(None).wait().ok();
}

// DisconnectOptions: normal disconnect with an explicit reason code.
#[test]
fn test_v5_disconnect_options() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");
    assert!(cli.is_connected());

    let opts = mqtt::DisconnectOptionsBuilder::new()
        .reason_code(mqtt::ReasonCode::NormalDisconnection)
        .timeout(Duration::from_secs(1))
        .finalize();

    cli.disconnect(opts).wait().expect("v5 disconnect failed");
    assert!(!cli.is_connected());
}

/////////////////////////////////////////////////////////////////////////////
// Session persistence
/////////////////////////////////////////////////////////////////////////////

// Connecting twice to the same client_id with clean_session=false should
// yield session_present=true on the second connect.
#[test]
fn test_session_persistence_v3() {
    const CLIENT_ID: &str = "paho-rust-v2-session-v3";

    let opts_persist = mqtt::ConnectOptionsBuilder::new_v3()
        .clean_session(false)
        .finalize();

    // First connect — establishes the session
    let cli = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    cli.connect(opts_persist.clone())
        .wait()
        .expect("first connect failed");
    cli.subscribe(topic!("session/v3"), mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");
    cli.disconnect(None).wait().expect("disconnect failed");

    // Second connect — broker should report the previous session
    let cli2 = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    let rsp = cli2
        .connect(opts_persist)
        .wait()
        .expect("second connect failed");
    assert!(
        rsp.session_present,
        "expected session_present=true on second connect"
    );

    // Clean up: reconnect with clean_session=true to remove stored session
    cli2.disconnect(None).wait().ok();
    let cleanup_opts = mqtt::ConnectOptionsBuilder::new_v3()
        .clean_session(true)
        .finalize();
    let cli3 = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    cli3.connect(cleanup_opts).wait().ok();
    cli3.disconnect(None).wait().ok();
}

// Same as above but for MQTT v5 with clean_start=false.
//
// In MQTT v5 the session only survives a disconnect when the client includes a
// non-zero SessionExpiryInterval in the CONNECT properties; the default of 0
// causes the broker to discard the session immediately on disconnect.
#[test]
fn test_session_persistence_v5() {
    use mqtt::{Properties, Property, PropertyId, PropertyValue};

    const CLIENT_ID: &str = "paho-rust-v2-session-v5";

    let mut props = Properties::new();
    props.push(Property::new(
        PropertyId::SessionExpiryInterval,
        PropertyValue::U32(300), // 5-minute session lifetime
    ));
    let opts_persist = mqtt::ConnectOptionsBuilder::new_v5()
        .clean_start(false)
        .properties(props.clone())
        .finalize();

    let cli = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    cli.connect(opts_persist.clone())
        .wait()
        .expect("first connect failed");
    cli.subscribe(topic!("session/v5"), mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");
    cli.disconnect(None).wait().expect("disconnect failed");

    let cli2 = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    let rsp = cli2
        .connect(opts_persist)
        .wait()
        .expect("second connect failed");
    assert!(
        rsp.session_present,
        "expected session_present=true on second connect (v5)"
    );

    cli2.disconnect(None).wait().ok();
    let cleanup = mqtt::ConnectOptionsBuilder::new_v5()
        .clean_start(true)
        .finalize();
    let cli3 = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    cli3.connect(cleanup).wait().ok();
    cli3.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Callbacks
/////////////////////////////////////////////////////////////////////////////

// The connected callback must fire when the connection is established.
#[test]
fn test_connected_callback() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let fired = Arc::new(AtomicBool::new(false));
    let fired2 = Arc::clone(&fired);

    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.set_connected_callback(move |_cli, rsp| {
        assert!(rsp.mqtt_version == 4 || rsp.mqtt_version == 5);
        fired2.store(true, Ordering::SeqCst);
    });

    cli.connect(None).wait().expect("connect failed");

    // Give the callback thread a moment to fire
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        fired.load(Ordering::SeqCst),
        "connected callback did not fire"
    );

    cli.disconnect(None).wait().ok();
}

// The connection_lost callback must fire when the broker closes our session
// from under us (duplicate client_id takeover).
#[test]
fn test_connection_lost_callback() {
    use std::sync::mpsc;

    const CLIENT_ID: &str = "paho-rust-v2-connlost-cb";

    let (tx, rx) = mpsc::channel::<()>();

    let cli = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    cli.set_connection_lost_callback(move |_cli| {
        let _ = tx.send(());
    });

    cli.connect(None).wait().expect("connect failed");

    // Kick our client off by connecting an intruder with the same ID
    let intruder = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    intruder
        .connect(None)
        .wait()
        .expect("intruder connect failed");

    rx.recv_timeout(Duration::from_secs(5))
        .expect("connection_lost callback did not fire within 5 s");

    drop(intruder);
}

/////////////////////////////////////////////////////////////////////////////
// Send-while-disconnected buffer
/////////////////////////////////////////////////////////////////////////////

// Messages published while the client is disconnected are buffered and
// flushed automatically after the next successful reconnect.
#[test]
fn test_send_while_disconnected() {
    const BUF_TOPIC: &str = topic!("buf/offline");
    const N_MSGS: usize = 3;

    // Subscriber — connects first and stays connected throughout
    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(None).wait().expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(BUF_TOPIC, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    // Publisher — created with send_while_disconnected enabled
    let pub_cli = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .max_buffered_messages(16)
            .send_while_disconnected(true)
            .finalize(),
    )
    .unwrap();

    // Establish then cleanly drop the connection so `send_while_disconnected`
    // applies (it only buffers after the first successful connection).
    pub_cli
        .connect(None)
        .wait()
        .expect("pub initial connect failed");
    pub_cli
        .disconnect(None)
        .wait()
        .expect("pub disconnect failed");

    // Publish while disconnected — messages enter the send-while-disconnected
    // buffer and are NOT yet delivered
    let tokens: Vec<_> = (0..N_MSGS)
        .map(|i| {
            let msg = mqtt::Message::new(BUF_TOPIC, format!("buf-{i}"), mqtt::QOS_1);
            pub_cli.publish(msg)
        })
        .collect();

    // Reconnect — buffered messages are flushed into the new session
    pub_cli.reconnect().wait().expect("pub reconnect failed");

    // Wait for the tokens to resolve (they resolve after PUBACK arrives)
    for tok in tokens {
        tok.wait().expect("buffered publish did not complete");
    }

    // Collect all N_MSGS at the subscriber
    let mut payloads: Vec<String> = (0..N_MSGS)
        .map(|_| {
            rx.recv_timeout(Duration::from_secs(8))
                .expect("recv timed out for buffered message")
                .expect("connection lost while receiving buffered messages")
                .payload_str()
                .into_owned()
        })
        .collect();
    payloads.sort();

    let mut expected: Vec<String> = (0..N_MSGS).map(|i| format!("buf-{i}")).collect();
    expected.sort();
    assert_eq!(payloads, expected);

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// max_inflight flow control
/////////////////////////////////////////////////////////////////////////////

// Sending more QoS-1 messages than max_inflight allows must still deliver
// all of them; the deferred queue drains as ACKs arrive.
#[test]
fn test_max_inflight() {
    const INFLIGHT_TOPIC: &str = topic!("inflight/many");
    const N_MSGS: usize = 20;
    const MAX_INFLIGHT: u16 = 3;

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(None).wait().expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(INFLIGHT_TOPIC, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new()
        .max_inflight(MAX_INFLIGHT)
        .finalize();
    pub_cli.connect(opts).wait().expect("pub connect failed");

    // Fire all publishes at once; some will be deferred internally
    let tokens: Vec<_> = (0..N_MSGS)
        .map(|i| {
            let msg =
                mqtt::Message::new(INFLIGHT_TOPIC, format!("msg-{i:02}"), mqtt::QOS_1);
            pub_cli.publish(msg)
        })
        .collect();

    // All tokens must resolve successfully
    for tok in tokens {
        tok.wait().expect("publish token failed");
    }

    // All N_MSGS must arrive at the subscriber
    let mut received: Vec<String> = (0..N_MSGS)
        .map(|_| {
            rx.recv_timeout(Duration::from_secs(10))
                .expect("recv timed out for inflight message")
                .expect("connection lost")
                .payload_str()
                .into_owned()
        })
        .collect();
    received.sort();

    let mut expected: Vec<String> = (0..N_MSGS).map(|i| format!("msg-{i:02}")).collect();
    expected.sort();
    assert_eq!(received, expected, "not all messages arrived");

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Async stream API
/////////////////////////////////////////////////////////////////////////////

// The get_stream() async channel API must deliver messages like start_consuming().
#[test]
fn test_get_stream() {
    let topic = topic!("stream/basic");

    let mut sub = mqtt::AsyncClient::new(HOST).unwrap();
    let stream = sub.get_stream(Some(16));
    sub.connect(None).wait().expect("connect failed");
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("pub connect failed");
    pub_cli
        .publish(mqtt::Message::new(topic, "stream-payload", mqtt::QOS_1))
        .wait()
        .expect("publish failed");

    // Poll the async-channel receiver from a background thread with a timeout.
    let (tx, rx_ch) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        loop {
            match stream.try_recv() {
                Ok(Some(m)) => {
                    let _ = tx.send(m);
                    return;
                }
                Ok(None) => {} // connection-lost sentinel; keep polling
                Err(_) => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    });

    let msg = rx_ch
        .recv_timeout(TIMEOUT)
        .expect("get_stream recv timed out");
    assert_eq!(msg.topic(), topic);
    assert_eq!(msg.payload_str(), "stream-payload");

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Sync Client wrapper
/////////////////////////////////////////////////////////////////////////////

// The synchronous Client API must provide a working publish/subscribe cycle.
#[test]
fn test_sync_client() {
    let topic = topic!("sync/pub_sub");

    let sub = mqtt::Client::new(HOST).unwrap();
    sub.connect(None).expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1).expect("subscribe failed");

    let pub_cli = mqtt::Client::new(HOST).unwrap();
    pub_cli.connect(None).expect("pub connect failed");
    pub_cli
        .publish(mqtt::Message::new(topic, "sync-payload", mqtt::QOS_1))
        .expect("publish failed");

    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("recv timed out")
        .expect("connection lost");
    assert_eq!(msg.topic(), topic);
    assert_eq!(msg.payload_str(), "sync-payload");

    pub_cli.disconnect(None).ok();
    sub.disconnect(None).ok();
}

// Verify that the sync Client delegates server_uri() and mqtt_version()
// correctly, and that try_publish() returns a usable DeliveryToken.
#[test]
fn test_sync_client_accessors() {
    let cli = mqtt::Client::new(HOST).unwrap();
    cli.connect(None).expect("connect failed");

    assert!(!cli.server_uri().is_empty());
    let ver = cli.mqtt_version_raw();
    assert!(ver == 4 || ver == 5, "unexpected mqtt version {ver}");

    let tok = cli
        .try_publish(mqtt::Message::new(
            topic!("sync/try_publish"),
            "tp",
            mqtt::QOS_0,
        ))
        .expect("try_publish failed");
    tok.wait().expect("try_publish token failed");

    cli.disconnect(None).ok();
}

/////////////////////////////////////////////////////////////////////////////
// Unix socket
/////////////////////////////////////////////////////////////////////////////

#[cfg(unix)]
#[test]
fn test_connect_unix_socket() {
    let cli = mqtt::AsyncClient::new(UNIX_HOST).unwrap();
    let rsp = cli.connect(None).wait().expect("unix connect failed");
    assert_eq!(rsp.mqtt_version, 5); // default is now v5
    assert!(!rsp.session_present);
    assert!(cli.is_connected());
    cli.disconnect(None).wait().expect("unix disconnect failed");
    assert!(!cli.is_connected());
}

#[cfg(unix)]
#[test]
fn test_publish_subscribe_unix_socket() {
    let topic = topic!("unix/pub_sub");

    let sub = mqtt::AsyncClient::new(UNIX_HOST).unwrap();
    sub.connect(None).wait().expect("unix sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("unix subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(UNIX_HOST).unwrap();
    pub_cli
        .connect(None)
        .wait()
        .expect("unix pub connect failed");
    pub_cli
        .publish(mqtt::Message::new(topic, "unix-payload", mqtt::QOS_1))
        .wait()
        .expect("unix publish failed");

    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("unix recv timed out")
        .expect("connection lost");
    assert_eq!(msg.topic(), topic);
    assert_eq!(msg.payload_str(), "unix-payload");

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Keep-alive (verify that a long-idle connection stays open)
/////////////////////////////////////////////////////////////////////////////

#[test]
fn test_keepalive() {
    let opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(2))
        .finalize();

    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(opts).wait().expect("connect failed");

    // Sleep longer than one keep-alive interval
    std::thread::sleep(Duration::from_secs(3));

    // Connection should still be alive
    assert!(cli.is_connected());
    let msg = mqtt::Message::new(topic!("keepalive"), "still here", mqtt::QOS_0);
    cli.publish(msg)
        .wait()
        .expect("publish after keepalive sleep failed");

    cli.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// WebSocket transport
//
// Requires a Mosquitto listener on port 9001 with `protocol websockets`
// (see the broker requirements at the top of this file). Compiled only with
// the `websocket` / `smol-websocket` feature.
/////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_connect_v3() {
    let cli = mqtt::AsyncClient::new(WS_HOST).unwrap();
    let rsp = cli
        .connect(mqtt::ConnectOptions::new_ws_v3())
        .wait()
        .expect("ws connect failed");
    assert_eq!(rsp.mqtt_version, 4);
    assert!(cli.is_connected());
    cli.disconnect(None).wait().expect("ws disconnect failed");
    assert!(!cli.is_connected());
}

#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_publish_subscribe() {
    let topic = topic!("ws/pub_sub");

    let sub = mqtt::AsyncClient::new(WS_HOST).unwrap();
    sub.connect(mqtt::ConnectOptions::new_ws())
        .wait()
        .expect("ws sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("ws subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(WS_HOST).unwrap();
    pub_cli
        .connect(mqtt::ConnectOptions::new_ws())
        .wait()
        .expect("ws pub connect failed");
    pub_cli
        .publish(mqtt::Message::new(topic, "ws-payload", mqtt::QOS_1))
        .wait()
        .expect("ws publish failed");

    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("ws recv timed out")
        .expect("ws connection lost");
    assert_eq!(msg.topic(), topic);
    assert_eq!(msg.payload_str(), "ws-payload");

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

// A payload above the inline threshold is written to the transport in two
// pieces; the WebSocket transport must still emit it as one frame.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_publish_subscribe_large() {
    let topic = topic!("ws/pub_sub_large");
    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 253) as u8).collect();

    let sub = mqtt::AsyncClient::new(WS_HOST).unwrap();
    sub.connect(mqtt::ConnectOptions::new_ws())
        .wait()
        .expect("ws sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("ws subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(WS_HOST).unwrap();
    pub_cli
        .connect(mqtt::ConnectOptions::new_ws())
        .wait()
        .expect("ws pub connect failed");
    pub_cli
        .publish(mqtt::Message::new(topic, payload.clone(), mqtt::QOS_1))
        .wait()
        .expect("ws publish failed");

    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("ws recv timed out")
        .expect("ws connection lost");
    assert_eq!(msg.topic(), topic);
    assert_eq!(msg.payload(), &payload[..]);

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// TLS transport
//
// Requires Mosquitto with TLS listener on port 8883.  Set TLS_CA_FILE below
// to the path of the CA certificate used to sign the broker's server cert.
// Marked `#[ignore]` — run with:
//   cargo test --features tls tls_ -- --ignored
/////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "tls", feature = "openssl"))]
#[test]
#[ignore]
fn test_tls_connect_publish() {
    // Path to the CA certificate that signed the broker's server cert.
    // Adjust to match the local Mosquitto TLS setup.
    const TLS_CA_FILE: &str = "/etc/mosquitto/certs/ca.crt";

    let tls = mqtt::TlsOptionsBuilder::new()
        .trust_store(TLS_CA_FILE)
        .enable_server_cert_auth(true)
        .finalize();

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .tls_options(tls)
        .finalize();

    let cli = mqtt::AsyncClient::new(TLS_HOST).unwrap();
    let rsp = cli.connect(conn_opts).wait().expect("tls connect failed");
    assert!(rsp.mqtt_version == 4 || rsp.mqtt_version == 5);
    assert!(cli.is_connected());

    cli.publish(mqtt::Message::new(
        topic!("tls/basic"),
        "tls-payload",
        mqtt::QOS_1,
    ))
    .wait()
    .expect("tls publish failed");

    cli.disconnect(None).wait().expect("tls disconnect failed");
    assert!(!cli.is_connected());
}
