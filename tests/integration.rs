// tests/integration.rs
//
// Integration tests for the Eclipse Paho MQTT Rust client library.
//
// Requires a Mosquitto broker running on localhost:1883 with anonymous access.
// For Unix socket tests: broker must also listen on /tmp/mosquitto.sock.

use paho_mqtt as mqtt;
use std::time::Duration;

const HOST: &str = "tcp://localhost:1883";
#[cfg(unix)]
const UNIX_HOST: &str = "unix:///tmp/mosquitto.sock";
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
    let msg = mqtt::Message::new(topic, payload.as_bytes(), qos);
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
