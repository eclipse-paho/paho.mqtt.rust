// The whole suite drives a client, which needs a runtime backend.
#![cfg(any(feature = "tokio", feature = "smol"))]
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
// The TLS tests need no broker configuration: they start their own TLS
// broker with a throw-away certificate (see `tests/common`), so they run as
// part of the normal suite with the `tls` or `smol-tls` feature.

mod common;

use paho_mqtt as mqtt;
use std::time::Duration;

const HOST: &str = "tcp://localhost:1883";
#[cfg(unix)]
const UNIX_HOST: &str = "unix:///tmp/mosquitto.sock";
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
const WS_HOST: &str = "ws://localhost:9001";

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

/// A second `connect()` on a connected client is refused with its own error
/// and leaves the connection up.
#[test]
fn test_connect_while_connected_fails() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(None).wait_for(TIMEOUT).expect("connect failed");

    assert!(matches!(
        cli.connect(None).wait_for(TIMEOUT),
        Err(mqtt::Error::AlreadyConnected)
    ));
    assert!(cli.is_connected());

    cli.disconnect(None)
        .wait_for(TIMEOUT)
        .expect("disconnect failed");
}

// MQTT v3.1 requires a client ID of 1-23 characters (no empty ID with clean
// session as in v3.1.1), so these tests set one explicitly.
fn v3_1_client(id: &str) -> mqtt::AsyncClient {
    mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(id)
            .finalize(),
    )
    .unwrap()
}

#[test]
fn test_connect_disconnect_v3_1() {
    // MQTT v3.1: protocol name "MQIsdp", level 3. Mosquitto still speaks it.
    let cli = v3_1_client("paho-v31-connect");
    let rsp = cli
        .connect(mqtt::ConnectOptions::with_mqtt_version(
            mqtt::MQTT_VERSION_3_1,
        ))
        .wait()
        .expect("v3.1 connect failed");
    assert_eq!(rsp.mqtt_version, 3);
    assert!(!rsp.session_present);
    cli.disconnect(None).wait().expect("disconnect failed");
}

#[test]
fn test_publish_subscribe_v3_1() {
    let topic = topic!("v31/pub_sub");
    let sub = v3_1_client("paho-v31-sub");
    sub.connect(mqtt::ConnectOptions::with_mqtt_version(
        mqtt::MQTT_VERSION_3_1,
    ))
    .wait()
    .expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    let pub_cli = v3_1_client("paho-v31-pub");
    pub_cli
        .connect(mqtt::ConnectOptions::with_mqtt_version(
            mqtt::MQTT_VERSION_3_1,
        ))
        .wait()
        .expect("pub connect failed");
    pub_cli
        .publish(mqtt::Message::new(topic, "hello 3.1", mqtt::QOS_1))
        .wait()
        .expect("publish failed");

    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("recv timed out")
        .expect("connection lost");
    assert_eq!(msg.topic(), topic);
    assert_eq!(msg.payload_str(), "hello 3.1");

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
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

// The try_ calls return working tokens when the request can be sent: the
// whole connect / subscribe / receive / unsubscribe / disconnect cycle,
// driven through them alone.
#[test]
fn test_try_calls_round_trip() {
    let topic = topic!("try_calls");
    let topics = [topic!("try_calls/a"), topic!("try_calls/b")];

    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.try_connect(None)
        .expect("try_connect refused")
        .wait()
        .expect("connect failed");
    let rx = cli.start_consuming();

    cli.try_subscribe(topic, mqtt::QOS_1)
        .expect("try_subscribe refused")
        .wait()
        .expect("subscribe failed");
    cli.try_subscribe_many_same_qos(&topics, mqtt::QOS_1)
        .expect("try_subscribe_many_same_qos refused")
        .wait()
        .expect("subscribe many failed");

    for t in [topic, topics[0], topics[1]] {
        cli.try_publish(mqtt::Message::new(t, "try", mqtt::QOS_1))
            .expect("try_publish refused")
            .wait()
            .expect("publish failed");
        let msg = rx
            .recv_timeout(TIMEOUT)
            .expect("recv timed out")
            .expect("connection lost");
        assert_eq!(msg.topic(), t);
    }

    cli.try_unsubscribe(topic)
        .expect("try_unsubscribe refused")
        .wait()
        .expect("unsubscribe failed");
    cli.try_unsubscribe_many(&topics)
        .expect("try_unsubscribe_many refused")
        .wait()
        .expect("unsubscribe many failed");

    cli.try_disconnect(None)
        .expect("try_disconnect refused")
        .wait()
        .expect("disconnect failed");
    assert!(!cli.is_connected());
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

/////////////////////////////////////////////////////////////////////////////
// Bulk publishing
//
// Publishes queued faster than the socket takes them go out together, many
// MQTT packets per write (and, over WebSocket, per frame). These check that
// every one of them arrives intact and in order.
/////////////////////////////////////////////////////////////////////////////

const BULK_N: usize = 100_000;

/// Publishes `BULK_N` numbered QoS 0 messages without waiting, then one
/// QoS 1 message, whose PUBACK means the broker has read everything before
/// it. Returns once that has been acknowledged.
fn publish_bulk(cli: &mqtt::AsyncClient, topic: &str) {
    let mut last = None;
    for i in 0..BULK_N {
        let payload = (i as u32).to_be_bytes().to_vec();
        last = Some(cli.publish(mqtt::Message::new(topic, payload, mqtt::QOS_0)));
    }
    last.unwrap()
        .wait_for(Duration::from_secs(30))
        .expect("last QoS 0 publish failed");
    cli.publish(mqtt::Message::new(topic, "end", mqtt::QOS_1))
        .wait_for(Duration::from_secs(30))
        .expect("fence publish failed");
}

/// Publishes in bulk through a broker at `uri` and checks what a subscriber
/// receives: every message, in order, then the fence. With `lossy`, the
/// broker may drop messages, the fence included, but those that arrive must
/// be intact and in order.
fn bulk_through_broker(
    uri: &str,
    opts: impl Fn() -> mqtt::ConnectOptions,
    topic: &str,
    lossy: bool,
) {
    let sub = mqtt::AsyncClient::new(uri).unwrap();
    sub.connect(opts()).wait().expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    let pub_cli = mqtt::AsyncClient::new(uri).unwrap();
    pub_cli.connect(opts()).wait().expect("pub connect failed");
    publish_bulk(&pub_cli, topic);

    // A lossy broker may drop the fence too: then the stream just goes quiet.
    let quiet = Duration::from_secs(if lossy { 2 } else { 30 });
    let mut next = 0u32;
    loop {
        let msg = match rx.recv_timeout(quiet) {
            Ok(msg) => msg.expect("connection lost"),
            Err(_) if lossy && next > 0 => break,
            Err(e) => panic!("recv timed out: {e:?}"),
        };
        if msg.payload() == b"end" {
            break;
        }
        let n = u32::from_be_bytes(msg.payload().try_into().expect("bad payload"));
        if lossy {
            assert!(n >= next, "message {n} after {}", next - 1);
        } else {
            assert_eq!(n, next, "message out of sequence");
        }
        next = n + 1;
    }
    if !lossy {
        assert_eq!(next as usize, BULK_N);
    }

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/// Checks that a scripted broker received everything [`publish_bulk`] sent,
/// in order.
fn check_bulk_received(broker: &common::ScriptedBroker) {
    use mqtt::paho_mqtt_codec::Packet;

    let payloads = broker.received_matching(|p| match p {
        Packet::Publish(p) => Some(p.payload.clone()),
        _ => None,
    });
    assert_eq!(payloads.len(), BULK_N + 1);
    for (i, p) in payloads[..BULK_N].iter().enumerate() {
        assert_eq!(&p[..], (i as u32).to_be_bytes(), "message {i}");
    }
    assert_eq!(&payloads[BULK_N][..], b"end");
}

/////////////////////////////////////////////////////////////////////////////
// Inbound backpressure
/////////////////////////////////////////////////////////////////////////////

/// Publishes `n` QoS 1 messages whose payload is the big-endian index,
/// pausing every `batch` until the broker has acknowledged.
///
/// A publisher that floods flat out can outrun a slow subscriber by more
/// than the broker is willing to hold for it — Mosquitto queues
/// `max_queued_messages` (1000 by default) and discards the rest — and what
/// the broker drops looks exactly like loss in the client. Pacing keeps the
/// test about the client. It does not pace the *subscriber*, which is what
/// these tests are slowing down on purpose: a QoS 1 PUBACK comes from the
/// broker on receipt, not from the far end.
fn publish_paced(cli: &mqtt::AsyncClient, topic: &str, n: u32, batch: u32) {
    let mut last = None;
    for i in 0..n {
        let tok = cli.publish(mqtt::Message::new(
            topic,
            i.to_be_bytes().to_vec(),
            mqtt::QOS_1,
        ));
        if (i + 1) % batch == 0 {
            tok.wait_for(TIMEOUT).expect("publish");
            last = None;
        } else {
            last = Some(tok);
        }
    }
    if let Some(tok) = last {
        tok.wait_for(TIMEOUT).expect("publish");
    }
}

/// A message callback that publishes and *waits* for each message, as a
/// forwarder or bridge does, while thousands arrive. The inbound queue fills
/// behind the waiting callback; the session must keep reading so that the
/// PUBACK it waits for, which arrives behind the backlog, can be seen.
#[test]
fn test_callback_waiting_on_publish_does_not_deadlock() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    const N: usize = 1500;
    let src = topic!("fwd/src");
    let dst = topic!("fwd/dst");

    let fwd = mqtt::AsyncClient::new(HOST).unwrap();
    let forwarded = Arc::new(AtomicUsize::new(0));
    {
        let forwarded = forwarded.clone();
        fwd.set_message_callback(move |cli, msg| {
            if let Some(msg) = msg {
                let out = mqtt::Message::new(dst, msg.payload().to_vec(), mqtt::QOS_1);
                cli.publish(out)
                    .wait_for(Duration::from_secs(30))
                    .expect("forward");
                forwarded.fetch_add(1, Ordering::SeqCst);
            }
        });
    }
    fwd.connect(None).wait().expect("fwd connect");
    fwd.subscribe(src, mqtt::QOS_1)
        .wait()
        .expect("fwd subscribe");

    let sink = mqtt::AsyncClient::new(HOST).unwrap();
    sink.connect(None).wait().expect("sink connect");
    let rx = sink.start_consuming();
    sink.subscribe(dst, mqtt::QOS_1)
        .wait()
        .expect("sink subscribe");

    let src_cli = mqtt::AsyncClient::new(HOST).unwrap();
    src_cli.connect(None).wait().expect("src connect");
    publish_paced(&src_cli, src, N as u32, 200);

    for i in 0..N {
        if rx.recv_timeout(Duration::from_secs(10)).is_err() {
            panic!(
                "DEBUG stalled after {i} received; forwarded {}; fwd connected {}",
                forwarded.load(Ordering::SeqCst),
                fwd.is_connected()
            );
        }
    }
    // The sink can see the last message before the callback that forwarded
    // it has returned and counted it, so let the counter catch up.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while forwarded.load(Ordering::SeqCst) < N && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(forwarded.load(Ordering::SeqCst), N);

    for c in [&src_cli, &sink, &fwd] {
        c.disconnect(None).wait().ok();
    }
}

/// A consumer slower than the messages arrive: the client holds messages
/// back and stops reading rather than buffering without limit, and every
/// QoS 1 message still arrives, in order.
#[test]
fn test_slow_consumer_receives_everything_in_order() {
    use std::sync::{Arc, Mutex};

    const N: u32 = 1500;
    let topic = topic!("slow_consumer");

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    let got = Arc::new(Mutex::new(Vec::new()));
    {
        let got = got.clone();
        sub.set_message_callback(move |_, msg| {
            if let Some(msg) = msg {
                let n = u32::from_be_bytes(msg.payload().try_into().unwrap());
                got.lock().unwrap().push(n);
                if n % 500 == 0 {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        });
    }
    sub.connect(None).wait().expect("sub connect");
    sub.subscribe(topic, mqtt::QOS_1).wait().expect("subscribe");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("pub connect");
    publish_paced(&pub_cli, topic, N, 200);

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while got.lock().unwrap().len() < N as usize && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let got = got.lock().unwrap().clone();
    assert_eq!(got, (0..N).collect::<Vec<_>>());

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/// A broker that just acknowledges, for the bulk tests that check every
/// packet arrives.
fn acking_handler(pkt: &mqtt::paho_mqtt_codec::Packet) -> Vec<mqtt::paho_mqtt_codec::Packet> {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};
    match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        Packet::Publish(p) => match p.packet_id {
            Some(id) => vec![Packet::PubAck(PubAck::new(id))],
            None => vec![],
        },
        _ => vec![],
    }
}

#[test]
fn test_bulk_qos0_publish() {
    let broker = common::spawn_broker(acking_handler);
    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    cli.connect(None).wait().expect("connect failed");
    publish_bulk(&cli, topic!("bulk/tcp"));
    check_bulk_received(&broker);
    cli.disconnect(None).wait().ok();
    broker.join();
}

/// The same through mosquitto, to a subscriber. mosquitto 2.0 drops QoS 0
/// messages it cannot queue for a subscriber fast enough, which a burst
/// like this (or other tests running alongside) can cause, so only order and
/// integrity are checked.
#[test]
fn test_bulk_qos0_publish_through_broker() {
    bulk_through_broker(HOST, mqtt::ConnectOptions::new, topic!("bulk/tcp"), true);
}

/// One WebSocket frame now carries several MQTT packets, which MQTT allows
/// (§6); this checks mosquitto agrees. It takes a whole frame at once, up to
/// some 1,700 of these messages, and drops QoS 0 it cannot queue for the
/// subscriber, so as above only order and integrity are checked;
/// `test_wss_bulk_qos0_publish` checks that every packet arrives.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_bulk_qos0_publish() {
    bulk_through_broker(
        WS_HOST,
        mqtt::ConnectOptions::new_ws,
        topic!("bulk/ws"),
        true,
    );
}

/// TLS holds written bytes until a flush; every message must still arrive.
#[cfg(any(feature = "tls", feature = "smol-tls", feature = "openssl"))]
#[test]
fn test_tls_bulk_qos0_publish() {
    let certs = common::TestCerts::new("broker.invalid");
    let broker = common::spawn_tls_broker(&certs, acking_handler);

    let tls = mqtt::TlsOptionsBuilder::new()
        .trust_store(certs.ca_path())
        .verify(false) // the certificate names broker.invalid, we dial 127.0.0.1
        .finalize();
    let opts = mqtt::ConnectOptionsBuilder::new()
        .tls_options(tls)
        .connect_timeout(Duration::from_secs(5))
        .finalize();

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    cli.connect(opts)
        .wait_for(Duration::from_secs(10))
        .expect("tls connect failed");
    publish_bulk(&cli, topic!("bulk/tls"));

    check_bulk_received(&broker);

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// Many MQTT packets per WebSocket frame, decoded by a broker that checks
/// every one arrives.
#[cfg(all(feature = "tls", feature = "websocket"))]
#[test]
fn test_wss_bulk_qos0_publish() {
    let certs = common::TestCerts::new("broker.invalid");
    let (broker, uri) = common::spawn_wss_broker(&certs, acking_handler);

    let tls = mqtt::TlsOptionsBuilder::new()
        .trust_store(certs.ca_path())
        .verify(false) // the certificate names broker.invalid, we dial 127.0.0.1
        .finalize();
    let opts = mqtt::ConnectOptionsBuilder::new_ws()
        .tls_options(tls)
        .connect_timeout(Duration::from_secs(5))
        .finalize();

    let cli = mqtt::AsyncClient::new(uri).unwrap();
    cli.connect(opts)
        .wait_for(Duration::from_secs(10))
        .expect("wss connect failed");
    publish_bulk(&cli, topic!("bulk/wss"));
    check_bulk_received(&broker);

    cli.disconnect(None).wait().ok();
    broker.join();
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
        .retain(true)
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
        .retain(true)
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

/// A v5 Will's properties reach the broker: a subscriber receives the LWT
/// with the user property the will was given.
#[test]
fn test_lwt_v5_properties() {
    use mqtt::{Property, PropertyId};
    let lwt_topic = topic!("lwt-v5");

    let sub = mqtt::AsyncClient::new(HOST).unwrap();
    sub.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("sub connect failed");
    let rx = sub.start_consuming();
    sub.subscribe(lwt_topic, mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");

    {
        let mut will = mqtt::Message::new(lwt_topic, "gone", mqtt::QOS_1);
        will.props
            .push(Property::UserProperty("cause".into(), "test".into()));
        will.props.push(Property::ContentType("text/plain".into()));
        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .will_message(will)
            .finalize();
        let lwt_cli = mqtt::AsyncClient::new(HOST).unwrap();
        lwt_cli
            .connect(conn_opts)
            .wait()
            .expect("lwt connect failed");
        // Dropped without a DISCONNECT: the broker publishes the will.
    }

    let msg = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("LWT not received within timeout")
        .expect("connection lost");
    assert_eq!(msg.topic(), lwt_topic);
    assert_eq!(
        msg.properties().get(PropertyId::ContentType),
        Some(&Property::ContentType("text/plain".into())),
        "will properties were not forwarded"
    );
    assert_eq!(
        msg.properties().get(PropertyId::UserProperty),
        Some(&Property::UserProperty("cause".into(), "test".into()))
    );
    sub.disconnect(None).wait().ok();
}

/// Will properties on a v3 connection are rejected up front.
#[test]
fn test_lwt_props_need_v5() {
    use mqtt::Property;
    let mut will = mqtt::Message::new(topic!("lwt-v3"), "gone", mqtt::QOS_0);
    will.props.push(Property::ContentType("text/plain".into()));
    let conn_opts = mqtt::ConnectOptionsBuilder::new_v3()
        .will_message(will)
        .finalize();
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    assert!(matches!(
        cli.connect(conn_opts).wait(),
        Err(mqtt::Error::WrongMqttVersion)
    ));
    assert!(!cli.is_connected());
}

/// A disconnect that waits for in-flight messages works with a file store.
/// The store runs on a thread of its own and a message goes out only once
/// it is stored; the disconnect's drain used not to notice the store
/// catching up, so the message never left, the disconnect took its full
/// timeout, and the publish failed.
#[test]
fn test_disconnect_after_with_a_file_store() {
    let dir = tempfile::tempdir().unwrap();
    let cli = mqtt::CreateOptionsBuilder::new()
        .server_uri(HOST)
        .client_id("disconnect-after-file-store")
        .persistence(dir.path())
        .create_async_client()
        .unwrap();
    cli.connect(mqtt::ConnectOptions::new_v5())
        .wait_for(TIMEOUT)
        .expect("connect failed");

    let tok = cli.publish(mqtt::Message::new(topic!("drain-store"), "x", mqtt::QOS_1));
    let start = std::time::Instant::now();
    cli.disconnect_after(Duration::from_secs(5))
        .wait_for(TIMEOUT)
        .expect("disconnect failed");
    let took = start.elapsed();

    tok.wait_for(TIMEOUT)
        .expect("the publish was acknowledged before the disconnect");
    assert!(
        took < Duration::from_secs(2),
        "the disconnect waited out its timeout: {took:?}"
    );
}

/// A DISCONNECT reason code is a v5-only field; a v3 DISCONNECT has no body to
/// carry one, so asking for one is rejected up front rather than sending a
/// DISCONNECT that silently says nothing.
#[test]
fn test_disconnect_reason_code_needs_v5() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    cli.connect(mqtt::ConnectOptionsBuilder::new_v3().finalize())
        .wait()
        .unwrap();

    let opts = mqtt::DisconnectOptionsBuilder::new()
        .reason_code(mqtt::ReasonCode::AdministrativeAction)
        .finalize();
    assert!(matches!(
        cli.disconnect(opts).wait(),
        Err(mqtt::Error::WrongMqttVersion)
    ));

    // The normal disconnection is the one a v3 connection can express.
    cli.disconnect(None).wait().unwrap();
    assert!(!cli.is_connected());
}

/////////////////////////////////////////////////////////////////////////////
// Connect timeout and cancellation
/////////////////////////////////////////////////////////////////////////////

/// A TCP listener that accepts connections and never answers, standing in for
/// a broker that hangs after the transport is up.
fn silent_listener() -> (std::net::TcpListener, String) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("tcp://{}", listener.local_addr().unwrap());
    (listener, uri)
}

/// `connect_timeout` bounds the whole attempt, including waiting for CONNACK.
#[test]
fn test_connect_timeout_waiting_for_connack() {
    let (listener, uri) = silent_listener();
    // Keep the accepted socket alive so the client sees an open connection.
    let accepted = std::thread::spawn(move || listener.accept().map(|(s, _)| s));

    let cli = mqtt::AsyncClient::new(uri).unwrap();
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .connect_timeout(Duration::from_millis(500))
        .finalize();
    let start = std::time::Instant::now();
    let res = cli.connect(conn_opts).wait_for(Duration::from_secs(5));
    let elapsed = start.elapsed();

    assert!(matches!(res, Err(mqtt::Error::Timeout)), "got {res:?}");
    assert!(
        elapsed >= Duration::from_millis(450) && elapsed < Duration::from_secs(3),
        "timed out after {elapsed:?}"
    );
    assert!(!cli.is_connected());
    drop(accepted);
}

/// `disconnect()` during a hanging connect cancels the attempt immediately
/// and resolves both tokens.
#[test]
fn test_disconnect_cancels_pending_connect() {
    let (listener, uri) = silent_listener();
    let accepted = std::thread::spawn(move || listener.accept().map(|(s, _)| s));

    let cli = mqtt::AsyncClient::new(uri).unwrap();
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .connect_timeout(Duration::from_secs(30))
        .finalize();
    let conn_tok = cli.connect(conn_opts);
    std::thread::sleep(Duration::from_millis(200));

    let start = std::time::Instant::now();
    cli.disconnect(None)
        .wait_for(Duration::from_secs(5))
        .expect("disconnect should resolve");
    // The connect token fails promptly rather than waiting out its 30 s.
    let res = conn_tok.wait_for(Duration::from_secs(5));
    assert!(res.is_err(), "connect should not succeed: {res:?}");
    assert!(start.elapsed() < Duration::from_secs(3));
    assert!(!cli.is_connected());

    // The client is still usable afterwards.
    let cli2 = mqtt::AsyncClient::new(HOST).unwrap();
    cli2.connect(None).wait().expect("fresh connect");
    cli2.disconnect(None).wait().ok();
    drop(accepted);
}

/// A second `connect()` while one is in progress fails on its own rather
/// than disturbing the first.
#[test]
fn test_second_connect_while_connecting_is_rejected() {
    let (listener, uri) = silent_listener();
    let accepted = std::thread::spawn(move || listener.accept().map(|(s, _)| s));

    let cli = mqtt::AsyncClient::new(uri).unwrap();
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .connect_timeout(Duration::from_millis(700))
        .finalize();
    let first = cli.connect(conn_opts.clone());
    std::thread::sleep(Duration::from_millis(100));
    let second = cli.connect(conn_opts);

    assert!(matches!(
        second.wait_for(Duration::from_secs(2)),
        Err(mqtt::Error::ConnectInProgress)
    ));
    assert!(matches!(
        first.wait_for(Duration::from_secs(5)),
        Err(mqtt::Error::Timeout)
    ));
    drop(accepted);
}

/////////////////////////////////////////////////////////////////////////////
// Error reason codes in acknowledgements
//
// Mosquitto has nothing configured that it would refuse, so these run
// against a scripted broker built on the codec: it accepts the connection
// and answers every SUBSCRIBE, UNSUBSCRIBE, and QoS 1/2 PUBLISH with
// `NotAuthorized`.
/////////////////////////////////////////////////////////////////////////////

/// Serves one client connection, refusing everything after CONNECT.
fn refusing_broker() -> (String, std::thread::JoinHandle<()>) {
    use bytes::BytesMut;
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, RawPacket, ReasonCode, SubAck, UnsubAck};
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("tcp://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        let mut ver = mqtt::MqttVersion::V5;
        let mut tmp = [0u8; 1024];
        loop {
            let n = match sock.read(&mut tmp) {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            buf.extend_from_slice(&tmp[..n]);
            while let Some(raw) = RawPacket::decode(&mut buf).unwrap() {
                let reply = match Packet::from_raw(raw, ver).unwrap() {
                    Packet::Connect(c) => {
                        ver = c.version;
                        Some(Packet::ConnAck(ConnAck::new(false, ReasonCode::Success)))
                    }
                    Packet::Subscribe(s) => Some(Packet::SubAck(SubAck::new(
                        s.packet_id,
                        vec![ReasonCode::NotAuthorized; s.subscriptions.len()],
                    ))),
                    Packet::Unsubscribe(u) => Some(Packet::UnsubAck(UnsubAck::with_properties(
                        u.packet_id,
                        vec![ReasonCode::NotAuthorized; u.topic_filters.len()],
                        None,
                    ))),
                    Packet::Publish(p) => match (p.qos, p.packet_id) {
                        (mqtt::QoS::QoS1, Some(id)) => Some(Packet::PubAck(
                            PubAck::with_properties(id, ReasonCode::NotAuthorized, None),
                        )),
                        (mqtt::QoS::QoS2, Some(id)) => Some(Packet::PubRec(
                            PubAck::with_properties(id, ReasonCode::NotAuthorized, None),
                        )),
                        _ => None,
                    },
                    Packet::PingReq => Some(Packet::PingResp),
                    Packet::Disconnect(_) => return,
                    _ => None,
                };
                if let Some(pkt) = reply {
                    let mut out = BytesMut::new();
                    pkt.encode(&mut out, ver).unwrap();
                    sock.write_all(&out).unwrap();
                }
            }
        }
    });
    (uri, handle)
}

fn not_authorized(res: &mqtt::Result<impl std::fmt::Debug>) -> bool {
    matches!(
        res,
        Err(mqtt::Error::ReasonCode(mqtt::ReasonCode::NotAuthorized, _))
    )
}

/// Single-target operations fail their token on an error reason code;
/// many-target operations resolve `Ok` with the per-filter codes.
#[test]
fn test_ack_error_codes_policy() {
    let (uri, broker) = refusing_broker();
    let cli = mqtt::AsyncClient::new(uri).unwrap();
    cli.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect");

    // subscribe(): one filter → its error fails the token.
    let res = cli.subscribe(topic!("denied"), mqtt::QOS_1).wait();
    assert!(not_authorized(&res), "subscribe: {res:?}");

    // subscribe_many(): partial success is possible → always Ok with codes.
    let rsp = cli
        .subscribe_many(&[topic!("a"), topic!("b")], &[mqtt::QOS_1, mqtt::QOS_0])
        .wait()
        .expect("subscribe_many resolves Ok even when refused");
    assert_eq!(
        rsp.reason_codes,
        vec![
            mqtt::ReasonCode::NotAuthorized,
            mqtt::ReasonCode::NotAuthorized
        ]
    );

    // publish() QoS 1 → PUBACK error, QoS 2 → PUBREC error; both fail.
    let res = cli
        .publish(mqtt::Message::new(topic!("denied"), "x", mqtt::QOS_1))
        .wait();
    assert!(not_authorized(&res), "publish qos1: {res:?}");
    let res = cli
        .publish(mqtt::Message::new(topic!("denied"), "x", mqtt::QOS_2))
        .wait();
    assert!(not_authorized(&res), "publish qos2: {res:?}");

    // unsubscribe() / unsubscribe_many() follow the subscribe rule.
    let res = cli.unsubscribe(topic!("denied")).wait();
    assert!(not_authorized(&res), "unsubscribe: {res:?}");
    let rsp = cli
        .unsubscribe_many(&[topic!("a"), topic!("b")])
        .wait()
        .expect("unsubscribe_many resolves Ok");
    assert_eq!(rsp.reason_codes.len(), 2);

    // The connection is intact throughout: a rejected operation is that
    // operation's failure, not the session's.
    assert!(cli.is_connected());
    cli.disconnect(None).wait().ok();
    broker.join().unwrap();
}

/////////////////////////////////////////////////////////////////////////////
// WebSocket over TLS
/////////////////////////////////////////////////////////////////////////////

/// `wss://` puts the whole stack together: TLS, a WebSocket upgrade, then
/// MQTT carried in binary frames.
#[cfg(all(feature = "tls", feature = "websocket"))]
#[test]
fn test_wss_connect_publish() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};

    let certs = common::TestCerts::new("broker.invalid");
    let (broker, uri) = common::spawn_wss_broker(&certs, |pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        Packet::Publish(p) => match p.packet_id {
            Some(id) => vec![Packet::PubAck(PubAck::new(id))],
            None => vec![],
        },
        _ => vec![],
    });

    let tls = mqtt::TlsOptionsBuilder::new()
        .trust_store(certs.ca_path())
        .verify(false) // the certificate names broker.invalid, we dial 127.0.0.1
        .finalize();
    let opts = mqtt::ConnectOptionsBuilder::new_ws()
        .tls_options(tls)
        .connect_timeout(Duration::from_secs(5))
        .finalize();

    let cli = mqtt::AsyncClient::new(uri).unwrap();
    cli.connect(opts)
        .wait_for(Duration::from_secs(10))
        .expect("wss connect failed");
    assert!(cli.is_connected());

    cli.publish(mqtt::Message::new(
        topic!("wss"),
        "over-tls-ws",
        mqtt::QOS_1,
    ))
    .wait_for(Duration::from_secs(10))
    .expect("wss publish failed");

    // The broker decoded real MQTT out of the WebSocket frames.
    let published: Vec<_> = broker
        .received()
        .into_iter()
        .filter_map(|p| match p {
            Packet::Publish(p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].topic, topic!("wss"));
    assert_eq!(&published[0].payload[..], b"over-tls-ws");

    cli.disconnect(None).wait().ok();
    broker.join();
}

/////////////////////////////////////////////////////////////////////////////
// Mutual TLS
/////////////////////////////////////////////////////////////////////////////

/// A broker may require a certificate from the client as well. Presenting one
/// gets in; omitting it does not.
#[cfg(feature = "tls")]
#[test]
fn test_mutual_tls() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, ReasonCode};

    let accepting = |pkt: &Packet| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        _ => vec![],
    };

    let certs = common::TestCerts::new("broker.invalid");

    // With a client certificate from the CA the broker trusts.
    {
        let broker = common::spawn_mtls_broker(&certs, accepting);
        let tls = mqtt::TlsOptionsBuilder::new()
            .trust_store(certs.ca_path())
            .key_store(&certs.client_cert_file)
            .private_key(&certs.client_key_file)
            .verify(false) // the certificate names broker.invalid, we dial 127.0.0.1
            .finalize();
        let opts = mqtt::ConnectOptionsBuilder::new()
            .tls_options(tls)
            .connect_timeout(Duration::from_secs(5))
            .finalize();
        mqtt::AsyncClient::new(broker.uri.clone())
            .unwrap()
            .connect(opts)
            .wait_for(Duration::from_secs(10))
            .expect("mutual TLS with a client certificate should connect");
        broker.join();
    }

    // Without one, the broker refuses the handshake.
    {
        let broker = common::spawn_mtls_broker(&certs, accepting);
        let tls = mqtt::TlsOptionsBuilder::new()
            .trust_store(certs.ca_path())
            .verify(false)
            .finalize();
        let opts = mqtt::ConnectOptionsBuilder::new()
            .tls_options(tls)
            .connect_timeout(Duration::from_secs(5))
            .finalize();
        let res = mqtt::AsyncClient::new(broker.uri.clone())
            .unwrap()
            .connect(opts)
            .wait_for(Duration::from_secs(10));
        assert!(
            res.is_err(),
            "a broker requiring a client certificate must refuse one that has none, got {res:?}"
        );
    }
}

/////////////////////////////////////////////////////////////////////////////
// Client identifier
/////////////////////////////////////////////////////////////////////////////

/// The configured client identifier is what goes out in the CONNECT packet.
#[test]
fn test_connect_uses_configured_client_id() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        _ => vec![],
    });

    let cli = mqtt::CreateOptionsBuilder::new()
        .server_uri(broker.uri.clone())
        .client_id("paho-rust-v2-id-test")
        .create_async_client()
        .unwrap();
    assert_eq!(cli.client_id(), "paho-rust-v2-id-test");

    cli.connect(None).wait().expect("connect failed");

    let connects: Vec<_> = broker
        .received()
        .into_iter()
        .filter_map(|p| match p {
            Packet::Connect(c) => Some(c),
            _ => None,
        })
        .collect();
    assert_eq!(connects.len(), 1);
    assert_eq!(
        connects[0].client_id, "paho-rust-v2-id-test",
        "the configured identifier must reach the broker"
    );
    // No identifier was assigned, so the configured one still stands.
    assert_eq!(cli.client_id(), "paho-rust-v2-id-test");

    cli.disconnect(None).wait().ok();
    broker.join();
}

/////////////////////////////////////////////////////////////////////////////
// MQTT v5 CONNACK negotiation
/////////////////////////////////////////////////////////////////////////////

/// A client that connects with an empty ID takes the one the broker assigns,
/// and reports it from `client_id()`.
#[test]
fn test_v5_assigned_client_id() {
    let cli = mqtt::CreateOptionsBuilder::new()
        .server_uri(HOST)
        .client_id("")
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .create_async_client()
        .unwrap();
    assert_eq!(cli.client_id(), "");

    let rsp = cli
        .connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");

    let assigned = rsp
        .assigned_client_id()
        .expect("broker assigned no client id")
        .to_string();
    assert!(!assigned.is_empty());
    assert_eq!(cli.client_id(), assigned, "client_id() must report it");
    cli.disconnect(None).wait().ok();
}

/// The CONNACK capability accessors report what the broker advertised.
#[test]
fn test_v5_connack_capabilities() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    let rsp = cli
        .connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");

    // Values Mosquitto advertises; assert only what any broker must honour.
    assert!(rsp.receive_maximum() > 0);
    assert_eq!(rsp.maximum_qos(), mqtt::QoS::ExactlyOnce);
    assert!(rsp.retain_available());
    assert!(rsp.wildcard_subscriptions_available());
    assert!(rsp.subscription_identifiers_available());
    assert!(rsp.shared_subscriptions_available());
    assert!(rsp.server_keep_alive().is_none() || rsp.server_keep_alive().is_some());
    cli.disconnect(None).wait().ok();
}

/// A Server Keep Alive in the CONNACK overrides the requested interval: the
/// client pings on the broker's schedule, not its own.
#[test]
fn test_v5_server_keep_alive_overrides() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Property, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => {
            let mut ack = ConnAck::new(false, ReasonCode::Success);
            ack.properties.push(Property::ServerKeepAlive(1));
            vec![Packet::ConnAck(ack)]
        }
        Packet::PingReq => vec![Packet::PingResp],
        _ => vec![],
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    // Ask for a minute; the broker insists on one second.
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .keep_alive_interval(Duration::from_secs(60))
        .finalize();
    cli.connect(opts).wait().expect("connect failed");

    let pinged = broker.wait_until(Duration::from_secs(5), |pkts| {
        pkts.iter().any(|p| matches!(p, Packet::PingReq))
    });
    assert!(pinged, "no PINGREQ within 5s; server keep-alive ignored");

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// The broker's Receive Maximum caps the in-flight window even when the
/// client asked for an unlimited one.
#[test]
fn test_v5_receive_maximum_caps_inflight() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Property, ReasonCode};

    // Advertise room for exactly one unacknowledged publish, and never ack.
    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => {
            let mut ack = ConnAck::new(false, ReasonCode::Success);
            ack.properties.push(Property::ReceiveMaximum(1));
            vec![Packet::ConnAck(ack)]
        }
        _ => vec![],
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .max_inflight(0) // unlimited, as far as this client is concerned
        .finalize();
    cli.connect(opts).wait().expect("connect failed");

    for i in 0..4 {
        let _tok = cli.publish(mqtt::Message::new(
            topic!("recv-max"),
            format!("m{i}"),
            mqtt::QOS_1,
        ));
    }
    std::thread::sleep(Duration::from_millis(400));

    let publishes = broker
        .received()
        .iter()
        .filter(|p| matches!(p, Packet::Publish(_)))
        .count();
    assert_eq!(
        publishes, 1,
        "sent {publishes} publishes into a window of 1"
    );

    drop(cli);
    broker.join();
}

/////////////////////////////////////////////////////////////////////////////
// MQTT v5 maximum packet size
/////////////////////////////////////////////////////////////////////////////

/// Connect properties advertising the largest packet we will accept.
#[cfg(test)]
fn max_packet_props(max: u32) -> mqtt::Properties {
    use mqtt::Property;
    let mut props = mqtt::Properties::new();
    props.push(Property::MaximumPacketSize(max));
    props
}

/// A publish larger than the broker will accept fails its own token, rather
/// than being sent and earning a DISCONNECT.
#[test]
fn test_outbound_packet_over_server_maximum_is_refused() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Property, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => {
            let mut ack = ConnAck::new(false, ReasonCode::Success);
            ack.properties.push(Property::MaximumPacketSize(128));
            vec![Packet::ConnAck(ack)]
        }
        _ => vec![],
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    cli.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");
    assert_eq!(
        cli.connect_options().mqtt_version,
        mqtt::MqttVersion::V5,
        "sanity: the negotiated version"
    );

    // Comfortably over the 128 bytes the broker said it would take.
    let big = "x".repeat(500);
    let res = cli
        .publish(mqtt::Message::new(topic!("too-big"), big, mqtt::QOS_1))
        .wait();
    assert!(
        matches!(
            res,
            Err(mqtt::Error::Protocol(
                mqtt::paho_mqtt_codec::Error::PacketTooLarge
            ))
        ),
        "oversize publish should fail its own token, got {res:?}"
    );

    // One publish refused locally is not the session's problem.
    assert!(cli.is_connected());
    let publishes = broker
        .received()
        .iter()
        .filter(|p| matches!(p, Packet::Publish(_)))
        .count();
    assert_eq!(publishes, 0, "nothing oversize should have gone out");

    // A small one still works.
    cli.publish(mqtt::Message::new(topic!("small"), "ok", mqtt::QOS_0))
        .wait()
        .expect("a small publish should still go");

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// An inbound packet larger than the maximum we advertised is rejected at the
/// framing layer, and the broker is told why before the connection closes.
#[test]
fn test_inbound_packet_over_our_maximum_is_rejected() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Publish, ReasonCode, SubAck};

    let broker = common::spawn_broker(move |pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        Packet::Subscribe(s) => vec![
            Packet::SubAck(SubAck::new(s.packet_id, vec![ReasonCode::GrantedQos0])),
            // Far larger than the 256 bytes we said we would accept.
            Packet::Publish(Publish::new(topic!("huge").to_string(), "y".repeat(2000))),
        ],
        _ => vec![],
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .properties(max_packet_props(256))
        .finalize();
    cli.connect(opts).wait().expect("connect failed");
    cli.subscribe(topic!("huge"), mqtt::QOS_0).wait().ok();

    let told = broker.wait_until(Duration::from_secs(5), |pkts| {
        pkts.iter().any(
            |p| matches!(p, Packet::Disconnect(d) if d.reason_code == ReasonCode::PacketTooLarge),
        )
    });
    assert!(
        told,
        "expected DISCONNECT(PacketTooLarge), got {:?}",
        broker.received()
    );
    broker.join();
}

/////////////////////////////////////////////////////////////////////////////
// MQTT v5 subscription identifiers
/////////////////////////////////////////////////////////////////////////////

/// A subscription identifier given on SUBSCRIBE comes back on every message
/// delivered for that subscription (§3.8.2.1.2, §3.3.2.3.8), which is how a
/// client with several overlapping filters tells them apart.
#[test]
fn test_v5_subscription_identifier() {
    use mqtt::{Property, PropertyId};

    let cli = mqtt::AsyncClient::new(HOST).unwrap();
    let rsp = cli
        .connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");
    if !rsp.subscription_identifiers_available() {
        eprintln!("broker does not support subscription identifiers; skipping");
        cli.disconnect(None).wait().ok();
        return;
    }

    let rx = cli.start_consuming();

    let mut props = mqtt::Properties::new();
    props.push(Property::SubscriptionIdentifier(42));
    cli.subscribe_with_options(topic!("subid"), mqtt::QOS_1, None, props)
        .wait()
        .expect("subscribe failed");

    cli.publish(mqtt::Message::new(topic!("subid"), "tagged", mqtt::QOS_1))
        .wait()
        .expect("publish failed");

    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("no message")
        .expect("connection lost");
    assert_eq!(msg.payload_str(), "tagged");
    assert_eq!(
        msg.properties().get(PropertyId::SubscriptionIdentifier),
        Some(&Property::SubscriptionIdentifier(42)),
        "the broker should tag the message with our subscription identifier"
    );

    cli.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// MQTT v5 topic aliases
/////////////////////////////////////////////////////////////////////////////

/// Connect properties advertising a topic-alias maximum to the broker.
#[cfg(test)]
fn alias_max_props(max: u16) -> mqtt::Properties {
    use mqtt::Property;
    let mut props = mqtt::Properties::new();
    props.push(Property::TopicAliasMaximum(max));
    props
}

/// A publish whose topic alias exceeds what the broker allows fails its own
/// token, rather than being sent and earning a DISCONNECT.
#[test]
fn test_v5_outbound_topic_alias_limits() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Property, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => {
            let mut ack = ConnAck::new(false, ReasonCode::Success);
            ack.properties.push(Property::TopicAliasMaximum(2));
            vec![Packet::ConnAck(ack)]
        }
        _ => vec![],
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    cli.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");

    let publish_with_alias = |alias: u16| {
        let mut msg = mqtt::Message::new(topic!("alias"), "x", mqtt::QOS_0);
        msg.props.push(Property::TopicAlias(alias));
        cli.publish(msg).wait()
    };

    // Above the broker's maximum, and the reserved value 0, are refused.
    for bad in [3, 9, 0] {
        let res = publish_with_alias(bad);
        assert!(
            matches!(
                res,
                Err(mqtt::Error::ReasonCode(
                    mqtt::ReasonCode::TopicAliasInvalid,
                    _
                ))
            ),
            "alias {bad} should be refused, got {res:?}"
        );
    }
    // Within the maximum it goes out normally.
    publish_with_alias(2).expect("alias 2 is within the broker's maximum");
    assert!(
        cli.is_connected(),
        "a refused alias must not end the session"
    );

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// An inbound PUBLISH that carries only an alias is delivered with the topic
/// name the alias was established with.
#[test]
fn test_v5_inbound_topic_alias_resolved() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Property, Publish, ReasonCode, SubAck};

    let aliased = |topic: &str, alias: u16, payload: &'static str| {
        let mut p = Publish::new(topic.to_string(), payload);
        p.properties.push(Property::TopicAlias(alias));
        Packet::Publish(p)
    };

    let broker = common::spawn_broker(move |pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        Packet::Subscribe(s) => vec![
            Packet::SubAck(SubAck::new(s.packet_id, vec![ReasonCode::GrantedQos0])),
            // First establishes alias 1 for the topic, then uses it alone.
            aliased(topic!("aliased"), 1, "first"),
            aliased("", 1, "second"),
        ],
        _ => vec![],
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    let rx = cli.start_consuming();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .properties(alias_max_props(10))
        .finalize();
    cli.connect(opts).wait().expect("connect failed");
    cli.subscribe(topic!("aliased"), mqtt::QOS_0)
        .wait()
        .expect("subscribe failed");

    for expected in ["first", "second"] {
        let msg = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("no message")
            .expect("connection lost");
        assert_eq!(msg.payload_str(), expected);
        assert_eq!(
            msg.topic(),
            topic!("aliased"),
            "alias was not resolved to its topic"
        );
    }

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// An inbound alias above the maximum we advertised is a protocol error: the
/// client closes the connection and tells the broker why (§3.3.2.3.4, §4.13).
#[test]
fn test_v5_inbound_topic_alias_out_of_range_disconnects() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Property, Publish, ReasonCode, SubAck};

    let broker = common::spawn_broker(move |pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        Packet::Subscribe(s) => {
            let mut p = Publish::new(topic!("bad-alias").to_string(), "x");
            // We advertised a maximum of 2.
            p.properties.push(Property::TopicAlias(7));
            vec![
                Packet::SubAck(SubAck::new(s.packet_id, vec![ReasonCode::GrantedQos0])),
                Packet::Publish(p),
            ]
        }
        _ => vec![],
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .properties(alias_max_props(2))
        .finalize();
    cli.connect(opts).wait().expect("connect failed");
    cli.subscribe(topic!("bad-alias"), mqtt::QOS_0).wait().ok();

    let told = broker.wait_until(Duration::from_secs(5), |pkts| {
        pkts.iter().any(|p| {
            matches!(p, Packet::Disconnect(d)
                if d.reason_code == ReasonCode::TopicAliasInvalid)
        })
    });
    assert!(
        told,
        "expected DISCONNECT(TopicAliasInvalid), got {:?}",
        broker.received()
    );
    broker.join();
}

/////////////////////////////////////////////////////////////////////////////
// MQTT v5 enhanced authentication
/////////////////////////////////////////////////////////////////////////////

/// Connect properties naming an authentication method.
#[cfg(test)]
fn auth_method_props(method: &str) -> mqtt::Properties {
    use mqtt::Property;
    let mut props = mqtt::Properties::new();
    props.push(Property::AuthenticationMethod(method.into()));
    props
}

/// A two-step challenge/response exchange between CONNECT and CONNACK.
#[test]
fn test_v5_auth_exchange() {
    use mqtt::paho_mqtt_codec::{Auth, ConnAck, Packet, Property, PropertyId, ReasonCode};
    use std::sync::{Arc, Mutex};

    let challenge = |data: &'static str| {
        let mut props = mqtt::Properties::new();
        props.push(Property::AuthenticationMethod("SCRAM-TEST".into()));
        props.push(Property::AuthenticationData(data.into()));
        Packet::Auth(Auth::new(ReasonCode::ContinueAuthentication, props))
    };

    // CONNECT -> challenge -> (client answers) -> challenge -> CONNACK.
    let mut step = 0;
    let broker = common::spawn_broker(move |pkt| match pkt {
        Packet::Connect(_) => vec![challenge("server-1")],
        Packet::Auth(_) => {
            step += 1;
            if step == 1 {
                vec![challenge("server-2")]
            } else {
                vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))]
            }
        }
        _ => vec![],
    });

    // The handler records what it was asked and answers in kind.
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&seen);

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .properties(auth_method_props("SCRAM-TEST"))
        .auth_handler(move |ch| {
            assert_eq!(ch.method, "SCRAM-TEST");
            let data = String::from_utf8_lossy(ch.data).to_string();
            recorder.lock().unwrap().push(data.clone());
            Ok(format!("client-for-{data}").into_bytes())
        })
        .finalize();

    cli.connect(opts).wait().expect("auth exchange failed");
    assert!(cli.is_connected());
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["server-1".to_string(), "server-2".to_string()],
        "handler should have answered both challenges"
    );

    // The broker saw both of the client's AUTH packets, each naming the method.
    let auths: Vec<_> = broker
        .received()
        .into_iter()
        .filter_map(|p| match p {
            Packet::Auth(a) => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(auths.len(), 2);
    for a in &auths {
        assert_eq!(a.reason_code, ReasonCode::ContinueAuthentication);
        assert!(matches!(
            a.properties.get(PropertyId::AuthenticationMethod),
            Some(Property::AuthenticationMethod(m)) if m == "SCRAM-TEST"
        ));
    }

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// A handler that refuses abandons the connection attempt with its error.
#[test]
fn test_v5_auth_handler_failure_fails_connect() {
    use mqtt::paho_mqtt_codec::{Auth, Packet, Property, ReasonCode};

    let broker = common::spawn_broker(move |pkt| match pkt {
        Packet::Connect(_) => {
            let mut props = mqtt::Properties::new();
            props.push(Property::AuthenticationMethod("SCRAM-TEST".into()));
            vec![Packet::Auth(Auth::new(
                ReasonCode::ContinueAuthentication,
                props,
            ))]
        }
        _ => vec![],
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .properties(auth_method_props("SCRAM-TEST"))
        .auth_handler(|_| Err(mqtt::Error::General("no credentials")))
        .finalize();

    let res = cli.connect(opts).wait();
    assert!(
        matches!(res, Err(mqtt::Error::General("no credentials"))),
        "got {res:?}"
    );
    assert!(!cli.is_connected());
    broker.join();
}

/// The two halves of enhanced authentication must be configured together.
#[test]
fn test_auth_configuration_is_checked() {
    let cli = mqtt::AsyncClient::new(HOST).unwrap();

    // A method with no handler to answer its challenges.
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .properties(auth_method_props("SCRAM-TEST"))
        .finalize();
    assert!(cli.connect(opts).wait().is_err());

    // A handler with no method for the broker to run.
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .auth_handler(|_| Ok(Vec::new()))
        .finalize();
    assert!(cli.connect(opts).wait().is_err());
    assert!(!cli.is_connected());
}

/////////////////////////////////////////////////////////////////////////////
// TLS certificate and host-name verification
//
// These run against a TLS broker started by the test, presenting a
// certificate from a throw-away CA that names `broker.invalid` while the
// client connects to `127.0.0.1`. The mismatch is the point: it is the only
// way to see whether the name is being checked.
/////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "tls", feature = "smol-tls", feature = "openssl"))]
mod tls_verify {
    use super::*;
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, ReasonCode};

    /// Accepts any connection that gets through the handshake.
    fn accepting(pkt: &Packet) -> Vec<Packet> {
        match pkt {
            Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
            _ => vec![],
        }
    }

    fn connect_with(
        certs: &common::TestCerts,
        uri: &str,
        trust: bool,
        cert_auth: bool,
        verify: bool,
    ) -> mqtt::Result<mqtt::ConnectResponse> {
        let mut builder = mqtt::TlsOptionsBuilder::new();
        if trust {
            builder.trust_store(certs.ca_path());
        }
        let tls = builder
            .enable_server_cert_auth(cert_auth)
            .verify(verify)
            .finalize();
        let opts = mqtt::ConnectOptionsBuilder::new()
            .tls_options(tls)
            .connect_timeout(Duration::from_secs(5))
            .finalize();
        mqtt::AsyncClient::new(uri.to_string())
            .unwrap()
            .connect(opts)
            .wait_for(Duration::from_secs(10))
    }

    /// The category of a failed connect, if it failed with a TLS error.
    fn tls_kind(res: &mqtt::Result<mqtt::ConnectResponse>) -> Option<mqtt::TlsErrorKind> {
        match res {
            Err(mqtt::Error::Tls(e)) => Some(e.kind()),
            _ => None,
        }
    }

    /// With verification on, a certificate that does not name the host is
    /// refused — the default, and the safe one.
    #[test]
    fn test_tls_hostname_mismatch_refused_by_default() {
        let certs = common::TestCerts::new("broker.invalid");
        let broker = common::spawn_tls_broker(&certs, accepting);
        let res = connect_with(&certs, &broker.uri, true, true, true);
        assert_eq!(
            tls_kind(&res),
            Some(mqtt::TlsErrorKind::HostnameMismatch),
            "a mismatched name must fail the handshake, got {res:?}"
        );
    }

    /// With `verify(false)` the chain is still checked but the name is not,
    /// so the same connection succeeds.
    #[test]
    fn test_tls_hostname_check_can_be_waived() {
        let certs = common::TestCerts::new("broker.invalid");
        let broker = common::spawn_tls_broker(&certs, accepting);
        connect_with(&certs, &broker.uri, true, true, false)
            .expect("waiving the name check should let this connect");
        broker.join();
    }

    /// Waiving the name check must not quietly waive the chain check: a
    /// certificate from a CA we do not trust is still refused.
    #[test]
    fn test_tls_untrusted_ca_refused_even_without_hostname_check() {
        let certs = common::TestCerts::new("broker.invalid");
        let broker = common::spawn_tls_broker(&certs, accepting);
        // No trust store, so the system roots are used and this CA is unknown.
        let res = connect_with(&certs, &broker.uri, false, true, false);
        assert_eq!(
            tls_kind(&res),
            Some(mqtt::TlsErrorKind::UntrustedCertificate),
            "an untrusted CA must still fail, got {res:?}"
        );
    }

    /// A certificate outside its validity period is refused and reported as
    /// such. The name check is waived because OpenSSL makes it first, and
    /// these certificates name some other host.
    #[test]
    fn test_tls_certificate_dates_refused() {
        use mqtt::TlsErrorKind::{CertificateExpired, CertificateNotYetValid};

        let certs = common::TestCerts::expired("broker.invalid");
        let broker = common::spawn_tls_broker(&certs, accepting);
        let res = connect_with(&certs, &broker.uri, true, true, false);
        assert_eq!(tls_kind(&res), Some(CertificateExpired), "got {res:?}");

        let certs = common::TestCerts::not_yet_valid("broker.invalid");
        let broker = common::spawn_tls_broker(&certs, accepting);
        let res = connect_with(&certs, &broker.uri, true, true, false);
        assert_eq!(tls_kind(&res), Some(CertificateNotYetValid), "got {res:?}");
    }

    /// Two refusals particular to rustls, both before any connection is made.
    #[cfg(any(feature = "tls", feature = "smol-tls"))]
    #[test]
    fn test_tls_rustls_refusals() {
        let connect = |uri: &str, tls| {
            let opts = mqtt::ConnectOptionsBuilder::new()
                .tls_options(tls)
                .finalize();
            mqtt::AsyncClient::new(uri.to_string())
                .unwrap()
                .connect(opts)
                .wait_for(TIMEOUT)
        };

        let tls = mqtt::TlsOptionsBuilder::new()
            .private_key_password("secret")
            .finalize();
        let res = connect("ssl://127.0.0.1:1", tls);
        assert_eq!(
            tls_kind(&res),
            Some(mqtt::TlsErrorKind::Unsupported),
            "got {res:?}"
        );

        let res = connect("ssl://a..b:8883", mqtt::TlsOptionsBuilder::new().finalize());
        assert_eq!(
            tls_kind(&res),
            Some(mqtt::TlsErrorKind::InvalidServerName),
            "got {res:?}"
        );
    }

    /// TLS options that cannot be made into a configuration fail the connect
    /// with `Error::Tls` and reach the TLS error callback, like a refused
    /// handshake. Both rustls and OpenSSL reject an unknown cipher suite.
    #[test]
    fn test_tls_bad_options_are_tls_errors() {
        use std::sync::{Arc, Mutex};

        let certs = common::TestCerts::new("localhost");
        let broker = common::spawn_tls_broker(&certs, accepting);

        let reported = Arc::new(Mutex::new(Vec::new()));
        let reported_cb = Arc::clone(&reported);
        let tls = mqtt::TlsOptionsBuilder::new()
            .trust_store(certs.ca_path())
            .enabled_cipher_suites("NOT_A_CIPHER_SUITE")
            .error_callback(move |_, err| reported_cb.lock().unwrap().push(err.kind()))
            .finalize();
        let opts = mqtt::ConnectOptionsBuilder::new()
            .tls_options(tls)
            .connect_timeout(Duration::from_secs(5))
            .finalize();
        // Kept alive until the callback has run: dropping the client stops
        // the dispatcher that runs it.
        let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
        let res = cli.connect(opts).wait_for(Duration::from_secs(10));
        assert_eq!(
            tls_kind(&res),
            Some(mqtt::TlsErrorKind::Config),
            "an unknown cipher suite is a TLS error, got {res:?}"
        );

        // The callback runs on the dispatcher thread.
        let deadline = std::time::Instant::now() + TIMEOUT;
        while reported.lock().unwrap().is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(*reported.lock().unwrap(), [mqtt::TlsErrorKind::Config]);
    }

    /// Turning off certificate authentication altogether accepts anything,
    /// name and chain alike.
    #[test]
    fn test_tls_no_cert_auth_accepts_anything() {
        let certs = common::TestCerts::new("broker.invalid");
        let broker = common::spawn_tls_broker(&certs, accepting);
        connect_with(&certs, &broker.uri, false, false, true)
            .expect("with cert auth off nothing is checked");
        broker.join();
    }
}

/////////////////////////////////////////////////////////////////////////////
// HTTP proxy tunnelling (ws://)
/////////////////////////////////////////////////////////////////////////////

/// A `ws://` connection can be made through an HTTP proxy, which the client
/// reaches with a `CONNECT` request before speaking MQTT.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_through_http_proxy() {
    let target = WS_HOST.trim_start_matches("ws://").to_string();
    let proxy = common::spawn_proxy("200 Connection established", Some(target));

    let opts = mqtt::ConnectOptionsBuilder::new_ws()
        .http_proxy(proxy.uri.clone())
        .connect_timeout(Duration::from_secs(5))
        .finalize();
    let cli = mqtt::AsyncClient::new(WS_HOST).unwrap();
    cli.connect(opts)
        .wait_for(Duration::from_secs(10))
        .expect("connect through the proxy failed");
    assert!(cli.is_connected());

    // Round-trip a message to prove the tunnel carries MQTT, not just bytes.
    let rx = cli.start_consuming();
    cli.subscribe(topic!("proxy"), mqtt::QOS_1)
        .wait()
        .expect("subscribe failed");
    cli.publish(mqtt::Message::new(topic!("proxy"), "through", mqtt::QOS_1))
        .wait()
        .expect("publish failed");
    let msg = rx
        .recv_timeout(TIMEOUT)
        .expect("no message")
        .expect("connection lost");
    assert_eq!(msg.payload_str(), "through");

    // The proxy was actually asked for a tunnel to the broker.
    let reqs = proxy.requests();
    assert_eq!(reqs.len(), 1, "expected one CONNECT, got {reqs:?}");
    assert!(
        reqs[0].starts_with(&format!("CONNECT {}", WS_HOST.trim_start_matches("ws://"))),
        "unexpected request: {}",
        reqs[0]
    );

    cli.disconnect(None).wait().ok();
}

/// A proxy that refuses says why: its status line reaches the caller rather
/// than a bare connection error.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_proxy_refusal_is_reported() {
    let proxy = common::spawn_proxy("407 Proxy Authentication Required", None);

    let opts = mqtt::ConnectOptionsBuilder::new_ws()
        .http_proxy(proxy.uri.clone())
        .connect_timeout(Duration::from_secs(5))
        .finalize();
    let cli = mqtt::AsyncClient::new(WS_HOST).unwrap();
    let res = cli.connect(opts).wait_for(Duration::from_secs(10));

    assert!(
        matches!(
            &res,
            Err(mqtt::Error::Proxy(mqtt::ProxyError::Refused { status: 407, status_line }))
                if status_line.contains("Proxy Authentication Required")
        ),
        "error should name the refusal: {res:?}"
    );
    assert!(!cli.is_connected());
}

/// Connects through `proxy`, which is expected to fail, and returns the result.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
fn connect_through_proxy(proxy: &common::TestProxy) -> mqtt::Result<mqtt::ConnectResponse> {
    let opts = mqtt::ConnectOptionsBuilder::new_ws()
        .http_proxy(proxy.uri.clone())
        .connect_timeout(Duration::from_secs(5))
        .finalize();
    let cli = mqtt::AsyncClient::new(WS_HOST).unwrap();
    let res = cli.connect(opts).wait_for(Duration::from_secs(10));
    assert!(!cli.is_connected());
    res
}

/// A proxy that hangs up without answering is reported as having done so.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_proxy_closing_is_reported() {
    let proxy = common::spawn_raw_proxy(b"");
    let res = connect_through_proxy(&proxy);
    assert!(
        matches!(res, Err(mqtt::Error::Proxy(mqtt::ProxyError::Closed))),
        "got {res:?}"
    );
    assert_eq!(proxy.requests().len(), 1);
}

/// Bytes behind the proxy's response cannot be pushed back into the socket,
/// so they fail the connect rather than being lost.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_proxy_early_data_is_reported() {
    let proxy = common::spawn_raw_proxy(b"HTTP/1.1 200 Connection established\r\n\r\nEARLY");
    let res = connect_through_proxy(&proxy);
    assert!(
        matches!(res, Err(mqtt::Error::Proxy(mqtt::ProxyError::EarlyData))),
        "got {res:?}"
    );
}

/// Credentials in the proxy URI become a `Proxy-Authorization` header.
#[cfg(any(feature = "websocket", feature = "smol-websocket"))]
#[test]
fn test_ws_proxy_sends_credentials() {
    let target = WS_HOST.trim_start_matches("ws://").to_string();
    let proxy = common::spawn_proxy("200 Connection established", Some(target));
    // Insert the credentials into the proxy's own URI.
    let authed = proxy.uri.replace("http://", "http://user:pass@");

    let opts = mqtt::ConnectOptionsBuilder::new_ws()
        .http_proxy(authed)
        .connect_timeout(Duration::from_secs(5))
        .finalize();
    let cli = mqtt::AsyncClient::new(WS_HOST).unwrap();
    cli.connect(opts)
        .wait_for(Duration::from_secs(10))
        .expect("connect through the proxy failed");

    let reqs = proxy.requests();
    assert!(
        reqs[0].contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"),
        "credentials missing from: {}",
        reqs[0]
    );

    cli.disconnect(None).wait().ok();
}

/////////////////////////////////////////////////////////////////////////////
// Session state across connections
/////////////////////////////////////////////////////////////////////////////

/// Connect options for a durable session that reconnects quickly.
#[cfg(test)]
fn resuming_opts() -> mqtt::ConnectOptions {
    mqtt::ConnectOptionsBuilder::new_v5()
        .clean_start(false)
        .automatic_reconnect(Duration::from_millis(100), Duration::from_millis(500))
        .finalize()
}

/// Once the server has taken delivery of a QoS 2 message (PUBREC), what is
/// persisted is the PUBREL — not the original PUBLISH, which must never be
/// sent again (§4.4).
#[test]
fn test_qos2_persists_pubrel_after_pubrec() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};

    // Acknowledge the PUBLISH with a PUBREC, then say nothing more, leaving
    // the exchange parked at exactly the phase under test.
    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        Packet::Publish(p) => match p.packet_id {
            Some(id) => vec![Packet::PubRec(PubAck::new(id))],
            None => vec![],
        },
        _ => vec![],
    });

    let store = common::SpyPersistence::new();
    let cli = mqtt::CreateOptionsBuilder::new()
        .server_uri(broker.uri.clone())
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .user_persistence(store.clone())
        .create_async_client()
        .unwrap();
    cli.connect(mqtt::ConnectOptions::new_v5())
        .wait()
        .expect("connect failed");

    let _tok = cli.publish(mqtt::Message::new(topic!("qos2-rel"), "x", mqtt::QOS_2));

    // Wait for the exchange to reach the PUBREL.
    let got = broker.wait_until(Duration::from_secs(5), |pkts| {
        pkts.iter().any(|p| matches!(p, Packet::PubRel(_)))
    });
    assert!(got, "client did not send PUBREL");
    std::thread::sleep(Duration::from_millis(100));

    let keys = store.keys();
    assert_eq!(keys.len(), 1, "expected one persisted entry, got {keys:?}");
    match store.packet(&keys[0], mqtt::MqttVersion::V5) {
        Some(Packet::PubRel(_)) => {}
        other => panic!("persisted {other:?}, expected a PUBREL"),
    }

    drop(cli);
    broker.join();
}

/// A client with several servers keeps one file store for all of them,
/// named after the URI it was created with. A message stored while
/// connected to one server and acknowledged by another is removed from the
/// store; it used to be removed from the second server's directory, where
/// it had never been, leaving the first server's copy to be published again
/// by the next process to connect there.
#[test]
fn test_one_file_store_for_every_server() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};

    // The first server drops the connection as soon as the message arrives;
    // the second resumes the session and acknowledges the re-send.
    let first = common::spawn_broker_seq(1, |pkt, _| match pkt {
        Packet::Connect(_) => {
            vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
        }
        Packet::Publish(_) => common::Reply::close_after(vec![]),
        _ => common::Reply::none(),
    });
    let second = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(true, ReasonCode::Success))],
        Packet::Publish(p) => p
            .packet_id
            .map(|id| vec![Packet::PubAck(PubAck::new(id))])
            .unwrap_or_default(),
        _ => vec![],
    });

    let dir = tempfile::tempdir().unwrap();
    let cli = mqtt::CreateOptionsBuilder::new()
        .server_uri(first.uri.clone())
        .client_id("one-store")
        .persistence(dir.path())
        .create_async_client()
        .unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .server_uris(&[&first.uri, &second.uri])
        .clean_start(false)
        .automatic_reconnect(Duration::from_millis(50), Duration::from_millis(100))
        .finalize();
    cli.connect(opts).wait().expect("connect failed");

    let tok = cli.publish(mqtt::Message::new(topic!("one-store"), "x", mqtt::QOS_1));
    tok.wait_for(Duration::from_secs(10))
        .expect("publish never completed on the second server");
    assert_eq!(
        second
            .received_matching(|p| matches!(p, Packet::Publish(_)).then_some(()))
            .len(),
        1
    );
    cli.disconnect(None).wait().ok();

    let stores: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(stores.len(), 1, "one store for both servers: {stores:?}");
    let left: Vec<_> = std::fs::read_dir(&stores[0])
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(
        left.is_empty(),
        "the acknowledged message is still stored: {left:?}"
    );

    first.join();
    second.join();
}

/// A QoS 1 publish left unacknowledged when the connection drops is re-sent,
/// with DUP set, on the reconnect that resumes the session — with no
/// persistence store involved.
#[test]
fn test_inflight_qos1_resent_across_reconnect() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};

    let broker = common::spawn_broker_seq(2, |pkt, conn| match (pkt, conn) {
        // First connection: accept, then drop it the moment the publish
        // arrives, leaving the message unacknowledged.
        (Packet::Connect(_), 0) => {
            vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
        }
        (Packet::Publish(_), 0) => common::Reply::close_after(vec![]),
        // Second connection: the session is resumed.
        (Packet::Connect(_), _) => {
            vec![Packet::ConnAck(ConnAck::new(true, ReasonCode::Success))].into()
        }
        // ... and this time the re-sent message is acknowledged.
        (Packet::Publish(p), _) => match p.packet_id {
            Some(id) => vec![Packet::PubAck(PubAck::new(id))].into(),
            None => common::Reply::none(),
        },
        _ => common::Reply::none(),
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    cli.connect(resuming_opts()).wait().expect("connect failed");

    // The token stays pending across the reconnect and completes when the
    // re-sent copy is finally acknowledged.
    let tok = cli.publish(mqtt::Message::new(topic!("resend"), "x", mqtt::QOS_1));
    tok.wait_for(Duration::from_secs(10))
        .expect("publish never completed across the reconnect");

    let publishes: Vec<_> = broker
        .received()
        .into_iter()
        .filter_map(|p| match p {
            Packet::Publish(p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(publishes.len(), 2, "message should have been sent twice");
    assert!(!publishes[0].dup, "the first send is not a duplicate");
    assert!(publishes[1].dup, "the re-send must set DUP (MQTT-3.3.1-1)");
    assert_eq!(publishes[0].packet_id, publishes[1].packet_id);

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// A QoS 2 message parked at PUBREC is completed by re-sending the PUBREL,
/// never the message, when the session resumes.
#[test]
fn test_inflight_qos2_resends_pubrel_across_reconnect() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};

    let broker = common::spawn_broker_seq(2, |pkt, conn| match (pkt, conn) {
        (Packet::Connect(_), 0) => {
            vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
        }
        // Take delivery of the message...
        (Packet::Publish(p), 0) => match p.packet_id {
            Some(id) => vec![Packet::PubRec(PubAck::new(id))].into(),
            None => common::Reply::none(),
        },
        // ... then die holding the PUBREL, without ever sending PUBCOMP.
        (Packet::PubRel(_), 0) => common::Reply::close_after(vec![]),
        (Packet::Connect(_), _) => {
            vec![Packet::ConnAck(ConnAck::new(true, ReasonCode::Success))].into()
        }
        // The resumed session must offer the PUBREL again, which we complete.
        (Packet::PubRel(r), _) => vec![Packet::PubComp(PubAck::new(r.packet_id))].into(),
        _ => common::Reply::none(),
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    cli.connect(resuming_opts()).wait().expect("connect failed");

    let tok = cli.publish(mqtt::Message::new(topic!("qos2-resume"), "x", mqtt::QOS_2));
    tok.wait_for(Duration::from_secs(10))
        .expect("QoS 2 publish never completed across the reconnect");

    let pkts = broker.received();
    let publishes = pkts
        .iter()
        .filter(|p| matches!(p, Packet::Publish(_)))
        .count();
    let pubrels = pkts
        .iter()
        .filter(|p| matches!(p, Packet::PubRel(_)))
        .count();
    assert_eq!(publishes, 1, "the message must not be published again");
    assert_eq!(pubrels, 2, "the PUBREL, and only it, is what gets re-sent");

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// When the server does not resume the session, the client discards its
/// half of it too (v5 §3.2.2.2): neither the unacknowledged QoS 1 message nor
/// the QoS 2 exchange parked at PUBREC is sent again, and both tokens fail
/// with a reason, rather than one being re-sent into a session the server
/// says does not exist and the other waiting for a PUBCOMP that cannot come.
#[test]
fn test_unresumed_session_discards_qos1_and_qos2() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    // Drop the connection once both messages are outstanding: the QoS 1
    // unacknowledged, the QoS 2 parked at PUBREC.
    let seen_qos2 = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&seen_qos2);

    let broker = common::spawn_broker_seq(2, move |pkt, conn| match (pkt, conn) {
        (Packet::Connect(_), 0) => {
            vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
        }
        (Packet::Publish(p), 0) => match (p.qos, p.packet_id) {
            (mqtt::QoS::ExactlyOnce, Some(id)) => {
                flag.store(true, Ordering::SeqCst);
                common::Reply::close_after(vec![Packet::PubRec(PubAck::new(id))])
            }
            _ => common::Reply::none(), // leave the QoS 1 unacknowledged
        },
        // Second connection: no session (session_present = false).
        (Packet::Connect(_), _) => {
            vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
        }
        _ => common::Reply::none(),
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    cli.connect(resuming_opts()).wait().expect("connect failed");

    let qos1 = cli.publish(mqtt::Message::new(topic!("unresumed-1"), "a", mqtt::QOS_1));
    let qos2 = cli.publish(mqtt::Message::new(topic!("unresumed-2"), "b", mqtt::QOS_2));

    // Neither can be completed, and each says why.
    for (qos, tok) in [(1, qos1), (2, qos2)] {
        match tok.wait_for(Duration::from_secs(10)) {
            Err(mqtt::Error::SessionNotResumed) => {}
            other => panic!("QoS {qos} should have been abandoned, got {other:?}"),
        }
    }
    assert!(
        seen_qos2.load(Ordering::SeqCst),
        "QoS 2 never reached PUBREC"
    );

    // Nothing was offered to a session the server does not have: each
    // PUBLISH went out once, on the first connection, and no PUBREL at all.
    // The tokens fail before anything is written on the new connection, so
    // give a re-send that should not happen the time to arrive if it does.
    std::thread::sleep(Duration::from_millis(300));
    let received = broker.received();
    let publishes = received
        .iter()
        .filter(|p| matches!(p, Packet::Publish(_)))
        .count();
    assert_eq!(publishes, 2, "must not re-send into a session-less server");
    let pubrels = received
        .iter()
        .filter(|p| matches!(p, Packet::PubRel(_)))
        .count();
    assert_eq!(
        pubrels, 0,
        "must not send PUBREL into a session-less server"
    );

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// A clean start discards whatever a previous run left in the store: it is
/// state for a session the server has already thrown away.
#[test]
fn test_clean_start_clears_persistence() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Publish, QoS, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        _ => vec![],
    });

    // A store as an earlier run would have left it: one unacknowledged QoS 1.
    let store = common::SpyPersistence::new();
    let mut stale = Publish::new(topic!("stale").to_string(), "old");
    stale.qos = QoS::AtLeastOnce;
    stale.packet_id = Some(9);
    store.preload("s9", &Packet::Publish(stale), mqtt::MqttVersion::V5);
    assert_eq!(store.keys(), vec!["s9".to_string()]);

    let cli = mqtt::CreateOptionsBuilder::new()
        .server_uri(broker.uri.clone())
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .user_persistence(store.clone())
        .create_async_client()
        .unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .clean_start(true)
        .finalize();
    cli.connect(opts).wait().expect("connect failed");
    std::thread::sleep(Duration::from_millis(200));

    assert!(
        store.keys().is_empty(),
        "clean start left {:?} behind",
        store.keys()
    );
    let republished = broker
        .received()
        .iter()
        .filter(|p| matches!(p, Packet::Publish(_)))
        .count();
    assert_eq!(
        republished, 0,
        "nothing should be restored into a clean session"
    );

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// A store that cannot be listed or cleared until `broken` is reset.
#[derive(Clone)]
struct UnlistableStore {
    store: common::SpyPersistence,
    broken: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl UnlistableStore {
    fn new(store: common::SpyPersistence) -> Self {
        Self {
            store,
            broken: std::sync::Arc::new(true.into()),
        }
    }

    fn check(&self) -> mqtt::Result<()> {
        if self.broken.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(mqtt::Error::Persistence("disk on fire".into()));
        }
        Ok(())
    }
}

impl mqtt::ClientPersistence for UnlistableStore {
    fn open(&mut self, client_id: &str, server_uri: &str) -> mqtt::Result<()> {
        self.store.open(client_id, server_uri)
    }
    fn close(&mut self) -> mqtt::Result<()> {
        self.store.close()
    }
    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> mqtt::Result<()> {
        self.store.put(key, buffers)
    }
    fn get(&mut self, key: &str) -> mqtt::Result<Vec<u8>> {
        self.store.get(key)
    }
    fn remove(&mut self, key: &str) -> mqtt::Result<()> {
        self.store.remove(key)
    }
    fn keys(&mut self) -> mqtt::Result<Vec<String>> {
        self.check()?;
        mqtt::ClientPersistence::keys(&mut self.store)
    }
    fn clear(&mut self) -> mqtt::Result<()> {
        self.check()?;
        self.store.clear()
    }
    fn contains_key(&mut self, key: &str) -> bool {
        self.store.contains_key(key)
    }
}

/// A store holding one unacknowledged QoS 1 message, as an earlier run would
/// have left it, that cannot be listed.
fn unlistable_store() -> UnlistableStore {
    use mqtt::paho_mqtt_codec::{Packet, Publish, QoS};

    let store = common::SpyPersistence::new();
    let mut unacked = Publish::new(topic!("unlistable").to_string(), "a");
    unacked.qos = QoS::AtLeastOnce;
    unacked.packet_id = Some(4);
    store.preload("s4", &Packet::Publish(unacked), mqtt::MqttVersion::V5);
    UnlistableStore::new(store)
}

fn unlistable_store_client(uri: &str, store: UnlistableStore, restore: bool) -> mqtt::AsyncClient {
    mqtt::CreateOptionsBuilder::new()
        .server_uri(uri)
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .restore_messages(restore)
        .user_persistence(store)
        .create_async_client()
        .unwrap()
}

/// A store that cannot be listed fails the connect, rather than passing for
/// an empty one, and does so before the broker hears anything. Its messages
/// are not lost: once the store is fixed, the same client connects and
/// re-sends them.
#[test]
fn test_unlistable_store_fails_the_connect() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(true, ReasonCode::Success))],
        Packet::Publish(p) => vec![Packet::PubAck(PubAck::new(p.packet_id.unwrap()))],
        _ => vec![],
    });
    let store = unlistable_store();
    let cli = unlistable_store_client(&broker.uri, store.clone(), true);

    let res = cli.connect(resuming_opts()).wait_for(TIMEOUT);
    assert!(
        matches!(&res, Err(mqtt::Error::Persistence(s)) if s.contains("disk on fire")),
        "got {res:?}"
    );
    assert_eq!(cli.state(), mqtt::ConnectionState::Disconnected);
    assert!(broker.received().is_empty(), "the broker heard from us");

    store
        .broken
        .store(false, std::sync::atomic::Ordering::SeqCst);
    cli.connect(resuming_opts())
        .wait_for(TIMEOUT)
        .expect("connect failed");
    let resent = broker.wait_until(TIMEOUT, |pkts| {
        pkts.iter().any(|p| matches!(p, Packet::Publish(_)))
    });
    assert!(resent, "the stored message was not re-sent");

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// With restoring turned off the store is not read, so one that cannot be
/// listed does not stop the client connecting, and its entries stay put.
#[test]
fn test_unlistable_store_connects_without_restoring() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(true, ReasonCode::Success))],
        _ => vec![],
    });
    let store = unlistable_store();
    let cli = unlistable_store_client(&broker.uri, store.clone(), false);

    cli.connect(resuming_opts())
        .wait_for(TIMEOUT)
        .expect("connect failed");
    assert_eq!(store.store.keys(), vec!["s4".to_string()]);

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// A clean start has nothing to restore, so it needs nothing from the store.
#[test]
fn test_unlistable_store_allows_a_clean_start() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        _ => vec![],
    });
    let cli = unlistable_store_client(&broker.uri, unlistable_store(), true);

    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .clean_start(true)
        .finalize();
    cli.connect(opts).wait_for(TIMEOUT).expect("connect failed");

    cli.disconnect(None).wait().ok();
    broker.join();
}

/// A store written by an earlier run is read back and its messages re-sent
/// when the session resumes.
#[test]
fn test_persisted_messages_restored_on_connect() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, Publish, QoS, ReasonCode};

    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(true, ReasonCode::Success))],
        Packet::Publish(p) => match p.packet_id {
            Some(id) => vec![Packet::PubAck(PubAck::new(id))],
            None => vec![],
        },
        Packet::PubRel(r) => vec![Packet::PubComp(PubAck::new(r.packet_id))],
        _ => vec![],
    });

    // One message never acknowledged, and one already PUBREC'd.
    let store = common::SpyPersistence::new();
    let mut unacked = Publish::new(topic!("restored").to_string(), "a");
    unacked.qos = QoS::AtLeastOnce;
    unacked.packet_id = Some(4);
    store.preload("s4", &Packet::Publish(unacked), mqtt::MqttVersion::V5);
    store.preload("s5", &Packet::PubRel(PubAck::new(5)), mqtt::MqttVersion::V5);

    let cli = mqtt::CreateOptionsBuilder::new()
        .server_uri(broker.uri.clone())
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .user_persistence(store.clone())
        .create_async_client()
        .unwrap();
    cli.connect(resuming_opts()).wait().expect("connect failed");

    let done = broker.wait_until(Duration::from_secs(5), |pkts| {
        pkts.iter().any(|p| matches!(p, Packet::Publish(_)))
            && pkts.iter().any(|p| matches!(p, Packet::PubRel(_)))
    });
    assert!(
        done,
        "restored messages were not re-sent: {:?}",
        broker.received()
    );

    // The PUBLISH goes out as a duplicate; the PUBREC'd one only as a PUBREL.
    let publishes: Vec<_> = broker
        .received()
        .into_iter()
        .filter_map(|p| match p {
            Packet::Publish(p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(publishes.len(), 1);
    assert!(publishes[0].dup, "a restored message is a re-send");
    assert_eq!(publishes[0].packet_id, Some(4));

    // Both complete, so the store empties.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !store.keys().is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(store.keys().is_empty(), "left {:?}", store.keys());

    cli.disconnect(None).wait().ok();
    broker.join();
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
    use mqtt::{Properties, Property, PropertyId};

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
    props.push(Property::UserProperty("env".into(), "test".into()));
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
        .find(|p| p.property_id() == PropertyId::UserProperty);
    assert!(
        user_prop.is_some(),
        "user property not present on received message"
    );
    if let Some(Property::UserProperty(k, v)) = user_prop {
        assert_eq!(k, "env");
        assert_eq!(v, "test");
    } else {
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
    use mqtt::{Properties, Property, PropertyId};

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
    props.push(Property::MessageExpiryInterval(60));
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
        .retain(true)
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

    let opts = mqtt::SubscribeOptions::with_retain_handling(RetainHandling::DontSendRetained);
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
        .retain(true)
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
    use mqtt::{Properties, Property};

    const CLIENT_ID: &str = "paho-rust-v2-session-v5";

    let mut props = Properties::new();
    // 5-minute session lifetime
    props.push(Property::SessionExpiryInterval(300));
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

    // Deliberately v3: it has no server DISCONNECT packet, so a takeover
    // simply closes the socket and the client has to infer the loss. That is
    // what `connection_lost` is for. The v5 case, where the broker announces
    // the takeover with a reason, is the test below.
    let cli = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
            .finalize(),
    )
    .unwrap();
    cli.set_connection_lost_callback(move |_cli| {
        let _ = tx.send(());
    });
    cli.set_disconnected_callback(|_, _, rc| {
        panic!("v3 has no server DISCONNECT, yet `disconnected` fired with {rc:?}");
    });

    let opts = mqtt::ConnectOptionsBuilder::new_v3().finalize();
    cli.connect(opts).wait().expect("connect failed");

    // Kick our client off by connecting an intruder with the same ID
    let intruder = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
            .finalize(),
    )
    .unwrap();
    intruder
        .connect(mqtt::ConnectOptionsBuilder::new_v3().finalize())
        .wait()
        .expect("intruder connect failed");

    rx.recv_timeout(Duration::from_secs(5))
        .expect("connection_lost callback did not fire within 5 s");

    drop(intruder);
}

/// The v5 counterpart, against a real broker: a session taken over by
/// another client with the same identifier is announced with a DISCONNECT
/// carrying `SessionTakenOver`, so it is reported as `disconnected` with
/// that reason — and **not** as `connection_lost`, which means a connection
/// lost without notice.
#[test]
fn test_v5_session_taken_over_reports_disconnected() {
    use std::sync::mpsc;

    const CLIENT_ID: &str = "paho-rust-v2-takeover-v5";

    let (tx, rx) = mpsc::channel::<Result<mqtt::ReasonCode, ()>>();

    let cli = mqtt::AsyncClient::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri(HOST)
            .client_id(CLIENT_ID)
            .finalize(),
    )
    .unwrap();
    let dtx = tx.clone();
    cli.set_disconnected_callback(move |_, _props, rc| {
        let _ = dtx.send(Ok(rc));
    });
    cli.set_connection_lost_callback(move |_cli| {
        let _ = tx.send(Err(()));
    });

    cli.connect(None).wait().expect("connect failed");

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

    // Whichever callback fires first; it must be `disconnected`.
    let first = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("neither callback fired within 5 s");
    assert_eq!(
        Ok(mqtt::ReasonCode::SessionTakenOver),
        first,
        "a takeover is announced, so it is `disconnected`, not `connection_lost`"
    );

    // And the other one must not follow.
    assert!(
        rx.recv_timeout(Duration::from_millis(500)).is_err(),
        "only one of `disconnected` and `connection_lost` may fire"
    );

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
            let msg = mqtt::Message::new(INFLIGHT_TOPIC, format!("msg-{i:02}"), mqtt::QOS_1);
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

/// A broker-initiated DISCONNECT (v5) is reported as `disconnected`, with
/// its reason code, and **not** as `connection_lost`: that one means the
/// connection was lost unexpectedly, which an announced close is not. Every
/// established connection ends with one of the two, never both.
///
/// The message routes still have to be told the connection is over, so the
/// DISCONNECT carries the `None` that a lost connection would have.
#[test]
fn test_server_disconnect_reports_disconnected_not_lost() {
    use mqtt::paho_mqtt_codec::{ConnAck, Disconnect, Packet, ReasonCode, SubAck};
    use std::sync::{Arc, Mutex};

    // `spawn_broker_seq` so the handler can return a `Reply` and close.
    let broker = common::spawn_broker_seq(1, |pkt, _conn| match pkt {
        Packet::Connect(_) => {
            vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
        }
        // Acknowledge the subscription, then shut down with a reason — and
        // close the socket, as a server does after sending DISCONNECT.
        Packet::Subscribe(sub) => common::Reply::close_after(vec![
            Packet::SubAck(SubAck {
                packet_id: sub.packet_id,
                reason_codes: vec![ReasonCode::GrantedQos1],
                properties: Default::default(),
            }),
            Packet::Disconnect(Disconnect::with_properties(
                ReasonCode::ServerShuttingDown,
                None,
            )),
        ]),
        _ => common::Reply::none(),
    });

    let mut cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();

    // One log for all three callbacks, so the test can see not just what
    // fired but in what order — and, more to the point, what did not.
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let l = log.clone();
    cli.set_connection_lost_callback(move |_| l.lock().unwrap().push("lost".into()));
    let l = log.clone();
    cli.set_disconnected_callback(move |_, _props, rc| {
        l.lock().unwrap().push(format!("disconnected:{}", rc as u8))
    });
    let l = log.clone();
    cli.set_message_callback(move |_, msg| {
        l.lock().unwrap().push(if msg.is_none() {
            "none".into()
        } else {
            "msg".into()
        })
    });

    // Both message routes, to check each gets its end-of-connection marker.
    let stream = cli.get_stream(Some(8));
    let consumer = cli.start_consuming();

    // No auto-reconnect: the loop should settle back to waiting, so once the
    // disconnect has been reported nothing further is coming.
    let opts = mqtt::ConnectOptionsBuilder::new_v5().finalize();
    cli.connect(opts).wait().expect("connect failed");
    cli.subscribe("test/disc", 1).wait().ok();

    // Wait for the disconnect to be reported...
    let deadline = std::time::Instant::now() + TIMEOUT;
    while std::time::Instant::now() < deadline {
        if log
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.starts_with("disconnected:"))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // ... then give anything else that was coming time to arrive. Without
    // this the assertion below would pass against the old behaviour, which
    // delivered the stream's `None` from the `ConnectionLost` arm *before*
    // invoking the lost callback.
    std::thread::sleep(Duration::from_millis(500));

    let log = log.lock().unwrap().clone();
    assert!(
        log.contains(&format!(
            "disconnected:{}",
            ReasonCode::ServerShuttingDown as u8
        )),
        "the disconnected callback should carry the server's reason code; log: {log:?}"
    );
    assert!(
        !log.contains(&"lost".to_string()),
        "connection_lost must not fire for an announced disconnect; log: {log:?}"
    );
    assert!(
        log.contains(&"none".to_string()),
        "the message callback should be given None to end the connection; log: {log:?}"
    );

    // Both message routes see the end of the connection.
    assert!(
        matches!(consumer.recv_timeout(TIMEOUT), Ok(None)),
        "a sync consumer should see None at the end of the connection"
    );
    assert!(
        matches!(stream.try_recv(), Ok(None)),
        "an async message stream should see None at the end of the connection"
    );

    cli.disconnect(None).wait().ok();
    broker.join();
}

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

/// Streams, consumers and a callback all receive every message, and none
/// displaces another. Before the runtime fed the application's channels
/// directly, each stream was a callback in disguise, so registering one
/// silently stopped whatever held the slot before it.
#[test]
fn test_streams_and_callback_all_receive() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    let topic = topic!("fanout/all");
    let mut sub = mqtt::AsyncClient::new(HOST).unwrap();

    let stream = sub.get_stream(Some(16));
    let events = sub.get_event_stream(Some(16));
    let consumer = sub.start_consuming();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    {
        let (calls, seen) = (calls.clone(), seen.clone());
        sub.set_message_callback(move |_, msg| {
            if let Some(msg) = msg {
                calls.fetch_add(1, Ordering::SeqCst);
                seen.lock().unwrap().push(msg.topic().to_string());
            }
        });
    }

    sub.connect(None).wait().expect("connect");
    sub.subscribe(topic, mqtt::QOS_1).wait().expect("subscribe");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("pub connect");
    pub_cli
        .publish(mqtt::Message::new(topic, "fanned-out", mqtt::QOS_1))
        .wait()
        .expect("publish");

    // The synchronous consumer is the one that can block, so wait on it.
    let msg = consumer.recv_timeout(TIMEOUT).expect("consumer").unwrap();
    assert_eq!(msg.payload_str(), "fanned-out");

    let deadline = std::time::Instant::now() + TIMEOUT;
    while calls.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(seen.lock().unwrap().clone(), [topic], "the callback");

    let msg = futures::executor::block_on(stream.recv())
        .expect("message stream")
        .expect("not the end of the stream");
    assert_eq!(msg.payload_str(), "fanned-out", "the message stream");

    // The event stream carries the connection events and the messages in
    // one sequence, so the message arrives behind the Connected.
    loop {
        match futures::executor::block_on(events.recv()).expect("event stream") {
            mqtt::Event::Message(m) => {
                assert_eq!(m.payload_str(), "fanned-out", "the event stream");
                break;
            }
            mqtt::Event::Connected(_) => {}
            ev => panic!("unexpected event: {ev:?}"),
        }
    }

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/// A message callback registered after a stream leaves the stream running,
/// and `remove_callbacks` leaves it running too: the callbacks belong to the
/// application, the streams to the runtime, and neither closes the other.
#[test]
fn test_callback_registration_does_not_close_a_stream() {
    let topic = topic!("fanout/no_contention");
    let mut sub = mqtt::AsyncClient::new(HOST).unwrap();
    let stream = sub.get_stream(Some(16));

    sub.set_message_callback(|_, _| {});
    sub.remove_callbacks();
    sub.stop_consuming();

    sub.connect(None).wait().expect("connect");
    sub.subscribe(topic, mqtt::QOS_1).wait().expect("subscribe");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("pub connect");
    pub_cli
        .publish(mqtt::Message::new(topic, "still-here", mqtt::QOS_1))
        .wait()
        .expect("publish");

    let msg = futures::executor::block_on(stream.recv())
        .expect("the stream was closed by a callback call")
        .expect("not the end of the stream");
    assert_eq!(msg.payload_str(), "still-here");

    pub_cli.disconnect(None).wait().ok();
    sub.disconnect(None).wait().ok();
}

/// A bounded stream that fills up is not dropped into: the client stops
/// reading, the broker holds the rest, and everything arrives in order once
/// the application drains it. A QoS 1 message is acknowledged only after it
/// has been handed over, so nothing can be lost in between.
#[test]
fn test_full_stream_applies_backpressure_without_loss() {
    const N: u32 = 200;
    let topic = topic!("stream/backpressure");

    let mut sub = mqtt::AsyncClient::new(HOST).unwrap();
    // Far smaller than the burst, so the stream is full almost at once.
    let stream = sub.get_stream(Some(4));
    sub.connect(None).wait().expect("connect");
    sub.subscribe(topic, mqtt::QOS_1).wait().expect("subscribe");

    let pub_cli = mqtt::AsyncClient::new(HOST).unwrap();
    pub_cli.connect(None).wait().expect("pub connect");
    let mut last = None;
    for i in 0..N {
        last = Some(pub_cli.publish(mqtt::Message::new(
            topic,
            i.to_be_bytes().to_vec(),
            mqtt::QOS_1,
        )));
    }
    last.unwrap().wait_for(TIMEOUT).expect("publish");

    // Let the burst reach the subscriber with nothing draining it, so the
    // stream really is full and the session really has stopped reading
    // before the first `recv` below.
    std::thread::sleep(Duration::from_millis(500));

    // Drained with a deadline rather than a blocking `recv`, so a message
    // dropped for want of room fails the test instead of hanging it.
    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + TIMEOUT;
    while got.len() < N as usize && std::time::Instant::now() < deadline {
        match stream.try_recv() {
            Ok(Some(msg)) => got.push(u32::from_be_bytes(msg.payload()[..].try_into().unwrap())),
            Ok(None) => panic!("the connection was lost"),
            Err(_) => std::thread::sleep(Duration::from_millis(5)),
        }
    }
    assert_eq!(got, (0..N).collect::<Vec<_>>());

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
// Runs against a TLS broker started by the test, presenting a certificate
// from a throw-away CA (see `common::TestCerts`), so it needs no broker
// configuration and no certificate files checked into the repository.
/////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "tls", feature = "smol-tls", feature = "openssl"))]
#[test]
fn test_tls_connect_publish() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, PubAck, ReasonCode};

    let certs = common::TestCerts::new("broker.invalid");
    let broker = common::spawn_tls_broker(&certs, |pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        Packet::Publish(p) => match p.packet_id {
            Some(id) => vec![Packet::PubAck(PubAck::new(id))],
            None => vec![],
        },
        _ => vec![],
    });

    // The certificate names `broker.invalid` but we dial 127.0.0.1, so the
    // name check is waived; the chain is still verified against our CA.
    let tls = mqtt::TlsOptionsBuilder::new()
        .trust_store(certs.ca_path())
        .enable_server_cert_auth(true)
        .verify(false)
        .finalize();
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .tls_options(tls)
        .connect_timeout(Duration::from_secs(5))
        .finalize();

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    let rsp = cli
        .connect(conn_opts)
        .wait_for(Duration::from_secs(10))
        .expect("tls connect failed");
    assert!(rsp.mqtt_version == 4 || rsp.mqtt_version == 5);
    assert!(cli.is_connected());

    cli.publish(mqtt::Message::new(
        topic!("tls/basic"),
        "tls-payload",
        mqtt::QOS_1,
    ))
    .wait_for(Duration::from_secs(10))
    .expect("tls publish failed");

    cli.disconnect(None).wait().expect("tls disconnect failed");
    assert!(!cli.is_connected());
    broker.join();
}

/// A SUBSCRIBE whose SUBACK has not arrived when the connection drops is
/// sent again once auto-reconnect succeeds, under the same packet
/// identifier, and its token resolves with the new connection's answer.
///
/// The broker drops the first connection on receiving the SUBSCRIBE, and
/// the reconnect does not resume the session — the case where re-sending
/// matters most, since the broker then holds no subscriptions at all.
#[test]
fn test_subscribe_in_flight_is_resent_after_reconnect() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, ReasonCode, SubAck};

    let broker = common::spawn_broker_seq(2, |pkt, conn| match pkt {
        Packet::Connect(_) => {
            vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
        }
        // The first connection drops without answering.
        Packet::Subscribe(_) if conn == 0 => common::Reply::close_after(Vec::new()),
        Packet::Subscribe(sub) => vec![Packet::SubAck(SubAck {
            packet_id: sub.packet_id,
            reason_codes: vec![ReasonCode::GrantedQos1],
            properties: Default::default(),
        })]
        .into(),
        _ => common::Reply::none(),
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .automatic_reconnect(Duration::from_millis(10), Duration::from_millis(100))
        .finalize();
    cli.connect(opts).wait().expect("connect failed");

    let rsp = cli
        .subscribe("test/resend", 1)
        .wait_for(TIMEOUT)
        .expect("the subscribe should complete on the second connection");
    assert_eq!(rsp.reason_codes, [ReasonCode::GrantedQos1]);

    let subs = broker.received_matching(|p| match p {
        Packet::Subscribe(s) => Some(s.clone()),
        _ => None,
    });
    assert_eq!(subs.len(), 2, "sent once on each connection");
    assert_eq!(subs[0].packet_id, subs[1].packet_id);
    assert_eq!(subs[1].subscriptions[0].topic_filter, "test/resend");
}

/// With a list of servers, `current_server_uri()` names the one that took
/// the connection — not the creation URI, nor the first in the list — and is
/// already set when the connected callback runs.
#[test]
fn test_current_server_uri_names_the_server_connected_to() {
    use std::sync::{Arc, Mutex};

    // Nothing listens on port 1, so the first server refuses at once.
    const REFUSED: &str = "tcp://localhost:1";

    let cli = mqtt::AsyncClient::new(REFUSED).unwrap();
    assert_eq!(cli.current_server_uri(), None);

    let seen = Arc::new(Mutex::new(None));
    let seen_cb = Arc::clone(&seen);
    cli.set_connected_callback(move |cli, rsp| {
        *seen_cb.lock().unwrap() = Some((cli.current_server_uri(), rsp.server_uri.clone()));
    });

    let opts = mqtt::ConnectOptionsBuilder::new()
        .server_uris(&[REFUSED, HOST])
        .finalize();
    let rsp = cli.connect(opts).wait_for(TIMEOUT).expect("connect failed");
    assert_eq!(rsp.server_uri, HOST);
    assert_eq!(cli.current_server_uri().as_deref(), Some(HOST));
    assert_eq!(cli.server_uri(), REFUSED);

    // The callback runs on the dispatcher thread; give it a moment.
    let deadline = std::time::Instant::now() + TIMEOUT;
    while seen.lock().unwrap().is_none() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        *seen.lock().unwrap(),
        Some((Some(HOST.to_string()), HOST.to_string()))
    );

    cli.disconnect(None)
        .wait_for(TIMEOUT)
        .expect("disconnect failed");
    assert_eq!(cli.current_server_uri(), None);
}

/// `is_connected()` stays true across an automatic reconnect even when the
/// callback thread is still working through the old connection's events.
///
/// The `connected` flag this replaced was also cleared by the callback
/// thread when it reached `ConnectionLost`. Held up here by a slow message
/// callback, it got there after the event loop had already reconnected, and
/// left a connected client reporting itself disconnected.
#[test]
fn test_is_connected_survives_a_late_connection_lost() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, Publish, ReasonCode, SubAck};

    let broker = common::spawn_broker_seq(2, |pkt, conn| match pkt {
        Packet::Connect(_) => {
            vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
        }
        // Answer, deliver a message, and drop the connection.
        Packet::Subscribe(sub) if conn == 0 => common::Reply::close_after(vec![
            Packet::SubAck(SubAck {
                packet_id: sub.packet_id,
                reason_codes: vec![ReasonCode::GrantedQos0],
                properties: Default::default(),
            }),
            Packet::Publish(Publish {
                dup: false,
                qos: mqtt::QoS::QoS0,
                retain: false,
                topic: "race/t".into(),
                packet_id: None,
                payload: "x".into(),
                properties: Default::default(),
            }),
        ]),
        _ => common::Reply::none(),
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    // Keeps the callback thread busy until well after the reconnect.
    cli.set_message_callback(|_, msg| {
        if msg.is_some() {
            std::thread::sleep(Duration::from_millis(500));
        }
    });
    let opts = mqtt::ConnectOptionsBuilder::new_v5()
        .automatic_reconnect(Duration::from_millis(10), Duration::from_millis(100))
        .finalize();
    cli.connect(opts).wait().expect("connect failed");
    cli.subscribe("race/t", 0).wait().expect("subscribe failed");

    assert!(
        broker.wait_until(TIMEOUT, |pkts| {
            pkts.iter()
                .filter(|p| matches!(p, Packet::Connect(_)))
                .count()
                == 2
        }),
        "the client should have reconnected"
    );
    // Long enough for the callback thread to reach the `ConnectionLost`.
    std::thread::sleep(Duration::from_millis(1000));

    assert_eq!(cli.state(), mqtt::ConnectionState::Connected);
    assert!(cli.is_connected());
}

/// The state has already moved on when the `connection_lost` callback runs:
/// `Disconnected` without automatic reconnect, `Retrying` with it.
#[test]
fn test_connection_lost_callback_sees_the_new_state() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, ReasonCode};
    use std::sync::{Arc, Mutex};

    for auto_reconnect in [false, true] {
        // Drops the first connection on the SUBSCRIBE; accepts the second.
        let broker = common::spawn_broker_seq(2, |pkt, conn| match pkt {
            Packet::Connect(_) => {
                vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))].into()
            }
            Packet::Subscribe(_) if conn == 0 => common::Reply::close_after(Vec::new()),
            _ => common::Reply::none(),
        });

        let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
        let seen: Arc<Mutex<Option<mqtt::ConnectionState>>> = Arc::default();
        let s = Arc::clone(&seen);
        cli.set_connection_lost_callback(move |cli| *s.lock().unwrap() = Some(cli.state()));

        let mut opts = mqtt::ConnectOptionsBuilder::new_v5();
        if auto_reconnect {
            // Long enough that the client is still waiting when it is seen.
            opts.automatic_reconnect(Duration::from_secs(2), Duration::from_secs(2));
        }
        cli.connect(opts.finalize()).wait().expect("connect failed");
        assert_eq!(cli.state(), mqtt::ConnectionState::Connected);
        let _sub = cli.subscribe("lost/t", 0);

        let deadline = std::time::Instant::now() + TIMEOUT;
        while seen.lock().unwrap().is_none() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let expected = if auto_reconnect {
            mqtt::ConnectionState::Retrying
        } else {
            mqtt::ConnectionState::Disconnected
        };
        assert_eq!(
            *seen.lock().unwrap(),
            Some(expected),
            "auto_reconnect = {auto_reconnect}"
        );
        if !auto_reconnect {
            assert!(!cli.is_connected());
        }
        cli.disconnect(None).wait().ok();
        assert_eq!(cli.state(), mqtt::ConnectionState::Disconnected);
    }
}

/// While a disconnect waits for in-flight work, new operations are answered
/// at once instead of waiting for it to finish: the client reports
/// `Disconnecting`, and a publish (with no offline buffering) and a
/// subscribe fail straight away with `Disconnected`.
#[test]
fn test_operations_during_disconnect_fail_at_once() {
    use mqtt::paho_mqtt_codec::{ConnAck, Packet, ReasonCode};
    use std::time::Instant;

    // Acknowledges nothing after the CONNACK, so the drain runs its course.
    let broker = common::spawn_broker(|pkt| match pkt {
        Packet::Connect(_) => vec![Packet::ConnAck(ConnAck::new(false, ReasonCode::Success))],
        _ => Vec::new(),
    });

    let cli = mqtt::AsyncClient::new(broker.uri.clone()).unwrap();
    cli.connect(mqtt::ConnectOptionsBuilder::new_v5().finalize())
        .wait()
        .expect("connect failed");
    let _in_flight = cli.publish(mqtt::Message::new("drain/t", "1", mqtt::QOS_1));

    let drain = Duration::from_secs(2);
    let disc = cli.disconnect(mqtt::DisconnectOptions::with_timeout(drain));
    let start = Instant::now();
    while cli.state() != mqtt::ConnectionState::Disconnecting {
        assert!(start.elapsed() < TIMEOUT, "never started disconnecting");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(!cli.is_connected());

    let t = Instant::now();
    let res = cli
        .publish(mqtt::Message::new("drain/t", "2", mqtt::QOS_1))
        .wait_for(Duration::from_millis(500));
    assert!(matches!(res, Err(mqtt::Error::Disconnected)), "{res:?}");
    let res = cli
        .subscribe("drain/#", 1)
        .wait_for(Duration::from_millis(500));
    assert!(matches!(res, Err(mqtt::Error::Disconnected)), "{res:?}");
    assert!(
        t.elapsed() < Duration::from_secs(1),
        "answered during the drain"
    );

    disc.wait_for(TIMEOUT).expect("disconnect failed");
    assert!(start.elapsed() >= drain - Duration::from_millis(100));
    assert_eq!(cli.state(), mqtt::ConnectionState::Disconnected);
}
