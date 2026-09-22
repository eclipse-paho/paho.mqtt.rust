// tests/common/mod.rs
//
// A scripted MQTT broker for the Eclipse Paho MQTT Rust client tests.
//
// Some protocol behaviour cannot be provoked from a stock Mosquitto: it will
// not refuse a subscription that its configuration allows, will not impose a
// Server Keep Alive, and has no enhanced-authentication method built in.
// These tests instead drive the client against a broker scripted for the
// case at hand, built on `paho-mqtt-codec` — which also exercises the codec
// from the server's side.

#![allow(dead_code)] // each test file uses a different part of this

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use bytes::BytesMut;
use paho_mqtt::{
    MqttVersion,
    paho_mqtt_codec::{Packet, RawPacket},
};

/// A broker that serves exactly one connection from a handler closure.
pub struct ScriptedBroker {
    /// URI to hand to `AsyncClient::new`.
    pub uri: String,
    received: Arc<Mutex<Vec<Packet>>>,
    handle: Option<JoinHandle<()>>,
}

impl ScriptedBroker {
    /// Every packet the broker has decoded so far, in arrival order.
    pub fn received(&self) -> Vec<Packet> {
        self.received.lock().unwrap().clone()
    }

    /// The packets of one type received so far.
    pub fn received_matching<T>(&self, f: impl Fn(&Packet) -> Option<T>) -> Vec<T> {
        self.received().iter().filter_map(f).collect()
    }

    /// Waits until `pred` holds over the received packets, or the timeout
    /// expires. Returns whether it held.
    pub fn wait_until(&self, timeout: Duration, pred: impl Fn(&[Packet]) -> bool) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if pred(&self.received()) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        pred(&self.received())
    }

    /// Waits for the broker thread to finish serving its connection.
    pub fn join(mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Starts a broker that answers each packet with whatever `handler` returns.
///
/// The handler sees every decoded packet, including CONNECT, so each test
/// decides its own CONNACK — that is how the negotiated properties (Receive
/// Maximum, Server Keep Alive, Assigned Client Identifier, Topic Alias
/// Maximum) get under test. Returning packets sends them in order.
///
/// The connection is served until the client disconnects or closes it.
pub fn spawn_broker<F>(mut handler: F) -> ScriptedBroker
where
    F: FnMut(&Packet) -> Vec<Packet> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("tcp://{}", listener.local_addr().unwrap());
    let received = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&received);

    let handle = std::thread::spawn(move || {
        let Ok((mut sock, _)) = listener.accept() else {
            return;
        };
        sock.set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        let mut tmp = [0u8; 1024];
        // Until CONNECT names a version, decode as v5; CONNECT is
        // self-describing, so this only has to be a placeholder.
        let mut ver = MqttVersion::V5;

        loop {
            let n = match sock.read(&mut tmp) {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            buf.extend_from_slice(&tmp[..n]);

            while let Some(raw) = match RawPacket::decode(&mut buf) {
                Ok(r) => r,
                Err(_) => return,
            } {
                let Ok(packet) = Packet::from_raw(raw, ver) else {
                    return;
                };
                if let Packet::Connect(c) = &packet {
                    ver = c.version;
                }
                let is_disconnect = matches!(packet, Packet::Disconnect(_));
                recorded.lock().unwrap().push(packet.clone());

                for reply in handler(&packet) {
                    let mut out = BytesMut::new();
                    if reply.encode(&mut out, ver).is_err() || sock.write_all(&out).is_err() {
                        return;
                    }
                }
                if is_disconnect {
                    return;
                }
            }
        }
    });

    ScriptedBroker {
        uri,
        received,
        handle: Some(handle),
    }
}
