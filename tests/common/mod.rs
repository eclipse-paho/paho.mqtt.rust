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

/// What a handler wants done in response to one packet.
pub struct Reply {
    /// Packets to send back, in order.
    pub packets: Vec<Packet>,
    /// Drop the connection once they have gone out — how a test provokes a
    /// reconnect.
    pub close: bool,
}

impl Reply {
    /// Send nothing and keep the connection.
    pub fn none() -> Self {
        Self {
            packets: Vec::new(),
            close: false,
        }
    }

    /// Send these packets, then drop the connection.
    pub fn close_after(packets: Vec<Packet>) -> Self {
        Self {
            packets,
            close: true,
        }
    }
}

impl From<Vec<Packet>> for Reply {
    fn from(packets: Vec<Packet>) -> Self {
        Self {
            packets,
            close: false,
        }
    }
}

/// A persistence store the test can look inside.
///
/// Every clone shares one map, so a test can watch what the client writes
/// while the client owns its own handle to the same store.
#[derive(Clone, Default)]
pub struct SpyPersistence(Arc<Mutex<std::collections::HashMap<String, Vec<u8>>>>);

impl SpyPersistence {
    pub fn new() -> Self {
        Self::default()
    }

    /// The keys currently held, sorted.
    pub fn keys(&self) -> Vec<String> {
        let mut k: Vec<_> = self.0.lock().unwrap().keys().cloned().collect();
        k.sort();
        k
    }

    /// Decodes the packet stored under `key`, for the given version.
    pub fn packet(&self, key: &str, ver: MqttVersion) -> Option<Packet> {
        let data = self.0.lock().unwrap().get(key).cloned()?;
        let mut buf = BytesMut::from(data.as_slice());
        let raw = RawPacket::decode(&mut buf).ok()??;
        Packet::from_raw(raw, ver).ok()
    }

    /// Writes an entry directly, to set up a store as a previous run of the
    /// program would have left it.
    pub fn preload(&self, key: &str, packet: &Packet, ver: MqttVersion) {
        let mut buf = BytesMut::new();
        packet.encode(&mut buf, ver).unwrap();
        self.0.lock().unwrap().insert(key.to_string(), buf.to_vec());
    }
}

impl paho_mqtt::ClientPersistence for SpyPersistence {
    fn open(&mut self, _client_id: &str, _server_uri: &str) -> paho_mqtt::Result<()> {
        Ok(())
    }
    fn close(&mut self) -> paho_mqtt::Result<()> {
        Ok(())
    }
    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> paho_mqtt::Result<()> {
        self.0
            .lock()
            .unwrap()
            .insert(key.to_string(), buffers.concat());
        Ok(())
    }
    fn get(&mut self, key: &str) -> paho_mqtt::Result<Vec<u8>> {
        self.0
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or(paho_mqtt::Error::General("key not found"))
    }
    fn remove(&mut self, key: &str) -> paho_mqtt::Result<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
    fn keys(&mut self) -> paho_mqtt::Result<Vec<String>> {
        Ok(self.0.lock().unwrap().keys().cloned().collect())
    }
    fn clear(&mut self) -> paho_mqtt::Result<()> {
        self.0.lock().unwrap().clear();
        Ok(())
    }
    fn contains_key(&mut self, key: &str) -> bool {
        self.0.lock().unwrap().contains_key(key)
    }
}

/// A broker that serves one or more connections from a handler closure.
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
    spawn_broker_seq(1, move |pkt, _| Reply::from(handler(pkt)))
}

/// Starts a broker that serves up to `max_conns` connections in turn.
///
/// The handler additionally receives the zero-based number of the connection
/// the packet arrived on, so a test can answer the first connection
/// differently from the reconnect that follows it — which is how session
/// resumption gets under test.
pub fn spawn_broker_seq<F>(max_conns: usize, mut handler: F) -> ScriptedBroker
where
    F: FnMut(&Packet, usize) -> Reply + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("tcp://{}", listener.local_addr().unwrap());
    let received = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&received);

    let handle = std::thread::spawn(move || {
        for conn in 0..max_conns {
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

            'conn: loop {
                let n = match sock.read(&mut tmp) {
                    Ok(0) | Err(_) => break 'conn,
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

                    let reply = handler(&packet, conn);
                    for pkt in reply.packets {
                        let mut out = BytesMut::new();
                        if pkt.encode(&mut out, ver).is_err() || sock.write_all(&out).is_err() {
                            break 'conn;
                        }
                    }
                    if reply.close || is_disconnect {
                        break 'conn;
                    }
                }
            }
            drop(sock);
        }
    });

    ScriptedBroker {
        uri,
        received,
        handle: Some(handle),
    }
}
