//! Paho MQTT v2 benchmark harness. See `bench/README.md`.
//!
//! ```text
//! mqttbench <scenario> <mode> [--url U] [--n N] [--size S] [--window W] [--rate R]
//!           [--max-inflight M] [--keep-alive SECS] [--rt-threads N] [--v5] [--persist]
//!   scenario: pub0-seq pub0-pipe pub1-seq pub1-pipe pub2-seq pub2-pipe sub rtt open
//!   mode:     wait  (plain threads, Token::wait)
//!             await (an application async runtime, .await)
//!             sync  (the synchronous Client)
//!             cb    (message callback; sub only)
//! mqttbench nullbroker <port>
//!   A minimal broker: ACKs everything and echoes PUBLISHes to a subscribed
//!   connection. Environment:
//!     BLAST_N, BLAST_SIZE  on SUBSCRIBE, flood that many QoS 0 messages of that size
//!     ACK_BATCH=K          hold PUBACKs until K are queued
//! ```
//!
//! Each run prints one `RESULT key=value ...` line.

use bytes::{Bytes, BytesMut};
use paho_mqtt as mqtt;
use paho_mqtt_codec::{self as codec, MqttVersion, Packet, QoS, RawPacket};
use std::{
    collections::VecDeque,
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(feature = "tokio")]
const BACKEND: &str = "tokio";
#[cfg(feature = "smol")]
const BACKEND: &str = "smol";

// ---------------------------------------------------------------------------
// Resource usage

#[derive(Clone, Copy)]
struct Usage {
    cpu: f64,
    vcsw: i64,
    ivcsw: i64,
}

fn usage() -> Usage {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    Usage {
        cpu: tv(ru.ru_utime) + tv(ru.ru_stime),
        vcsw: ru.ru_nvcsw,
        ivcsw: ru.ru_nivcsw,
    }
}

// ---------------------------------------------------------------------------
// Args

#[derive(Clone)]
struct Args {
    scenario: String,
    mode: String,
    url: String,
    n: usize,
    size: usize,
    window: usize,
    rate: f64,
    max_inflight: Option<u16>,
    keep_alive: u64,
    rt_threads: Option<usize>,
    v5: bool,
    persist: bool,
    /// Overrides the per-process topic, so that two runs can share one.
    topic: Option<String>,
    /// How long `sub-stall` leaves its stream unread.
    secs: f64,
}

fn parse_args() -> Args {
    let mut a = env::args().skip(1);
    let scenario = a.next().expect("scenario");
    let mode = a.next().unwrap_or_else(|| "wait".into());
    let mut args = Args {
        scenario,
        mode,
        url: "tcp://localhost:1883".into(),
        n: 100_000,
        size: 32,
        window: 0,
        rate: 0.0,
        max_inflight: None,
        keep_alive: 60,
        rt_threads: None,
        v5: false,
        persist: false,
        topic: None,
        secs: 5.0,
    };
    while let Some(k) = a.next() {
        match k.as_str() {
            "--url" => args.url = a.next().unwrap(),
            "--n" => args.n = a.next().unwrap().parse().unwrap(),
            "--size" => args.size = a.next().unwrap().parse().unwrap(),
            "--window" => args.window = a.next().unwrap().parse().unwrap(),
            "--rate" => args.rate = a.next().unwrap().parse().unwrap(),
            "--max-inflight" => args.max_inflight = Some(a.next().unwrap().parse().unwrap()),
            "--keep-alive" => args.keep_alive = a.next().unwrap().parse().unwrap(),
            "--rt-threads" => args.rt_threads = Some(a.next().unwrap().parse().unwrap()),
            "--topic" => args.topic = Some(a.next().unwrap()),
            "--secs" => args.secs = a.next().unwrap().parse().unwrap(),
            "--v5" => args.v5 = true,
            "--persist" => args.persist = true,
            _ => panic!("bad arg {k}"),
        }
    }
    args
}

// ---------------------------------------------------------------------------
// Client setup

fn client_id(args: &Args) -> String {
    format!("bench-{}-{}-{}", BACKEND, args.mode, std::process::id())
}

fn create_opts(args: &Args) -> mqtt::CreateOptions {
    let mut b = mqtt::CreateOptionsBuilder::new()
        .server_uri(args.url.clone())
        .client_id(client_id(args));
    if args.persist {
        let dir = env::temp_dir().join(format!("mqttbench-persist-{}", std::process::id()));
        b = b.persistence(dir);
    }
    if args.v5 {
        b = b.mqtt_version(mqtt::MQTT_VERSION_5);
    }
    #[cfg(feature = "tokio")]
    if let Some(n) = args.rt_threads {
        b = b.shared_runtime(n);
    }
    b.finalize()
}

fn conn_opts(args: &Args) -> mqtt::ConnectOptions {
    let mut b = if args.v5 {
        mqtt::ConnectOptionsBuilder::new_v5()
    } else {
        mqtt::ConnectOptionsBuilder::new()
    };
    b.keep_alive_interval(Duration::from_secs(args.keep_alive))
        .clean_session(!args.v5);
    if args.v5 {
        b.clean_start(true);
    }
    if let Some(n) = args.max_inflight {
        b.max_inflight(n);
    }
    b.finalize()
}

fn qos_of(s: &str) -> mqtt::QoS {
    if s.starts_with("pub1") || s == "open" {
        mqtt::QoS::QoS1
    } else if s.starts_with("pub2") {
        mqtt::QoS::QoS2
    } else {
        mqtt::QoS::QoS0
    }
}

fn topic(args: &Args) -> String {
    match &args.topic {
        Some(t) => t.clone(),
        None => format!("bench/{}/{}", BACKEND, std::process::id()),
    }
}

/// This process's resident set, in KiB, from `/proc/self/statm`.
///
/// What `sub-stall` is for: a payload is a slice of the buffer it arrived
/// in, so a message waiting for a consumer that has stopped reading keeps
/// its whole read chunk alive. This says how much that costs.
fn rss_kb() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    pages * 4
}

// ---------------------------------------------------------------------------
// Reporting

struct Report {
    elapsed: Duration,
    lat: Vec<Duration>,
    u0: Usage,
    u1: Usage,
    /// Open loop: the rate the publisher actually achieved.
    sent_rate: f64,
}

fn pct(v: &mut [Duration], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i].as_secs_f64() * 1e6
}

fn report(args: &Args, mut r: Report) {
    let n = args.n as f64;
    let secs = r.elapsed.as_secs_f64();
    let cpu = r.u1.cpu - r.u0.cpu;
    let (p50, p99, p999, max, mean) = if r.lat.is_empty() {
        (0.0, 0.0, 0.0, 0.0, 0.0)
    } else {
        let mean = r.lat.iter().map(|d| d.as_secs_f64()).sum::<f64>() / r.lat.len() as f64 * 1e6;
        (
            pct(&mut r.lat, 0.5),
            pct(&mut r.lat, 0.99),
            pct(&mut r.lat, 0.999),
            pct(&mut r.lat, 1.0),
            mean,
        )
    };
    println!(
        "RESULT backend={} scenario={} mode={} v5={} persist={} size={} n={} window={} \
         target_rate={:.0} sent_rate={:.0} secs={:.4} rate={:.0} MBps={:.2} cpu_us_per_msg={:.3} \
         cpu_util={:.2} vcsw_per_msg={:.3} ivcsw={} lat_mean_us={:.1} lat_p50_us={:.1} \
         lat_p99_us={:.1} lat_p999_us={:.1} lat_max_us={:.1}",
        BACKEND,
        args.scenario,
        args.mode,
        args.v5,
        args.persist,
        args.size,
        args.n,
        args.window,
        args.rate,
        r.sent_rate,
        secs,
        n / secs,
        n * args.size as f64 / secs / 1e6,
        cpu / n * 1e6,
        cpu / secs,
        (r.u1.vcsw - r.u0.vcsw) as f64 / n,
        r.u1.ivcsw - r.u0.ivcsw,
        mean,
        p50,
        p99,
        p999,
        max
    );
}

// ---------------------------------------------------------------------------
// Open loop

/// Publishes QoS 1 at `--rate` msg/s from a pacing thread, independently of
/// completions, and records the latency of each message from its *scheduled*
/// send time to its PUBACK. Measuring from the schedule rather than the actual
/// call keeps a stalled publisher from hiding queueing delay (coordinated
/// omission).
///
/// The pacing thread spins, so `cpu_*` figures include one busy core.
///
/// With `LAT_TRACE=<path>`, writes each message's scheduled time and latency,
/// in microseconds, one message per line, to show when stalls happen.
fn run_open(args: Args) {
    assert!(args.rate > 0.0, "open needs --rate");
    let cli = mqtt::AsyncClient::new(create_opts(&args)).unwrap();
    cli.connect(conn_opts(&args)).wait().unwrap();
    let payload = Bytes::from(vec![b'x'; args.size]);
    let t = topic(&args);
    let n = args.n;
    let period = Duration::from_secs_f64(1.0 / args.rate);

    let (tx, rx) = std::sync::mpsc::channel::<(Instant, mqtt::DeliveryToken)>();
    let start = Instant::now();
    let collector = std::thread::spawn(move || {
        let mut lat = Vec::with_capacity(n);
        let mut trace = Vec::new();
        let tracing = env::var_os("LAT_TRACE").is_some();
        for (sched, tok) in rx {
            tok.wait().unwrap();
            let l = sched.elapsed();
            if tracing {
                trace.push(((sched - start).as_secs_f64() * 1e6, l.as_secs_f64() * 1e6));
            }
            lat.push(l);
        }
        (lat, trace)
    });

    let u0 = usage();
    for i in 0..n {
        let sched = start + period * i as u32;
        while Instant::now() < sched {
            std::hint::spin_loop();
        }
        let tok = cli.publish(mqtt::Message::new(
            t.as_str(),
            payload.clone(),
            mqtt::QoS::QoS1,
        ));
        tx.send((sched, tok)).unwrap();
    }
    let sent_secs = start.elapsed().as_secs_f64();
    drop(tx);
    let (lat, trace) = collector.join().unwrap();
    // LAT_TRACE=path: one "<scheduled_us> <latency_us>" line per message.
    if let Some(path) = env::var_os("LAT_TRACE") {
        let mut out = String::with_capacity(trace.len() * 24);
        for (t, l) in &trace {
            out.push_str(&format!("{t:.1} {l:.1}\n"));
        }
        std::fs::write(path, out).unwrap();
    }
    let elapsed = start.elapsed();
    let u1 = usage();
    report(
        &args,
        Report {
            elapsed,
            lat,
            u0,
            u1,
            sent_rate: n as f64 / sent_secs,
        },
    );
    cli.disconnect(None).wait().unwrap();
}

// ---------------------------------------------------------------------------
// Blocking (Token::wait) and sync Client scenarios

fn run_wait(args: Args) {
    let mut cli = mqtt::AsyncClient::new(create_opts(&args)).unwrap();
    cli.connect(conn_opts(&args)).wait().unwrap();
    let payload = Bytes::from(vec![b'x'; args.size]);
    let t = topic(&args);
    let qos = qos_of(&args.scenario);
    let n = args.n;
    let mut lat = Vec::new();

    let (u0, start) = match args.scenario.as_str() {
        s if s.ends_with("-seq") => {
            lat.reserve(n);
            let (u0, start) = (usage(), Instant::now());
            for _ in 0..n {
                let t0 = Instant::now();
                cli.publish(mqtt::Message::new(t.as_str(), payload.clone(), qos))
                    .wait()
                    .unwrap();
                lat.push(t0.elapsed());
            }
            (u0, start)
        }
        s if s.ends_with("-pipe") => {
            let (u0, start) = (usage(), Instant::now());
            let mut win = VecDeque::new();
            for _ in 0..n {
                win.push_back(cli.publish(mqtt::Message::new(t.as_str(), payload.clone(), qos)));
                if args.window > 0 && win.len() >= args.window {
                    win.pop_front().unwrap().wait().unwrap();
                }
            }
            for tok in win {
                tok.wait().unwrap();
            }
            (u0, start)
        }
        "sub" => {
            let rx = cli.start_consuming();
            cli.subscribe(t.as_str(), mqtt::QoS::QoS0).wait().unwrap();
            let (u0, start) = (usage(), Instant::now());
            for _ in 0..n {
                rx.recv().unwrap().unwrap();
            }
            (u0, start)
        }
        // A consumer that has stopped reading: the stream fills, the client
        // stops reading the socket, and what it is left holding is the
        // question. Reports `rss_kb` rather than a rate.
        "sub-stall" => {
            let before = rss_kb();
            let _stream = cli.get_stream(Some(n));
            cli.subscribe(t.as_str(), mqtt::QoS::QoS0).wait().unwrap();
            let (u0, start) = (usage(), Instant::now());
            std::thread::sleep(Duration::from_secs_f64(args.secs));
            println!(
                "STALL backend={} n={} secs={:.1} rss_before_kb={} rss_kb={} held_kb={}",
                BACKEND,
                n,
                args.secs,
                before,
                rss_kb(),
                rss_kb().saturating_sub(before),
            );
            (u0, start)
        }
        "rtt" => {
            let rx = cli.start_consuming();
            cli.subscribe(t.as_str(), mqtt::QoS::QoS0).wait().unwrap();
            lat.reserve(n);
            let (u0, start) = (usage(), Instant::now());
            for _ in 0..n {
                let t0 = Instant::now();
                cli.publish(mqtt::Message::new(
                    t.as_str(),
                    payload.clone(),
                    mqtt::QoS::QoS0,
                ));
                rx.recv().unwrap().unwrap();
                lat.push(t0.elapsed());
            }
            (u0, start)
        }
        s => panic!("scenario {s} not supported in wait mode"),
    };
    let elapsed = start.elapsed();
    let u1 = usage();
    report(
        &args,
        Report {
            elapsed,
            lat,
            u0,
            u1,
            sent_rate: 0.0,
        },
    );
    cli.disconnect(None).wait().unwrap();
}

fn run_sync(args: Args) {
    let cli = mqtt::Client::new(create_opts(&args)).unwrap();
    cli.connect(conn_opts(&args)).unwrap();
    let payload = Bytes::from(vec![b'x'; args.size]);
    let t = topic(&args);
    let qos = qos_of(&args.scenario);
    let n = args.n;
    let mut lat = Vec::with_capacity(n);
    let (u0, start) = match args.scenario.as_str() {
        s if s.ends_with("-seq") => {
            let (u0, start) = (usage(), Instant::now());
            for _ in 0..n {
                let t0 = Instant::now();
                cli.publish(mqtt::Message::new(t.as_str(), payload.clone(), qos))
                    .unwrap();
                lat.push(t0.elapsed());
            }
            (u0, start)
        }
        "rtt" => {
            let rx = cli.start_consuming();
            cli.subscribe(t.as_str(), mqtt::QoS::QoS0).unwrap();
            let (u0, start) = (usage(), Instant::now());
            for _ in 0..n {
                let t0 = Instant::now();
                cli.publish(mqtt::Message::new(
                    t.as_str(),
                    payload.clone(),
                    mqtt::QoS::QoS0,
                ))
                .unwrap();
                rx.recv().unwrap().unwrap();
                lat.push(t0.elapsed());
            }
            (u0, start)
        }
        s => panic!("scenario {s} not supported in sync mode"),
    };
    let elapsed = start.elapsed();
    let u1 = usage();
    report(
        &args,
        Report {
            elapsed,
            lat,
            u0,
            u1,
            sent_rate: 0.0,
        },
    );
    cli.disconnect(None).unwrap();
}

fn run_cb(args: Args) {
    assert_eq!(args.scenario, "sub");
    let cli = mqtt::AsyncClient::new(create_opts(&args)).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = std::sync::mpsc::channel::<Instant>();
    let n = args.n;
    {
        let count = count.clone();
        cli.set_message_callback(move |_, msg| {
            if msg.is_some() && count.fetch_add(1, Ordering::Relaxed) + 1 == n {
                let _ = done_tx.send(Instant::now());
            }
        });
    }
    cli.connect(conn_opts(&args)).wait().unwrap();
    let t = topic(&args);
    cli.subscribe(t.as_str(), mqtt::QoS::QoS0).wait().unwrap();
    let (u0, start) = (usage(), Instant::now());
    let end = done_rx.recv().unwrap();
    let u1 = usage();
    report(
        &args,
        Report {
            elapsed: end - start,
            lat: vec![],
            u0,
            u1,
            sent_rate: 0.0,
        },
    );
    cli.disconnect(None).wait().unwrap();
}

// ---------------------------------------------------------------------------
// Async (.await) scenarios

async fn run_await(args: Args) {
    let mut cli = mqtt::AsyncClient::new(create_opts(&args)).unwrap();
    cli.connect(conn_opts(&args)).await.unwrap();
    let payload = Bytes::from(vec![b'x'; args.size]);
    let t = topic(&args);
    let qos = qos_of(&args.scenario);
    let n = args.n;
    let mut lat = Vec::new();

    let (u0, start) = match args.scenario.as_str() {
        s if s.ends_with("-seq") => {
            lat.reserve(n);
            let (u0, start) = (usage(), Instant::now());
            for _ in 0..n {
                let t0 = Instant::now();
                cli.publish(mqtt::Message::new(t.as_str(), payload.clone(), qos))
                    .await
                    .unwrap();
                lat.push(t0.elapsed());
            }
            (u0, start)
        }
        s if s.ends_with("-pipe") => {
            let (u0, start) = (usage(), Instant::now());
            let mut win = VecDeque::new();
            for _ in 0..n {
                win.push_back(cli.publish(mqtt::Message::new(t.as_str(), payload.clone(), qos)));
                if args.window > 0 && win.len() >= args.window {
                    win.pop_front().unwrap().await.unwrap();
                }
            }
            for tok in win {
                tok.await.unwrap();
            }
            (u0, start)
        }
        "sub" => {
            let rx = cli.get_stream(None);
            cli.subscribe(t.as_str(), mqtt::QoS::QoS0).await.unwrap();
            let (u0, start) = (usage(), Instant::now());
            for _ in 0..n {
                rx.recv().await.unwrap().unwrap();
            }
            (u0, start)
        }
        "rtt" => {
            let rx = cli.get_stream(None);
            cli.subscribe(t.as_str(), mqtt::QoS::QoS0).await.unwrap();
            lat.reserve(n);
            let (u0, start) = (usage(), Instant::now());
            for _ in 0..n {
                let t0 = Instant::now();
                let _ = cli.publish(mqtt::Message::new(
                    t.as_str(),
                    payload.clone(),
                    mqtt::QoS::QoS0,
                ));
                rx.recv().await.unwrap().unwrap();
                lat.push(t0.elapsed());
            }
            (u0, start)
        }
        s => panic!("scenario {s} not supported in await mode"),
    };
    let elapsed = start.elapsed();
    let u1 = usage();
    report(
        &args,
        Report {
            elapsed,
            lat,
            u0,
            u1,
            sent_rate: 0.0,
        },
    );
    cli.disconnect(None).await.unwrap();
}

#[cfg(feature = "tokio")]
fn block_on_app<F: Future<Output = ()> + Send + 'static>(f: F) {
    let threads = env::var("APP_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap()
        .block_on(f);
}

#[cfg(feature = "smol")]
fn block_on_app<F: Future<Output = ()> + Send + 'static>(f: F) {
    smol::block_on(f);
}

// ---------------------------------------------------------------------------
// Null broker

fn null_broker(port: u16) {
    let blast_n: usize = env::var("BLAST_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let blast_size: usize = env::var("BLAST_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    let l = TcpListener::bind(("127.0.0.1", port)).unwrap();
    eprintln!("nullbroker listening on {port}");
    for s in l.incoming() {
        let s = s.unwrap();
        std::thread::spawn(move || {
            let _ = serve_conn(s, blast_n, blast_size);
        });
    }
}

fn serve_conn(mut s: TcpStream, blast_n: usize, blast_size: usize) -> std::io::Result<()> {
    s.set_nodelay(true)?;
    let mut buf = BytesMut::with_capacity(256 * 1024);
    let mut tmp = vec![0u8; 256 * 1024];
    let mut out = BytesMut::with_capacity(64 * 1024);
    let mut ver = MqttVersion::V3_1_1;
    let mut subscribed: Option<String> = None;
    let ack_batch: usize = env::var("ACK_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let mut held = BytesMut::new();
    let mut held_n = 0usize;
    loop {
        let n = s.read(&mut tmp)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        while let Some(raw) = RawPacket::decode(&mut buf).map_err(std::io::Error::other)? {
            // CONNECT carries its own version; decode with V5 tolerance
            let pkt = match Packet::from_raw(raw.clone(), ver) {
                Ok(p) => p,
                Err(_) => Packet::from_raw(raw, MqttVersion::V5).map_err(std::io::Error::other)?,
            };
            match pkt {
                Packet::Connect(c) => {
                    ver = c.version;
                    Packet::ConnAck(codec::ConnAck::new(false, codec::ReasonCode::Success))
                        .encode(&mut out, ver)
                        .unwrap();
                }
                Packet::Publish(p) => {
                    match p.qos {
                        QoS::QoS1 => {
                            Packet::PubAck(codec::PubAck::new(p.packet_id.unwrap()))
                                .encode(&mut held, ver)
                                .unwrap();
                            held_n += 1;
                            if held_n >= ack_batch {
                                out.extend_from_slice(&held);
                                held.clear();
                                held_n = 0;
                            }
                        }
                        QoS::QoS2 => Packet::PubRec(codec::PubAck::new(p.packet_id.unwrap()))
                            .encode(&mut out, ver)
                            .unwrap(),
                        QoS::QoS0 => {}
                    }
                    if subscribed.is_some() && blast_n == 0 {
                        let echo = codec::Publish {
                            dup: false,
                            qos: QoS::QoS0,
                            retain: false,
                            topic: p.topic,
                            packet_id: None,
                            payload: p.payload,
                            properties: Default::default(),
                        };
                        Packet::Publish(echo).encode(&mut out, ver).unwrap();
                    }
                }
                Packet::PubRel(r) => Packet::PubComp(codec::PubAck::new(r.packet_id))
                    .encode(&mut out, ver)
                    .unwrap(),
                Packet::Subscribe(sub) => {
                    let codes = vec![codec::ReasonCode::Success; sub.subscriptions.len()];
                    Packet::SubAck(codec::SubAck::new(sub.packet_id, codes))
                        .encode(&mut out, ver)
                        .unwrap();
                    let topic = sub.subscriptions[0].topic_filter.clone();
                    subscribed = Some(topic.clone());
                    if blast_n > 0 {
                        s.write_all(&out)?;
                        out.clear();
                        let mut w = s.try_clone()?;
                        let v = ver;
                        std::thread::spawn(move || blast(&mut w, v, &topic, blast_n, blast_size));
                    }
                }
                Packet::Unsubscribe(u) => {
                    Packet::UnsubAck(codec::UnsubAck::new(u.packet_id))
                        .encode(&mut out, ver)
                        .unwrap();
                }
                Packet::PingReq => Packet::PingResp.encode(&mut out, ver).unwrap(),
                Packet::Disconnect(_) => {
                    s.write_all(&out)?;
                    return Ok(());
                }
                _ => {}
            }
        }
        if !out.is_empty() {
            s.write_all(&out)?;
            out.clear();
        }
    }
}

fn blast(w: &mut TcpStream, ver: MqttVersion, topic: &str, n: usize, size: usize) {
    let p = codec::Publish {
        dup: false,
        qos: QoS::QoS0,
        retain: false,
        topic: topic.to_string(),
        packet_id: None,
        payload: Bytes::from(vec![b'y'; size]),
        properties: Default::default(),
    };
    let mut one = BytesMut::new();
    Packet::Publish(p).encode(&mut one, ver).unwrap();
    let per = (256 * 1024 / one.len()).max(1);
    let mut chunk = BytesMut::with_capacity(per * one.len());
    for _ in 0..per {
        chunk.extend_from_slice(&one);
    }
    let mut left = n;
    while left > 0 {
        let k = left.min(per);
        if w.write_all(&chunk[..k * one.len()]).is_err() {
            return;
        }
        left -= k;
    }
}

// ---------------------------------------------------------------------------

fn main() {
    let first = env::args().nth(1).unwrap_or_default();
    if first == "nullbroker" {
        let port = env::args().nth(2).unwrap().parse().unwrap();
        null_broker(port);
        return;
    }
    let args = parse_args();
    let persist = args.persist;
    match (args.scenario.as_str(), args.mode.as_str()) {
        ("open", "wait") => run_open(args),
        ("open", _) => panic!("open runs in wait mode only"),
        (_, "wait") => run_wait(args),
        (_, "sync") => run_sync(args),
        (_, "cb") => run_cb(args),
        (_, "await") => block_on_app(run_await(args)),
        (_, m) => panic!("bad mode {m}"),
    }
    if persist {
        let dir = env::temp_dir().join(format!("mqttbench-persist-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(dir);
    }
}
