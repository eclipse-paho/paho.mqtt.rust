# Benchmarks

A harness for measuring the client's throughput, latency and CPU cost, used to decide and verify the performance work in `V2_PLAN.md` (R7). The numbers it gives are the client's ceiling: the default broker is a null broker that acknowledges instantly, run as a separate process so that `getrusage` measures the client alone.

## Layout

| Path | What |
|---|---|
| `mqttbench/` | The harness: the benchmark client, and the null broker (`mqttbench nullbroker`) |
| `matrix.sh` | Runs a list of configurations against one or more client builds, interleaved |
| `summarize.py` | Medians per configuration, and before/after ratios |
| `results/` | Saved result files, e.g. `baseline-<commit>.txt` |

`mqttbench` is not a workspace member: it is built once per backend, and enabling `tokio` and `smol` together is a compile error.

## Building

```sh
cd bench/mqttbench
cargo build --release --target-dir target/tokio
cargo build --release --no-default-features --features smol --target-dir target/smol
```

The release profile keeps debug info, for profiling.

To compare against an older commit, build that commit in a worktree, with its own target directory, and keep the binary:

```sh
git worktree add /tmp/paho-base <commit>
cd /tmp/paho-base/bench/mqttbench && cargo build --release --target-dir target/tokio
```

(The harness was added after commit `7e0bf97`; for a commit that old, copy `bench/` into the worktree first.)

## Running one benchmark

```sh
mqttbench nullbroker 18830 &
mqttbench <scenario> <mode> --url tcp://127.0.0.1:18830 [options]
```

| Scenario | What it measures |
|---|---|
| `pub0-seq`, `pub1-seq`, `pub2-seq` | Publish one message, wait for its token, repeat: per-message latency |
| `pub0-pipe`, `pub1-pipe`, `pub2-pipe` | Publish with up to `--window` tokens outstanding (0 = all): throughput |
| `sub` | Receive `--n` messages that the broker floods (`BLAST_N`; see below) |
| `sub-stall` | Subscribe with a bounded stream of `--n` and never read it, for `--secs`; reports the resident memory the client is left holding rather than a rate |
| `rtt` | Publish QoS 0 to a topic we subscribe to, wait for it to come back |
| `open` | Open loop: QoS 1 published at `--rate` msg/s regardless of completions; latency from each message's *scheduled* time to its PUBACK |

| Mode | How the application waits |
|---|---|
| `wait` | Plain threads, `Token::wait()` |
| `await` | An application runtime (tokio multi-thread with `APP_THREADS` workers, default 2; or `smol::block_on`), `.await` |
| `sync` | The synchronous `Client` |
| `cb` | A message callback (`sub` only) |

`open` and `sub-stall` run in `wait` mode only. Options: `--url`, `--n` (messages), `--size` (payload bytes, default 32), `--window`, `--rate`, `--v5`, `--persist` (file persistence in a temporary directory), `--topic` (overrides the per-process topic, so two runs can share one — `sub-stall` needs it to be fed by a separate publisher), `--secs` (`sub-stall` only).

`sub-stall` prints a `STALL backend=… rss_before_kb=… rss_kb=… held_kb=…` line instead of a `RESULT`. It exists for the question R7.3.4 had to answer: a payload is a slice of the buffer it arrived in, so a message waiting for a consumer that has stopped reading keeps its whole read chunk alive, and a bigger chunk could cost more memory per queued message. It does not: the buffer is filled across however many reads it takes, so what is pinned follows the bytes received, not the number of messages.

The closed-loop scenarios wait on their own results, so they cannot show queueing delay under load; `open` can. Its pacing thread spins, so its CPU figures include one busy core.

Null broker environment:

- `BLAST_N`, `BLAST_SIZE`: on SUBSCRIBE, flood that many QoS 0 messages of that size (for `sub`).
- `ACK_BATCH=K`: hold PUBACKs until K are queued, to force a deep in-flight window.

### Output

Each run prints one line:

```
RESULT backend=tokio scenario=pub1-seq mode=wait v5=false persist=false size=32 n=20000 window=0 target_rate=0 sent_rate=0 secs=0.66 rate=30190 MBps=0.97 cpu_us_per_msg=30.45 cpu_util=0.92 vcsw_per_msg=2.460 ivcsw=12 lat_mean_us=33.0 lat_p50_us=29.3 lat_p99_us=79.4 lat_p999_us=529.4 lat_max_us=1104.0
```

- `rate`: messages per second over the whole run.
- `cpu_us_per_msg`, `cpu_util`: user + system CPU of the client process, per message and per second of wall time.
- `vcsw_per_msg`: voluntary context switches per message, a proxy for thread hops and wakeups. `ivcsw`: involuntary ones, in total, a proxy for interference from other load.
- `lat_*`: per-message latency, for the `-seq`, `rtt` and `open` scenarios.
- `sent_rate`: for `open`, the rate the publisher actually achieved.

## Running the matrix

```sh
cd bench
PIN_CLIENT=0-5 PIN_BROKER=6-11 ./matrix.sh -r 5 -s full results/new.txt \
    base-tokio=path/to/base/mqttbench new-tokio=mqttbench/target/tokio/release/mqttbench
./summarize.py results/new.txt --compare base-tokio:new-tokio
```

`matrix.sh` starts the null brokers itself (from the first binary, on ports 18830 and 18831) and stops them on exit. Sets: `quick` (seven configurations, for a fast check), `full`, and `mosquitto` (against a real broker at `MQTT_URL`, default `tcp://127.0.0.1:1883`, which you start yourself). `-c FILE` takes the configurations from a file instead, one per line as `<broker label> <mqttbench arguments...>`, with `$NB`, `$BL` and `$MQ` standing for the broker URLs. A run that crashes or times out is recorded as a `FAIL` line.

`summarize.py` prints the median of each metric per configuration and variant; with `--compare BASE:NEW` it adds a table of NEW's medians as ratios of BASE's (above 1 means higher: better for `rate`, worse for everything else).

## Protocol

- **Interleave the variants.** `matrix.sh` runs every variant for each configuration before moving on, every repetition. Never run all of A and then all of B: background load drifts, and the first matrix of the 2026-10-11 analysis was skewed 3× by it.
- **Five repetitions, medians.** `summarize.py` takes medians, not means.
- **Watch the load.** The results file records `uptime` at the start and end. `summarize.py` discards runs whose involuntary context switches exceed 1% of `n` (`--keep-noisy` keeps them); `open` runs are exempt, since their pacing thread spins and is preempted by design.
- **Pin, if you can.** `PIN_CLIENT` / `PIN_BROKER` take CPU lists for `taskset`, keeping client and broker on disjoint cores.
- **Save the baseline** for a piece of work as `results/baseline-<commit>.txt`, measured at the commit the work starts from, and compare against it.

Other tools:

- When the machine is too busy for timings to settle (runs of the same binary differing by more than the effect being measured), compare work instead of time: instructions per message from callgrind, and syscalls per message from strace. Both are independent of load.

- Syscalls per message: `strace -f -c -o trace.txt mqttbench pub0-pipe wait --n 100000 ...`, then divide the counts by `n`.
- User-space profiles: `valgrind --tool=callgrind mqttbench ...`, then `callgrind_annotate`. (`perf` is unavailable on a machine with `perf_event_paranoid` above 2.)
