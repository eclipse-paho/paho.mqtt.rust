#!/bin/bash
#
# Runs the benchmark matrix against one or more client builds, interleaving
# the builds on every repetition so that background load affects them all
# equally. See README.md.
#
# usage: matrix.sh [-r REPS] [-s SET | -c FILE] OUT LABEL=BINARY [LABEL=BINARY ...]
#
#   -r REPS   repetitions of every configuration (default 5)
#   -s SET    quick | full | mosquitto (default full)
#   -c FILE   configurations from FILE instead, one per line:
#             <broker label> <mqttbench arguments...>, with $NB, $BL and $MQ
#             standing for the null, blast and mosquitto broker URLs
#   OUT       results file (RESULT lines, plus uptime and failures)
#
# Environment:
#   PIN_CLIENT, PIN_BROKER   CPU lists for taskset, e.g. PIN_CLIENT=0-5 PIN_BROKER=6-11
#   MQTT_URL                 mosquitto URL for SET=mosquitto (default tcp://127.0.0.1:1883)
#
# The null brokers are started from the first binary on ports 18830 (echo)
# and 18831 (blast), and stopped on exit.

set -u

REPS=5
SET=full
CFG_FILE=
while getopts "r:s:c:" opt; do
    case $opt in
        r) REPS=$OPTARG ;;
        s) SET=$OPTARG ;;
        c) CFG_FILE=$OPTARG SET=file ;;
        *) exit 2 ;;
    esac
done
shift $((OPTIND - 1))

if [ $# -lt 2 ]; then
    sed -n '6,22p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
fi

OUT=$1
shift
VARIANTS=("$@")
FIRST_BIN=${VARIANTS[0]#*=}

NB=tcp://127.0.0.1:18830
BL=tcp://127.0.0.1:18831
MQ=${MQTT_URL:-tcp://127.0.0.1:1883}

# Command prefixes that pin a process to a CPU list, or nothing.
PIN_C=()
PIN_B=()
[ -n "${PIN_CLIENT:-}" ] && PIN_C=(taskset -c "$PIN_CLIENT")
[ -n "${PIN_BROKER:-}" ] && PIN_B=(taskset -c "$PIN_BROKER")

BROKER_PIDS=()
cleanup() {
    for p in "${BROKER_PIDS[@]}"; do kill "$p" 2>/dev/null; done
}
trap cleanup EXIT

if [ "$SET" != mosquitto ]; then
    "${PIN_B[@]}" "$FIRST_BIN" nullbroker 18830 2>/dev/null &
    BROKER_PIDS+=($!)
    BLAST_N=1000000 "${PIN_B[@]}" "$FIRST_BIN" nullbroker 18831 2>/dev/null &
    BROKER_PIDS+=($!)
    sleep 0.5
fi

# One configuration per line: <broker label> <mqttbench arguments...>
configs() {
    case $SET in
    file)
        grep -v '^\s*\(#\|$\)' "$CFG_FILE" | sed "s|\$NB|$NB|g; s|\$BL|$BL|g; s|\$MQ|$MQ|g"
        ;;
    quick)
        cat <<EOF
null pub0-pipe wait --url $NB --n 500000 --size 32
null pub1-pipe wait --url $NB --n 200000 --window 1000
null pub2-pipe wait --url $NB --n 100000 --window 1000
null pub1-seq wait --url $NB --n 20000
null rtt wait --url $NB --n 20000
null open wait --url $NB --rate 10000 --n 20000
null open wait --url $NB --rate 100000 --n 200000
EOF
        ;;
    full)
        for m in wait await sync; do echo "null pub0-seq $m --url $NB --n 50000"; done
        for m in wait await; do
            echo "null pub0-pipe $m --url $NB --n 500000 --size 32"
            echo "null pub0-pipe $m --url $NB --n 200000 --size 1024"
            echo "null pub0-pipe $m --url $NB --n 20000 --size 65536"
        done
        for q in 1 2; do
            for m in wait await sync; do echo "null pub$q-seq $m --url $NB --n 20000"; done
        done
        for m in wait await; do
            echo "null pub1-pipe $m --url $NB --n 200000 --window 100"
            echo "null pub1-pipe $m --url $NB --n 200000 --window 1000"
            echo "null pub1-pipe $m --url $NB --n 200000 --window 20000"
            echo "null pub1-pipe $m --url $NB --n 200000 --window 1000 --v5"
            echo "null pub2-pipe $m --url $NB --n 100000 --window 1000"
        done
        for m in wait await cb; do echo "blast sub $m --url $BL --n 1000000"; done
        for m in wait await sync; do echo "null rtt $m --url $NB --n 20000"; done
        echo "null open wait --url $NB --rate 10000 --n 20000"
        echo "null open wait --url $NB --rate 50000 --n 100000"
        echo "null open wait --url $NB --rate 100000 --n 200000"
        echo "null open wait --url $NB --rate 250000 --n 500000"
        echo "null pub1-seq wait --url $NB --n 5000 --persist"
        ;;
    mosquitto)
        for m in wait await; do
            echo "mosquitto pub0-pipe $m --url $MQ --n 200000"
            echo "mosquitto pub1-pipe $m --url $MQ --n 100000 --window 1000"
            echo "mosquitto pub1-pipe $m --url $MQ --n 100000 --window 1000 --v5"
        done
        for m in wait await sync; do
            echo "mosquitto pub1-seq $m --url $MQ --n 10000"
            echo "mosquitto rtt $m --url $MQ --n 10000"
        done
        ;;
    *)
        echo "unknown set: $SET" >&2
        exit 2
        ;;
    esac
}

: >"$OUT"
echo "# set=$SET reps=$REPS variants=${VARIANTS[*]}" >>"$OUT"
echo "# uptime-start $(uptime)" >>"$OUT"

mapfile -t CONFIGS < <(configs)
for ((r = 1; r <= REPS; r++)); do
    for cfg in "${CONFIGS[@]}"; do
        read -r broker args <<<"$cfg"
        for v in "${VARIANTS[@]}"; do
            label=${v%%=*}
            bin=${v#*=}
            # shellcheck disable=SC2086
            timeout 120 "${PIN_C[@]}" "$bin" $args 2>/dev/null |
                sed "s|^RESULT|RESULT variant=$label broker=$broker rep=$r|" >>"$OUT"
            # A run that crashed or timed out printed no RESULT line.
            if ! tail -n1 "$OUT" | grep -q "^RESULT variant=$label broker=$broker rep=$r "; then
                echo "FAIL variant=$label rep=$r $args" >>"$OUT"
            fi
        done
    done
    echo "rep $r/$REPS done" >&2
done

echo "# uptime-end $(uptime)" >>"$OUT"
echo "# DONE" >>"$OUT"
