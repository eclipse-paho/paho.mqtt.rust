#!/bin/bash
#
# Builds and tests the Paho MQTT Rust v2 workspace the way the CI does, across
# the feature sets and toolchains it checks. Run before opening a pull request.
#
# Usage: ./buildtst.sh [options]
#
#   --no-msrv        Skip the MSRV toolchain (stable only)
#   --integration    Also run tests/integration.rs (needs a Mosquitto broker on
#                    localhost:1883 with a WebSocket listener on 9001)
#   --fuzz           Also run each fuzz target briefly (needs nightly + cargo-fuzz)
#   --bench          Also run the codec benchmarks (they are always compiled)
#
# The MSRV is read from Cargo.toml. Steps that need a tool or target that is
# not installed are skipped with a notice rather than failing.

set -u

NO_MSRV=0
INTEGRATION=0
FUZZ=0
BENCH=0
for arg in "$@"; do
    case "$arg" in
        --no-msrv)     NO_MSRV=1 ;;
        --integration) INTEGRATION=1 ;;
        --fuzz)        FUZZ=1 ;;
        --bench)       BENCH=1 ;;
        -h|--help)     sed -n '2,17p' "$0"; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

# Feature sets for the main crate, matching .github/workflows/rust.yml.
FEATURE_SETS=(
    "tokio"
    "tokio,tls,websocket,serde"
    "smol,smol-tls,smol-websocket,serde"
    "openssl,websocket"
)

# A bare-metal target with no std at all, to prove the codec's no_std claim.
NOSTD_TARGET="riscv32imac-unknown-none-elf"

# Extract MSRV from Cargo.toml and ensure it's the full version triplet.
get_crate_msrv() {
    MSRV=$(awk -F'"' '/^rust-version/ { print $2 }' Cargo.toml)
    local N_DOT
    N_DOT=$(echo "${MSRV}" | grep -o "\." | wc -l | xargs)
    [[ ${N_DOT} == 1 ]] && MSRV="${MSRV}".0
}

step() { printf "\n==> %s\n" "$*"; }
run()  { echo "    $*"; "$@" || { printf "\n*** FAILED: %s\n" "$*"; exit 1; }; }
skip() { printf "    (skipped: %s)\n" "$*"; }

have_toolchain() { rustup run "$1" rustc --version >/dev/null 2>&1; }

# ---------------------------------------------------------------------------

step "Format check (nightly rustfmt)"
if have_toolchain nightly; then
    run cargo +nightly fmt --all --check
else
    skip "nightly toolchain not installed"
fi

get_crate_msrv
TOOLCHAINS=(stable)
if [[ ${NO_MSRV} == 0 ]]; then
    if have_toolchain "${MSRV}"; then
        TOOLCHAINS+=("${MSRV}")
    else
        skip "MSRV toolchain ${MSRV} not installed (rustup toolchain install ${MSRV})"
    fi
fi
printf "\nToolchains: %s\n" "${TOOLCHAINS[*]}"

# ---- Main crate: check + unit tests per feature set and toolchain ----------

for VER in "${TOOLCHAINS[@]}"; do
    for FEATURES in "${FEATURE_SETS[@]}"; do
        step "Check [${FEATURES}] on ${VER}"
        run cargo "+${VER}" check --workspace --no-default-features --features "${FEATURES}" --all-targets
        step "Unit tests [${FEATURES}] on ${VER}"
        run cargo "+${VER}" test --workspace --lib --no-default-features --features "${FEATURES}" --no-fail-fast
        run cargo "+${VER}" test --workspace --doc --no-default-features --features "${FEATURES}" --no-fail-fast
    done
done

# ---- Clippy on the MSRV (or stable if MSRV is skipped) ---------------------

CLIPPY_VER="${TOOLCHAINS[${#TOOLCHAINS[@]}-1]}"
for FEATURES in "${FEATURE_SETS[@]}"; do
    step "Clippy [${FEATURES}] on ${CLIPPY_VER}"
    run cargo "+${CLIPPY_VER}" clippy --workspace --no-deps --all-targets --no-default-features --features "${FEATURES}" -- -D warnings
done

# ---- Docs ------------------------------------------------------------------

step "Docs (all features, warnings are errors)"
RUSTDOCFLAGS="-D warnings" run cargo doc --workspace --no-deps --all-features

# ---- Codec: no_std ---------------------------------------------------------

step "Codec tests without std"
run cargo test -p paho-mqtt-codec --no-default-features --no-fail-fast
run cargo test -p paho-mqtt-codec --no-default-features --features serde --no-fail-fast
run cargo clippy -p paho-mqtt-codec --no-deps --all-targets --no-default-features -- -D warnings

step "Codec build for a bare-metal target (${NOSTD_TARGET})"
if rustup target list --installed | grep -q "^${NOSTD_TARGET}$"; then
    run cargo build -p paho-mqtt-codec --no-default-features --target "${NOSTD_TARGET}"
    run cargo build -p paho-mqtt-codec --no-default-features --features serde --target "${NOSTD_TARGET}"
else
    skip "target not installed (rustup target add ${NOSTD_TARGET})"
fi

# ---- Codec: benchmarks -----------------------------------------------------

step "Codec benchmarks compile"
run cargo bench -p paho-mqtt-codec --no-run
if [[ ${BENCH} == 1 ]]; then
    step "Codec benchmarks run"
    run cargo bench -p paho-mqtt-codec --bench codec
fi

# ---- Codec: fuzz targets ---------------------------------------------------

if [[ ${FUZZ} == 1 ]]; then
    step "Codec fuzz targets (60 s each)"
    if have_toolchain nightly && command -v cargo-fuzz >/dev/null; then
        for TARGET in decode roundtrip; do
            run bash -c "cd paho-mqtt-codec && cargo +nightly fuzz run ${TARGET} -- -max_total_time=60 -max_len=4096"
        done
    else
        skip "needs nightly toolchain and cargo-fuzz (cargo install cargo-fuzz)"
    fi
fi

# ---- Integration tests -----------------------------------------------------

if [[ ${INTEGRATION} == 1 ]]; then
    step "Integration tests against localhost broker"
    for FEATURES in "tokio,websocket" "smol,smol-websocket"; do
        run cargo test --test integration --no-default-features --features "${FEATURES}" --no-fail-fast
    done
fi

printf "\n*** All builds succeeded ***\n"
