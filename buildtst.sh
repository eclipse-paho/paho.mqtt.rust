#!/bin/bash
#
# This tests building the Paho MQTT Rust crate with various features enabled and
# disabled. This should be run before any pull request of new fetures.

# Extract MSRV from Cargo.toml and ensure it's the full version triplet
# The value is stored in the variable `MSRV`
get_crate_msrv() {
    MSRV=$(awk '/rust-version/ { print substr($3, 2, length($3)-2) }' Cargo.toml)
    local N_DOT
    N_DOT=$(echo "${MSRV}" | grep -o "\." | wc -l | xargs)
    [[ ${N_DOT} == 1 ]] && MSRV="${MSRV}".0
}

printf "\nFormat check...\n"
! cargo +nightly fmt --check --all && exit 1
printf "    Ok\n"

get_crate_msrv
printf "\nUsing MSRV %s\n" "${MSRV}"

FEATURES="bundled bundled,build_bindgen bundled,ssl,vendored-ssl"

for VER in stable ${MSRV} ; do
    printf "\n\nChecking default features for version: %s...\n" "${VER}"
    cargo clean && \
        cargo +${VER} check && \
        cargo +${VER} test
    [ "$?" -ne 0 ] && exit 1

    for FEATURE in ${FEATURES}; do
        printf "\n\nBuilding with feature(s) [%s] for version: %s...\n" "${FEATURE}" "${VER}"
        cargo clean && \
            cargo +${VER} check --no-default-features --features="$FEATURE" && \
            cargo +${VER} test --no-default-features --features="$FEATURE"
        [ "$?" -ne 0 ] && exit 1
    done
done

printf "\nCreating docs for version: %s...\n" "${MSRV}"
cargo clean
! cargo +"${MSRV}" doc --no-deps --all-features && exit 1
printf "    Ok\n"

printf "\nChecking clippy for version: %s...\n" "${MSRV}"
cargo clean
! cargo +"${MSRV}" clippy && exit 1

cargo clean
printf "\n\n*** All builds succeeded ***\n"
