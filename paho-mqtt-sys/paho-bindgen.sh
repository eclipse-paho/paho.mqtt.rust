#!/bin/bash
#
# Generates the Paho C bindings for the current host.

if [ -z "$1" ]; then
	if [ -f paho.mqtt.c/version.major ]; then
		MAJOR=$(<paho.mqtt.c/version.major)
		MINOR=$(<paho.mqtt.c/version.minor)
		PATCH=$(<paho.mqtt.c/version.patch)
		VERSION="${MAJOR}.${MINOR}.${PATCH}"
	else
		printf "USAGE: paho-bindgen.sh [paho-c-version]\n"
		printf "  Can't locate Paho C to automatically detect version\n"
		exit 1
	fi
else
	VERSION="$1"
fi

if [ -z "${VERSION}" ]; then
    printf "Unable to determine the Paho C version.\n"
    exit 1
fi

HOST_TRIPLE="$(rustc -vV | awk '/^host/ { print $2 }')"

if [ -z "${HOST_TRIPLE}" ]; then
    printf "Unable to determine host target.\n"
    exit 1
fi

printf "Generating bindings for: Paho C v%s, on %s\n" "${VERSION}" "${HOST_TRIPLE}"

# Note: It would be really nice to have the doc comments from the C lib, 
#     but some are odd and cause warnings when generating Rust docs.

bindgen --rust-target 1.75.0 --no-doc-comments wrapper.h -- -I./paho.mqtt.c/src \
    > bindings/bindings_paho_mqtt_c_${VERSION}-${HOST_TRIPLE}.rs
