// paho-mqtt/src/types.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2019-2026 Frank Pagliughi <fpagliughi@mindspring.com>
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Frank Pagliughi - initial implementation and documentation
 *******************************************************************************/

//! MQTT support types.

pub use paho_mqtt_codec::{
    MQTT_VERSION_3_1, MQTT_VERSION_3_1_1, MQTT_VERSION_5, MQTT_VERSION_DEFAULT,
    MqttVersion, QOS_0, QOS_1, QOS_2, QoS,
};
