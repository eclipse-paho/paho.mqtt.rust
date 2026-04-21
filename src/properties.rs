// paho-mqtt/src/properties.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.

/*******************************************************************************
 * Copyright (c) 2018-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! MQTT v5 properties.
//!
//! Properties are key/value pairs carried in MQTT v5 packets. The
//! [`Properties`] collection holds any number of [`Property`] values, each
//! identified by a [`PropertyId`] and typed as a [`PropertyValue`].
//!
//! On v3.1.1 connections properties are always empty.

pub use paho_mqtt_codec::{Properties, Property, PropertyId, PropertyType, PropertyValue};

/// Backward-compatible alias for [`PropertyId`].
pub type PropertyCode = PropertyId;
