// c_trace.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2023 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Module for passing Paho C trace statements to the Rust log facility.
//!

use crate::ffi;
use std::{ffi::CStr, os::raw::c_char};

/// The log target (module) for the Paho C trace logs
const PAHO_C_LOG_TARGET: &str = "paho_mqtt_c";

// Low-level callback from the C library for log/trace messages.
// We send the traces to the Rust logger, approximating the log level from
// the C library, which has more levels than the Rust logger.
// This should be installed by the client once when the first one is
// created.
pub(crate) unsafe extern "C" fn on_c_trace(
    lvl: ffi::MQTTASYNC_TRACE_LEVELS,
    msg: *mut c_char,
) {
    if msg.is_null() {
        return;
    }

    let cmsg = unsafe { CStr::from_ptr(msg) };
    let msg = match cmsg.to_str() {
        Ok(s) => s,
        Err(_) => return,
    };

    let lvl = log_from_c_trace_level(lvl);
    log!(target: PAHO_C_LOG_TARGET, lvl, "{}", msg);
}

/// Converts a Paho C trace level into a Rust log level.
pub fn log_from_c_trace_level(level: ffi::MQTTASYNC_TRACE_LEVELS) -> log::Level {
    match level {
        ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_FATAL
        | ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_SEVERE => log::Level::Error,

        ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_ERROR => log::Level::Warn,

        ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_PROTOCOL
        | ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_MINIMUM
        | ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_MEDIUM => log::Level::Debug,

        _ => log::Level::Trace,
    }
}

/// Converts a Rust log level into a Paho C trace level.
/// This gives the most verbose C trace level for the log level.
pub fn log_into_c_trace_level(level: log::Level) -> ffi::MQTTASYNC_TRACE_LEVELS {
    use log::Level::*;
    match level {
        Error => ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_SEVERE,
        Warn => ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_ERROR,
        Info | Debug => ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_MEDIUM,
        Trace => ffi::MQTTASYNC_TRACE_LEVELS_MQTTASYNC_TRACE_MAXIMUM,
    }
}

/// Gets the trace level, if any, to set the Paho C library
pub fn c_trace_level() -> Option<ffi::MQTTASYNC_TRACE_LEVELS> {
    use log::Level::*;
    if log_enabled!(target: PAHO_C_LOG_TARGET, Trace) {
        Some(log_into_c_trace_level(Trace))
    }
    else if log_enabled!(target: PAHO_C_LOG_TARGET, Debug) {
        Some(log_into_c_trace_level(Debug))
    }
    else if log_enabled!(target: PAHO_C_LOG_TARGET, Info) {
        Some(log_into_c_trace_level(Info))
    }
    else if log_enabled!(target: PAHO_C_LOG_TARGET, Warn) {
        Some(log_into_c_trace_level(Warn))
    }
    else if log_enabled!(target: PAHO_C_LOG_TARGET, Error) {
        Some(log_into_c_trace_level(Error))
    }
    else {
        None
    }
}
