// macros.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// This contains the macro definitions for the crate.
//

/*******************************************************************************
 * Copyright (c) 2017-2022 Frank Pagliughi <fpagliughi@mindspring.com>
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

#![macro_use]

/// Creates a collection of properties.
///
/// Each entry is a [`Property`](crate::Property), and the collection is
/// built with [`Properties::push`](crate::Properties::push):
///
/// ```
/// use paho_mqtt::{Property, properties};
///
/// let props = properties![
///     Property::MessageExpiryInterval(60),
///     Property::ContentType("application/json".into()),
/// ];
/// assert_eq!(props.len(), 2);
/// ```
///
/// # Migrating from v0.14
///
/// The `id => value` form taken by this macro in v0.14 is gone. A
/// [`Property`](crate::Property) now carries its value in the variant, so
/// the identifier itself fixes the type and there is nothing for the macro
/// to look up: `Property::MessageExpiryInterval(60)` is a `u32` because the
/// identifier says so, where v0.14 asked the identifier for its wire width
/// at run time and could fail. That form now produces a `compile_error!`
/// naming the replacement rather than a bare "no rules expected `=>`".
///
/// Either name the variant, as above, or use the per-identifier setter:
///
/// ```
/// use paho_mqtt::Properties;
///
/// let mut props = Properties::new();
/// props.push_message_expiry_interval(60);
/// props.push_content_type("application/json");
/// ```
#[macro_export]
macro_rules! properties {
    () => (
        $crate::Properties::new()
    );
    // The v0.14 spelling. This arm must come first: `=>` is a legal follower
    // for an `expr` fragment, so it is matched here deliberately in order to
    // report something useful instead of letting the entry arm fail.
    ($($key:expr => $val:expr),+ $(,)?) => {
        compile_error!(
            "the `properties!{ id => value }` syntax of paho-mqtt v0.14 is no longer supported. \
             A property now carries its value in the variant, so write \
             `properties![Property::MessageExpiryInterval(60)]`, or build the collection with \
             the per-identifier setters, as in `props.push_message_expiry_interval(60)`."
        )
    };
    ($($x:expr),+ $(,)?) => {
        {
            let mut _props = $crate::Properties::new();
            $(_props.push($x);)+
            _props
        }
    };
}
