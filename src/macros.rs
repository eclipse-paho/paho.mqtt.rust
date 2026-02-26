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

/// Return an error from a function.
macro_rules! bail {
    ($expr:expr) => {
        return Err(::std::convert::From::from($expr));
    };
}

/// Creates a collection of properties
#[macro_export]
macro_rules! properties(
    { $($key:expr => $value:expr),* } => {
        {
            #[allow(unused_mut)]
            let mut p = $crate::properties::Properties::new();
            $( let _ = p.push_val($key, $value); )*
            p
        }
    };
    { $($key:expr => $value:expr),+ ,} => {
        properties![ $( $key => $value ),* ]
    };
);
