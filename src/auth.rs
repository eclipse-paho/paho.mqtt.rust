// auth.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2026 Frank Pagliughi <fpagliughi@mindspring.com>
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
//! MQTT v5 enhanced authentication.
//!
//! Beyond a user name and password, MQTT v5 can carry a challenge/response
//! exchange — SASL mechanisms such as SCRAM, a Kerberos ticket, an OAuth
//! token — between CONNECT and CONNACK (§4.12). The client names a method in
//! its CONNECT properties and supplies an [`AuthHandler`]; the broker replies
//! with AUTH packets, each of which the handler answers, until the broker
//! accepts or refuses the connection.

use std::{fmt, sync::Arc};

use crate::{Properties, Property, PropertyId, Result};

/// One step of an authentication exchange, as sent by the broker.
///
/// The data is method-specific and opaque to this library.
#[derive(Debug)]
pub struct AuthChallenge<'a> {
    /// The authentication method named in the CONNECT properties, echoed by
    /// the broker (for example `"SCRAM-SHA-256"`).
    pub method: &'a str,
    /// The broker's authentication data. Empty when it sent none, which is
    /// normal for the first step of methods where the client speaks first.
    pub data: &'a [u8],
    /// Every property on the AUTH packet, for a method that needs more than
    /// the authentication data.
    pub props: &'a Properties,
}

/// Answers an [`AuthChallenge`] with the client's next authentication data.
///
/// Returning `Err` abandons the connection attempt with that error; the
/// broker is told with a DISCONNECT carrying `BadAuthenticationMethod`.
///
/// Called on the client's internal event-loop task, so it must not block.
pub type AuthCallback = dyn Fn(&AuthChallenge<'_>) -> Result<Vec<u8>> + Send + Sync + 'static;

/// A handler for an MQTT v5 enhanced-authentication exchange.
///
/// Set with
/// [`ConnectOptionsBuilder::auth_handler`](crate::ConnectOptionsBuilder::auth_handler),
/// alongside an `AuthenticationMethod` property on the same options — one
/// without the other is rejected before any connection is attempted.
///
/// # Example
///
/// ```no_run
/// use paho_mqtt as mqtt;
/// use mqtt::Property;
///
/// let mut props = mqtt::Properties::new();
/// props.push(Property::AuthenticationMethod("SCRAM-SHA-256".into()));
///
/// let opts = mqtt::ConnectOptionsBuilder::new_v5()
///     .properties(props)
///     .auth_handler(|challenge| {
///         // Answer the broker's data with the method's next message.
///         Ok(scram_step(challenge.data))
///     })
///     .finalize();
/// # fn scram_step(_: &[u8]) -> Vec<u8> { Vec::new() }
/// ```
#[derive(Clone)]
pub struct AuthHandler(Arc<AuthCallback>);

impl AuthHandler {
    /// Wraps `f` as an authentication handler.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&AuthChallenge<'_>) -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Runs the handler for one challenge.
    pub(crate) fn respond(&self, challenge: &AuthChallenge<'_>) -> Result<Vec<u8>> {
        (self.0)(challenge)
    }
}

impl fmt::Debug for AuthHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthHandler(..)")
    }
}

impl<F> From<F> for AuthHandler
where
    F: Fn(&AuthChallenge<'_>) -> Result<Vec<u8>> + Send + Sync + 'static,
{
    fn from(f: F) -> Self {
        Self::new(f)
    }
}

/// The `AuthenticationMethod` named in a set of CONNECT properties, if any.
pub(crate) fn auth_method(props: &Properties) -> Option<&str> {
    match props.get(PropertyId::AuthenticationMethod) {
        Some(Property::AuthenticationMethod(s)) => Some(s),
        _ => None,
    }
}

/// The `AuthenticationData` in a set of properties, if any.
pub(crate) fn auth_data(props: &Properties) -> &[u8] {
    match props.get(PropertyId::AuthenticationData) {
        Some(Property::AuthenticationData(b)) => b,
        _ => &[],
    }
}
