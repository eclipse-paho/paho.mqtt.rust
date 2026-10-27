// subscribe_options.rs
//
// The set of options for responses coming back to an MQTT client.
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2019-2022 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! MQTT v5 subscribe options for the Paho MQTT Rust client library.
//! These are defined in section 3.8.3.1 of the MQTT v5 spec.
//! The defaults use the behavior that was present in MQTT v3.1.1.

use crate::{Error, Result};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, fmt};

/// Receive our own publications when subscribed to the same topics.
/// This is the default and the same behavior as MQTT v3.x
pub const SUBSCRIBE_LOCAL: bool = false;
/// Don't receive our own publications when subscribed to the same topics.
pub const SUBSCRIBE_NO_LOCAL: bool = true;

/// Retain flag is only set on publications sent by a broker if in
/// response to a subscribe request.
/// This is the default and the same behavior as MQTT v3.x
pub const NO_RETAIN_AS_PUBLISHED: bool = false;
/// Keep the retain flag as on the original publish message
pub const RETAIN_AS_PUBLISHED: bool = true;

/// The options for subscription retain handling.
#[repr(u8)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RetainHandling {
    /// Send retained messages at the time of the subscribe
    /// This is the default and the same behavior as MQTT v3.x
    #[default]
    SendRetainedOnSubscribe = 0,
    /// Send retained messages on subscribe only if subscription is new
    SendRetainedOnNew = 1,
    /// Do not send retained messages at all
    DontSendRetained = 2,
}

impl fmt::Display for RetainHandling {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RetainHandling::*;
        match *self {
            SendRetainedOnSubscribe => write!(f, "Send Retain on Subscribe"),
            SendRetainedOnNew => write!(f, "Send Retain on New"),
            DontSendRetained => write!(f, "Don't Send Retain"),
        }
    }
}

impl TryFrom<i32> for RetainHandling {
    type Error = Error;

    fn try_from(val: i32) -> Result<Self> {
        use RetainHandling::*;
        match val {
            0 => Ok(SendRetainedOnSubscribe),
            1 => Ok(SendRetainedOnNew),
            2 => Ok(DontSendRetained),
            _ => Err(Error::InvalidArgument(format!(
                "invalid value for RetainHandling: {val}"
            ))),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////

/// The MQTT v5 subscribe options.
///
/// Defaults for each of these keep the original v3 behavior.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[derive(Debug, Default, Copy, Clone)]
pub struct SubscribeOptions {
    /// Whether the client should receive its own published messages.
    /// Set to `true` to prevent this from happening.
    pub no_local: bool,
    /// Whether to keep the retain flag as it was on the original published message.
    pub retain_as_published: bool,
    /// The retain handling.
    /// See `RetainHandling`.
    pub retain_handling: RetainHandling,
}

impl SubscribeOptions {
    /// Creates set of subscribe options.
    pub fn new<H>(no_local: bool, retain_as_published: bool, retain_handling: H) -> Self
    where
        H: Into<Option<RetainHandling>>,
    {
        SubscribeOptions {
            no_local,
            retain_as_published,
            retain_handling: retain_handling.into().unwrap_or_default(),
        }
    }

    /// Creates set of subscribe options with NO_LOCAL set.
    pub fn with_no_local() -> Self {
        SubscribeOptions {
            no_local: true,
            ..SubscribeOptions::default()
        }
    }

    /// Creates set of subscribe options with RETAIN_AS_PUBLISHED set.
    pub fn with_retain_as_published() -> Self {
        SubscribeOptions {
            retain_as_published: true,
            ..SubscribeOptions::default()
        }
    }

    /// Creates set of subscribe options with retain handling set.
    pub fn with_retain_handling(retain_handling: RetainHandling) -> Self {
        SubscribeOptions {
            retain_handling,
            ..SubscribeOptions::default()
        }
    }

    /// Gets whether the client asks not to be sent its own publications.
    ///
    /// The fields of this struct are public, so new code can simply read
    /// [`no_local`](Self::no_local). The accessors are kept for
    /// compatibility with the v1.x API.
    pub fn no_local(&self) -> bool {
        self.no_local
    }

    /// Gets whether the retain flag is kept as the publisher set it.
    pub fn retain_as_published(&self) -> bool {
        self.retain_as_published
    }

    /// Gets when retained messages are sent for a new subscription.
    pub fn retain_handling(&self) -> RetainHandling {
        self.retain_handling
    }
}

// TODO Does any of this still make sense?!?!

impl From<bool> for SubscribeOptions {
    fn from(no_local: bool) -> Self {
        SubscribeOptions::new(no_local, false, None)
    }
}

impl From<Option<bool>> for SubscribeOptions {
    fn from(no_local: Option<bool>) -> Self {
        SubscribeOptions::new(no_local.unwrap_or_default(), false, None)
    }
}

impl From<(bool, bool)> for SubscribeOptions {
    fn from((no_local, retain_as_published): (bool, bool)) -> Self {
        SubscribeOptions::new(no_local, retain_as_published, None)
    }
}

impl From<(bool, bool, RetainHandling)> for SubscribeOptions {
    fn from(
        (no_local, retain_as_published, retain_handling): (bool, bool, RetainHandling),
    ) -> Self {
        SubscribeOptions::new(no_local, retain_as_published, retain_handling)
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Builder
/////////////////////////////////////////////////////////////////////////////

/// Builder for creating subscription options.
#[derive(Debug, Default, Clone, Copy)]
pub struct SubscribeOptionsBuilder(SubscribeOptions);

impl SubscribeOptionsBuilder {
    /// Create a new `SubscribeOptionsBuilder`
    pub fn new() -> Self {
        SubscribeOptionsBuilder::default()
    }

    /// Set so that the client doesn't receive its own messages that it
    /// publishes on the topic.
    pub fn no_local(&mut self, on: bool) -> &mut Self {
        self.0.no_local = on;
        self
    }

    /// Set to keep the retain flag as on the original published message.
    /// If not set, the original MQTT behavior is where the retain flag is
    /// only set on publications sent by a broker if in response to a
    /// subscribe request.
    pub fn retain_as_published(&mut self, retain: bool) -> &mut Self {
        self.0.retain_as_published = retain;
        self
    }

    /// Sets how retained messages are handled.
    pub fn retain_handling(&mut self, handling: RetainHandling) -> &mut Self {
        self.0.retain_handling = handling;
        self
    }

    /// Finalizes the builder to create the subscribe options.
    pub fn finalize(&self) -> SubscribeOptions {
        self.0
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retain_handling_from_int() {
        assert_eq!(
            RetainHandling::try_from(2).unwrap(),
            RetainHandling::DontSendRetained
        );
        assert!(matches!(
            RetainHandling::try_from(3),
            Err(Error::InvalidArgument(msg)) if msg.contains('3')
        ));
    }

    #[test]
    fn test_with() {
        let opts = SubscribeOptions::with_no_local();

        assert!(opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());

        let opts = SubscribeOptions::with_retain_as_published();

        assert!(!opts.no_local);
        assert!(opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());

        let opts = SubscribeOptions::with_retain_handling(RetainHandling::SendRetainedOnNew);

        assert!(!opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::SendRetainedOnNew);
    }

    #[test]
    fn test_from_tuple_one() {
        let opts = SubscribeOptions::from(true);

        assert!(opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());
    }

    #[test]
    fn test_from_tuple_two() {
        let opts = SubscribeOptions::from((true, true));

        assert!(opts.no_local);
        assert!(opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());
    }

    #[test]
    fn test_from_tuple_three() {
        let opts = SubscribeOptions::from((true, true, RetainHandling::SendRetainedOnNew));

        assert!(opts.no_local);
        assert!(opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::SendRetainedOnNew);
    }

    #[test]
    fn test_builder_default() {
        let opts = SubscribeOptionsBuilder::new().finalize();

        assert!(!opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());
    }

    #[test]
    fn test_builder_no_local() {
        let opts = SubscribeOptionsBuilder::new().no_local(true).finalize();

        assert!(opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());
    }

    #[test]
    fn test_builder_retain_as_published() {
        let opts = SubscribeOptionsBuilder::new()
            .retain_as_published(true)
            .finalize();

        assert!(!opts.no_local);
        assert!(opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::default());
    }

    #[test]
    fn test_builder_retain_handling() {
        let opts = SubscribeOptionsBuilder::new()
            .retain_handling(RetainHandling::DontSendRetained)
            .finalize();

        assert!(!opts.no_local);
        assert!(!opts.retain_as_published);
        assert_eq!(opts.retain_handling, RetainHandling::DontSendRetained);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_round_trip() {
        let opts = SubscribeOptions::new(true, true, RetainHandling::DontSendRetained);
        let back: SubscribeOptions =
            serde_json::from_str(&serde_json::to_string(&opts).unwrap()).unwrap();
        assert!(back.no_local);
        assert!(back.retain_as_published);
        assert_eq!(back.retain_handling, RetainHandling::DontSendRetained);

        let empty: SubscribeOptions = serde_json::from_str("{}").unwrap();
        assert!(!empty.no_local);
    }

    // The accessors removed when the fields became public.
    #[test]
    fn test_accessors() {
        let opts = SubscribeOptionsBuilder::new()
            .no_local(true)
            .retain_as_published(true)
            .retain_handling(RetainHandling::DontSendRetained)
            .finalize();
        assert!(opts.no_local());
        assert!(opts.retain_as_published());
        assert_eq!(RetainHandling::DontSendRetained, opts.retain_handling());

        let opts = SubscribeOptions::default();
        assert!(!opts.no_local());
        assert!(!opts.retain_as_published());
        assert_eq!(RetainHandling::default(), opts.retain_handling());
    }
}
