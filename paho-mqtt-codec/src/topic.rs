// paho-mqtt-codec/src/topic.rs
//
// This file is part of the Eclipse Paho MQTT Rust Codec library.

/*******************************************************************************
 * Copyright (c) 2026 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! Topic name and topic filter validation (MQTT §4.7).
//!
//! A *topic name* is what a PUBLISH carries: any non-empty UTF-8 string
//! without wildcard characters. A *topic filter* is what a SUBSCRIBE or
//! UNSUBSCRIBE carries: it may contain the wildcards `+` (one level) and
//! `#` (all remaining levels), each of which must occupy a whole level, and
//! `#` must be last. A filter may also be a *shared subscription* of the
//! form `$share/{ShareName}/{filter}`.
//!
//! These checks are applied during decode and encode of the packets that
//! carry topics; they are public so an application can validate a topic
//! before building a packet, and so a server can extract a share name.

use crate::{Error, Result};

/// Prefix of a shared-subscription topic filter.
pub const SHARE_PREFIX: &str = "$share/";

/// Checks that `name` is a valid topic name (MQTT-4.7.3-1, MQTT-4.7.3-2):
/// non-empty and free of `+` and `#`.
///
/// Returns [`Error::TopicNameInvalid`] otherwise. (An empty name is legal in
/// a v5 PUBLISH that carries a topic alias; the PUBLISH decoder handles that
/// case itself.)
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.bytes().any(|b| b == b'+' || b == b'#') {
        return Err(Error::TopicNameInvalid);
    }
    Ok(())
}

/// Checks that `filter` is a valid topic filter, including the shared
/// subscription form.
///
/// Rules (MQTT-4.7.1-1, -2, -3; §4.8.2):
///
/// - non-empty;
/// - `#` may appear only as the last character, and only as a whole level
///   (`#`, `a/#`; not `a#` or `a/#/b`);
/// - `+` may appear only as a whole level (`+`, `a/+/b`; not `a+` or `+b`);
/// - a shared subscription is `$share/{ShareName}/{filter}` where the share
///   name is non-empty and contains no `/`, `+`, or `#`, and the remaining
///   filter obeys the rules above.
///
/// Returns [`Error::TopicFilterInvalid`] otherwise.
pub fn validate_filter(filter: &str) -> Result<()> {
    let filter = if filter.starts_with(SHARE_PREFIX) {
        // Anything that starts like a shared subscription must be one.
        let (share, rest) = split_shared(filter).ok_or(Error::TopicFilterInvalid)?;
        if share.is_empty() || share.bytes().any(|b| matches!(b, b'/' | b'+' | b'#')) {
            return Err(Error::TopicFilterInvalid);
        }
        rest
    } else {
        filter
    };
    if filter.is_empty() {
        return Err(Error::TopicFilterInvalid);
    }
    let bytes = filter.as_bytes();
    let n = bytes.len();
    // Only wildcard bytes need their neighbours examined; everything else is
    // a plain scan.
    for (i, &b) in bytes.iter().enumerate() {
        if b != b'+' && b != b'#' {
            continue;
        }
        let level_start = i == 0 || bytes[i - 1] == b'/';
        let ok = if b == b'#' {
            level_start && i + 1 == n
        } else {
            level_start && (i + 1 == n || bytes[i + 1] == b'/')
        };
        if !ok {
            return Err(Error::TopicFilterInvalid);
        }
    }
    Ok(())
}

/// Splits a shared-subscription filter `$share/{ShareName}/{filter}` into
/// `(ShareName, filter)`. Returns `None` for an ordinary filter.
///
/// No validation is performed; see [`validate_filter`].
pub fn split_shared(filter: &str) -> Option<(&str, &str)> {
    let rest = filter.strip_prefix(SHARE_PREFIX)?;
    let slash = rest.find('/')?;
    Some((&rest[..slash], &rest[slash + 1..]))
}

/// Returns `true` if `filter` contains a wildcard character.
pub fn has_wildcards(filter: &str) -> bool {
    filter.bytes().any(|b| b == b'+' || b == b'#')
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name() {
        for ok in [
            "a",
            "a/b",
            "/",
            "a/",
            "/a",
            "$SYS/broker",
            "sport/tennis/player1",
        ] {
            assert_eq!(validate_name(ok), Ok(()), "{ok:?}");
        }
        for bad in ["", "+", "#", "a/+", "a/#", "a+b", "a#"] {
            assert_eq!(validate_name(bad), Err(Error::TopicNameInvalid), "{bad:?}");
        }
    }

    #[test]
    fn test_validate_filter_accepts_spec_examples() {
        for ok in [
            "#",
            "+",
            "sport/#",
            "sport/tennis/#",
            "+/tennis/#",
            "sport/+/player1",
            "+/+",
            "/+",
            "+/",
            "/",
            "a",
            "a/b/c",
            "$SYS/#",
            "$share/group/sport/#",
            "$share/g/+",
            "$share/g/a/b",
        ] {
            assert_eq!(validate_filter(ok), Ok(()), "{ok:?}");
        }
    }

    #[test]
    fn test_validate_filter_rejects_spec_counterexamples() {
        for bad in [
            "",
            "sport/tennis#",          // # not a whole level
            "sport/tennis/#/ranking", // # not last
            "sport+",                 // + not a whole level
            "+sport",
            "sport/+player",
            "a/#/b",
            "##",
            "++",
            "#/",
            "$share//a",   // empty share name
            "$share/g",    // no filter
            "$share/g/",   // empty filter
            "$share/g+/a", // wildcard in share name
            "$share/g#/a",
            "$share/a/b/tennis#", // bad inner filter
        ] {
            assert_eq!(
                validate_filter(bad),
                Err(Error::TopicFilterInvalid),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn test_split_shared() {
        assert_eq!(split_shared("$share/g/a/b"), Some(("g", "a/b")));
        assert_eq!(split_shared("$share/g/"), Some(("g", "")));
        assert_eq!(split_shared("$share//a"), Some(("", "a")));
        assert_eq!(split_shared("$share/g"), None);
        assert_eq!(split_shared("a/b"), None);
        assert_eq!(split_shared("$shared/g/a"), None);
    }

    #[test]
    fn test_has_wildcards() {
        assert!(has_wildcards("a/+"));
        assert!(has_wildcards("#"));
        assert!(!has_wildcards("a/b"));
    }
}
