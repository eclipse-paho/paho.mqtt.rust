// topic_matcher.rs
//
// A collection for matching a topic to a set of filters.
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2021-2026 Frank Pagliughi <fpagliughi@mindspring.com>
 * Copyright (c) 2024 Altair Bueno <altair@uma.es>
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
 *    Frank Pagliughi - TopicFilter trie collection & iterators
 *    Altair Bueno - TopicFilterExt trait and topic matches functions
 *******************************************************************************/

//! Code to match MQTT topics to filters that may contain wildcards.
//!

use std::{collections::HashMap, str::Split};

////////////////////////////////////////////////////////////////////////////
// Utility functions

/// Checks if a filter matches a given topic.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    topic_matches_iter(filter.split('/'), topic.split('/'))
}

/// Checks if a split filter matches a given split topic.
pub fn topic_matches_iter<'a, 'b, F, T>(filter: F, topic: T) -> bool
where
    F: IntoIterator<Item = &'a str>,
    T: IntoIterator<Item = &'b str>,
{
    let mut filter = filter.into_iter().peekable();
    let mut topic = topic.into_iter().peekable();

    // Topics starting with '$' don't match a wildcard in the first field.
    // See https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901246
    if matches!(filter.peek(), Some(&"#" | &"+"))
        && matches!(topic.peek(), Some(x) if x.starts_with('$'))
    {
        return false;
    }

    loop {
        match (filter.next(), topic.next()) {
            // Exhausted both filter and topic
            (None, None) => return true,
            // Wildcard on filter
            (Some("#"), _) => return true,
            // Single level wildcard on filter
            (Some("+"), Some(_)) => (),
            // Equal levels
            (Some(filter), Some(topic)) if filter == topic => (),
            // Otherwise, no match
            _ => return false,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
// Node (for TopicMatcher)

/// A single node in the topic matcher collection.
///
/// A terminal (leaf) node has some `content`, whereas intermediate nodes
/// do not. We also cache the full topic at the leaf. This should allow for
/// more efficient searches through the collection, so that the iterators
/// don't have to keep the stack of keys that lead down to the final leaf.
///
/// Note that although we could put the wildcard keys into the `children`
/// map, we specifically have separate fields for them. That allows us to
/// have separate mutable references for each, allowing for a mutable
/// iterator.
#[derive(Debug)]
struct Node<T> {
    /// The value that matches the topic at this node, if any.
    /// This includes a ached value of the filter.
    value: Option<(String, T)>,
    /// The explicit, non-wildcard child nodes mapped by the next field of
    /// the topic.
    children: HashMap<Box<str>, Node<T>>,
}

impl<T> Node<T> {
    /// Determines if the node does not contain a value.
    fn is_empty(&self) -> bool {
        self.value.is_none() && self.children.is_empty()
    }

    /// Gets a reference to the value
    fn value_ref(&self) -> Option<(&str, &T)> {
        self.value.as_ref().map(|(k, v)| (k.as_str(), v))
    }

    /// Gets an iterator for the node and _all_ of its children.
    fn iter(&self) -> NodeIter<'_, T> {
        Box::new(
            self.value
                .iter()
                .map(|(k, v)| (k.as_str(), v))
                .chain(self.children.values().flat_map(|n| n.iter())),
        )
    }

    /// Gets a mutable iterator for the node and _all_ of its children.
    fn iter_mut(&mut self) -> NodeIterMut<'_, T> {
        Box::new(
            self.value
                .iter_mut()
                .map(|(k, v)| (k.as_str(), v))
                .chain(self.children.values_mut().flat_map(|n| n.iter_mut())),
        )
    }

    /// Removes empty child nodes.
    fn prune(&mut self) {
        // Recursively prune children
        for node in self.children.values_mut() {
            node.prune();
        }

        // Remove empty children and shrink the has hmaps
        self.children.retain(|_, node| !node.is_empty());
    }

    /// Removes empty child nodes and shrinks the capacity of the collection
    /// as much as possible.
    fn shrink_to_fit(&mut self) {
        // Recursively shrink children
        for node in self.children.values_mut() {
            node.shrink_to_fit();
        }

        // Remove empty children and shrink the has hmaps
        self.children.retain(|_, node| !node.is_empty());
        self.children.shrink_to_fit();
    }
}

// We manually implement Default, otherwise the derived one would
// require T: Default.

impl<T> Default for Node<T> {
    /// Creates a default, empty node.
    fn default() -> Self {
        Node {
            value: None,
            children: HashMap::new(),
        }
    }
}

/// An iterator to visit all values in a node and its children.
type NodeIter<'a, T> = Box<dyn Iterator<Item = (&'a str, &'a T)> + 'a>;

/// A mutable iterator to visit all values in a node and its children.
type NodeIterMut<'a, T> = Box<dyn Iterator<Item = (&'a str, &'a mut T)> + 'a>;

/////////////////////////////////////////////////////////////////////////////
// TopicMatcher

/// A collection of topic filters to arbitrary objects.
///
/// This can be used to get an iterator to all items that have a filter that
/// matches a topic. To test against a single filter, see
/// [`TopicFilter`](crate::TopicFilter). This collection is more commonly used
/// when there are a number of filters and each needs to be associated with a
/// particular action or piece of data. Note, though, that a single incoming
/// topic could match against several items in the collection. For example,
/// the topic:
///     data/temperature/engine
///
/// Could match against the filters:
///     data/temperature/engine
///     data/temperature/#
///     data/+/engine
///
/// Thus, the collection gives an iterator for the items matching a topic.
///
/// A common use for this would be to store callbacks to process incoming
/// messages based on topics.
///
/// This code was adapted from the Eclipse Python `MQTTMatcher` class:
/// <https://github.com/eclipse/paho.mqtt.python/blob/master/src/paho/mqtt/matcher.py>
///
/// which use a prefix tree (trie) to store the values.
///
/// This is particularly useful at creating a lookup table of callbacks or
/// individual channels for subscriptions, especially when the subscriptions
/// contain wildcards.
///
/// Note, however that there might be an issue with overlapped subscriptions
/// where callbacks are invoked multiple times for a message that matches
/// more than one subscription.
///
/// When using MQTT v5, subscription identifiers would be more efficient
/// and solve the problem of multiple overlapped callbacks. See:
/// <https://github.com/eclipse/paho.mqtt.rust/blob/master/examples/sync_consume_v5.rs>
///
#[derive(Debug)]
pub struct TopicMatcher<T> {
    root: Node<T>,
}

impl<T> TopicMatcher<T> {
    /// Creates a new, empty, topic matcher collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Determines if the collection contains no values.
    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }

    /// Removes all the entries in the collection.
    pub fn clear(&mut self) {
        self.root = Node::default();
    }

    /// Inserts a new topic filter and value into the collection.
    pub fn insert<S: Into<String>>(&mut self, filter: S, val: T) {
        let filter = filter.into();
        let mut curr = &mut self.root;

        for field in filter.split('/') {
            curr = curr.children.entry(field.into()).or_default()
        }
        curr.value = Some((filter, val));
    }

    /// Gets a reference to a value from the collection using the precise
    /// filter. This does not do wildcard matching but looks for the value
    /// for the exact, specified filter.
    pub fn get(&self, filter: &str) -> Option<&T> {
        self.get_key_value(filter).map(|(_, v)| v)
    }

    /// Gets a reference to a value from the collection using the precise
    /// filter. This does not do wildcard matching but looks for the entry
    /// for the exact, specified filter.
    pub fn get_key_value(&self, filter: &str) -> Option<(&str, &T)> {
        let mut curr = &self.root;

        for field in filter.split('/') {
            curr = curr.children.get(field)?;
        }
        curr.value.as_ref().map(|(k, v)| (k.as_str(), v))
    }

    /// Gets a mutable mutable reference to a value from the collection
    /// using an exact filter as the key.
    pub fn get_mut(&mut self, filter: &str) -> Option<&mut T> {
        let mut curr = &mut self.root;

        for field in filter.split('/') {
            curr = curr.children.get_mut(field)?;
        }
        curr.value.as_mut().map(|(_, v)| v)
    }

    /// Removes the entry, returning the value for it, if found.
    ///
    /// Any node left with neither a value nor children is removed with it,
    /// so the collection holds nothing for filters that are gone, and
    /// [`is_empty`](Self::is_empty) is true once every entry has been
    /// removed.
    pub fn remove(&mut self, filter: &str) -> Option<T> {
        Self::remove_at(&mut self.root, filter.split('/'))
    }

    /// Removes the value for the remaining `fields` below `node`, and
    /// whatever nodes on the way down that leaves empty.
    fn remove_at(node: &mut Node<T>, mut fields: Split<'_, char>) -> Option<T> {
        let Some(field) = fields.next() else {
            return node.value.take().map(|(_, v)| v);
        };
        let child = node.children.get_mut(field)?;
        let val = Self::remove_at(child, fields);
        if child.is_empty() {
            node.children.remove(field);
        }
        val
    }

    /// Removes empty nodes in the collection.
    ///
    /// Since [`remove`](Self::remove) removes the nodes it empties, there
    /// are none to find; this is kept for compatibility.
    pub fn prune(&mut self) {
        self.root.prune()
    }

    /// Removes empty nodes and shrinks the capacity of the collection as
    /// much as possible.
    pub fn shrink_to_fit(&mut self) {
        self.root.shrink_to_fit()
    }

    /// Gets an iterator over all the items in the collection.
    pub fn iter(&self) -> NodeIter<'_, T> {
        self.root.iter()
    }

    /// Gets a muable iterator over all the items in the collection.
    pub fn iter_mut(&mut self) -> NodeIterMut<'_, T> {
        self.root.iter_mut()
    }

    /// Gets an iterator for all the matches to the specified topic.
    pub fn matches<'a, 'b>(&'a self, topic: &'b str) -> MatchIter<'a, 'b, T> {
        MatchIter::new(&self.root, topic)
    }

    /// Determines if the topic matches any of the filters in the collection.
    pub fn has_match(&self, topic: &str) -> bool {
        self.matches(topic).next().is_some()
    }
}

impl<T: Clone> TopicMatcher<T> {
    /// Inserts multiple filters all with (a clone of) the same value.
    pub fn insert_many<S: AsRef<str>>(&mut self, filters: &[S], val: T) {
        for filter in filters {
            self.insert(filter.as_ref(), val.clone());
        }
    }
}

// We manually implement Default, otherwise the derived one would
// require T: Default.

impl<T> Default for TopicMatcher<T> {
    /// Create an empty TopicMatcher collection.
    fn default() -> Self {
        TopicMatcher {
            root: Node::default(),
        }
    }
}

impl<T> From<HashMap<&str, T>> for TopicMatcher<T> {
    fn from(mut m: HashMap<&str, T>) -> Self {
        let mut matcher = Self::new();
        for (filt, val) in m.drain() {
            matcher.insert(filt, val);
        }
        matcher
    }
}

impl<T> From<HashMap<String, T>> for TopicMatcher<T> {
    fn from(mut m: HashMap<String, T>) -> Self {
        let mut matcher = Self::new();
        for (filt, val) in m.drain() {
            matcher.insert(filt, val);
        }
        matcher
    }
}

impl<'a, T: 'a> IntoIterator for &'a TopicMatcher<T> {
    type Item = (&'a str, &'a T);
    type IntoIter = NodeIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: 'a> IntoIterator for &'a mut TopicMatcher<T> {
    type Item = (&'a str, &'a mut T);
    type IntoIter = NodeIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/////////////////////////////////////////////////////////////////////////////

/// Iterator for the topic matcher collection.
/// This is created from a specific topic string and will find the contents
/// of all the matching filters in the collection.
///
/// Lifetimes:
/// ```text
/// 'a - The TopicMatcher collection
/// 'b - The topic string
/// ```
///
/// We keep a stack of work still to do: nodes to search with the topic
/// fields left to match against them, and `#` nodes, whose values match
/// whatever is left of the topic, waiting to be yielded.
#[derive(Debug)]
pub struct MatchIter<'a, 'b, T> {
    remaining: Vec<Pending<'a, 'b, T>>,
}

/// One item of [`MatchIter`]'s work.
#[derive(Debug)]
enum Pending<'a, 'b, T> {
    /// Search `node` with the remaining topic fields. `first` is set for the
    /// root, whose field is the first of the topic.
    Search(&'a Node<T>, Split<'b, char>, bool),
    /// Yield the value of a `#` node, if it has one.
    Yield(&'a Node<T>),
}

impl<'a, 'b, T> MatchIter<'a, 'b, T> {
    fn new(node: &'a Node<T>, topic: &'b str) -> Self {
        let fields = topic.split('/');
        Self {
            remaining: vec![Pending::Search(node, fields, true)],
        }
    }
}

impl<'a, 'b, T> Iterator for MatchIter<'a, 'b, T> {
    /// Gets the full filter string and value at the matching nodes
    type Item = (&'a str, &'a T);

    /// Gets the next value that matches the iterator's topic.
    ///
    /// A node can be left without a value — `remove` takes the value but
    /// keeps the node — so a node is never taken as a match just for being
    /// there: an empty one is passed over and the search goes on.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (node, mut fields, first) = match self.remaining.pop()? {
                Pending::Yield(node) => match node.value_ref() {
                    Some(item) => return Some(item),
                    None => continue,
                },
                Pending::Search(node, fields, first) => (node, fields, first),
            };

            let Some(field) = fields.next() else {
                // The whole topic is matched at this node: its own value,
                // and also that of a `#` below it, since `#` matches the
                // parent level too (`a/#` matches `a`). Both, not either.
                if let Some(child) = node.children.get("#") {
                    self.remaining.push(Pending::Yield(child));
                }
                match node.value_ref() {
                    Some(item) => return Some(item),
                    None => continue,
                }
            };

            if let Some(child) = node.children.get(field) {
                self.remaining
                    .push(Pending::Search(child, fields.clone(), false));
            }

            // Topics starting with '$' don't match wildcards in the first field
            // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901246
            if !first || !field.starts_with('$') {
                if let Some(child) = node.children.get("+") {
                    self.remaining.push(Pending::Search(child, fields, false));
                }
                // By protocol definition, a '#' must be a terminating leaf.
                if let Some(child) = node.children.get("#") {
                    self.remaining.push(Pending::Yield(child));
                }
            }
        }
    }
}

/// Macro to create a `TopicMatcher` collection.
#[macro_export]
macro_rules! topic_matcher {
    { $($filter:expr => $val:expr),+ } => {
        {
            let mut tm = $crate::topic_matcher::TopicMatcher::new();
            $(
                tm.insert($filter, $val);
            )+
            tm
        }
     };
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_basic_topic_matcher() {
        let tm: TopicMatcher<i32> = topic_matcher! {
            "some/test/#" => 99,
            "some/test/topic" => 19,
            "some/+/topic" => 42,
            "some/prod/topic" => 155
        };

        // 'get()' is a standard string comparison looking for the filter
        // itself as the key.
        assert_eq!(tm.get("some/test/topic"), Some(&19));
        assert_eq!(tm.get("some/test/bubba"), None);

        assert!(tm.has_match("some/random/topic"));
        assert!(!tm.has_match("some/other/thing"));

        // Test the iterator

        assert_eq!(3, tm.matches("some/test/topic").count());

        let mut set = HashSet::new();
        set.insert(19);
        set.insert(42);
        set.insert(99);

        let mut match_set = HashSet::new();
        for (_k, v) in tm.matches("some/test/topic") {
            match_set.insert(*v);
        }

        assert_eq!(set, match_set);

        let tm = topic_matcher! {
            "hello/#" => 99,
            "hi/there" => 13,
            "hello/world" => 42,
            "hello/there/bubba" => 96,
            "hello/+/bubba" => 27
        };

        assert_eq!(2, tm.matches("hello/world").count());
    }

    #[test]
    fn test_topic_matcher() {
        use crate::topic_matcher as tm;

        // Should match

        assert!(tm! {"foo/bar" => ()}.has_match("foo/bar"));
        assert!(tm! {"foo/+" => ()}.has_match("foo/bar"));
        assert!(tm! {"foo/+/baz" => ()}.has_match("foo/bar/baz"));
        assert!(tm! {"foo/+/#"=> ()}.has_match("foo/bar/baz"));
        assert!(tm! {"foo/bar/#"=> ()}.has_match("foo/bar/baz"));
        assert!(tm! {"foo/bar/#"=> ()}.has_match("foo/bar"));
        assert!(tm! {"A/B/+/#"=> ()}.has_match("A/B/B/C"));
        assert!(tm! {"#"=> ()}.has_match("foo/bar/baz"));
        assert!(tm! {"#"=> ()}.has_match("/foo/bar"));
        assert!(tm! {"/#"=> ()}.has_match("/foo/bar"));
        assert!(tm! {"$SYS/bar"=> ()}.has_match("$SYS/bar"));
        assert!(tm! {"foo/#"=> ()}.has_match("foo/$bar"));
        assert!(tm! {"foo/+/baz"=> ()}.has_match("foo/$bar/baz"));

        // Should not match

        assert!(!tm! {"test/6/#"=> ()}.has_match("test/3"));
        assert!(!tm! {"foo/bar"=> ()}.has_match("foo"));
        assert!(!tm! {"foo/+"=> ()}.has_match("foo/bar/baz"));
        assert!(!tm! {"foo/+/baz"=> ()}.has_match("foo/bar/bar"));
        assert!(!tm! {"foo/+/#"=> ()}.has_match("fo2/bar/baz"));
        assert!(!tm! {"/#"=> ()}.has_match("foo/bar"));
        assert!(!tm! {"#"=> ()}.has_match("$SYS/bar"));
        assert!(!tm! {"$BOB/bar"=> ()}.has_match("$SYS/bar"));
        assert!(!tm! {"+/bar"=> ()}.has_match("$SYS/bar"));
    }

    #[test]
    fn test_topic_matcher_fn() {
        // Should match

        assert!(topic_matches("foo/bar", "foo/bar"));
        assert!(topic_matches("foo/+", "foo/bar"));
        assert!(topic_matches("foo/+/baz", "foo/bar/baz"));
        assert!(topic_matches("foo/+/#", "foo/bar/baz"));
        assert!(topic_matches("foo/bar/#", "foo/bar/baz"));
        assert!(topic_matches("foo/bar/#", "foo/bar"));
        assert!(topic_matches("A/B/+/#", "A/B/B/C"));
        assert!(topic_matches("#", "foo/bar/baz"));
        assert!(topic_matches("#", "/foo/bar"));
        assert!(topic_matches("/#", "/foo/bar"));
        assert!(topic_matches("$SYS/bar", "$SYS/bar"));
        assert!(topic_matches("foo/#", "foo/$bar"));
        assert!(topic_matches("foo/+/baz", "foo/$bar/baz"));

        // Should not match

        assert!(!topic_matches("test/6/#", "test/3"));
        assert!(!topic_matches("foo/bar", "foo"));
        assert!(!topic_matches("foo/+", "foo/bar/baz"));
        assert!(!topic_matches("foo/+/baz", "foo/bar/bar"));
        assert!(!topic_matches("foo/+/#", "fo2/bar/baz"));
        assert!(!topic_matches("/#", "foo/bar"));
        assert!(!topic_matches("#", "$SYS/bar"));
        assert!(!topic_matches("$BOB/bar", "$SYS/bar"));
        assert!(!topic_matches("+/bar", "$SYS/bar"));
    }

    #[test]
    fn test_topic_matcher_callback() {
        let mut matcher = TopicMatcher::new();

        matcher.insert("some/+/topic", Box::new(|n: u32| n * 2));

        for (_t, f) in matcher.matches("some/random/topic") {
            let n = f(2);
            assert_eq!(n, 4);
        }
    }

    #[test]
    fn test_topic_matcher_many() {
        let mut tm = TopicMatcher::new();
        tm.insert("some/test/#", 99);
        tm.insert_many(&["some/test/topic", "some/+/topic", "some/prod/topic"], 42);

        assert_eq!(tm.get("some/test/#"), Some(&99));
        assert_eq!(tm.get("some/test/topic"), Some(&42));
        assert_eq!(tm.get("some/+/topic"), Some(&42));
        assert_eq!(tm.get("some/prod/topic"), Some(&42));
        assert_eq!(tm.get("some/test/bubba"), None);
    }

    #[test]
    fn test_topic_matcher_values() {
        let tm = topic_matcher! {
            "some/topic/#" => 42
        };

        let m: Vec<_> = tm.matches("some/topic/thing").map(|(_, v)| *v).collect();
        assert_eq!(m, &[42]);

        let m: Vec<_> = tm.matches("some/topic").map(|(_, v)| *v).collect();
        assert_eq!(m, &[42]);

        let m: Vec<_> = tm.matches("some/bad/topic").map(|(_, v)| *v).collect();
        assert!(m.is_empty());
    }

    fn matched(tm: &TopicMatcher<i32>, topic: &str) -> Vec<i32> {
        let mut m: Vec<_> = tm.matches(topic).map(|(_, v)| *v).collect();
        m.sort();
        m
    }

    /// Adds an empty node for `filter`, as `remove` used to leave behind.
    /// It now prunes them, but the search must not depend on that.
    fn add_empty_node(tm: &mut TopicMatcher<i32>, filter: &str) {
        let mut node = &mut tm.root;
        for field in filter.split('/') {
            node = node.children.entry(field.into()).or_default();
        }
    }

    /// A `#` node without a value is passed over. It used to end the
    /// search, hiding every match not yet found.
    #[test]
    fn test_matches_past_an_emptied_node() {
        let mut tm = TopicMatcher::new();
        add_empty_node(&mut tm, "a/#");
        tm.insert("a/b", 2);
        tm.insert("a/+", 3);
        assert_eq!(matched(&tm, "a/b"), [2, 3]);
        assert_eq!(matched(&tm, "a"), [0; 0]);
        assert!(tm.has_match("a/b"));
    }

    /// Removing an entry removes the nodes it leaves empty, so a collection
    /// whose entries have all been removed is empty. `is_empty` used to stay
    /// false, the nodes kept, after an insert and a remove.
    #[test]
    fn test_remove_prunes_what_it_empties() {
        let mut tm = TopicMatcher::new();
        tm.insert("a/b/c", 1);
        tm.insert("a/b", 2);
        tm.insert("a/+/#", 3);
        assert!(!tm.is_empty());

        // Not there: nothing changes.
        assert_eq!(tm.remove("a/b/c/d"), None);
        assert_eq!(tm.remove("a/x"), None);

        // A node with a value or children of its own stays.
        assert_eq!(tm.remove("a/b/c"), Some(1));
        assert_eq!(tm.get("a/b"), Some(&2));
        assert!(
            !tm.root.children["a"].children["b"]
                .children
                .contains_key("c")
        );
        assert_eq!(matched(&tm, "a/b"), [2, 3]);

        assert_eq!(tm.remove("a/b"), Some(2));
        assert!(!tm.root.children["a"].children.contains_key("b"));
        assert_eq!(tm.remove("a/+/#"), Some(3));
        assert!(tm.is_empty());
        assert!(tm.root.children.is_empty());
        assert!(!tm.has_match("a/b"));

        // And it fills again as before.
        tm.insert("a/b", 4);
        assert_eq!(matched(&tm, "a/b"), [4]);
    }

    /// A topic that ends at a node with a value matches both that value and
    /// a `#` below it, which matches the parent level too. It used to yield
    /// only the first.
    #[test]
    fn test_matches_a_value_and_its_hash_child() {
        let tm = topic_matcher! {
            "a/b" => 1,
            "a/b/#" => 2,
            "a/+" => 3,
            "#" => 4
        };
        assert_eq!(matched(&tm, "a/b"), [1, 2, 3, 4]);
        assert_eq!(matched(&tm, "a/b/c"), [2, 4]);
    }

    /// `matches` agrees with `topic_matches` for every filter and topic in a
    /// set chosen to exercise the corners: wildcards at each level, `$`
    /// topics, empty levels, a filter that is a prefix of another, and a
    /// node left behind by a removed value.
    #[test]
    fn test_matches_agrees_with_topic_matches() {
        let filters = [
            "#", "+", "+/+", "+/#", "a", "a/#", "a/+", "a/b", "a/b/#", "a/+/c", "a/b/c", "+/b/#",
            "/", "/#", "a//c", "$SYS/#", "$SYS/+", "+/x",
        ];
        let topics = [
            "a", "a/b", "a/b/c", "a/x/c", "a/b/c/d", "b", "x/b/y", "/", "/a", "a//c", "a/", "$SYS",
            "$SYS/x", "$SYS/x/y", "", "q/x",
        ];

        let mut tm = TopicMatcher::new();
        for (i, f) in filters.iter().enumerate() {
            tm.insert(*f, i as i32);
        }
        // An empty node in among the rest.
        add_empty_node(&mut tm, "a/b/x/#");
        add_empty_node(&mut tm, "+/#");

        for topic in topics {
            let expected: Vec<i32> = filters
                .iter()
                .enumerate()
                .filter(|(_, f)| topic_matches(f, topic))
                .map(|(i, _)| i as i32)
                .collect();
            assert_eq!(matched(&tm, topic), expected, "topic {topic:?}");
        }
    }
}
