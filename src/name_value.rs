// name_value.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// A name/value is a helper to bridge between a collection of
// string pairs in Rust to an array of NUL terminated char string pointer
// pairs that the C library expects.
//
// It is useful when a C API takes a `const char *arg[]` parameter.
//

/*******************************************************************************
 * Copyright (c) 2020-2022 Frank Pagliughi <fpagliughi@mindspring.com>
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

use std::{collections::hash_map::HashMap, ffi::CString, pin::Pin};

/// The name/value pointer pair from the C library.
type CNameValue = ffi::MQTTAsync_nameValue;

/// A collection of C-compatible (NUL-terminated) string pairs that is
/// useful with C APIs that require an array of string pair pointers,
/// normally specified as `const* MQTTAsync_nameValue`
#[derive(Debug)]
pub struct NameValueCollection {
    /// A vector of c-compatible pointers into the data collection.
    /// Note that this will have an addition entry at the end containing
    /// NULL name/value pointers to indicate the end-of-collection to
    /// the C library.
    c_coll: Vec<CNameValue>,
    /// The pinned data cache
    data: Pin<Box<NameValueData>>,
}

/// The cache of Rust name/value string pairs
#[derive(Debug, Default, Clone)]
struct NameValueData {
    /// A collection of C-compatible (NUL-terminated) string pairs.
    coll: Vec<(CString, CString)>,
}

impl NameValueCollection {
    /// Creates a NameValueCollection from a vector of string pair references.
    ///
    /// # Arguments
    ///
    /// * `coll` A collection of string pair references.
    ///
    pub fn new<N, V>(coll: &[(N, V)]) -> Self
    where
        N: AsRef<str>,
        V: AsRef<str>,
    {
        let data = NameValueData {
            coll: Self::to_cstring_pair(coll),
        };
        Self::from_data(data)
    }

    // Convert a collection of string references to a vector of CString pairs.
    fn to_cstring_pair<N, V>(coll: &[(N, V)]) -> Vec<(CString, CString)>
    where
        N: AsRef<str>,
        V: AsRef<str>,
    {
        coll.iter()
            .map(|(n, v)| {
                (
                    CString::new(n.as_ref()).unwrap(),
                    CString::new(v.as_ref()).unwrap(),
                )
            })
            .collect()
    }

    // Convert a collection of CString's to a vector of pairs of C
    // char pointers.
    //
    // This also appends a pair of (NULL,NULL) pointers at the end of the
    // collection which is used to indicate the end of the collection to
    // the C library.
    //
    // Note that the pointers are invalidated if the original vector or
    // any of the strings in it change.
    fn to_c_vec(sv: &[(CString, CString)]) -> Vec<CNameValue> {
        let mut coll: Vec<CNameValue> = sv
            .iter()
            .map(|csp| CNameValue::new(csp.0.as_ptr(), csp.1.as_ptr()))
            .collect();
        coll.push(CNameValue::default());
        coll
    }

    // Updates the cached vector to correspond to the string.
    fn from_data(data: NameValueData) -> Self {
        let data = Box::pin(data);
        let c_coll = Self::to_c_vec(&data.coll);
        Self { c_coll, data }
    }

    /// Returns true if the collection contains elements.
    pub fn is_empty(&self) -> bool {
        self.data.coll.is_empty()
    }

    /// Gets the number of strings in the collection.
    pub fn len(&self) -> usize {
        self.data.coll.len()
    }

    /// Gets the collection as a pointer to const C string pair pointers.
    ///
    /// This returns a pointer that can be sent to a C API that takes a
    /// pointer to an array of name/value char pointer pairs, like
    /// `const MQTTAsync_nameValue*`
    ///
    /// This function is inherently unsafe. The pointer it returns is only
    /// valid while the collection remains unmodified. In general, it
    /// should be requested when needed and not stored for future use.
    pub fn as_c_arr_ptr(&self) -> *const CNameValue {
        self.c_coll.as_ptr()
    }
}

impl Default for NameValueCollection {
    fn default() -> Self {
        Self::from_data(NameValueData::default())
    }
}

impl Clone for NameValueCollection {
    fn clone(&self) -> Self {
        Self::from_data((*self.data).clone())
    }
}

impl From<HashMap<&str, &str>> for NameValueCollection {
    fn from(hmap: HashMap<&str, &str>) -> Self {
        let v: Vec<(CString, CString)> = hmap
            .into_iter()
            .map(|(n, v)| (CString::new(n).unwrap(), CString::new(v).unwrap()))
            .collect();
        Self::from_data(NameValueData { coll: v })
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! vec_of_string_pairs {
        ($($x:expr),*) => (vec![$(($x.0.to_string(),$x.1.to_string())),*]);
    }

    #[test]
    fn test_default() {
        let sc = NameValueCollection::default();
        assert_eq!(0, sc.len());
    }

    #[test]
    fn test_new() {
        let v = [("name0", "val0"), ("name1", "val1"), ("name2", "val2")];
        let n = v.len();

        let sc = NameValueCollection::new(&v);

        assert_eq!(n, sc.len());
        assert_eq!(n + 1, sc.c_coll.len());
        assert_eq!(n, sc.data.coll.len());

        assert_eq!(v[0].0.as_bytes(), sc.data.coll[0].0.as_bytes());
        assert_eq!(v[0].1.as_bytes(), sc.data.coll[0].1.as_bytes());

        assert_eq!(v[1].0.as_bytes(), sc.data.coll[1].0.as_bytes());
        assert_eq!(v[1].1.as_bytes(), sc.data.coll[1].1.as_bytes());

        assert_eq!(v[2].0.as_bytes(), sc.data.coll[2].0.as_bytes());
        assert_eq!(v[2].1.as_bytes(), sc.data.coll[2].1.as_bytes());

        assert_eq!(sc.data.coll[0].0.as_ptr(), sc.c_coll[0].name);
        assert_eq!(sc.data.coll[0].1.as_ptr(), sc.c_coll[0].value);

        assert_eq!(sc.data.coll[1].0.as_ptr(), sc.c_coll[1].name);
        assert_eq!(sc.data.coll[1].1.as_ptr(), sc.c_coll[1].value);

        assert_eq!(sc.data.coll[2].0.as_ptr(), sc.c_coll[2].name);
        assert_eq!(sc.data.coll[2].1.as_ptr(), sc.c_coll[2].value);
    }

    #[test]
    fn test_new_from_vec_strings() {
        let v = vec_of_string_pairs![("name0", "val0"), ("name1", "val1"), ("name2", "val2")];
        let n = v.len();

        let sc = NameValueCollection::new(&v);

        assert_eq!(n, sc.len());
        assert_eq!(n + 1, sc.c_coll.len());
        assert_eq!(n, sc.data.coll.len());

        assert_eq!(v[0].0.as_bytes(), sc.data.coll[0].0.as_bytes());
        assert_eq!(v[0].1.as_bytes(), sc.data.coll[0].1.as_bytes());

        assert_eq!(v[1].0.as_bytes(), sc.data.coll[1].0.as_bytes());
        assert_eq!(v[1].1.as_bytes(), sc.data.coll[1].1.as_bytes());

        assert_eq!(v[2].0.as_bytes(), sc.data.coll[2].0.as_bytes());
        assert_eq!(v[2].1.as_bytes(), sc.data.coll[2].1.as_bytes());

        assert_eq!(sc.data.coll[0].0.as_ptr(), sc.c_coll[0].name);
        assert_eq!(sc.data.coll[0].1.as_ptr(), sc.c_coll[0].value);

        assert_eq!(sc.data.coll[1].0.as_ptr(), sc.c_coll[1].name);
        assert_eq!(sc.data.coll[1].1.as_ptr(), sc.c_coll[1].value);

        assert_eq!(sc.data.coll[2].0.as_ptr(), sc.c_coll[2].name);
        assert_eq!(sc.data.coll[2].1.as_ptr(), sc.c_coll[2].value);
    }

    #[test]
    fn test_from_hashmap_str() {
        let mut hmap = HashMap::new();
        hmap.insert("name0", "val0");
        hmap.insert("name1", "val1");
        hmap.insert("name2", "val2");

        let n = hmap.len();

        let sc: NameValueCollection = hmap.into();

        assert_eq!(n, sc.len());
        assert_eq!(n + 1, sc.c_coll.len());
        assert_eq!(n, sc.data.coll.len());

        // TODO: Check the entries, remembering they may be in any order.

        /*
        assert_eq!(v[0].0.as_bytes(), sc.data.coll[0].0.as_bytes());
        assert_eq!(v[0].1.as_bytes(), sc.data.coll[0].1.as_bytes());

        assert_eq!(v[1].0.as_bytes(), sc.data.coll[1].0.as_bytes());
        assert_eq!(v[1].1.as_bytes(), sc.data.coll[1].1.as_bytes());

        assert_eq!(v[2].0.as_bytes(), sc.data.coll[2].0.as_bytes());
        assert_eq!(v[2].1.as_bytes(), sc.data.coll[2].1.as_bytes());

        assert_eq!(sc.data.coll[0].0.as_ptr(), sc.c_coll[0].name);
        assert_eq!(sc.data.coll[0].1.as_ptr(), sc.c_coll[0].value);

        assert_eq!(sc.data.coll[1].0.as_ptr(), sc.c_coll[1].name);
        assert_eq!(sc.data.coll[1].1.as_ptr(), sc.c_coll[1].value);

        assert_eq!(sc.data.coll[2].0.as_ptr(), sc.c_coll[2].name);
        assert_eq!(sc.data.coll[2].1.as_ptr(), sc.c_coll[2].value);
        */
    }

    #[test]
    fn test_assign() {
        let v = [("name0", "val0"), ("name1", "val1"), ("name2", "val2")];
        let n = v.len();

        let org_sc = NameValueCollection::new(&v);

        let sc = org_sc;

        assert_eq!(n, sc.len());
        assert_eq!(n + 1, sc.c_coll.len());
        assert_eq!(n, sc.data.coll.len());

        assert_eq!(v[0].0.as_bytes(), sc.data.coll[0].0.as_bytes());
        assert_eq!(v[0].1.as_bytes(), sc.data.coll[0].1.as_bytes());

        assert_eq!(v[1].0.as_bytes(), sc.data.coll[1].0.as_bytes());
        assert_eq!(v[1].1.as_bytes(), sc.data.coll[1].1.as_bytes());

        assert_eq!(v[2].0.as_bytes(), sc.data.coll[2].0.as_bytes());
        assert_eq!(v[2].1.as_bytes(), sc.data.coll[2].1.as_bytes());

        assert_eq!(sc.data.coll[0].0.as_ptr(), sc.c_coll[0].name);
        assert_eq!(sc.data.coll[0].1.as_ptr(), sc.c_coll[0].value);

        assert_eq!(sc.data.coll[1].0.as_ptr(), sc.c_coll[1].name);
        assert_eq!(sc.data.coll[1].1.as_ptr(), sc.c_coll[1].value);

        assert_eq!(sc.data.coll[2].0.as_ptr(), sc.c_coll[2].name);
        assert_eq!(sc.data.coll[2].1.as_ptr(), sc.c_coll[2].value);
    }

    #[test]
    fn test_clone() {
        let v = [("name0", "val0"), ("name1", "val1"), ("name2", "val2")];
        let n = v.len();

        let sc = {
            let org_sc = NameValueCollection::new(&v);
            org_sc.clone()
        };

        assert_eq!(n, sc.len());
        assert_eq!(n + 1, sc.c_coll.len());
        assert_eq!(n, sc.data.coll.len());

        assert_eq!(v[0].0.as_bytes(), sc.data.coll[0].0.as_bytes());
        assert_eq!(v[0].1.as_bytes(), sc.data.coll[0].1.as_bytes());

        assert_eq!(v[1].0.as_bytes(), sc.data.coll[1].0.as_bytes());
        assert_eq!(v[1].1.as_bytes(), sc.data.coll[1].1.as_bytes());

        assert_eq!(v[2].0.as_bytes(), sc.data.coll[2].0.as_bytes());
        assert_eq!(v[2].1.as_bytes(), sc.data.coll[2].1.as_bytes());

        assert_eq!(sc.data.coll[0].0.as_ptr(), sc.c_coll[0].name);
        assert_eq!(sc.data.coll[0].1.as_ptr(), sc.c_coll[0].value);

        assert_eq!(sc.data.coll[1].0.as_ptr(), sc.c_coll[1].name);
        assert_eq!(sc.data.coll[1].1.as_ptr(), sc.c_coll[1].value);

        assert_eq!(sc.data.coll[2].0.as_ptr(), sc.c_coll[2].name);
        assert_eq!(sc.data.coll[2].1.as_ptr(), sc.c_coll[2].value);
    }
}
