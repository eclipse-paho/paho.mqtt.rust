// string_collection.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// A string_collection is a helper to bridge between a collection of
// strings in Rust to an array of NUL terminated char string pointers
// that  the C library expects.
//
// It is useful when a C API takes a `const char *arg[]` parameter.
//

/*******************************************************************************
 * Copyright (c) 2017-2025 Frank Pagliughi <fpagliughi@mindspring.com>
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

use std::{ffi::CString, os::raw::c_char, pin::Pin};

/// A collection of C-compatible (NUL-terminated) strings that is useful
/// with C API's that require an array of strings, normally specified as:
/// `const char* arr[]` or  `const char** arr`
#[derive(Debug)]
pub struct StringCollection {
    /// A vector cache of pointers into the data `coll`
    /// This must be updated any time the data is modified.
    c_coll: Vec<*const c_char>,
    /// A vector cache of mut pointers into the data `coll`
    /// This must be updated any time the data is modified.
    c_mut_coll: Vec<*mut c_char>,
    /// The pinned data cache
    data: Pin<Box<StringCollectionData>>,
}

#[derive(Debug, Default, Clone)]
struct StringCollectionData {
    /// The owned NUL-terminated strings
    coll: Vec<CString>,
}

impl StringCollection {
    /// Creates a StringCollection from a vector of string references.
    ///
    /// # Arguments
    ///
    /// `coll` A collection of string references.
    ///
    pub fn new<T>(coll: &[T]) -> Self
    where
        T: AsRef<str>,
    {
        let data = StringCollectionData {
            coll: Self::to_cstring(coll),
        };
        Self::from_data(data)
    }

    // Convert a collection of string references to a vector of CStrings.
    fn to_cstring<T>(coll: &[T]) -> Vec<CString>
    where
        T: AsRef<str>,
    {
        coll.iter()
            .map(|s| CString::new(s.as_ref()).unwrap())
            .collect()
    }

    // Convert a collection of CString's to a vector of C char pointers.
    //
    // Note that the pointers are invalidated if the original vector or
    // any of the strings in it change.
    fn to_c_vec(sv: &[CString]) -> Vec<*const c_char> {
        sv.iter().map(|cs| cs.as_ptr()).collect()
    }

    // Convert a collection of CString's to a vector of C char pointers.
    //
    // Note that the pointers are invalidated if the original vector or
    // any of the strings in it change.
    fn to_c_mut_vec(sv: &[CString]) -> Vec<*mut c_char> {
        sv.iter().map(|cs| cs.as_ptr() as *mut c_char).collect()
    }

    // Updates the cached vector to correspond to the string.
    fn from_data(data: StringCollectionData) -> Self {
        let data = Box::pin(data);
        let c_coll = Self::to_c_vec(&data.coll);
        let c_mut_coll = Self::to_c_mut_vec(&data.coll);
        Self {
            c_coll,
            c_mut_coll,
            data,
        }
    }

    /// Returns true if the collection contains elements.
    pub fn is_empty(&self) -> bool {
        self.data.coll.is_empty()
    }

    /// Gets the number of strings in the collection.
    pub fn len(&self) -> usize {
        self.data.coll.len()
    }

    /// Gets the collection as a pointer to const C string pointers.
    ///
    /// This returns a pointer that can be sent to a C API that takes a
    /// pointer to an array of char pointers, like `const char* arr[]`
    /// This function is inherently unsafe. The pointer it returns is only
    /// valid while the collection remains unmodified. In general, it
    /// should be requested when needed and not stored for future use.
    pub fn as_c_arr_ptr(&self) -> *const *const c_char {
        self.c_coll.as_ptr()
    }

    /// Gets the collection as a pointer to mutable C string pointers.
    ///
    /// This returns a pointer that can be sent to a C API that takes a
    /// pointer to an array of mutable char pointers, like `char* arr[]`
    /// This function is inherently unsafe. The pointer it returns is only
    /// valid while the collection remains unmodified. In general, it
    /// should be requested when needed and not stored for future use.
    ///
    /// This function is required due to the lax nature of the use of
    /// const strings in the C API. Hopefully the API will be fixed and
    /// this function can be removed
    pub fn as_c_arr_mut_ptr(&self) -> *const *mut c_char {
        self.c_mut_coll.as_ptr()
    }
}

impl Default for StringCollection {
    fn default() -> Self {
        Self::from_data(StringCollectionData::default())
    }
}

impl Clone for StringCollection {
    fn clone(&self) -> Self {
        Self::from_data((*self.data).clone())
    }
}

impl TryFrom<StringCollection> for Vec<String> {
    type Error = std::ffi::IntoStringError;

    fn try_from(mut coll: StringCollection) -> std::result::Result<Self, Self::Error> {
        coll.data.coll.drain(..).map(CString::into_string).collect()
    }
}

impl TryFrom<&StringCollection> for Vec<String> {
    type Error = std::ffi::IntoStringError;

    fn try_from(coll: &StringCollection) -> std::result::Result<Self, Self::Error> {
        coll.data
            .coll
            .iter()
            .map(|s| s.clone().into_string())
            .collect()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! vec_of_strings {
        ($($x:expr),*) => (vec![$($x.to_string()),*]);
    }

    #[test]
    fn test_default() {
        let sc = StringCollection::default();
        assert_eq!(0, sc.len());
    }

    #[test]
    fn test_new() {
        let v = ["string0", "string1", "string2"];
        let n = v.len();

        let sc = StringCollection::new(&v);

        assert_eq!(n, sc.len());
        assert_eq!(n, sc.c_coll.len());
        assert_eq!(n, sc.c_mut_coll.len());
        assert_eq!(n, sc.data.coll.len());

        assert_eq!(v[0].as_bytes(), sc.data.coll[0].as_bytes());
        assert_eq!(v[1].as_bytes(), sc.data.coll[1].as_bytes());
        assert_eq!(v[2].as_bytes(), sc.data.coll[2].as_bytes());

        assert_eq!(sc.data.coll[0].as_ptr(), sc.c_coll[0]);
        assert_eq!(sc.data.coll[1].as_ptr(), sc.c_coll[1]);
        assert_eq!(sc.data.coll[2].as_ptr(), sc.c_coll[2]);
    }

    #[test]
    fn test_new_from_vec_strings() {
        let v = vec_of_strings!["string0", "string1", "string2"];
        let n = v.len();

        let sc = StringCollection::new(&v);

        assert_eq!(n, sc.len());
        assert_eq!(n, sc.c_coll.len());
        assert_eq!(n, sc.c_mut_coll.len());
        assert_eq!(n, sc.data.coll.len());

        assert_eq!(v[0].as_bytes(), sc.data.coll[0].as_bytes());
        assert_eq!(v[1].as_bytes(), sc.data.coll[1].as_bytes());
        assert_eq!(v[2].as_bytes(), sc.data.coll[2].as_bytes());

        assert_eq!(sc.data.coll[0].as_ptr(), sc.c_coll[0]);
        assert_eq!(sc.data.coll[1].as_ptr(), sc.c_coll[1]);
        assert_eq!(sc.data.coll[2].as_ptr(), sc.c_coll[2]);
    }

    #[test]
    fn test_assign() {
        let v = ["string0", "string1", "string2"];
        let n = v.len();

        let org_sc = StringCollection::new(&v);

        let sc = org_sc;

        assert_eq!(n, sc.len());
        assert_eq!(n, sc.c_coll.len());
        assert_eq!(n, sc.c_mut_coll.len());
        assert_eq!(n, sc.data.coll.len());

        assert_eq!(v[0].as_bytes(), sc.data.coll[0].as_bytes());
        assert_eq!(v[1].as_bytes(), sc.data.coll[1].as_bytes());
        assert_eq!(v[2].as_bytes(), sc.data.coll[2].as_bytes());

        assert_eq!(sc.data.coll[0].as_ptr(), sc.c_coll[0]);
        assert_eq!(sc.data.coll[1].as_ptr(), sc.c_coll[1]);
        assert_eq!(sc.data.coll[2].as_ptr(), sc.c_coll[2]);
    }

    #[test]
    fn test_clone() {
        let v = ["string0", "string1", "string2"];
        let n = v.len();

        let sc = {
            let org_sc = StringCollection::new(&v);
            org_sc.clone()
        };

        assert_eq!(n, sc.len());
        assert_eq!(n, sc.c_coll.len());
        assert_eq!(n, sc.c_mut_coll.len());
        assert_eq!(n, sc.data.coll.len());

        assert_eq!(v[0].as_bytes(), sc.data.coll[0].as_bytes());
        assert_eq!(v[1].as_bytes(), sc.data.coll[1].as_bytes());
        assert_eq!(v[2].as_bytes(), sc.data.coll[2].as_bytes());

        assert_eq!(sc.data.coll[0].as_ptr(), sc.c_coll[0]);
        assert_eq!(sc.data.coll[1].as_ptr(), sc.c_coll[1]);
        assert_eq!(sc.data.coll[2].as_ptr(), sc.c_coll[2]);
    }
}
