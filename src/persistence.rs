// persistence.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//

/*******************************************************************************
 * Copyright (c) 2017-2024 Frank Pagliughi <fpagliughi@mindspring.com>
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

//! MQTT client persistence options.
//!
//! The Paho MQTT client can use persistence to store in-flight messages out
//! of process to be recovered in the event of an application crash or
//! restart. The client has built-in support for storing messages in the
//! local file system, but also allows the user to define their own strategy.
//! This can be done with anything that can be used as a key/value store.
//!

use std::{
    ffi::{CStr, CString},
    fmt, mem,
    os::raw::{c_char, c_int, c_void},
    path::{Path, PathBuf},
    ptr, slice,
};

use crate::{errors::Result, ffi, to_c_bool};

// TODO: Should we have a specific PersistenceResult/Error?

/// Return value for a successful persistence operation
pub const PERSISTENCE_SUCCESS: c_int = ffi::MQTTASYNC_SUCCESS as c_int;

/// Return value for a failed persistence operation
pub const PERSISTENCE_ERROR: c_int = ffi::MQTTCLIENT_PERSISTENCE_ERROR;

/// A boxed Client Persistence
pub type ClientPersistenceBox = Box<dyn ClientPersistence>;

/////////////////////////////////////////////////////////////////////////////
//                          PersistenceType
/////////////////////////////////////////////////////////////////////////////

/// The type of persistence for the client.
///
/// There is a built-in mechanism for using the file system to persist
/// messages and data. The `File` selection will use that in the current
/// working directory (CWD). A path can also be specified, with `FilePath`
/// and then the persistence store will be created under that directory.
/// Each client will create a directory under that using the MQTT client
/// ID and server URL to name the directory. Therefore, a client ID is
/// required by the library when using persistence.
///
/// The persistence type can be created from any string or path to indicate
/// file persistence to that directory. If a directory with that name can't
/// be found or created by the library, then a persistence error is returned
/// when attempting to create the MQTT client object.
#[derive(Default)]
pub enum PersistenceType {
    /// Messages are persisted to files in a local directory (default).
    File,
    /// Messages are persisted to files under the specified directory.
    FilePath(PathBuf),
    /// No persistence is used.
    #[default]
    None,
    /// A user-defined persistence provided by the application.
    User(UserPersistence),
}

impl fmt::Debug for PersistenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PersistenceType::*;
        match *self {
            File => write!(f, "File"),
            FilePath(_) => write!(f, "File with Path"),
            None => write!(f, "None"),
            User(_) => write!(f, "User"),
        }
    }
}

impl From<&str> for PersistenceType {
    /// A string slice can be used to create a file path persistence.
    fn from(path: &str) -> Self {
        PersistenceType::FilePath(PathBuf::from(path))
    }
}

impl From<String> for PersistenceType {
    /// A string can be used to create a file path persistence.
    fn from(path: String) -> Self {
        PersistenceType::FilePath(PathBuf::from(path))
    }
}

impl From<&Path> for PersistenceType {
    /// A path can be used to create a file path persistence.
    fn from(path: &Path) -> Self {
        PersistenceType::FilePath(path.into())
    }
}

impl From<PathBuf> for PersistenceType {
    /// A path buffer can be used to create a file path persistence.
    fn from(path: PathBuf) -> Self {
        PersistenceType::FilePath(path)
    }
}

impl From<Option<PersistenceType>> for PersistenceType {
    fn from(opt: Option<PersistenceType>) -> Self {
        match opt {
            Some(typ) => typ,
            None => PersistenceType::None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
//                       ClientPersistence trait
/////////////////////////////////////////////////////////////////////////////

/// Trait to implement custom persistence in the client.
pub trait ClientPersistence: Send {
    /// Open and initialize the persistent store.
    /// * `client_id` The unique client identifier.
    /// * `server_uri` The address of the server to which the client is
    ///   connected.
    fn open(&mut self, client_id: &str, server_uri: &str) -> Result<()>;

    /// Close the persistence store.
    fn close(&mut self) -> Result<()>;

    /// Put data into the persistence store.
    /// * `key` The key to the data.
    /// * `buffers` The data to place into the store. Note that these can be
    ///   concatenated into a single, contiguous unit if helpful.
    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> Result<()>;

    /// Gets data from the persistence store.
    /// * `key` They key for the desired data.
    fn get(&mut self, key: &str) -> Result<Vec<u8>>;

    /// Removes data for the specified key.
    /// * `key` The key for the data to remove.
    fn remove(&mut self, key: &str) -> Result<()>;

    /// Gets the keys that are currently in the persistence store
    fn keys(&mut self) -> Result<Vec<String>>;

    /// Clear the persistence store so that it no longer contains any data.
    fn clear(&mut self) -> Result<()>;

    /// Determines if the persistence store contains the key.
    /// * `key` The key to look for.
    fn contains_key(&mut self, key: &str) -> bool;
}

/////////////////////////////////////////////////////////////////////////////
//                          UserPersistence
/////////////////////////////////////////////////////////////////////////////

/// A struct to wrap the user-defined client persistence objects for the
/// C library, including the callback funtions from the C library.
/// These functions receive the persistence callbacks from the C library and
/// then pass them on to the user-supplied struct which implements the
/// ClientPersistence trait.
///
/// Note that the C library _does not_ keep a copy of the
/// MQTTClient_persistence object, so the client must keep one and keep it
/// at a consistent address. Thus it should be kept in a box on the heap.
pub struct UserPersistence(Box<ClientPersistenceBox>);

impl UserPersistence {
    /// Creates a new user persistence object.
    pub fn new(persistence: ClientPersistenceBox) -> Self {
        Self::from(persistence)
    }

    /// Gets the FFI C structure for this object.
    /// TODO: Figure out how to mark this with a lifetime matched to self
    pub(crate) fn c_struct(&self) -> ffi::MQTTClient_persistence {
        let context =
            &*self.0 as *const ClientPersistenceBox as *mut ClientPersistenceBox as _;

        ffi::MQTTClient_persistence {
            context,
            popen: Some(Self::on_open),
            pclose: Some(Self::on_close),
            pput: Some(Self::on_put),
            pget: Some(Self::on_get),
            premove: Some(Self::on_remove),
            pkeys: Some(Self::on_keys),
            pclear: Some(Self::on_clear),
            pcontainskey: Some(Self::on_contains_key),
        }
    }

    /// Callback from the C library to open the persistence store.
    /// On entry, the 'context' has the address of the user's persistence
    /// object which is reassigned to the 'handle'.
    /// Subsequent calls have the object address as the handle.
    ///
    /// # Safety
    ///
    /// This is a callback directly from the C library. All parameters
    /// parameters must be checked for validity (NULL pointers, etc)
    ///
    pub(crate) unsafe extern "C" fn on_open(
        handle: *mut *mut c_void,
        client_id: *const c_char,
        server_uri: *const c_char,
        context: *mut c_void,
    ) -> c_int {
        trace!("UserPersistence::on_open");
        if !handle.is_null()
            && !client_id.is_null()
            && !server_uri.is_null()
            && !context.is_null()
        {
            let client_id = CStr::from_ptr(client_id).to_str().unwrap();
            let server_uri = CStr::from_ptr(server_uri).to_str().unwrap();

            let persist = &mut *(context as *mut ClientPersistenceBox);

            if persist.open(client_id, server_uri).is_ok() {
                *handle = context;
                return PERSISTENCE_SUCCESS;
            }
        }
        PERSISTENCE_ERROR
    }

    /// Callback from the C library to close the persistence store.
    ///
    /// # Safety
    ///
    /// This is a callback directly from the C library. All parameters
    /// parameters must be checked for validity (NULL pointers, etc)
    ///
    pub(crate) unsafe extern "C" fn on_close(handle: *mut c_void) -> c_int {
        trace!("UserPersistence::on_close");
        if handle.is_null() {
            return PERSISTENCE_ERROR;
        }

        let persist = &mut *(handle as *mut ClientPersistenceBox);

        match persist.close() {
            Ok(_) => PERSISTENCE_SUCCESS,
            Err(_) => PERSISTENCE_ERROR,
        }
    }

    /// Callback from the C library to add data to the persistence store.
    ///
    /// # Safety
    ///
    /// This is a callback directly from the C library. All parameters
    /// parameters must be checked for validity (NULL pointers, etc)
    ///
    pub(crate) unsafe extern "C" fn on_put(
        handle: *mut c_void,
        key: *mut c_char,
        bufcount: c_int,
        buffers: *mut *mut c_char,
        buflens: *mut c_int,
    ) -> c_int {
        trace!("UserPersistence::on_put");
        if handle.is_null() || key.is_null() || buffers.is_null() || buflens.is_null() {
            return PERSISTENCE_ERROR;
        }
        if bufcount == 0 {
            return PERSISTENCE_SUCCESS;
        }
        let persist = &mut *(handle as *mut ClientPersistenceBox);
        let key = CStr::from_ptr(key).to_str().unwrap();

        let mut bufs: Vec<&[u8]> = Vec::new();

        for i in 0..bufcount as isize {
            let buf = slice::from_raw_parts_mut(
                *buffers.offset(i) as *mut u8,
                *buflens.offset(i) as usize,
            );
            bufs.push(buf);
        }
        match persist.put(key, bufs) {
            Ok(_) => PERSISTENCE_SUCCESS,
            Err(_) => PERSISTENCE_ERROR,
        }
    }

    /// Callback from the C library to retrieve data from the
    /// persistence store.
    ///
    /// # Safety
    ///
    /// This is a callback directly from the C library. All parameters
    /// parameters must be checked for validity (NULL pointers, etc)
    ///
    pub(crate) unsafe extern "C" fn on_get(
        handle: *mut c_void,
        key: *mut c_char,
        buffer: *mut *mut c_char,
        buflen: *mut c_int,
    ) -> c_int {
        trace!("UserPersistence::on_get");
        if handle.is_null() || key.is_null() || buffer.is_null() || buflen.is_null() {
            return PERSISTENCE_ERROR;
        }
        let persist = &mut *(handle as *mut ClientPersistenceBox);
        let key = CStr::from_ptr(key).to_str().unwrap();

        match persist.get(key) {
            Ok(buf) => {
                // buf: Vec<u8>
                let n = buf.len();
                let cbuf = libc::malloc(n) as *mut u8;
                ptr::copy(buf.as_ptr(), cbuf, n);
                *buffer = cbuf as *mut c_char;
                *buflen = n as c_int;
                PERSISTENCE_SUCCESS
            }
            Err(_) => PERSISTENCE_ERROR,
        }
    }

    /// Callback from the C library to delete specific data from the
    /// persistence store.
    ///
    /// # Safety
    ///
    /// This is a callback directly from the C library. All parameters
    /// parameters must be checked for validity (NULL pointers, etc)
    ///
    pub(crate) unsafe extern "C" fn on_remove(
        handle: *mut c_void,
        key: *mut c_char,
    ) -> c_int {
        trace!("UserPersistence::on_remove");
        if handle.is_null() || key.is_null() {
            return PERSISTENCE_ERROR;
        }
        let persist = &mut *(handle as *mut ClientPersistenceBox);
        let key = CStr::from_ptr(key).to_str().unwrap();

        match persist.remove(key) {
            Ok(_) => PERSISTENCE_SUCCESS,
            Err(_) => PERSISTENCE_ERROR,
        }
    }

    /// Callback from the C library to retrieve the set of keys from the
    /// persistence store.
    ///
    /// # Safety
    ///
    /// This is a callback directly from the C library. All parameters
    /// parameters must be checked for validity (NULL pointers, etc)
    ///
    pub(crate) unsafe extern "C" fn on_keys(
        handle: *mut c_void,
        keys: *mut *mut *mut c_char,
        nkeys: *mut c_int,
    ) -> c_int {
        trace!("UserPersistence::on_keys");
        if handle.is_null() || keys.is_null() || nkeys.is_null() {
            return PERSISTENCE_ERROR;
        }

        let persist = &mut *(handle as *mut ClientPersistenceBox);

        *keys = ptr::null_mut();
        *nkeys = 0;

        match persist.keys() {
            Ok(k) => {
                // k: Vec<String>
                let n = k.len();
                if n != 0 {
                    // TODO OPTIMIZE: This does a lot of copying
                    let ckeys =
                        libc::malloc(n * mem::size_of::<usize>()) as *mut *mut c_char;
                    for (i, s) in k.into_iter().enumerate() {
                        let s = CString::new(s).unwrap();
                        let sb = s.as_bytes_with_nul();
                        let sn = sb.len();
                        let cbuf = libc::malloc(sn) as *mut c_char;
                        ptr::copy(sb.as_ptr(), cbuf as *mut u8, sn);

                        *ckeys.add(i) = cbuf;
                    }
                    *keys = ckeys;
                    *nkeys = n as c_int;
                }
                PERSISTENCE_SUCCESS
            }
            Err(_) => PERSISTENCE_ERROR,
        }
    }

    /// Callback from the C library to remove all the data from the
    /// persistence store.
    ///
    /// # Safety
    ///
    /// This is a callback directly from the C library. All parameters
    /// parameters must be checked for validity (NULL pointers, etc)
    ///
    pub(crate) unsafe extern "C" fn on_clear(handle: *mut c_void) -> c_int {
        trace!("UserPersistence::on_clear");
        if handle.is_null() {
            return PERSISTENCE_ERROR;
        }
        let persist = &mut *(handle as *mut ClientPersistenceBox);

        match persist.clear() {
            Ok(_) => PERSISTENCE_SUCCESS,
            Err(_) => PERSISTENCE_ERROR,
        }
    }

    /// Callback from the C library to determine if the store contains
    /// the specified key.
    ///
    /// # Safety
    ///
    /// This is a callback directly from the C library. All parameters
    /// must be checked for validity (NULL pointers, etc)
    ///
    pub(crate) unsafe extern "C" fn on_contains_key(
        handle: *mut c_void,
        key: *mut c_char,
    ) -> c_int {
        trace!("UserPersistence::on_contains_key");
        if handle.is_null() || key.is_null() {
            return PERSISTENCE_ERROR;
        }

        match CStr::from_ptr(key).to_str() {
            Ok(key) => {
                let persist = &mut *(handle as *mut ClientPersistenceBox);
                to_c_bool(persist.contains_key(key))
            }
            Err(_) => PERSISTENCE_ERROR,
        }
    }
}

impl From<Box<dyn ClientPersistence>> for UserPersistence {
    fn from(persistence: ClientPersistenceBox) -> Self {
        Self(Box::new(persistence))
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    /*
    use super::*;
    //use std::ffi::{CStr};

    struct TestClientPersistence;

    impl ClientPersistence for TestClientPersistence {
        fn open(&self, client_id: &str, server_uri: &str) -> Result<()> {
            Ok(())
        }

        fn close(&self) -> Result<()> {
            Ok(())
        }

        fn clear(&self) -> Result<()> {
            Ok(())
        }

        fn put(&self, key: &str, buffers: Vec<&[u8]>) -> Result<()> {
            Ok(())
        }

        fn get(&self, key: &str) -> Result<&[u8]> {
            let x = b"Bubba";   //: &'static [u8] = &'static [ 0u8, 1u8, 2u8, 3u8 ];
            Ok(x)
        }

        fn remove(&self, key: &str) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_new() {
        let tcp = TestClientPersistence {};
        let tcpp = Box::new(Box::new(tcp));
        let context = Box::into_raw(tcpp);

        let persist: &mut Box<ClientPersistence> = unsafe { mem::transmute(context) };
        /*
        let persist: &mut Box<ClientPersistence> = unsafe { mem::transmute(context) };
        let res = persist.open("clientid", "tcp://localhost:1883");
        assert!(res.is_ok());
        */

        let _ = unsafe { Box::from_raw(context) };
    }
    */
}
