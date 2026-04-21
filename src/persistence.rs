// persistence.rs
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

//! MQTT client persistence options.
//!
//! The MQTT client can use persistence to store in-flight messages so they
//! can be recovered in the event of an application crash or restart.

use crate::errors::Result;
use std::{
    fmt,
    path::{Path, PathBuf},
};

/// Return value for a successful persistence operation
pub const PERSISTENCE_SUCCESS: i32 = 0;

/// Return value for a failed persistence operation
pub const PERSISTENCE_ERROR: i32 = -2;

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
/// file persistence to that directory.
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

/// A wrapper around a user-defined client persistence object.
pub struct UserPersistence(Box<ClientPersistenceBox>);

impl UserPersistence {
    /// Creates a new user persistence object.
    pub fn new(persistence: ClientPersistenceBox) -> Self {
        Self::from(persistence)
    }
}

impl From<Box<dyn ClientPersistence>> for UserPersistence {
    fn from(persistence: ClientPersistenceBox) -> Self {
        Self(Box::new(persistence))
    }
}
