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
    io::Write,
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

impl UserPersistence {
    /// Extracts the inner [`ClientPersistence`] implementation.
    pub fn into_inner(self) -> Box<dyn ClientPersistence> {
        *self.0
    }
}

impl From<Box<dyn ClientPersistence>> for UserPersistence {
    fn from(persistence: ClientPersistenceBox) -> Self {
        Self(Box::new(persistence))
    }
}

/////////////////////////////////////////////////////////////////////////////
//                          FilePersistence
/////////////////////////////////////////////////////////////////////////////

/// Sanitizes a string for use as a directory-name component.
fn sanitize_path_component(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c => c,
        })
        .collect()
}

/// File-system persistence store.
///
/// Stores each entry as a separate file under a subdirectory of the given
/// base directory. The subdirectory is named
/// `<client_id>-<server_uri>` (with characters invalid in path names
/// replaced by underscores), so a separate store is maintained for each
/// unique client/server pair.
pub struct FilePersistence {
    /// Root directory supplied at construction time.
    base_dir: PathBuf,
    /// Active store directory; set by [`open`](Self::open).
    dir: PathBuf,
}

impl FilePersistence {
    /// Creates a new `FilePersistence` instance rooted at `base_dir`.
    ///
    /// The actual storage directory is not created until
    /// [`open`](ClientPersistence::open) is called.
    pub fn new<P: Into<PathBuf>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.into(),
            dir: PathBuf::new(),
        }
    }

    fn key_path(&self, key: &str) -> PathBuf {
        self.dir.join(key)
    }
}

impl ClientPersistence for FilePersistence {
    fn open(&mut self, client_id: &str, server_uri: &str) -> Result<()> {
        let dir_name = format!(
            "{}-{}",
            sanitize_path_component(client_id),
            sanitize_path_component(server_uri)
        );
        self.dir = self.base_dir.join(dir_name);
        std::fs::create_dir_all(&self.dir)?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> Result<()> {
        let mut f = std::fs::File::create(self.key_path(key))?;
        for buf in buffers {
            f.write_all(buf)?;
        }
        Ok(())
    }

    fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        Ok(std::fs::read(self.key_path(key))?)
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        let path = self.key_path(key);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn keys(&mut self) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                keys.push(name.to_string());
            }
        }
        Ok(keys)
    }

    fn clear(&mut self) -> Result<()> {
        for entry in std::fs::read_dir(&self.dir)? {
            std::fs::remove_file(entry?.path())?;
        }
        Ok(())
    }

    fn contains_key(&mut self, key: &str) -> bool {
        self.key_path(key).exists()
    }
}

/////////////////////////////////////////////////////////////////////////////
//                              Unit Tests
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Opens a `FilePersistence` in a fresh temp directory and returns it
    /// together with the temp dir handle (dropped at end of test).
    fn open_temp() -> (FilePersistence, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut p = FilePersistence::new(tmp.path());
        p.open("test_client", "tcp://localhost:1883").expect("open");
        (p, tmp)
    }

    #[test]
    fn test_put_get_roundtrip() {
        let (mut p, _tmp) = open_temp();
        p.put("key1", vec![b"hello", b" world"]).expect("put");
        let got = p.get("key1").expect("get");
        assert_eq!(got, b"hello world");
    }

    #[test]
    fn test_contains_key() {
        let (mut p, _tmp) = open_temp();
        assert!(!p.contains_key("k"));
        p.put("k", vec![b"v"]).expect("put");
        assert!(p.contains_key("k"));
    }

    #[test]
    fn test_remove() {
        let (mut p, _tmp) = open_temp();
        p.put("k", vec![b"v"]).expect("put");
        assert!(p.contains_key("k"));
        p.remove("k").expect("remove");
        assert!(!p.contains_key("k"));
        // Removing a non-existent key must not error.
        p.remove("k").expect("remove missing");
    }

    #[test]
    fn test_keys() {
        let (mut p, _tmp) = open_temp();
        p.put("a", vec![b"1"]).expect("put a");
        p.put("b", vec![b"2"]).expect("put b");
        p.put("c", vec![b"3"]).expect("put c");
        let keys: HashSet<String> = p.keys().expect("keys").into_iter().collect();
        assert!(keys.contains("a"));
        assert!(keys.contains("b"));
        assert!(keys.contains("c"));
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_clear() {
        let (mut p, _tmp) = open_temp();
        p.put("x", vec![b"1"]).expect("put");
        p.put("y", vec![b"2"]).expect("put");
        p.clear().expect("clear");
        assert!(p.keys().expect("keys").is_empty());
    }

    #[test]
    fn test_close_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut p = FilePersistence::new(tmp.path());
        p.open("cli", "tcp://localhost:1883").expect("open");
        p.put("persist_me", vec![b"data"]).expect("put");
        p.close().expect("close");

        // Re-open should see the persisted data.
        let mut p2 = FilePersistence::new(tmp.path());
        p2.open("cli", "tcp://localhost:1883").expect("reopen");
        assert_eq!(p2.get("persist_me").expect("get"), b"data");
    }
}
