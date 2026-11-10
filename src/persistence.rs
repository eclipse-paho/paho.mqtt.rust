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

use crate::error::{Error, Result};
use bytes::Bytes;
use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    fmt,
    io::Write,
    path::PathBuf,
    sync::Mutex,
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

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
/// required by the library when using persistence: creating a client with
/// `File` or `FilePath` and an empty ID fails with
/// [`Error::InvalidArgument`].
///
/// The default is [`None`](Self::None).
///
/// The persistence type can be created from any string, OS string, or path,
/// owned or by reference, to indicate file persistence to that directory.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Default)]
pub enum PersistenceType {
    /// Messages are persisted to files in the current working directory.
    File,
    /// Messages are persisted to files under the specified directory.
    FilePath(PathBuf),
    /// No persistence is used.
    #[default]
    None,
    /// A user-defined persistence provided by the application.
    ///
    /// Skipped by `serde`: the store is a trait object supplied in code, so
    /// it cannot be written out or read back. Serializing a value of this
    /// variant is an error rather than a silent downgrade to
    /// [`None`](Self::None); the other three round-trip.
    #[cfg_attr(feature = "serde", serde(skip))]
    User(UserPersistence),
}

impl fmt::Debug for PersistenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PersistenceType::*;
        match *self {
            File => write!(f, "File"),
            FilePath(ref path) => write!(f, "File with Path: {}", path.to_string_lossy()),
            None => write!(f, "None"),
            User(_) => write!(f, "User"),
        }
    }
}

impl<T: ?Sized + AsRef<OsStr>> From<&T> for PersistenceType {
    /// A reference to any string or path can be used to create a file path
    /// persistence.
    fn from(path: &T) -> Self {
        PersistenceType::FilePath(PathBuf::from(path.as_ref()))
    }
}

impl From<String> for PersistenceType {
    /// A string can be used to create a file path persistence.
    fn from(path: String) -> Self {
        PersistenceType::FilePath(PathBuf::from(path))
    }
}

impl From<OsString> for PersistenceType {
    /// An OS string can be used to create a file path persistence.
    fn from(path: OsString) -> Self {
        PersistenceType::FilePath(PathBuf::from(path))
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
    ///
    /// Called at the start of every connection attempt, always with the
    /// same arguments for a given client, so it must be safe to call again
    /// on a store that is already open.
    ///
    /// * `client_id` The unique client identifier.
    /// * `server_uri` The server URI the client was created with
    ///   ([`CreateOptionsBuilder::server_uri`](crate::CreateOptionsBuilder::server_uri)).
    ///   When the connect options list several servers, this is not the
    ///   server being connected to: the session, and so the stored
    ///   messages, goes with the client from one server to the next, so the
    ///   store must be the same for all of them.
    fn open(&mut self, client_id: &str, server_uri: &str) -> Result<()>;

    /// Close the persistence store.
    fn close(&mut self) -> Result<()>;

    /// Put data into the persistence store.
    /// * `key` The key to the data.
    /// * `buffers` The data to place into the store. Note that these can be
    ///   concatenated into a single, contiguous unit if helpful.
    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> Result<()>;

    /// Puts several entries, which a store may do more cheaply than one at
    /// a time — one directory flush, one transaction, one round trip.
    ///
    /// The default implementation calls [`put`](Self::put) for each in
    /// turn, so an existing store needs no change. The client batches
    /// whatever a burst of publishes queued together into one call.
    fn put_many(&mut self, items: Vec<(String, Vec<Bytes>)>) -> Result<()> {
        for (key, parts) in items {
            self.put(&key, parts.iter().map(|b| &b[..]).collect())?;
        }
        Ok(())
    }

    /// Whether this store's calls may block for long enough to matter.
    ///
    /// The default is `true`, which is the safe answer: the client then
    /// runs the store on a thread of its own, so that a slow disk cannot
    /// stall the event loop — nor, when the client borrows the
    /// application's runtime, one of its worker threads. A message is
    /// still stored before it goes on the wire; the client holds the bytes
    /// back until the store has it.
    ///
    /// Return `false` only for a store that is as quick as a memory write
    /// and cannot block, as [`MemPersistence`] does. It then runs inline on
    /// the event loop, with no thread and no hand-off.
    fn is_blocking(&self) -> bool {
        true
    }

    /// Gets data from the persistence store.
    /// * `key` They key for the desired data.
    fn get(&mut self, key: &str) -> Result<Vec<u8>>;

    /// Removes data for the specified key.
    /// * `key` The key for the data to remove.
    fn remove(&mut self, key: &str) -> Result<()>;

    /// Gets the keys that are currently in the persistence store.
    ///
    /// An error here fails a connect that has messages to restore, since an
    /// empty list would say that there are none.
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
///
/// The store is held behind a `Mutex` so that the wrapper — and therefore
/// [`CreateOptions`](crate::CreateOptions) and the client that retains them —
/// is `Sync` without requiring `ClientPersistence` implementations to be.
pub struct UserPersistence(Mutex<ClientPersistenceBox>);

impl UserPersistence {
    /// Creates a new user persistence object.
    pub fn new(persistence: ClientPersistenceBox) -> Self {
        Self::from(persistence)
    }
}

impl UserPersistence {
    /// Extracts the inner [`ClientPersistence`] implementation.
    pub fn into_inner(self) -> Box<dyn ClientPersistence> {
        self.0.into_inner().unwrap_or_else(|e| e.into_inner())
    }
}

impl From<Box<dyn ClientPersistence>> for UserPersistence {
    fn from(persistence: ClientPersistenceBox) -> Self {
        Self(Mutex::new(persistence))
    }
}

/////////////////////////////////////////////////////////////////////////////
//                          MemPersistence
/////////////////////////////////////////////////////////////////////////////

/// A persistence store held in memory.
///
/// In-flight messages already survive a dropped connection without any store
/// at all — the client carries them across a reconnect itself. A store is
/// what carries them across a *process*, so an in-memory one cannot do the
/// job [`FilePersistence`] does, and choosing it is close to choosing
/// [`PersistenceType::None`].
///
/// It is useful where a real store would be in the way: tests that want to
/// see what the client persisted, and applications that want the
/// bookkeeping without touching a filesystem — a read-only root, or a
/// platform where the client has nowhere to write.
///
/// ```
/// use paho_mqtt as mqtt;
///
/// let opts = mqtt::CreateOptionsBuilder::new()
///     .server_uri("tcp://localhost:1883")
///     .persistence(mqtt::MemPersistence::new());
/// # #[cfg(any(feature = "tokio", feature = "smol"))]
/// let cli = opts.create_async_client();
/// ```
#[derive(Debug, Default)]
pub struct MemPersistence {
    store: HashMap<String, Vec<u8>>,
}

impl MemPersistence {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ClientPersistence for MemPersistence {
    /// A map in this process: nothing to wait for, so the client keeps it
    /// on the event loop rather than giving it a thread.
    fn is_blocking(&self) -> bool {
        false
    }

    /// Nothing to open: the store is created empty and stays in this
    /// process, so it holds nothing from a previous run to reopen.
    fn open(&mut self, _client_id: &str, _server_uri: &str) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> Result<()> {
        self.store.insert(key.to_string(), buffers.concat());
        Ok(())
    }

    fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        self.store
            .get(key)
            .cloned()
            .ok_or_else(|| Error::Persistence(format!("key not found: {key}")))
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        self.store.remove(key);
        Ok(())
    }

    fn keys(&mut self) -> Result<Vec<String>> {
        Ok(self.store.keys().cloned().collect())
    }

    fn clear(&mut self) -> Result<()> {
        self.store.clear();
        Ok(())
    }

    fn contains_key(&mut self, key: &str) -> bool {
        self.store.contains_key(key)
    }
}

impl From<MemPersistence> for PersistenceType {
    fn from(p: MemPersistence) -> Self {
        PersistenceType::User(UserPersistence::new(Box::new(p)))
    }
}

/////////////////////////////////////////////////////////////////////////////
//                          FilePersistence
/////////////////////////////////////////////////////////////////////////////

/// The most bytes of the readable part of a store's directory name; the
/// name adds a `-` and 16 hex digits. Well inside the 255-byte limit of
/// common file systems, and the 143 of eCryptfs.
const STORE_NAME_MAX: usize = 100;

/// The name of the store directory for `client_id` and `server_uri`.
///
/// It is `<client_id>-<server_uri>-<hash>`: the two made safe for a path,
/// so a person can tell which store is which, and cut short if long; then
/// a hash of the two exactly as given, which is what tells stores apart.
/// Making a name safe loses information — `a/b` and `a_b` come out the
/// same, and the `-` between client and server cannot be told from one
/// within either — so without the hash two clients could share a store,
/// each restoring and publishing the other's messages and clearing its
/// store on a clean start.
fn store_dir_name(client_id: &str, server_uri: &str) -> String {
    let safe = |s: &str| -> String {
        s.chars()
            .map(|c| match c {
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
                c if c.is_control() => '_',
                c => c,
            })
            .collect()
    };
    let mut name = format!("{}-{}", safe(client_id), safe(server_uri));
    if name.len() > STORE_NAME_MAX {
        let mut end = STORE_NAME_MAX;
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        name.truncate(end);
    }

    // The ID's length first, so that no pair's bytes run into another's.
    let mut hash = Fnv1a::new();
    hash.write(&(client_id.len() as u64).to_le_bytes());
    hash.write(client_id.as_bytes());
    hash.write(server_uri.as_bytes());
    format!("{name}-{:016x}", hash.finish())
}

/// 64-bit FNV-1a. A store's name must come out the same from every build
/// of the library that will ever open it, which `std`'s hashers do not
/// promise across Rust releases.
struct Fnv1a(u64);

impl Fnv1a {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// File-system persistence store.
///
/// Stores each entry as a separate file under a subdirectory of the given
/// base directory, one per client. The subdirectory is named
/// `<client_id>-<server_uri>-<hash>`, where `server_uri` is the one the
/// client was created with. The first two are there to be read, with
/// characters invalid in path names replaced by underscores and cut short
/// if long; the hash, of the client ID and URI exactly as given, is what
/// keeps the stores of different clients apart. A client that connects to
/// several servers keeps one store for all of them; see
/// [`ClientPersistence::open`].
pub struct FilePersistence {
    /// Root directory supplied at construction time.
    base_dir: PathBuf,
    /// Active store directory; set by [`open`](Self::open).
    dir: PathBuf,
    /// Whether each batch is flushed to the disk before it is reported
    /// stored; see [`with_sync`](Self::with_sync).
    sync: bool,
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
            sync: false,
        }
    }

    /// Whether to flush each batch to the disk before reporting it stored.
    ///
    /// Off by default, which is what this store has always done: an entry
    /// is written and left in the kernel's page cache, so it survives the
    /// process dying but not the machine losing power. Turning it on makes
    /// a stored message durable in the stronger sense, at the cost of
    /// waiting for the disk — set it through
    /// [`CreateOptionsBuilder::persistence_sync`](crate::CreateOptionsBuilder::persistence_sync)
    /// rather than here, unless you are building the store yourself.
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    fn key_path(&self, key: &str) -> PathBuf {
        self.dir.join(key)
    }

    /// Writes one entry, returning the file so that a batch can flush them
    /// together afterwards.
    fn write_file<'a>(
        &self,
        key: &str,
        parts: impl Iterator<Item = &'a [u8]>,
    ) -> Result<std::fs::File> {
        let mut f = std::fs::File::create(self.key_path(key))?;
        for buf in parts {
            f.write_all(buf)?;
        }
        Ok(f)
    }
}

impl ClientPersistence for FilePersistence {
    fn open(&mut self, client_id: &str, server_uri: &str) -> Result<()> {
        self.dir = self.base_dir.join(store_dir_name(client_id, server_uri));
        std::fs::create_dir_all(&self.dir)?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> Result<()> {
        let f = self.write_file(key, buffers.into_iter())?;
        if self.sync {
            f.sync_all()?;
        }
        Ok(())
    }

    /// Writes every entry, then — with
    /// [`persistence_sync`](crate::CreateOptionsBuilder::persistence_sync)
    /// on — flushes them all to the disk. Doing the writes first lets the
    /// kernel start them together, so a batch costs one wait rather than
    /// one per message.
    fn put_many(&mut self, items: Vec<(String, Vec<Bytes>)>) -> Result<()> {
        let mut files = Vec::with_capacity(if self.sync { items.len() } else { 0 });
        for (key, parts) in items {
            let f = self.write_file(&key, parts.iter().map(|b| &b[..]))?;
            if self.sync {
                files.push(f);
            }
        }
        for f in files {
            f.sync_all()?;
        }
        Ok(())
    }

    fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        Ok(std::fs::read(self.key_path(key))?)
    }

    /// Removing a key that is not there succeeds. One `unlink`, rather than
    /// a check followed by a removal that another process could race.
    fn remove(&mut self, key: &str) -> Result<()> {
        match std::fs::remove_file(self.key_path(key)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
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
    use std::{collections::HashSet, path::Path};

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

    /// Only a missing key counts as removed: other failures are reported.
    #[test]
    fn test_remove_reports_other_errors() {
        let (mut p, _tmp) = open_temp();
        std::fs::create_dir(p.key_path("d")).expect("mkdir");
        assert!(p.remove("d").is_err(), "a directory cannot be unlinked");
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

    /// The hash is FNV-1a, checked against its published test vectors, so
    /// a store's name cannot drift between builds.
    #[test]
    fn test_fnv1a() {
        let hash = |b: &[u8]| {
            let mut h = Fnv1a::new();
            h.write(b);
            h.finish()
        };
        assert_eq!(hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(hash(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(hash(b"foobar"), 0x8594_4171_f739_67e8);
    }

    /// Names are readable, bounded, and different for every pair. Pairs
    /// that used to share a store — characters made safe the same way, or
    /// a `-` moved across the join — no longer do.
    #[test]
    fn test_store_dir_names() {
        assert_eq!(
            store_dir_name("cli", "tcp://localhost:1883"),
            "cli-tcp___localhost_1883-c02ea4fae6184a68"
        );
        for (a, b) in [
            (("a/b", "tcp://x"), ("a_b", "tcp://x")),
            (("a", "b-c"), ("a-b", "c")),
            (("cli", "tcp://h:1"), ("cli", "tcp://h_1")),
        ] {
            assert_ne!(
                store_dir_name(a.0, a.1),
                store_dir_name(b.0, b.1),
                "{a:?} {b:?}"
            );
        }

        // Long, and non-ASCII, IDs are cut short on a character boundary.
        for id in ["x".repeat(1000), "é".repeat(300), "\u{1}\n".repeat(10)] {
            let name = store_dir_name(&id, "tcp://localhost:1883");
            assert!(name.len() <= STORE_NAME_MAX + 17, "{}", name.len());
            assert!(!name.chars().any(|c| c.is_control() || c == '/'));
        }
        assert_ne!(
            store_dir_name(&"x".repeat(1000), "u"),
            store_dir_name(&"x".repeat(1001), "u"),
            "the cut-off part still counts"
        );
    }

    /// Two clients whose names used to collide keep separate stores, and a
    /// client ID far over the file-name limit still gets one.
    #[test]
    fn test_stores_kept_apart() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut a = FilePersistence::new(tmp.path());
        let mut b = FilePersistence::new(tmp.path());
        a.open("a", "b-c").unwrap();
        b.open("a-b", "c").unwrap();
        a.put("s1", vec![b"from a"]).unwrap();
        assert!(b.keys().unwrap().is_empty());
        b.put("s2", vec![b"from b"]).unwrap();
        b.clear().unwrap();
        assert_eq!(a.keys().unwrap(), ["s1"]);

        let mut long = FilePersistence::new(tmp.path());
        long.open(&"x".repeat(1000), "tcp://localhost:1883")
            .unwrap();
        long.put("s1", vec![b"data"]).unwrap();
        assert_eq!(long.get("s1").unwrap(), b"data");
    }

    // ---- MemPersistence ----

    #[test]
    fn test_mem_persistence_round_trip() {
        let mut p = MemPersistence::new();
        p.open("client", "tcp://localhost:1883").unwrap();

        // Buffers are stored concatenated, as the runtime writes a packet
        // header and payload separately.
        p.put("s1", vec![b"head".as_slice(), b"tail".as_slice()])
            .unwrap();
        assert_eq!(p.get("s1").unwrap(), b"headtail");
        assert!(p.contains_key("s1"));
        assert_eq!(p.keys().unwrap(), vec!["s1".to_string()]);

        p.remove("s1").unwrap();
        assert!(!p.contains_key("s1"));
        assert!(p.get("s1").is_err());

        p.put("s2", vec![b"x".as_slice()]).unwrap();
        p.clear().unwrap();
        assert!(p.keys().unwrap().is_empty());
        p.close().unwrap();
    }

    #[test]
    fn test_strings_and_paths_are_file_paths() {
        let string = String::from("some/dir");
        let os_string = OsString::from("some/dir");
        let path_buf = PathBuf::from("some/dir");

        for ty in [
            PersistenceType::from("some/dir"),
            PersistenceType::from(&string),
            PersistenceType::from(os_string.as_os_str()),
            PersistenceType::from(&os_string),
            PersistenceType::from(path_buf.as_path()),
            PersistenceType::from(&path_buf),
            PersistenceType::from(string),
            PersistenceType::from(os_string),
            PersistenceType::from(path_buf),
        ] {
            assert!(matches!(ty, PersistenceType::FilePath(p) if p == Path::new("some/dir")));
        }
    }

    #[test]
    fn test_mem_persistence_is_a_persistence_type() {
        let pt: PersistenceType = MemPersistence::new().into();
        assert!(matches!(pt, PersistenceType::User(_)));
    }

    /// The three configurable persistence choices round-trip.
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_round_trip() {
        for ty in [
            PersistenceType::None,
            PersistenceType::File,
            PersistenceType::FilePath(PathBuf::from("/var/mqtt")),
        ] {
            let json = serde_json::to_string(&ty).unwrap();
            let back: PersistenceType = serde_json::from_str(&json).unwrap();
            assert_eq!(format!("{back:?}"), format!("{ty:?}"), "{json}");
        }
    }

    /// A user-supplied store is a trait object, so it cannot be written out.
    /// Serializing says so rather than quietly becoming `None`, which would
    /// turn "persist through my own store" into "do not persist".
    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_user_persistence_cannot_be_written() {
        let ty = PersistenceType::from(MemPersistence::new());
        let err = serde_json::to_string(&ty).expect_err("a trait object has no representation");
        assert!(err.to_string().contains("cannot be serialized"), "{err}");
    }
}
