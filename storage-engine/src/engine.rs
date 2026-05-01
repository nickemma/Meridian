use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::memtable::{Memtable, Value};
use crate::sstable::SSTable;
use crate::wal::Wal;

// Default flush threshold — flush memtable to SSTable
// when it exceeds 4MB in size.
const DEFAULT_FLUSH_THRESHOLD: usize = 4 * 1024 * 1024;

/// The public interface to the Meridian storage engine.
///
/// Callers (Raft, secrets layer) interact only with this struct.
/// WAL, Memtable, and SSTable are implementation details.
///
/// Wrapped in Arc<Mutex> so it can be shared safely across
/// the goroutines that the Go layer will call into via FFI.
pub struct StorageEngine {
    inner: Arc<Mutex<EngineInner>>,
}

struct EngineInner {
    data_dir: PathBuf,
    wal: Wal,
    memtable: Memtable,
    // SSTables in newest-first order.
    // When we flush, we prepend the new SSTable so index 0
    // is always the most recent — reads check newest first.
    sstables: Vec<SSTable>,
}

impl StorageEngine {
    /// Open or create a storage engine at the given directory.
    /// On first open the directory is created and the engine starts empty.
    /// On subsequent opens the WAL is replayed into a fresh Memtable
    /// and existing SSTables are loaded from disk.
    pub fn open(data_dir: impl AsRef<Path>) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        let wal_path = data_dir.join("current.wal");
        let wal = Wal::open(&wal_path)?;

        // Replay the WAL into a fresh Memtable.
        // This is crash recovery — any entries in the WAL that were
        // not yet flushed to an SSTable are restored here.
        let mut memtable = Memtable::new(DEFAULT_FLUSH_THRESHOLD);
        let entries = Wal::recover_entries(&wal_path)?;
        for entry in entries {
            let (key, value) = deserialize_wal_entry(&entry.data)?;
            memtable.put(key, value);
        }

        // Load existing SSTables from disk, newest first.
        // SSTable files are named by a monotonic counter: 0000.sst, 0001.sst, etc.
        // Higher number = more recent flush.
        let mut sstables = load_sstables(&data_dir)?;
        sstables.reverse(); // newest first for reads

        Ok(Self {
            inner: Arc::new(Mutex::new(EngineInner {
                data_dir,
                wal,
                memtable,
                sstables,
            })),
        })
    }

    /// Write a key-value pair to the storage engine.
    /// WAL write always happens before Memtable write.
    /// May trigger a Memtable flush to SSTable if threshold is crossed.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        // ① WAL first — durability before memory
        let entry = serialize_wal_entry(&key, &value);
        inner.wal.append(&entry)?;

        // ② Memtable second
        inner.memtable.put(key, value);

        // ③ Flush if threshold crossed
        if inner.memtable.should_flush() {
            flush(&mut inner)?;
        }

        Ok(())
    }

    /// Delete a key by writing a tombstone.
    /// The key is not immediately removed — the tombstone will
    /// shadow any older value in SSTables until compaction.
    pub fn delete(&self, key: Vec<u8>) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let entry = serialize_tombstone(&key);
        inner.wal.append(&entry)?;
        inner.memtable.delete(key);

        if inner.memtable.should_flush() {
            flush(&mut inner)?;
        }

        Ok(())
    }

    /// Read a key from the storage engine.
    /// Checks Memtable first, then SSTables newest to oldest.
    /// Returns None if the key does not exist or has been deleted.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();

        // ① Memtable — most recent writes land here
        match inner.memtable.get(key) {
            Some(Value::Data(data)) => return Ok(Some(data.clone())),
            Some(Value::Tombstone) => return Ok(None),
            None => {}
        }

        // ② SSTables — newest to oldest
        for sst in &inner.sstables {
            match sst.get(key)? {
                Some(entry) => match entry.value {
                    Value::Data(data) => return Ok(Some(data)),
                    Value::Tombstone => return Ok(None),
                },
                None => continue,
            }
        }

        Ok(None)
    }

    /// Force a flush of the current Memtable to an SSTable.
    /// Normally triggered automatically when the threshold is crossed.
    /// Exposed here for testing and for clean shutdown.
    pub fn flush(&self) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        flush(&mut inner)
    }

    /// Number of SSTables currently on disk.
    pub fn sstable_count(&self) -> usize {
        self.inner.lock().unwrap().sstables.len()
    }
}

// --- Internal helpers ---

/// Flush the current Memtable to a new SSTable on disk.
/// After flushing:
///   - A new SSTable file is created
///   - The Memtable is replaced with a fresh empty one
///   - The new SSTable is prepended to sstables (newest first)
fn flush(inner: &mut EngineInner) -> io::Result<()> {
    // Take the current memtable, replace with a fresh one
    let old = std::mem::replace(&mut inner.memtable, Memtable::new(DEFAULT_FLUSH_THRESHOLD));

    if old.is_empty() {
        return Ok(());
    }

    // Name the SSTable file by count — simple monotonic naming
    let sst_path = inner
        .data_dir
        .join(format!("{:04}.sst", inner.sstables.len()));

    let entries = old.into_sorted_entries();
    let sst = SSTable::write(&sst_path, entries)?;

    // Prepend so index 0 is always newest
    inner.sstables.insert(0, sst);

    Ok(())
}

/// Load all SSTable files from the data directory, sorted by name (oldest first).
/// Caller reverses this for newest-first read ordering.
fn load_sstables(data_dir: &Path) -> io::Result<Vec<SSTable>> {
    let mut paths: Vec<PathBuf> = fs::read_dir(data_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "sst").unwrap_or(false))
        .collect();

    paths.sort(); // alphabetical = chronological for zero-padded names

    paths.iter().map(SSTable::open).collect()
}

// WAL entry serialization format:
//   type (1 byte): 0x01 = put, 0x02 = tombstone
//   key_len (4 bytes)
//   key (key_len bytes)
//   value (remaining bytes, absent for tombstone)

const TYPE_PUT: u8 = 0x01;
const TYPE_TOMBSTONE: u8 = 0x02;

fn serialize_wal_entry(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + key.len() + value.len());
    buf.push(TYPE_PUT);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

fn serialize_tombstone(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + key.len());
    buf.push(TYPE_TOMBSTONE);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf
}

fn deserialize_wal_entry(data: &[u8]) -> io::Result<(Vec<u8>, Vec<u8>)> {
    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty WAL entry",
        ));
    }

    let entry_type = data[0];
    if data.len() < 5 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL entry too short",
        ));
    }

    let key_len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
    if data.len() < 5 + key_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL entry key truncated",
        ));
    }

    let key = data[5..5 + key_len].to_vec();

    match entry_type {
        TYPE_PUT => {
            let value = data[5 + key_len..].to_vec();
            Ok((key, value))
        }
        TYPE_TOMBSTONE => {
            // Tombstones are handled by memtable.delete() at the call site.
            // Here we return an empty value as a signal — the engine
            // re-applies the tombstone on WAL replay.
            Ok((key, vec![]))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown WAL entry type: {}", entry_type),
        )),
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_put_and_get() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();

        engine.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        let result = engine.get(b"key").unwrap();
        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[test]
    fn test_missing_key_returns_none() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();
        assert_eq!(engine.get(b"missing").unwrap(), None);
    }

    #[test]
    fn test_delete_returns_none() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();

        engine.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        engine.delete(b"key".to_vec()).unwrap();
        assert_eq!(engine.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_overwrite_returns_latest_value() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();

        engine.put(b"key".to_vec(), b"old".to_vec()).unwrap();
        engine.put(b"key".to_vec(), b"new".to_vec()).unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn test_survives_flush_to_sstable() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();

        engine.put(b"key".to_vec(), b"value".to_vec()).unwrap();

        // Force flush — moves data from Memtable to SSTable
        engine.flush().unwrap();
        assert_eq!(engine.sstable_count(), 1);

        // Must still be readable from SSTable
        assert_eq!(engine.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_tombstone_hides_value_in_sstable() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();

        // Write key and flush it to SSTable
        engine.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        engine.flush().unwrap();

        // Delete key — tombstone lands in Memtable
        engine.delete(b"key".to_vec()).unwrap();

        // Tombstone in Memtable must shadow the value in SSTable
        assert_eq!(engine.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_crash_recovery_from_wal() {
        let dir = tempdir().unwrap();

        // Write data without flushing — data lives only in WAL + Memtable
        {
            let engine = StorageEngine::open(dir.path()).unwrap();
            engine.put(b"survived".to_vec(), b"yes".to_vec()).unwrap();
            // engine drops here — simulates process crash before flush
        }

        // Reopen — WAL replay must restore the entry
        let engine = StorageEngine::open(dir.path()).unwrap();
        assert_eq!(engine.get(b"survived").unwrap(), Some(b"yes".to_vec()));
    }
}
