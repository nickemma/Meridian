use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::memtable::Value;

// u32::MAX is our on-disk tombstone marker.
// A value_len of u32::MAX means "this key was deleted".
const TOMBSTONE_MARKER: u32 = u32::MAX;

/// A single entry read back from an SSTable.
#[derive(Debug, Clone)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub value: Value,
}

/// An index entry — maps a key to its byte offset in the SSTable file.
/// Kept in memory after the SSTable is opened so we can binary search
/// for keys without reading the whole file.
#[derive(Debug, Clone)]
struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
}

/// A handle to an SSTable file on disk.
/// Once written, the file is never modified.
/// The index is loaded into memory on open for fast point reads.
pub struct SSTable {
    path: PathBuf,
    index: Vec<IndexEntry>, // sorted by key, loaded once on open
}

impl SSTable {
    /// Write a new SSTable from a sorted list of entries.
    /// The entries MUST be sorted by key — callers use
    /// Memtable::into_sorted_entries() which guarantees this.
    pub fn write(path: impl AsRef<Path>, entries: Vec<(Vec<u8>, Value)>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let mut writer = BufWriter::new(file);

        let mut index: Vec<IndexEntry> = Vec::with_capacity(entries.len());
        let mut offset: u64 = 0;

        // Write each entry, recording its offset in the index.
        for (key, value) in entries {
            index.push(IndexEntry {
                key: key.clone(),
                offset,
            });

            let key_len = key.len() as u32;

            match &value {
                Value::Data(data) => {
                    let value_len = data.len() as u32;
                    writer.write_all(&key_len.to_le_bytes())?;
                    writer.write_all(&value_len.to_le_bytes())?;
                    writer.write_all(&key)?;
                    writer.write_all(data)?;
                    offset += 4 + 4 + key.len() as u64 + data.len() as u64;
                }
                Value::Tombstone => {
                    // value_len = u32::MAX signals tombstone on disk.
                    // No value bytes follow.
                    writer.write_all(&key_len.to_le_bytes())?;
                    writer.write_all(&TOMBSTONE_MARKER.to_le_bytes())?;
                    writer.write_all(&key)?;
                    offset += 4 + 4 + key.len() as u64;
                }
            }
        }

        // Write the index after all entries.
        let index_offset = offset;
        for entry in &index {
            let key_len = entry.key.len() as u32;
            writer.write_all(&key_len.to_le_bytes())?;
            writer.write_all(&entry.key)?;
            writer.write_all(&entry.offset.to_le_bytes())?;
        }

        // Write the index offset as the last 8 bytes of the file.
        // This is the anchor — on open we read the last 8 bytes first
        // to know where the index starts.
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.flush()?;

        Ok(Self { path, index })
    }

    /// Open an existing SSTable and load its index into memory.
    /// The index is small relative to the data — loading it once
    /// means every subsequent read is a binary search + one seek.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        // Read the last 8 bytes to find where the index starts.
        file.seek(SeekFrom::End(-8))?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let index_offset = u64::from_le_bytes(buf);

        // Seek to the index and read all index entries.
        file.seek(SeekFrom::Start(index_offset))?;

        // Read until we hit the last 8 bytes (the index offset itself).
        let file_len = file.seek(SeekFrom::End(0))?;
        let index_end = file_len - 8;
        file.seek(SeekFrom::Start(index_offset))?;

        let mut reader = BufReader::new(file);
        let mut index = Vec::new();
        let mut bytes_read = index_offset;

        while bytes_read < index_end {
            let mut key_len_buf = [0u8; 4];
            reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            bytes_read += 4;

            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;
            bytes_read += key_len as u64;

            let mut offset_buf = [0u8; 8];
            reader.read_exact(&mut offset_buf)?;
            let offset = u64::from_le_bytes(offset_buf);
            bytes_read += 8;

            index.push(IndexEntry { key, offset });
        }

        Ok(Self { path, index })
    }

    /// Look up a key using binary search on the in-memory index,
    /// then seek directly to the entry on disk.
    /// Returns None if the key is not in this SSTable.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<SstEntry>> {
        // Binary search the index for this key.
        let pos = self.index.binary_search_by(|e| e.key.as_slice().cmp(key));

        let idx = match pos {
            Ok(i) => i,
            Err(_) => return Ok(None), // key not in this SSTable
        };

        let offset = self.index[idx].offset;

        // Seek to the entry and read it.
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut reader = BufReader::new(file);

        let mut key_len_buf = [0u8; 4];
        reader.read_exact(&mut key_len_buf)?;
        let key_len = u32::from_le_bytes(key_len_buf) as usize;

        let mut value_len_buf = [0u8; 4];
        reader.read_exact(&mut value_len_buf)?;
        let value_len = u32::from_le_bytes(value_len_buf);

        let mut entry_key = vec![0u8; key_len];
        reader.read_exact(&mut entry_key)?;

        let value = if value_len == TOMBSTONE_MARKER {
            Value::Tombstone
        } else {
            let mut data = vec![0u8; value_len as usize];
            reader.read_exact(&mut data)?;
            Value::Data(data)
        };

        Ok(Some(SstEntry {
            key: entry_key,
            value,
        }))
    }

    /// Read all entries from this SSTable in sorted key order.
    /// Used during compaction to merge multiple SSTables.
    pub fn scan_all(&self) -> io::Result<Vec<SstEntry>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        // Read only the data section — stop before the index.
        // The index starts at the offset stored in the last 8 bytes.
        // We know this from self.index being populated on open,
        // but we need the raw index_offset byte position.
        // Derive it: the first index entry's offset tells us nothing,
        // but we can compute the data section end from the file.
        //
        // Simpler: read entries until we hit a key that does not
        // match what the index says should be at that position.
        // Even simpler: use the index to read each entry by offset.
        for index_entry in &self.index {
            reader.seek(SeekFrom::Start(index_entry.offset))?;

            let mut key_len_buf = [0u8; 4];
            reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            let mut value_len_buf = [0u8; 4];
            reader.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_le_bytes(value_len_buf);

            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            let value = if value_len == TOMBSTONE_MARKER {
                Value::Tombstone
            } else {
                let mut data = vec![0u8; value_len as usize];
                reader.read_exact(&mut data)?;
                Value::Data(data)
            };

            entries.push(SstEntry { key, value });
        }

        Ok(entries)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Value;
    use tempfile::tempdir;

    fn make_entries(pairs: &[(&[u8], &[u8])]) -> Vec<(Vec<u8>, Value)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_vec(), Value::Data(v.to_vec())))
            .collect()
    }

    #[test]
    fn test_write_and_read_back() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let entries = make_entries(&[(b"apple", b"1"), (b"banana", b"2"), (b"cherry", b"3")]);

        SSTable::write(&path, entries).unwrap();
        let sst = SSTable::open(&path).unwrap();

        let result = sst.get(b"banana").unwrap().unwrap();
        assert_eq!(result.value, Value::Data(b"2".to_vec()));
    }

    #[test]
    fn test_missing_key_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let entries = make_entries(&[(b"only-key", b"value")]);
        SSTable::write(&path, entries).unwrap();
        let sst = SSTable::open(&path).unwrap();

        assert!(sst.get(b"missing").unwrap().is_none());
    }

    #[test]
    fn test_tombstone_round_trips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let entries = vec![
            (b"deleted-key".to_vec(), Value::Tombstone),
            (b"live-key".to_vec(), Value::Data(b"alive".to_vec())),
        ];

        SSTable::write(&path, entries).unwrap();
        let sst = SSTable::open(&path).unwrap();

        assert_eq!(
            sst.get(b"deleted-key").unwrap().unwrap().value,
            Value::Tombstone
        );
        assert_eq!(
            sst.get(b"live-key").unwrap().unwrap().value,
            Value::Data(b"alive".to_vec())
        );
    }

    #[test]
    fn test_scan_returns_sorted_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let entries = make_entries(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);

        SSTable::write(&path, entries).unwrap();
        let sst = SSTable::open(&path).unwrap();

        let all = sst.scan_all().unwrap();
        assert_eq!(all[0].key, b"a");
        assert_eq!(all[1].key, b"b");
        assert_eq!(all[2].key, b"c");
    }

    #[test]
    fn test_large_entry_count() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        // Write 1000 entries with zero-padded keys so they sort correctly
        let raw: Vec<(Vec<u8>, Value)> = (0u32..1000)
            .map(|i| {
                let key = format!("key-{:04}", i).into_bytes();
                let val = format!("value-{}", i).into_bytes();
                (key, Value::Data(val))
            })
            .collect();

        SSTable::write(&path, raw).unwrap();
        let sst = SSTable::open(&path).unwrap();

        // Spot check a few entries
        let entry = sst.get(b"key-0042").unwrap().unwrap();
        assert_eq!(entry.value, Value::Data(b"value-42".to_vec()));

        let entry = sst.get(b"key-0999").unwrap().unwrap();
        assert_eq!(entry.value, Value::Data(b"value-999".to_vec()));
    }
}
