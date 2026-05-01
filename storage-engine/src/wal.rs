use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

// Each entry in the WAL has this layout on disk:

//  [ sequence (8 bytes) | checksum (4 bytes) | length (4 bytes) | data (N bytes) ]

// sequence — monotonically increasing, never reused
// checksum — CRC32 of the data bytes only
// length   — how many bytes of data follow
// data     — the raw bytes of the log entry (Raft command)

// On recovery we read entries in order. If we reach an entry whose
// checksum does not match its data, we stop — everything from that
// point on is from a crash and is discarded.

const HEADER_SIZE: usize = 8 + 4 + 4; // sequence + checksum + length

pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
    next_sequence: u64,
}

// A single entry recovered from the WAL during replay.
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub sequence: u64,
    pub data: Vec<u8>,
}

impl Wal {
    /// Open or create a WAL file at the given path.
    /// If the file already exists, we recover the next sequence number
    /// from the last valid entry so we never reuse a sequence number.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Recover existing entries to find the highest sequence number.
        // If the file does not exist yet, this returns an empty vec.
        let existing = Self::recover_entries(&path).unwrap_or_default();
        let next_sequence = existing.last().map(|e| e.sequence + 1).unwrap_or(0);

        // Open the file for appending — we never overwrite existing data.
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
            next_sequence,
        })
    }

    /// Append a new entry to the WAL.
    /// This MUST complete before the caller writes to the memtable.
    /// If this returns Ok, the entry is durable on disk.
    pub fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let sequence = self.next_sequence;
        let checksum = checksum(data);
        let length = data.len() as u32;

        // Write header: sequence (8) + checksum (4) + length (4)
        self.writer.write_all(&sequence.to_le_bytes())?;
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&length.to_le_bytes())?;

        // Write data
        self.writer.write_all(data)?;

        // Flush to OS buffer and fsync to disk.
        // fsync is what makes this durable — without it the data
        // is in the OS page cache and survives only a process crash,
        // not a power failure.
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        self.next_sequence += 1;
        Ok(sequence)
    }

    /// Read all valid entries from a WAL file.
    /// Stops at the first entry with a bad checksum — everything
    /// after a corruption is considered part of an incomplete write
    /// from a crash and is silently dropped.
    pub fn recover_entries(path: impl AsRef<Path>) -> io::Result<Vec<WalEntry>> {
        let path = path.as_ref();

        // If the file does not exist yet, there is nothing to recover.
        if !path.exists() {
            return Ok(vec![]);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            // Read the fixed-size header first.
            let mut header = [0u8; HEADER_SIZE];
            match reader.read_exact(&mut header) {
                Ok(_) => {}
                // Clean EOF — we've read all complete entries.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let sequence = u64::from_le_bytes(header[0..8].try_into().unwrap());
            let stored_checksum = u32::from_le_bytes(header[8..12].try_into().unwrap());
            let length = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;

            // Read the data payload.
            let mut data = vec![0u8; length];
            match reader.read_exact(&mut data) {
                Ok(_) => {}
                // Incomplete data write — crash happened mid-entry. Stop here.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            // Verify integrity. Bad checksum means corruption — stop.
            let computed = checksum(&data);
            if computed != stored_checksum {
                break;
            }

            entries.push(WalEntry { sequence, data });
        }

        Ok(entries)
    }

    /// The path this WAL is writing to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The sequence number that will be assigned to the next entry.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
}

fn checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_append_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write 3 entries
        let mut wal = Wal::open(&path).unwrap();
        let seq0 = wal.append(b"entry-zero").unwrap();
        let seq1 = wal.append(b"entry-one").unwrap();
        let seq2 = wal.append(b"entry-two").unwrap();

        assert_eq!(seq0, 0);
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);

        // Recover and verify all 3 entries survive
        let entries = Wal::recover_entries(&path).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].data, b"entry-zero");
        assert_eq!(entries[1].data, b"entry-one");
        assert_eq!(entries[2].data, b"entry-two");
    }

    #[test]
    fn test_sequence_continuity_after_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write 2 entries, close
        let mut wal = Wal::open(&path).unwrap();
        wal.append(b"first").unwrap();
        wal.append(b"second").unwrap();
        drop(wal);

        // Reopen — sequence must continue from 2, not reset to 0
        let mut wal = Wal::open(&path).unwrap();
        let seq = wal.append(b"third").unwrap();
        assert_eq!(seq, 2);
    }

    #[test]
    fn test_corrupted_entry_stops_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write 2 good entries
        let mut wal = Wal::open(&path).unwrap();
        wal.append(b"good-one").unwrap();
        wal.append(b"good-two").unwrap();
        drop(wal);

        // Corrupt the file by appending garbage at the end
        // simulating a crash mid-write
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"garbage-corrupt-data").unwrap();
        drop(file);

        // Recovery must return only the 2 good entries
        // and silently discard the garbage
        let entries = Wal::recover_entries(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].data, b"good-one");
        assert_eq!(entries[1].data, b"good-two");
    }
}
