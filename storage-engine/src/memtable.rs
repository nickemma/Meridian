use std::collections::BTreeMap;

/// A single value stored in the memtable.
/// Either a live value or a tombstone marking deletion.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Data(Vec<u8>),
    Tombstone,
}

/// The Memtable — an in-memory sorted buffer of recent writes.
///
/// Writes go here after the WAL confirms durability.
/// Reads check here before touching any SSTable on disk.
/// When size_bytes exceeds the flush threshold, the caller
/// flushes this memtable to an SSTable and replaces it with
/// a fresh empty one.
pub struct Memtable {
    entries: BTreeMap<Vec<u8>, Value>,
    size_bytes: usize,
    flush_threshold: usize,
}

impl Memtable {
    /// Create a new empty Memtable.
    /// flush_threshold is the size in bytes at which the caller
    /// should flush this memtable to an SSTable.
    pub fn new(flush_threshold: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            size_bytes: 0,
            flush_threshold,
        }
    }

    /// Insert or update a key with a live value.
    /// Called after a successful WAL append — never before.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Track size: key bytes + value bytes.
        // If this key already existed we subtract the old size first.
        if let Some(old) = self.entries.get(&key) {
            self.size_bytes -= key.len() + value_size(old);
        }
        self.size_bytes += key.len() + value.len();
        self.entries.insert(key, Value::Data(value));
    }

    /// Mark a key as deleted by writing a tombstone.
    /// The key is not removed — the tombstone IS the deletion record.
    pub fn delete(&mut self, key: Vec<u8>) {
        if let Some(old) = self.entries.get(&key) {
            self.size_bytes -= key.len() + value_size(old);
        }
        // Tombstone takes up key.len() bytes in our size accounting.
        self.size_bytes += key.len();
        self.entries.insert(key, Value::Tombstone);
    }

    /// Look up a key.
    /// Returns:
    ///   Some(Value::Data(bytes)) — key exists with this value
    ///   Some(Value::Tombstone)   — key was deleted
    ///   None                     — key not in memtable (check SSTables)
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.entries.get(key)
    }

    /// Returns true if the memtable has grown past the flush threshold.
    /// The caller (storage engine) checks this after every write and
    /// triggers a flush when it returns true.
    pub fn should_flush(&self) -> bool {
        self.size_bytes >= self.flush_threshold
    }

    /// Current size of the memtable in bytes.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Number of entries (including tombstones).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Consume the memtable and return its entries in sorted key order.
    /// Called by the storage engine when flushing to an SSTable.
    /// After this call the memtable is gone — caller replaces it
    /// with a fresh Memtable::new().
    pub fn into_sorted_entries(self) -> Vec<(Vec<u8>, Value)> {
        // BTreeMap iterates in sorted key order already.
        self.entries.into_iter().collect()
    }
}

fn value_size(value: &Value) -> usize {
    match value {
        Value::Data(bytes) => bytes.len(),
        Value::Tombstone => 0,
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut mem = Memtable::new(4096);

        mem.put(b"key-a".to_vec(), b"value-a".to_vec());
        mem.put(b"key-b".to_vec(), b"value-b".to_vec());

        assert_eq!(mem.get(b"key-a"), Some(&Value::Data(b"value-a".to_vec())));
        assert_eq!(mem.get(b"key-b"), Some(&Value::Data(b"value-b".to_vec())));
    }

    #[test]
    fn test_missing_key_returns_none() {
        let mem = Memtable::new(4096);
        // None means "not in memtable" — caller must check SSTables
        assert_eq!(mem.get(b"missing"), None);
    }

    #[test]
    fn test_delete_writes_tombstone() {
        let mut mem = Memtable::new(4096);

        mem.put(b"key".to_vec(), b"value".to_vec());
        mem.delete(b"key".to_vec());

        // Must see tombstone, not the old value
        assert_eq!(mem.get(b"key"), Some(&Value::Tombstone));
    }

    #[test]
    fn test_overwrite_updates_value() {
        let mut mem = Memtable::new(4096);

        mem.put(b"key".to_vec(), b"old".to_vec());
        mem.put(b"key".to_vec(), b"new".to_vec());

        assert_eq!(mem.get(b"key"), Some(&Value::Data(b"new".to_vec())));
    }

    #[test]
    fn test_flush_threshold_triggers() {
        // Threshold of 20 bytes
        let mut mem = Memtable::new(20);
        assert!(!mem.should_flush());

        // Write enough data to cross the threshold
        mem.put(b"key-one".to_vec(), b"value-one".to_vec());
        mem.put(b"key-two".to_vec(), b"value-two".to_vec());

        assert!(mem.should_flush());
    }

    #[test]
    fn test_sorted_order_on_flush() {
        let mut mem = Memtable::new(4096);

        // Insert in non-sorted order
        mem.put(b"c".to_vec(), b"3".to_vec());
        mem.put(b"a".to_vec(), b"1".to_vec());
        mem.put(b"b".to_vec(), b"2".to_vec());

        let entries = mem.into_sorted_entries();

        // Must come out sorted: a, b, c
        assert_eq!(entries[0].0, b"a");
        assert_eq!(entries[1].0, b"b");
        assert_eq!(entries[2].0, b"c");
    }

    #[test]
    fn test_size_accounting_on_overwrite() {
        let mut mem = Memtable::new(4096);

        mem.put(b"key".to_vec(), b"small".to_vec());
        let size_after_first = mem.size_bytes();

        // Overwrite with a larger value
        mem.put(b"key".to_vec(), b"much-larger-value".to_vec());
        let size_after_second = mem.size_bytes();

        // Size must grow to reflect the larger value
        assert!(size_after_second > size_after_first);
    }
}
