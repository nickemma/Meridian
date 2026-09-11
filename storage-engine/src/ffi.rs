//! C ABI for the Go service boundary.
//!
//! The ABI copies caller-owned key and value bytes into Rust. Values returned
//! by `meridian_storage_get` are owned by Rust and must be released with
//! `meridian_storage_free_buffer` using the length and capacity returned by the
//! call. All functions return one of the status codes below and never unwind
//! across the FFI boundary.

use std::ffi::{c_char, CStr};
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use crate::engine::StorageEngine;

pub const MERIDIAN_STORAGE_OK: i32 = 0;
pub const MERIDIAN_STORAGE_INVALID_ARGUMENT: i32 = 1;
pub const MERIDIAN_STORAGE_IO_ERROR: i32 = 2;
pub const MERIDIAN_STORAGE_PANIC: i32 = 3;

/// Opaque handle owned by the C caller between open and close.
pub struct MeridianStorage {
    engine: StorageEngine,
}

fn status_from_io(_: io::Error) -> i32 {
    MERIDIAN_STORAGE_IO_ERROR
}

fn ffi_status(operation: impl FnOnce() -> Result<(), i32>) -> i32 {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => MERIDIAN_STORAGE_OK,
        Ok(Err(status)) => status,
        Err(_) => MERIDIAN_STORAGE_PANIC,
    }
}

unsafe fn input_bytes<'a>(data: *const u8, length: usize) -> Result<&'a [u8], i32> {
    if length == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(MERIDIAN_STORAGE_INVALID_ARGUMENT);
    }
    Ok(unsafe { std::slice::from_raw_parts(data, length) })
}

unsafe fn handle<'a>(storage: *mut MeridianStorage) -> Result<&'a MeridianStorage, i32> {
    unsafe { storage.as_ref().ok_or(MERIDIAN_STORAGE_INVALID_ARGUMENT) }
}

#[no_mangle]
/// Opens a storage engine and stores its owned handle in `out_storage`.
///
/// # Safety
/// `data_dir` must point to a NUL-terminated UTF-8 string for the duration of
/// this call. `out_storage` must point to writable memory. On success, the
/// caller owns the returned handle and must close it exactly once.
pub unsafe extern "C" fn meridian_storage_open(
    data_dir: *const c_char,
    out_storage: *mut *mut MeridianStorage,
) -> i32 {
    ffi_status(|| {
        if data_dir.is_null() || out_storage.is_null() {
            return Err(MERIDIAN_STORAGE_INVALID_ARGUMENT);
        }
        let path = unsafe { CStr::from_ptr(data_dir) }
            .to_str()
            .map_err(|_| MERIDIAN_STORAGE_INVALID_ARGUMENT)?;
        let engine = StorageEngine::open(path).map_err(status_from_io)?;
        unsafe { out_storage.write(Box::into_raw(Box::new(MeridianStorage { engine }))) };
        Ok(())
    })
}

#[no_mangle]
/// Closes a handle returned by [`meridian_storage_open`].
///
/// # Safety
/// `storage` must be null or a live handle returned by `meridian_storage_open`
/// that is not in use by another thread and has not already been closed.
pub unsafe extern "C" fn meridian_storage_close(storage: *mut MeridianStorage) {
    // C close functions conventionally accept null so cleanup can be written
    // without a separate null check. The handle must not be closed twice.
    if !storage.is_null() {
        unsafe { drop(Box::from_raw(storage)) };
    }
}

#[no_mangle]
/// Copies a key and value into the engine and persists the write.
///
/// # Safety
/// `storage` must be a live handle that is not concurrently closed. Each
/// non-empty input range must be valid for reads for the duration of the call.
pub unsafe extern "C" fn meridian_storage_put(
    storage: *mut MeridianStorage,
    key: *const u8,
    key_length: usize,
    value: *const u8,
    value_length: usize,
) -> i32 {
    ffi_status(|| {
        let storage = unsafe { handle(storage) }?;
        let key = unsafe { input_bytes(key, key_length) }?;
        let value = unsafe { input_bytes(value, value_length) }?;
        storage
            .engine
            .put(key.to_vec(), value.to_vec())
            .map_err(status_from_io)
    })
}

#[no_mangle]
/// Persists a tombstone for `key`.
///
/// # Safety
/// `storage` must be a live handle that is not concurrently closed. A
/// non-empty key range must be valid for reads for the duration of the call.
pub unsafe extern "C" fn meridian_storage_delete(
    storage: *mut MeridianStorage,
    key: *const u8,
    key_length: usize,
) -> i32 {
    ffi_status(|| {
        let storage = unsafe { handle(storage) }?;
        let key = unsafe { input_bytes(key, key_length) }?;
        storage.engine.delete(key.to_vec()).map_err(status_from_io)
    })
}

#[no_mangle]
/// Reads a key and transfers an owned result buffer to the caller when found.
///
/// # Safety
/// `storage` must be a live handle that is not concurrently closed. A
/// non-empty key range must be valid for reads, and all output pointers must
/// reference writable memory for the duration of the call. When `out_found` is
/// true, release the returned buffer exactly once with
/// `meridian_storage_free_buffer` using the returned length and capacity.
pub unsafe extern "C" fn meridian_storage_get(
    storage: *mut MeridianStorage,
    key: *const u8,
    key_length: usize,
    out_found: *mut bool,
    out_data: *mut *mut u8,
    out_length: *mut usize,
    out_capacity: *mut usize,
) -> i32 {
    ffi_status(|| {
        if out_found.is_null()
            || out_data.is_null()
            || out_length.is_null()
            || out_capacity.is_null()
        {
            return Err(MERIDIAN_STORAGE_INVALID_ARGUMENT);
        }
        unsafe {
            out_found.write(false);
            out_data.write(ptr::null_mut());
            out_length.write(0);
            out_capacity.write(0);
        }

        let storage = unsafe { handle(storage) }?;
        let key = unsafe { input_bytes(key, key_length) }?;
        if let Some(mut value) = storage.engine.get(key).map_err(status_from_io)? {
            let data = value.as_mut_ptr();
            let length = value.len();
            let capacity = value.capacity();
            std::mem::forget(value);
            unsafe {
                out_found.write(true);
                out_data.write(data);
                out_length.write(length);
                out_capacity.write(capacity);
            }
        }
        Ok(())
    })
}

#[no_mangle]
/// Materializes current mutable data in a durable SSTable.
///
/// # Safety
/// `storage` must be a live handle that is not concurrently closed.
pub unsafe extern "C" fn meridian_storage_flush(storage: *mut MeridianStorage) -> i32 {
    ffi_status(|| {
        let storage = unsafe { handle(storage) }?;
        storage.engine.flush().map_err(status_from_io)
    })
}

#[no_mangle]
/// Compacts immutable SSTables into one table.
///
/// # Safety
/// `storage` must be a live handle that is not concurrently closed.
pub unsafe extern "C" fn meridian_storage_compact(storage: *mut MeridianStorage) -> i32 {
    ffi_status(|| {
        let storage = unsafe { handle(storage) }?;
        storage.engine.compact().map_err(status_from_io)
    })
}

#[no_mangle]
/// Releases a buffer returned by [`meridian_storage_get`].
///
/// # Safety
/// `data`, `length`, and `capacity` must be exactly the values returned by one
/// successful `meridian_storage_get` call and must not have been freed before.
/// A zero capacity may be freed with any pointer value returned for an empty
/// value.
pub unsafe extern "C" fn meridian_storage_free_buffer(
    data: *mut u8,
    length: usize,
    capacity: usize,
) -> i32 {
    ffi_status(|| {
        if capacity < length || (data.is_null() && capacity != 0) {
            return Err(MERIDIAN_STORAGE_INVALID_ARGUMENT);
        }
        if capacity != 0 {
            unsafe { drop(Vec::from_raw_parts(data, length, capacity)) };
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn c_abi_round_trips_empty_and_non_empty_values() {
        let dir = tempdir().unwrap();
        let path = CString::new(dir.path().to_str().unwrap()).unwrap();
        let mut storage = ptr::null_mut();

        assert_eq!(
            unsafe { meridian_storage_open(path.as_ptr(), &mut storage) },
            MERIDIAN_STORAGE_OK
        );
        assert_eq!(
            unsafe { meridian_storage_put(storage, b"a".as_ptr(), 1, b"value".as_ptr(), 5) },
            MERIDIAN_STORAGE_OK
        );
        assert_eq!(
            unsafe { meridian_storage_put(storage, b"empty".as_ptr(), 5, ptr::null(), 0) },
            MERIDIAN_STORAGE_OK
        );

        let mut found = false;
        let mut data = ptr::null_mut();
        let mut length = 0;
        let mut capacity = 0;
        assert_eq!(
            unsafe {
                meridian_storage_get(
                    storage,
                    b"a".as_ptr(),
                    1,
                    &mut found,
                    &mut data,
                    &mut length,
                    &mut capacity,
                )
            },
            MERIDIAN_STORAGE_OK
        );
        assert!(found);
        assert_eq!(
            unsafe { std::slice::from_raw_parts(data, length) },
            b"value"
        );
        assert_eq!(
            unsafe { meridian_storage_free_buffer(data, length, capacity) },
            MERIDIAN_STORAGE_OK
        );

        assert_eq!(
            unsafe {
                meridian_storage_get(
                    storage,
                    b"empty".as_ptr(),
                    5,
                    &mut found,
                    &mut data,
                    &mut length,
                    &mut capacity,
                )
            },
            MERIDIAN_STORAGE_OK
        );
        assert!(found);
        assert_eq!(length, 0);
        assert_eq!(
            unsafe { meridian_storage_free_buffer(data, length, capacity) },
            MERIDIAN_STORAGE_OK
        );
        unsafe { meridian_storage_close(storage) };
    }
}
