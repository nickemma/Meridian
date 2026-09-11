//go:build storageffi

package storage

/*
#cgo CFLAGS: -I${SRCDIR}/../../storage-engine/include
#cgo LDFLAGS: ${SRCDIR}/../../storage-engine/target/release/libmeridian_storage.a -ldl -lm -lpthread
#include <stdlib.h>
#include "meridian_storage.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

var ErrClosed = errors.New("meridian storage engine is closed")

// StatusError is returned when the native storage engine rejects an operation.
// It intentionally preserves the stable C ABI status so callers can separate a
// malformed request from an I/O failure.
type StatusError struct {
	Operation string
	Status    int32
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("storage %s failed with status %d", e.Operation, e.Status)
}

// Engine owns one Rust StorageEngine handle. Close waits for in-flight calls so
// a native handle is never freed while a cgo call is using it.
type Engine struct {
	mu     sync.RWMutex
	handle *C.MeridianStorage
}

func Open(dataDir string) (*Engine, error) {
	if dataDir == "" {
		return nil, errors.New("storage data directory is required")
	}
	path := C.CString(dataDir)
	defer C.free(unsafe.Pointer(path))

	var handle *C.MeridianStorage
	if status := C.meridian_storage_open(path, &handle); status != C.MERIDIAN_STORAGE_OK {
		return nil, statusError("open", status)
	}
	return &Engine{handle: handle}, nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handle == nil {
		return nil
	}
	C.meridian_storage_close(e.handle)
	e.handle = nil
	return nil
}

func (e *Engine) Put(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.withHandle("put", func(handle *C.MeridianStorage) C.int32_t {
		return C.meridian_storage_put(handle, bytePointer(key), C.size_t(len(key)), bytePointer(value), C.size_t(len(value)))
	})
}

func (e *Engine) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.withHandle("delete", func(handle *C.MeridianStorage) C.int32_t {
		return C.meridian_storage_delete(handle, bytePointer(key), C.size_t(len(key)))
	})
}

func (e *Engine) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == nil {
		return nil, false, ErrClosed
	}

	var found C.bool
	var data *C.uint8_t
	var length, capacity C.size_t
	status := C.meridian_storage_get(
		e.handle,
		bytePointer(key),
		C.size_t(len(key)),
		&found,
		&data,
		&length,
		&capacity,
	)
	if status != C.MERIDIAN_STORAGE_OK {
		return nil, false, statusError("get", status)
	}
	if !bool(found) {
		return nil, false, nil
	}
	defer C.meridian_storage_free_buffer(data, length, capacity)
	return C.GoBytes(unsafe.Pointer(data), C.int(length)), true, nil
}

func (e *Engine) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.withHandle("flush", func(handle *C.MeridianStorage) C.int32_t {
		return C.meridian_storage_flush(handle)
	})
}

func (e *Engine) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.withHandle("compact", func(handle *C.MeridianStorage) C.int32_t {
		return C.meridian_storage_compact(handle)
	})
}

func (e *Engine) withHandle(operation string, call func(*C.MeridianStorage) C.int32_t) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == nil {
		return ErrClosed
	}
	if status := call(e.handle); status != C.MERIDIAN_STORAGE_OK {
		return statusError(operation, status)
	}
	return nil
}

func statusError(operation string, status C.int32_t) error {
	return &StatusError{Operation: operation, Status: int32(status)}
}

func bytePointer(value []byte) *C.uint8_t {
	if len(value) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&value[0]))
}
