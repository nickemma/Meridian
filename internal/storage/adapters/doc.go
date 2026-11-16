// Package adapters contains the cgo binding to the Rust LSM storage engine.
// It is compiled only with the storageffi build tag; without it, the package
// is empty and nodes refuse to start (see server/adapters).
package adapters
