// Package storage defines Meridian's durable key-value storage boundary.
//
// The production implementation is enabled with the storageffi build tag and
// calls the Rust LSM through its documented C ABI. Keeping that boundary small
// prevents the Go replication layer from growing a second storage engine.
package storage
