// Package ports declares where configuration values come from.
package ports

// Source looks up one named setting. An empty value is treated as unset.
type Source interface {
	Lookup(key string) (string, bool)
}
