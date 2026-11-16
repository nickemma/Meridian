// Package ports declares where registered policies are kept.
package ports

import "github.com/nickemma/meridian/internal/policy/domain"

// Repository stores policies by name. The application serialises access.
type Repository interface {
	Put(*domain.Policy)
	Delete(name string)
	All() []*domain.Policy
}
