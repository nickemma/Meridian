package domain

// DenialKind distinguishes a missing identity from an insufficient one.
type DenialKind uint8

const (
	Unauthenticated DenialKind = iota + 1
	PermissionDenied
)

// Denial is returned before any handler or mutation runs.
type Denial struct {
	Kind   DenialKind
	Reason string
}

func (d *Denial) Error() string { return d.Reason }

func deny(kind DenialKind, reason string) *Denial { return &Denial{Kind: kind, Reason: reason} }

var (
	ErrClientIdentityRequired = deny(Unauthenticated, "client identity required")
	ErrOperationDenied        = deny(PermissionDenied, "operation denied")
	ErrTenantDenied           = deny(PermissionDenied, "tenant or operation denied")
	ErrPeerNotAuthorized      = deny(PermissionDenied, "peer not authorized")
	ErrPeerOperationDenied    = deny(PermissionDenied, "peer operation denied")
	ErrSenderMismatch         = deny(PermissionDenied, "sender identity mismatch")
)
