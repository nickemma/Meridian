package adapters

import (
	"context"
	"errors"
	"time"

	"github.com/nickemma/meridian/internal/server/domain"
	"github.com/nickemma/meridian/internal/server/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errorCodes = map[domain.ErrorKind]codes.Code{
	domain.InvalidArgument:    codes.InvalidArgument,
	domain.FailedPrecondition: codes.FailedPrecondition,
	domain.DeadlineExceeded:   codes.DeadlineExceeded,
	domain.Unavailable:        codes.Unavailable,
	domain.Unimplemented:      codes.Unimplemented,
	domain.Internal:           codes.Internal,
}

// toStatus maps a use-case error to its gRPC status. A caller's own context
// cancellation or deadline keeps its context status.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	var classified *domain.Error
	if errors.As(err, &classified) {
		if code, ok := errorCodes[classified.Kind]; ok {
			return status.Error(code, classified.Message)
		}
	}
	return status.Error(codes.Internal, err.Error())
}

// statusObserver labels dependency waits with the gRPC code the client saw.
type statusObserver struct{ next ports.DependencyObserver }

func (o statusObserver) Waited(start time.Time, err error) { o.next.Waited(start, toStatus(err)) }
