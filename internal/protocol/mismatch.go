package protocol

import "fmt"

// VersionMismatch retains the advertised dialect that failed negotiation.
type VersionMismatch struct{ Local, Remote uint64 }

func (e *VersionMismatch) Error() string {
	return fmt.Sprintf("%v: local %d, remote %d", ErrProtocol, e.Local, e.Remote)
}
func (e *VersionMismatch) Unwrap() error { return ErrProtocol }

// DepthMismatch retains both configured typed payload traversal limits.
type DepthMismatch struct{ Local, Remote uint64 }

func (e *DepthMismatch) Error() string {
	return fmt.Sprintf("%v: local %d, remote %d", ErrDepth, e.Local, e.Remote)
}
func (e *DepthMismatch) Unwrap() error { return ErrDepth }
