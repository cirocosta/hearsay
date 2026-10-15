// Package link defines the independent streams used by gossip sessions.
// A link carries one persistent, fully duplex control stream and lazily opened
// unidirectional data streams. Each stream must have independent receiver-paced
// flow control. Sessions on a link must be serialized by the caller.
package link

import (
	"context"
	"io"
)

// StreamCount is the maximum data streams per direction in the 32-level wire
// schedule: one stream per two heights, plus the opening/terminal stream.
const StreamCount = 17

// Link supplies the transport for consecutive gossip sessions. Open may run
// concurrently. Accept must not lose a stream when its context is canceled.
// The control stream must support simultaneous reads and writes, even when a
// frame exceeds its buffering. Closing a link must unblock outstanding I/O.
// A transport that pools flow-control credit must reserve at least
// (StreamCount+1) times its per-stream window in each direction.
//
// The application authenticates and authorizes peers and provides integrity,
// confidentiality, and replay protection when the deployment requires them.
// An authorized peer has write authority over the entire gossip set.
type Link interface {
	Control() io.ReadWriteCloser
	Open(context.Context) (io.WriteCloser, error)
	Accept(context.Context) (io.ReadCloser, error)
	Close() error
}
