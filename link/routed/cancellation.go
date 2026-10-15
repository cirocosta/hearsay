package routed

import "context"

// closeOnCancellation returns a disarm function that must be called once before
// handing the connection to its next owner. Stopping an AfterFunc alone does
// not join a callback that has started: that callback could close a later
// pooled generation after an otherwise successful Open.
func closeOnCancellation(ctx context.Context, closeConn func()) func() {
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(finished); closeConn() })
	return func() {
		if !stop() {
			<-finished
		}
	}
}
