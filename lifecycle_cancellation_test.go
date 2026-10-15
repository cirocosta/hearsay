package hearsay

import (
	"context"
	"errors"
	"testing"
)

func TestPreCanceledIntoPeerConsumesSoleHandle(t *testing.T) {
	for range 100 {
		r := seededBytes(t)
		observer := r.Changes()
		if _, err := r.Send([]byte("final")); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		peer, err := r.IntoPeer(ctx)
		if peer != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-canceled claim: peer=%v err=%v", peer != nil, err)
		}
		if _, err := r.Clone(); !errors.Is(err, ErrClosed) {
			t.Fatalf("canceled handle usable: %v", err)
		}
		if observer.TryNext() != Ready || observer.TryNext() != Ended {
			t.Fatal("canceled final claim did not drain and end observers")
		}
		if r.Snapshot().Len() != 1 {
			t.Fatal("cancellation lost final snapshot")
		}
	}
}
