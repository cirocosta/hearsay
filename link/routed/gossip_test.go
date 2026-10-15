package routed_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/link/routed"
)

func closeIntegration(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil {
		t.Error(err)
	}
}

func integrationEndpoint(t *testing.T, disabled bool) (*routed.Endpoint, []byte) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address, err := routed.EncodeTCPAddr(listener.Addr().(*net.TCPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := routed.New(
		listener,
		address,
		routed.TCPDial,
		routed.Config{DisablePooling: disabled, RoutingTimeout: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeIntegration(t, endpoint) })
	return endpoint, address
}

func integrationPair(
	ctx context.Context,
	t *testing.T,
	disabled bool,
) (*link.Managed, *link.Managed) {
	t.Helper()
	near, _ := integrationEndpoint(t, disabled)
	far, address := integrationEndpoint(t, disabled)
	_, left, err := near.Link(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	_, right, err := far.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a, b := link.Manage(left), link.Manage(right)
	t.Cleanup(func() { closeIntegration(t, a); closeIntegration(t, b) })
	return a, b
}

func seededIntegration(t *testing.T) *hearsay.Rumors[[]byte] {
	t.Helper()
	seed, err := hearsay.Seed(hearsay.BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	replica, err := seed.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeIntegration(t, replica) })
	for i := range 32 {
		if _, err = replica.Send([]byte(fmt.Sprintf("initial-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	return replica
}

func integrationGossip(
	ctx context.Context,
	t *testing.T,
	a, b *hearsay.Rumors[[]byte],
	near, far *link.Managed,
) {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := a.GossipOnce(ctx, far); done <- err }()
	if _, err := b.GossipOnce(ctx, near); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !a.Snapshot().Equal(b.Snapshot()) {
		t.Fatal("routed gossip did not converge")
	}
}

func TestBootstrapAndRepeatedGossipOverRoutedStreams(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		name := "pooled"
		if disabled {
			name = "fresh"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			near, far := integrationPair(ctx, t, disabled)
			source := seededIntegration(t)
			served := make(chan error, 1)
			go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
			joined, err := hearsay.Bootstrap(hearsay.BytesCodec()).Join(ctx, near)
			if err != nil {
				t.Fatal(err)
			}
			if err = <-served; err != nil {
				t.Fatal(err)
			}
			if joined.Peer == nil {
				t.Fatal("bootstrap returned no peer")
			}
			replica, err := joined.Peer.IntoRumors()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeIntegration(t, replica) })
			for round := range 4 {
				if _, err = source.Send([]byte(fmt.Sprintf("source-%d", round))); err != nil {
					t.Fatal(err)
				}
				payload := make([]byte, 1024)
				payload[0] = byte(round)
				if _, err = replica.Send(payload); err != nil {
					t.Fatal(err)
				}
				integrationGossip(ctx, t, source, replica, near, far)
			}
			for _, managed := range []*link.Managed{near, far} {
				state := managed.State()
				if state.Epoch != 5 || state.Poisoned || state.Active {
					t.Fatalf("session state after routed reuse: %+v", state)
				}
			}
		})
	}
}
