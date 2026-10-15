package hearsay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

type departingLink struct {
	control *departingControl
	done    chan struct{}
	once    sync.Once
}
type departingControl struct{ io.Reader }

func (c *departingControl) Write(p []byte) (int, error) { return len(p), nil }
func (c *departingControl) Close() error                { return nil }
func (l *departingLink) Control() io.ReadWriteCloser    { return l.control }

func (l *departingLink) Open(
	context.Context,
) (io.WriteCloser, error) {
	return discardStream{}, nil
}

func (l *departingLink) Accept(ctx context.Context) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.done:
		return nil, io.EOF
	}
}
func (l *departingLink) Close() error { l.once.Do(func() { close(l.done) }); return nil }

type discardStream struct{}

func (discardStream) Write(p []byte) (int, error) { return len(p), nil }
func (discardStream) Close() error                { return nil }

func TestControlDeparturePreemptsMissingDataStreams(t *testing.T) {
	for _, prefix := range [][]byte{nil, {0x61}, {0x61, '.'}, {0xd9, 0xd2}} {
		t.Run(string(prefix), func(t *testing.T) {
			source := seededBytes(t)
			version := before.Seed().Tick()
			remote := tree.Tree[[]byte]{}.Insert(version, []byte{0x41, 'x'})
			preamble, err := (protocol.Preamble{Network: protocol.Network(source.Network())}).AppendTo(
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			greeting, err := makeGreeting(remote, source.SynchronizationSettings()).AppendTo(nil)
			if err != nil {
				t.Fatal(err)
			}
			script := append(append(preamble, greeting...), prefix...)
			carrier := link.Manage(
				&departingLink{
					control: &departingControl{bytes.NewReader(script)},
					done:    make(chan struct{}),
				},
			)
			defer func() { _ = carrier.Close() }()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err = source.GossipOnce(ctx, carrier)
			if err == nil {
				t.Fatal("departure accepted as a completed session")
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatal(
					"missing data stream waited for external cancellation after control departure",
				)
			}
			if !source.Snapshot().IsEmpty() {
				t.Fatal("departure published partial reconciliation")
			}
			if !carrier.State().Poisoned {
				t.Fatal("departure left link reusable")
			}
		})
	}
}
