package conformance_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/bookmark"
	"github.com/cirocosta/hearsay/conformance"
	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/link/routed"
)

func TestMemoryLink(t *testing.T) {
	conformance.CheckLinkCapacities(
		t,
		func(_ context.Context, capacity int) (conformance.LinkPair, error) {
			near, far := link.MemoryWithCapacity(capacity)
			return conformance.LinkPair{Near: near, Far: far}, nil
		},
		1,
		7,
		8192,
	)
}

func newEndpoint(ctx context.Context) (*routed.Endpoint, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	address, err := routed.EncodeTCPAddr(listener.Addr().(*net.TCPAddr).AddrPort())
	if err != nil {
		_ = listener.Close()
		return nil, nil, err
	}
	endpoint, err := routed.New(
		listener,
		address,
		routed.TCPDial,
		routed.Config{RoutingTimeout: time.Second},
	)
	if err != nil {
		_ = listener.Close()
		return nil, nil, err
	}
	return endpoint, address, nil
}

func routedPair(ctx context.Context) (conformance.LinkPair, error) {
	near, _, err := newEndpoint(ctx)
	if err != nil {
		return conformance.LinkPair{}, err
	}
	far, address, err := newEndpoint(ctx)
	if err != nil {
		_ = near.Close()
		return conformance.LinkPair{}, err
	}
	cleanup := func() error { return errors.Join(near.Close(), far.Close()) }
	_, left, err := near.Link(ctx, address)
	if err != nil {
		_ = cleanup()
		return conformance.LinkPair{}, err
	}
	_, right, err := far.Accept(ctx)
	if err != nil {
		_ = cleanup()
		return conformance.LinkPair{}, err
	}
	return conformance.LinkPair{Near: left, Far: right, Cleanup: cleanup}, nil
}
func TestRoutedLink(t *testing.T) { conformance.CheckLink(t, routedPair) }
func TestMemoryBookmark(t *testing.T) {
	conformance.CheckBookmark(t, func(context.Context) (conformance.BookmarkFixture, error) {
		return conformance.BookmarkFixture{Storage: &bookmark.Memory{}}, nil
	})
}

func TestFileBookmark(t *testing.T) {
	conformance.CheckBookmark(t, func(context.Context) (conformance.BookmarkFixture, error) {
		path := filepath.Join(t.TempDir(), "record")
		return conformance.BookmarkFixture{
			Storage: bookmark.NewFile(path),
			Reopen:  func(context.Context) (bookmark.Storage, error) { return bookmark.NewFile(path), nil },
		}, nil
	})
}

type stagedStorage struct {
	storage bookmark.Memory
	mu      sync.Mutex
	calls   int
	ready   chan struct{}
	fail    bool
}

func (s *stagedStorage) Load(ctx context.Context) ([]byte, error) { return s.storage.Load(ctx) }
func (s *stagedStorage) Store(ctx context.Context, b []byte) error {
	s.mu.Lock()
	s.calls++
	index := s.calls
	s.mu.Unlock()
	if index == 2 {
		close(s.ready)
		if s.fail {
			return errors.New("injected second-store failure")
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return s.storage.Store(ctx, b)
}

func stagedFactory(fail bool) conformance.BookmarkFactory {
	return func(context.Context) (conformance.BookmarkFixture, error) {
		storage := &stagedStorage{ready: make(chan struct{}), fail: fail}
		return conformance.BookmarkFixture{
			Storage: storage,
			CancelReady: func(ctx context.Context) error {
				select {
				case <-storage.ready:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}, nil
	}
}

func TestBookmarkControlledCancellation(t *testing.T) {
	conformance.CheckBookmarkCanceledStore(t, stagedFactory(false))
}

func TestBookmarkFailedStore(t *testing.T) {
	conformance.CheckBookmarkFailedStore(t, stagedFactory(true))
}

// lateStorage intentionally violates the canceled-store contract. The explicit
// settle barrier makes its overwrite deterministic instead of relying on sleep.
type lateStorage struct {
	storage               bookmark.Memory
	mu                    sync.Mutex
	calls                 int
	ready, third, settled chan struct{}
}

func (s *lateStorage) Load(ctx context.Context) ([]byte, error) { return s.storage.Load(ctx) }
func (s *lateStorage) Store(ctx context.Context, b []byte) error {
	s.mu.Lock()
	s.calls++
	index := s.calls
	s.mu.Unlock()
	if index == 2 {
		captured := append([]byte(nil), b...)
		go func() { <-s.third; _ = s.storage.Store(context.Background(), captured); close(s.settled) }()
		close(s.ready)
		<-ctx.Done()
		return ctx.Err()
	}
	if err := s.storage.Store(ctx, b); err != nil {
		return err
	}
	if index == 3 {
		close(s.third)
	}
	return nil
}

func lateFactory(context.Context) (conformance.BookmarkFixture, error) {
	s := &lateStorage{
		ready:   make(chan struct{}),
		third:   make(chan struct{}),
		settled: make(chan struct{}),
	}
	wait := func(ctx context.Context, ch <-chan struct{}) error {
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return conformance.BookmarkFixture{
		Storage:     s,
		CancelReady: func(ctx context.Context) error { return wait(ctx, s.ready) },
		Settle:      func(ctx context.Context) error { return wait(ctx, s.settled) },
	}, nil
}

func TestRejectsLateBookmarkOverwrite(t *testing.T) {
	if os.Getenv("HEARSAY_CONFORMANCE_LATE_STORE") == "1" {
		conformance.CheckBookmarkCanceledStore(t, lateFactory)
		return
	}
	cmd := exec.CommandContext(
		t.Context(),
		os.Args[0],
		"-test.run=^TestRejectsLateBookmarkOverwrite$",
	)
	cmd.Env = append(os.Environ(), "HEARSAY_CONFORMANCE_LATE_STORE=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("suite accepted a delayed overwrite")
	}
	if !bytes.Contains(output, []byte("stored record changed")) {
		t.Fatalf("unexpected child failure: %v\n%s", err, output)
	}
}

func TestRejectsWrongDeclaredWindow(t *testing.T) {
	if os.Getenv("HEARSAY_CONFORMANCE_WRONG_WINDOW") == "1" {
		conformance.CheckLink(t, func(context.Context) (conformance.LinkPair, error) {
			a, b := link.MemoryWithCapacity(8)
			return conformance.LinkPair{Near: a, Far: b}, nil
		}, conformance.LinkOptions{StreamCapacity: 1})
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRejectsWrongDeclaredWindow$")
	cmd.Env = append(os.Environ(), "HEARSAY_CONFORMANCE_WRONG_WINDOW=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("suite accepted an incorrect buffer bound")
	}
	if !bytes.Contains(output, []byte("write exceeded declared stream capacity")) {
		t.Fatalf("unexpected child failure: %v\n%s", err, output)
	}
}
