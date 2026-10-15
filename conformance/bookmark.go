package conformance

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/bookmark"
)

// BookmarkFixture is fresh, absent storage and optional backend fault controls.
// Reopen may create a new handle after all operations on the previous handle
// finish; it checks a live-process reopen, not crash durability.
type BookmarkFixture struct {
	Storage bookmark.Storage
	Cleanup func() error
	Reopen  func(context.Context) (bookmark.Storage, error)
	// CancelReady waits until the second Store reaches a backend-defined stage.
	// Without it, cancellation samples a scheduler handoff and may precede Store.
	CancelReady func(context.Context) error
	// Settle waits for any background writes left by a canceled/failed Store.
	// The next successful Store precedes this barrier, so a late overwrite becomes
	// observable. Without a backend barrier only repeated live loads are sampled.
	Settle func(context.Context) error
}

// BookmarkFactory constructs fresh, absent test storage and honors its context.
type BookmarkFactory func(context.Context) (BookmarkFixture, error)

// BookmarkOptions bounds each fresh storage check, including construction.
type BookmarkOptions struct{ Timeout time.Duration }

type bookmarkCheck struct {
	ctx     context.Context
	fixture BookmarkFixture
}

func freshBookmark(t *testing.T, factory BookmarkFactory, options BookmarkOptions) *bookmarkCheck {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeoutValue(options.Timeout))
	t.Cleanup(cancel)
	fixture, err := factory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.Storage == nil {
		t.Fatal("factory returned nil bookmark storage")
	}
	if fixture.Cleanup != nil {
		t.Cleanup(func() {
			if err := fixture.Cleanup(); err != nil {
				t.Error(err)
			}
		})
	}
	return &bookmarkCheck{ctx: ctx, fixture: fixture}
}

// CheckBookmark checks absent and empty records, exact replacement across buffer
// boundaries, repeatable independent loads, and canceled Store ordering. Success
// is live-process evidence. Review fsync/rename ordering and run a backend crash
// harness separately before claiming process- or power-loss durability.
func CheckBookmark(t *testing.T, factory BookmarkFactory, options ...BookmarkOptions) {
	t.Helper()
	var config BookmarkOptions
	if len(options) > 0 {
		config = options[0]
	}
	t.Run(
		"storage",
		func(t *testing.T) { checkBookmarkStorage(t, freshBookmark(t, factory, config)) },
	)
	t.Run(
		"canceled-store",
		func(t *testing.T) { checkBookmarkCancellation(t, freshBookmark(t, factory, config)) },
	)
}

// CheckBookmarkCanceledStore checks cancellation separately with a controlled
// fixture. Its first Store must succeed; CancelReady can hold the second Store
// at the exact stage being tested. Later stores must succeed normally.
func CheckBookmarkCanceledStore(t *testing.T, factory BookmarkFactory, options ...BookmarkOptions) {
	t.Helper()
	var config BookmarkOptions
	if len(options) > 0 {
		config = options[0]
	}
	checkBookmarkCancellation(t, freshBookmark(t, factory, config))
}

// CheckBookmarkFailedStore requires a fixture whose second Store fails, with
// all other operations succeeding. Repeat with each backend fault point. A
// failed Store may leave the complete previous or replacement record, but may
// neither tear the record nor roll back a later successful Store.
func CheckBookmarkFailedStore(t *testing.T, factory BookmarkFactory, options ...BookmarkOptions) {
	t.Helper()
	var config BookmarkOptions
	if len(options) > 0 {
		config = options[0]
	}
	check := freshBookmark(t, factory, config)
	previous, replacement := contents(4097, 49), contents(113, 114)
	storeBookmark(t, check, previous)
	if err := check.fixture.Storage.Store(check.ctx, replacement); err == nil {
		t.Fatal("fault fixture second store succeeded")
	}
	assertAtomicBookmark(t, check, previous, replacement)
	assertNextBookmark(t, check)
}

func storeBookmark(t *testing.T, check *bookmarkCheck, b []byte) {
	t.Helper()
	if err := check.fixture.Storage.Store(check.ctx, b); err != nil {
		t.Fatal(err)
	}
}

func loadBookmark(t *testing.T, check *bookmarkCheck) []byte {
	t.Helper()
	b, err := check.fixture.Storage.Load(check.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertBookmark(t *testing.T, check *bookmarkCheck, want []byte) {
	t.Helper()
	got := loadBookmark(t, check)
	if got == nil || !bytes.Equal(got, want) {
		t.Fatalf(
			"stored record changed: got %d bytes (present %v), want %d",
			len(got),
			got != nil,
			len(want),
		)
	}
}

func checkBookmarkStorage(t *testing.T, check *bookmarkCheck) {
	for range 2 {
		if got := loadBookmark(t, check); got != nil {
			t.Fatal("fresh bookmark is present")
		}
	}
	for i, size := range []int{4097, 17, 8192, 1, 0, 4096} {
		checkBookmarkSnapshot(t, check, contents(size, byte(i)))
	}
}

func checkBookmarkSnapshot(t *testing.T, check *bookmarkCheck, want []byte) {
	t.Helper()
	input := append([]byte{}, want...)
	storeBookmark(t, check, input)
	if len(input) > 0 {
		input[0] ^= 255
	}
	assertBookmark(t, check, want)
	loaded := loadBookmark(t, check)
	if len(loaded) > 0 {
		loaded[0] ^= 255
	}
	assertBookmark(t, check, want)
	assertBookmark(t, check, want)
	if check.fixture.Reopen != nil {
		storage, err := check.fixture.Reopen(check.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if storage == nil {
			t.Fatal("reopen returned nil bookmark storage")
		}
		check.fixture.Storage = storage
		assertBookmark(t, check, want)
	}
}

func assertAtomicBookmark(t *testing.T, check *bookmarkCheck, old, replacement []byte) {
	t.Helper()
	got := loadBookmark(t, check)
	if got == nil || (!bytes.Equal(got, old) && !bytes.Equal(got, replacement)) {
		t.Fatal("failed/canceled store left an absent or partial record")
	}
}

func assertNextBookmark(t *testing.T, check *bookmarkCheck) {
	t.Helper()
	next := contents(8193, 159)
	storeBookmark(t, check, next)
	if check.fixture.Settle != nil {
		if err := check.fixture.Settle(check.ctx); err != nil {
			t.Fatal(err)
		}
	}
	for range 8 {
		assertBookmark(t, check, next)
		runtime.Gosched()
	}
}

func checkBookmarkCancellation(t *testing.T, check *bookmarkCheck) {
	previous, replacement := contents(4097, 49), contents(113, 114)
	storeBookmark(t, check, previous)
	ctx, cancel := context.WithCancel(check.ctx)
	defer cancel()
	started := make(chan struct{})
	stored := make(chan error, 1)
	go func() { close(started); stored <- check.fixture.Storage.Store(ctx, replacement) }()
	<-started
	if check.fixture.CancelReady != nil {
		if err := check.fixture.CancelReady(check.ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
	} else {
		runtime.Gosched()
	}
	cancel()
	select {
	case err := <-stored:
		if err == nil {
			assertBookmark(t, check, replacement)
		} else {
			assertAtomicBookmark(t, check, previous, replacement)
		}
	case <-check.ctx.Done():
		t.Fatal(fmt.Errorf("canceled store did not exit: %w", check.ctx.Err()))
	}
	assertNextBookmark(t, check)
}
