package bookmark

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStorage(t *testing.T) {
	for name, makeStore := range map[string]func(t *testing.T) Storage{
		"memory": func(_ *testing.T) Storage { return new(Memory) },
		"file":   func(t *testing.T) Storage { return NewFile(filepath.Join(t.TempDir(), "record")) },
	} {
		t.Run(name, func(t *testing.T) {
			s := makeStore(t)
			ctx := context.Background()
			if got, err := s.Load(ctx); err != nil || got != nil {
				t.Fatalf("absent: %q, %v", got, err)
			}
			for _, input := range [][]byte{nil, []byte("first"), {}, []byte("replacement")} {
				want := append([]byte{}, input...)
				if err := s.Store(ctx, input); err != nil {
					t.Fatal(err)
				}
				if len(input) > 0 {
					input[0] ^= 0xff
				}
				got, err := s.Load(ctx)
				if err != nil || got == nil || !bytes.Equal(got, want) {
					t.Fatalf("load: %q, %v; want %q", got, err, want)
				}
				if len(got) > 0 {
					got[0] ^= 0xff
				}
				again, err := s.Load(ctx)
				if err != nil || !bytes.Equal(again, want) {
					t.Fatalf("load altered storage: %q, %v", again, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := s.Store(cancelled, []byte("cancelled")); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, err := s.Load(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := s.(interface{ Clear(context.Context) error }).Clear(
				cancelled,
			); !errors.Is(
				err,
				context.Canceled,
			) {
				t.Fatal(err)
			}
			got, err := s.Load(ctx)
			if err != nil || !bytes.Equal(got, []byte("replacement")) {
				t.Fatalf("cancelled operation changed record: %q, %v", got, err)
			}
			for range 2 {
				if err := s.(interface{ Clear(context.Context) error }).Clear(ctx); err != nil {
					t.Fatal(err)
				}
				if got, err := s.Load(ctx); got != nil || err != nil {
					t.Fatalf("clear: %q, %v", got, err)
				}
			}
		})
	}
}

func TestFileRestartAndBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bookmark")
	f, err := NewFileWithLimit(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := f.Store(ctx, []byte("four")); err != nil {
		t.Fatal(err)
	}
	if err := f.Store(ctx, []byte("oversize")); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	restarted := NewFile(path)
	got, err := restarted.Load(ctx)
	if err != nil || string(got) != "four" {
		t.Fatalf("restart: %q, %v", got, err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary file leaked: %v, %v", entries, err)
	}
	if err := os.WriteFile(path, []byte("oversize"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Load(ctx); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	for _, limit := range []int64{-1, 0, int64(^uint(0) >> 1)} {
		if _, err := NewFileWithLimit(path, limit); err == nil {
			t.Fatalf("accepted limit %d", limit)
		}
	}
}

func TestFileRejectsNonRegular(t *testing.T) {
	for _, kind := range []string{"directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bookmark")
			if kind == "directory" {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Symlink("missing-target", path); err != nil {
					t.Fatal(err)
				}
			}
			f := NewFile(path)
			ctx := context.Background()
			if _, err := f.Load(ctx); !errors.Is(err, ErrNotRegular) {
				t.Fatalf("load: %v", err)
			}
			if err := f.Store(ctx, []byte("record")); !errors.Is(err, ErrNotRegular) {
				t.Fatalf("store: %v", err)
			}
			if err := f.Clear(ctx); !errors.Is(err, ErrNotRegular) {
				t.Fatalf("clear: %v", err)
			}
		})
	}
}

func TestFileCancellationWhileWaiting(t *testing.T) {
	f := NewFile(filepath.Join(t.TempDir(), "record"))
	if err := f.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer f.unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Store(ctx, []byte("obsolete")) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled store waited for unrelated I/O")
	}
}

func TestFileConcurrentOperations(t *testing.T) {
	f := NewFile(filepath.Join(t.TempDir(), "record"))
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			value := bytes.Repeat([]byte{byte(i)}, 4096)
			for range 10 {
				if err := f.Store(ctx, value); err != nil {
					t.Error(err)
					return
				}
				got, err := f.Load(ctx)
				if err != nil || len(got) != len(value) {
					t.Errorf("load: %d bytes, %v", len(got), err)
					return
				}
				if !bytes.Equal(got, bytes.Repeat(got[:1], len(got))) {
					t.Error("partial record")
					return
				}
			}
		})
	}
	wg.Wait()
}
