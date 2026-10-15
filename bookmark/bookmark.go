// Package bookmark stores opaque restart records. A bookmark belongs to one
// peer across restarts; sharing or copying it between live peers is unsafe.
package bookmark

import (
	"context"
	"errors"
	"sync"
)

// DefaultSizeLimit is the recommended 16 MiB encoded record retention limit.
// File storage preserves opaque bytes without imposing this policy itself.
const DefaultSizeLimit int64 = 16 * 1024 * 1024

var (
	// ErrTooLarge reports a record larger than the configured storage limit.
	ErrTooLarge = errors.New("bookmark exceeds size limit")
	// ErrNotRegular reports a target that is a directory, symlink, or special file.
	ErrNotRegular = errors.New("bookmark is not a regular file")
)

// Storage preserves opaque records across restarts. Load returns nil when no
// record exists and a non-nil slice for a present, possibly empty record. Store
// atomically replaces the record and returns success only after it is durable.
// A failed Store may leave the old or new record, but never a partial record.
// Work from a cancelled Store must not overwrite a later successful Store.
type Storage interface {
	Load(context.Context) ([]byte, error)
	Store(context.Context, []byte) error
}

// Memory is a concurrent in-memory storage backend, useful in tests. Its zero
// value has no record. It does not preserve records across process restarts.
type Memory struct {
	mu    sync.Mutex
	bytes []byte
}

// Load returns an independent snapshot of the current record.
func (m *Memory) Load(ctx context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.bytes == nil {
		return nil, nil
	}
	return append([]byte{}, m.bytes...), nil
}

// Store copies bytes into memory before returning.
func (m *Memory) Store(ctx context.Context, bytes []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.bytes = append([]byte{}, bytes...)
	return nil
}

// Clear removes the record. Clearing restart bookkeeping forfeits recovery of
// the identities it contains; callers must coordinate this with the peer.
func (m *Memory) Clear(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.bytes = nil
	return nil
}
