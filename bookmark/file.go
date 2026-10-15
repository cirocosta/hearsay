package bookmark

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// File stores a record by syncing a temporary file, renaming it over the
// destination, and syncing the parent directory. The parent must already exist
// and must be controlled by the application. All access to a given path must
// use this instance; multiple instances or processes are not coordinated.
// File must be constructed with NewFile or NewFileWithLimit.
type File struct {
	path   string
	limit  int64
	serial chan struct{}
}

// NewFile stores opaque records at path without an application size limit.
// Use NewFileWithLimit to apply an explicit bound to physical reads and writes.
func NewFile(path string) *File {
	f, _ := NewFileWithLimit(path, int64(^uint(0)>>1)-1)
	return f
}

// NewFileWithLimit bounds both loads and stores. The limit must be positive.
func NewFileWithLimit(path string, limit int64) (*File, error) {
	if limit <= 0 || limit >= int64(^uint(0)>>1) {
		return nil, fmt.Errorf("invalid bookmark size limit: %d", limit)
	}
	return &File{path: path, limit: limit, serial: make(chan struct{}, 1)}, nil
}

func (f *File) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case f.serial <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-f.serial
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *File) unlock() { <-f.serial }

func regular(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return &os.PathError{Op: "bookmark", Path: path, Err: ErrNotRegular}
	}
	return nil
}

// Load reads the current complete record, bounded by the configured size limit.
func (f *File) Load(ctx context.Context) ([]byte, error) {
	if err := f.lock(ctx); err != nil {
		return nil, err
	}
	defer f.unlock()
	if err := regular(f.path); err != nil {
		return nil, err
	}
	file, err := os.Open(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrNotRegular
	}
	if info.Size() > f.limit {
		return nil, ErrTooLarge
	}
	bytes, err := io.ReadAll(io.LimitReader(contextReader{ctx, file}, f.limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(bytes)) > f.limit {
		return nil, ErrTooLarge
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return bytes, nil
}

// Store atomically replaces the record and makes the replacement durable.
// Cancellation after replacement starts does not interrupt the durability
// barrier. Store does not retain bytes after returning.
func (f *File) Store(ctx context.Context, bytes []byte) error {
	if err := f.lock(ctx); err != nil {
		return err
	}
	defer f.unlock()
	if int64(len(bytes)) > f.limit {
		return ErrTooLarge
	}
	if err := regular(f.path); err != nil {
		return err
	}
	parent := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(parent, ".bookmark-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	defer func() { _ = tmp.Close() }()
	if _, err := tmp.Write(bytes); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), f.path); err != nil {
		return err
	}
	// once published, finish the durability barrier even if cancellation arrives.
	return syncDirectory(parent)
}

// Clear removes the stored record and syncs the directory before returning.
func (f *File) Clear(ctx context.Context) error {
	if err := f.lock(ctx); err != nil {
		return err
	}
	defer f.unlock()
	if err := regular(f.path); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(f.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(f.path))
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(bytes []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(bytes)
}
