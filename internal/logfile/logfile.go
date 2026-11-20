package logfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/quacktel/cli/internal/paths"
)

const (
	DefaultMaxBytes int64 = 10 << 20
	DefaultKeep           = 3
)

type writer struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	keep     int
	f        *os.File
	size     int64
	closed   bool
	// fallback receives writes while the file can't be reopened; launchd captures stderr.
	fallback io.Writer
}

// Open appends to path, rotating before a write that would exceed maxBytes.
func Open(path string, maxBytes int64, keep int) (io.WriteCloser, error) {
	if err := paths.EnsureDir(filepath.Dir(path)); err != nil {
		return nil, err
	}

	w := &writer{path: path, maxBytes: maxBytes, keep: keep, fallback: os.Stderr}
	if err := w.open(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *writer) open() error {
	f, err := os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return errors.Join(fmt.Errorf("failed to stat log file: %w", err), f.Close())
	}
	w.f, w.size = f, info.Size()

	return nil
}

func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, os.ErrClosed
	}
	// A failed rotate or reopen leaves no file, and dropping log lines silently would hide the cause.
	if w.f == nil && w.open() != nil {
		return w.fallback.Write(p)
	}
	// An empty file is never rotated, so an oversized write still lands whole.
	if w.size > 0 && w.size+int64(len(p)) > w.maxBytes {
		if w.rotate() != nil {
			return w.fallback.Write(p)
		}
	}
	n, err := w.f.Write(p)
	w.size += int64(n)

	return n, err
}

func (w *writer) rotate() error {
	err := w.f.Close()
	w.f = nil
	if err != nil {
		return fmt.Errorf("failed to close log file: %w", err)
	}
	for i := w.keep; i >= 1; i-- {
		src := w.path
		if i > 1 {
			src = w.path + "." + strconv.Itoa(i-1)
		}
		if err := os.Rename(src, w.path+"."+strconv.Itoa(i)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to rotate log file: %w", err)
		}
	}
	if w.keep < 1 {
		if err := os.Remove(w.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to rotate log file: %w", err)
		}
	}

	return w.open()
}

func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	if w.f == nil {
		return nil
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("failed to close log file: %w", err)
	}

	return nil
}
