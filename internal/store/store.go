package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/quacktel/cli/internal/paths"
	"github.com/quacktel/cli/internal/table"
)

const (
	defaultMaxServices   = 256
	defaultBufferBytes   = 64 << 20
	defaultFlushInterval = 2 * time.Second

	maxSlugLen      = 64
	manifestName    = "manifest.json"
	manifestVersion = 1
	limitMsg        = "service limit reached"
)

var ErrFull = errors.New("store buffer is full")

type Options struct {
	DataDir       string
	MaxServices   int
	BufferBytes   int64
	FlushInterval time.Duration
	Logger        *slog.Logger
	Now           func() time.Time
}

type Stats struct {
	RecordsAccepted int64
	RecordsRejected int64
	Flushes         int64
	FilesWritten    int64
	BytesWritten    int64
	Services        int
}

type manifestFile struct {
	Path    string    `json:"path"`
	Signal  string    `json:"signal"`
	Rows    int64     `json:"rows"`
	Bytes   int64     `json:"bytes"`
	MinTime time.Time `json:"min_time,omitzero"`
	MaxTime time.Time `json:"max_time,omitzero"`
}

type manifest struct {
	SchemaVersion int            `json:"schema_version"`
	ServiceName   string         `json:"service_name"`
	Generation    int64          `json:"generation"`
	Files         []manifestFile `json:"files"`
}

type service struct {
	dir      string
	builder  *table.LogsBuilder
	manifest manifest
}

type Store struct {
	opts Options
	log  *slog.Logger
	mem  memory.Allocator

	mu       sync.Mutex
	lock     *os.File
	services map[string]*service
	// active counts services fed in this process, so stale on-disk dirs never block new ones.
	active map[string]struct{}
	stats  Stats
	closed bool
}

var slugReplacer = func(r rune) rune {
	switch {
	case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
		return r
	}

	return '_'
}

// ServiceDir keeps untrusted service names off the filesystem as paths; the hash separates names that slug alike.
func ServiceDir(name string) string {
	if name == "" {
		name = "unknown_service"
	}
	slug := strings.TrimLeft(strings.Map(slugReplacer, strings.ToLower(name)), ".-")
	if len(slug) > maxSlugLen {
		slug = slug[:maxSlugLen]
	}
	// A leading '-' would read as a flag to shell tools, so an empty slug gets a placeholder.
	if slug == "" {
		slug = "_"
	}
	sum := sha256.Sum256([]byte(name))

	return slug + "-" + hex.EncodeToString(sum[:])[:8]
}

func Open(ctx context.Context, opts Options) (*Store, error) {
	if opts.MaxServices == 0 {
		opts.MaxServices = defaultMaxServices
	}
	if opts.BufferBytes == 0 {
		opts.BufferBytes = defaultBufferBytes
	}
	if opts.FlushInterval == 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}

	if err := paths.EnsureDir(opts.DataDir); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(opts.DataDir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(fmt.Errorf("another quacktel serve is using data dir %s: %w", opts.DataDir, err), lock.Close())
	}

	s := &Store{opts: opts, log: opts.Logger, mem: memory.DefaultAllocator, lock: lock, services: map[string]*service{}, active: map[string]struct{}{}}
	if err := s.init(ctx); err != nil {
		return nil, errors.Join(err, s.unlock())
	}

	return s, nil
}

func (s *Store) init(ctx context.Context) error {
	tmp := s.tmpDir()
	if err := os.RemoveAll(tmp); err != nil {
		return fmt.Errorf("failed to clear tmp dir: %w", err)
	}
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return fmt.Errorf("failed to create tmp dir: %w", err)
	}

	root := filepath.Join(s.opts.DataDir, "services")
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to list services: %w", err)
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !e.IsDir() {
			continue
		}
		if err := s.load(filepath.Join(root, e.Name())); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) load(dir string) error {
	data, err := os.ReadFile(filepath.Join(dir, manifestName)) //nolint:gosec // G304: path is derived from the data dir, never from request input
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read manifest in %s: %w", dir, err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("failed to parse manifest in %s: %w", dir, err)
	}
	if filepath.Join(filepath.Dir(dir), ServiceDir(m.ServiceName)) != dir {
		return fmt.Errorf("manifest in %s names service %q, which maps to a different directory", dir, m.ServiceName)
	}

	listed := make(map[string]bool, len(m.Files))
	for _, f := range m.Files {
		listed[f.Path] = true
	}
	changed := false
	err = filepath.WalkDir(filepath.Join(dir, "logs"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}

			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".parquet") {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if listed[rel] {
			return nil
		}
		mf, err := describe(path, rel)
		if err != nil {
			return err
		}
		m.Files = append(m.Files, mf)
		changed = true

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile %s: %w", dir, err)
	}

	sv := &service{dir: dir, manifest: m}
	if changed {
		if err := s.publish(sv); err != nil {
			return err
		}
	}
	s.services[m.ServiceName] = sv

	return nil
}

func describe(path, rel string) (manifestFile, error) {
	info, err := os.Stat(path)
	if err != nil {
		return manifestFile{}, fmt.Errorf("failed to stat %s: %w", path, err)
	}
	r, err := file.OpenParquetFile(path, false)
	if err != nil {
		return manifestFile{}, fmt.Errorf("failed to open parquet %s: %w", path, err)
	}
	rows := r.NumRows()
	if err := r.Close(); err != nil {
		return manifestFile{}, fmt.Errorf("failed to close parquet %s: %w", path, err)
	}

	return manifestFile{Path: rel, Signal: "logs", Rows: rows, Bytes: info.Size()}, nil
}

func (s *Store) tmpDir() string { return filepath.Join(s.opts.DataDir, ".tmp") }

func (s *Store) AppendLogs(_ context.Context, ld plog.Logs) (int64, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, "", errors.New("store is closed")
	}
	var buffered int64
	for _, sv := range s.services {
		if sv.builder != nil {
			buffered += sv.builder.EstimatedBytes()
		}
	}
	// The wire size over-estimates slightly, which keeps the check all-or-nothing without a rollback path in the builders.
	if buffered+int64((&plog.ProtoMarshaler{}).LogsSize(ld)) > s.opts.BufferBytes {
		return 0, "", fmt.Errorf("batch does not fit in %d buffered bytes: %w", s.opts.BufferBytes, ErrFull)
	}

	now := s.opts.Now()
	var rejected, accepted int64
	rls := ld.ResourceLogs()
	for i := range rls.Len() {
		rl := rls.At(i)
		name := table.ServiceName(rl.Resource())
		_, isActive := s.active[name]
		if !isActive && len(s.active) >= s.opts.MaxServices {
			sls := rl.ScopeLogs()
			for j := range sls.Len() {
				rejected += int64(sls.At(j).LogRecords().Len())
			}

			continue
		}
		s.active[name] = struct{}{}
		sv, ok := s.services[name]
		if !ok {
			sv = &service{
				dir:      filepath.Join(s.opts.DataDir, "services", ServiceDir(name)),
				manifest: manifest{SchemaVersion: manifestVersion, ServiceName: name},
			}
			s.services[name] = sv
		}
		if sv.builder == nil {
			sv.builder = table.NewLogsBuilder(s.mem)
		}
		sls := rl.ScopeLogs()
		for j := range sls.Len() {
			sl := sls.At(j)
			recs := sl.LogRecords()
			for k := range recs.Len() {
				sv.builder.Append(rl.Resource(), sl.Scope(), recs.At(k), now)
				accepted++
			}
		}
	}
	s.stats.RecordsAccepted += accepted
	s.stats.RecordsRejected += rejected

	msg := ""
	if rejected > 0 {
		msg = limitMsg
	}

	return rejected, msg, nil
}

func (s *Store) Run(ctx context.Context) error {
	t := time.NewTicker(s.opts.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.Flush(ctx); err != nil && ctx.Err() == nil {
				s.log.ErrorContext(ctx, "flush failed", "error", err)
			}
		}
	}
}

func (s *Store) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.flush(ctx)
}

func (s *Store) flush(ctx context.Context) error {
	var errs []error
	for _, sv := range s.services {
		if sv.builder == nil || sv.builder.Len() == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if err := s.flushService(sv); err != nil {
			errs = append(errs, fmt.Errorf("failed to flush service %q: %w", sv.manifest.ServiceName, err))
		}
	}

	return errors.Join(errs...)
}

func (s *Store) flushService(sv *service) (err error) {
	minT, maxT := sv.builder.MinMaxTime()
	rec := sv.builder.NewRecord()
	defer rec.Release()

	tmpPath := filepath.Join(s.tmpDir(), ulid.Make().String()+".parquet")
	size, err := writeParquet(tmpPath, rec)
	if err != nil {
		return errors.Join(err, os.Remove(tmpPath))
	}

	part := filepath.Join(sv.dir, "logs", s.opts.Now().UTC().Format("date=2006-01-02/hour=15"))
	if err := os.MkdirAll(part, 0o700); err != nil {
		return errors.Join(fmt.Errorf("failed to create partition dir: %w", err), os.Remove(tmpPath))
	}
	final := filepath.Join(part, "tail-"+ulid.Make().String()+".parquet")
	if err := os.Rename(tmpPath, final); err != nil {
		return errors.Join(fmt.Errorf("failed to rename parquet into place: %w", err), os.Remove(tmpPath))
	}
	if err := syncDir(part); err != nil {
		return err
	}

	rel, err := filepath.Rel(sv.dir, final)
	if err != nil {
		return fmt.Errorf("failed to relativise %s: %w", final, err)
	}
	prev := sv.manifest
	sv.manifest.Files = append(append([]manifestFile(nil), prev.Files...), manifestFile{
		Path: rel, Signal: "logs", Rows: rec.NumRows(), Bytes: size, MinTime: minT.UTC(), MaxTime: maxT.UTC(),
	})
	if err := s.publish(sv); err != nil {
		sv.manifest = prev

		return err
	}

	s.stats.Flushes++
	s.stats.FilesWritten++
	s.stats.BytesWritten += size

	return nil
}

// nopCloser keeps pqarrow from closing the file, so the caller can sync it first.
type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func writeParquet(path string, rec arrow.RecordBatch) (size int64, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: path is derived from the data dir, never from request input
	if err != nil {
		return 0, fmt.Errorf("failed to create %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close %s: %w", path, cerr)
		}
	}()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	w, err := pqarrow.NewFileWriter(rec.Schema(), nopCloser{f}, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return 0, fmt.Errorf("failed to create parquet writer: %w", err)
	}
	if err := w.Write(rec); err != nil {
		return 0, errors.Join(fmt.Errorf("failed to write parquet rows: %w", err), w.Close())
	}
	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("failed to finish parquet file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat %s: %w", path, err)
	}

	return info.Size(), nil
}

// publish bumps the generation and renames a fully synced copy over manifest.json.
func (s *Store) publish(sv *service) error {
	next := sv.manifest
	next.Generation++
	data, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode manifest: %w", err)
	}

	tmpPath := filepath.Join(s.tmpDir(), "manifest-"+ulid.Make().String()+".json")
	if err := writeSynced(tmpPath, data); err != nil {
		return errors.Join(err, os.Remove(tmpPath))
	}
	if err := os.MkdirAll(sv.dir, 0o700); err != nil {
		return errors.Join(fmt.Errorf("failed to create service dir: %w", err), os.Remove(tmpPath))
	}
	if err := os.Rename(tmpPath, filepath.Join(sv.dir, manifestName)); err != nil {
		return errors.Join(fmt.Errorf("failed to publish manifest: %w", err), os.Remove(tmpPath))
	}
	if err := syncDir(sv.dir); err != nil {
		return err
	}
	sv.manifest = next

	return nil
}

func writeSynced(path string, data []byte) (err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: path is derived from the data dir, never from request input
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close %s: %w", path, cerr)
		}
	}()
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync %s: %w", path, err)
	}

	return nil
}

func syncDir(path string) (err error) {
	d, err := os.Open(path) //nolint:gosec // G304: path is derived from the data dir, never from request input
	if err != nil {
		return fmt.Errorf("failed to open dir %s: %w", path, err)
	}
	defer func() {
		if cerr := d.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close dir %s: %w", path, cerr)
		}
	}()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("failed to sync dir %s: %w", path, err)
	}

	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	err := s.flush(context.Background())
	for _, sv := range s.services {
		if sv.builder != nil {
			sv.builder.Release()
		}
	}

	return errors.Join(err, s.unlock())
}

func (s *Store) unlock() error {
	return errors.Join(syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN), s.lock.Close())
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.stats
	st.Services = len(s.active)

	return st
}
