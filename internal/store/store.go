package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/paths"
	"github.com/quacktel/cli/internal/table"
)

const (
	defaultMaxServices   = 256
	defaultBufferBytes   = 64 << 20
	defaultFlushInterval = 2 * time.Second
	defaultCompactEvery  = 5 * time.Minute
	defaultSealMinFiles  = 10
	defaultRetireGrace   = 60 * time.Second
	defaultRetention     = 720 * time.Hour
	defaultMaxDiskBytes  = 10 << 30

	maxSlugLen = 64
	limitMsg   = "service limit reached"

	signalLogs  = "logs"
	signalSpans = "spans"
)

// signals lists the per-signal directories under a service dir.
var signals = []string{signalLogs, signalSpans}

var ErrFull = errors.New("store buffer is full")

func (s *Store) diskCapError() error {
	return fmt.Errorf("open hour exceeds the %d byte disk cap: %w", s.opts.MaxDiskBytes, ErrFull)
}

type Options struct {
	DataDir       string
	MaxServices   int
	BufferBytes   int64
	FlushInterval time.Duration
	Logger        *slog.Logger
	Now           func() time.Time

	CompactInterval time.Duration
	SealMinFiles    int
	RetireGrace     time.Duration
	Retention       time.Duration
	MaxDiskBytes    int64
}

type Stats struct {
	RecordsAccepted int64
	RecordsRejected int64
	SpansAccepted   int64
	SpansRejected   int64
	Flushes         int64
	FilesWritten    int64
	BytesWritten    int64
	Services        int
	Compactions     int64
	FilesRetired    int64
	BytesOnDisk     int64
	PendingBatches  int64
	WriteErrors     int64
	// DiskCapExceeded is true while the open hour alone is over the disk cap and writes are refused.
	DiskCapExceeded bool
}

// signalBuilder is what the store needs from a per-signal Arrow builder.
type signalBuilder interface {
	Len() int
	EstimatedBytes() int64
	MinMaxTime() (time.Time, time.Time)
	NewRecord() arrow.RecordBatch
	Release()
}

type service struct {
	dir      string
	builders map[string]signalBuilder
	manifest manifest.Manifest
	// pending holds swapped-out batches awaiting a successful write, per signal.
	pending map[string][]pendingBatch
	// unpublished lists renamed files whose manifest publish failed.
	unpublished []manifest.File
}

type Store struct {
	opts Options
	log  *slog.Logger
	mem  memory.Allocator
	// writeParquet is a seam so tests can block or fail writes.
	writeParquet func(path string, rec arrow.RecordBatch) (int64, error)

	// mm serialises maintenance passes and Close; lastHousekeeping is guarded by it.
	mm               sync.Mutex
	lastHousekeeping time.Time

	mu       sync.Mutex
	lock     *os.File
	services map[string]*service
	// active counts services fed in this process, so stale on-disk dirs never block new ones.
	active map[string]struct{}
	stats  Stats
	closed bool
	// capExceeded is set by retain when only open-hour data is left over the disk cap.
	capExceeded bool
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
	if opts.CompactInterval == 0 {
		opts.CompactInterval = defaultCompactEvery
	}
	if opts.SealMinFiles == 0 {
		opts.SealMinFiles = defaultSealMinFiles
	}
	if opts.RetireGrace == 0 {
		opts.RetireGrace = defaultRetireGrace
	}
	if opts.Retention == 0 {
		opts.Retention = defaultRetention
	}
	if opts.MaxDiskBytes == 0 {
		opts.MaxDiskBytes = defaultMaxDiskBytes
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

	s := &Store{opts: opts, log: opts.Logger, mem: memory.DefaultAllocator, lock: lock, writeParquet: writeParquet, services: map[string]*service{}, active: map[string]struct{}{}}
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
	m, err := manifest.Read(filepath.Join(dir, manifest.Name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if filepath.Join(filepath.Dir(dir), ServiceDir(m.ServiceName)) != dir {
		return fmt.Errorf("manifest in %s names service %q, which maps to a different directory", dir, m.ServiceName)
	}

	listed := make(map[string]bool, len(m.Files)+len(m.Retired))
	for _, f := range m.Files {
		listed[f.Path] = true
	}
	// No reader survives a restart, so retired files go immediately.
	changed := len(m.Retired) > 0
	for _, r := range m.Retired {
		if err := removeFile(dir, r.Path); err != nil {
			return err
		}
		s.stats.FilesRetired++
	}
	m.Retired = nil
	for _, signal := range signals {
		found, err := reconcile(dir, signal, listed)
		if err != nil {
			return fmt.Errorf("failed to reconcile %s: %w", dir, err)
		}
		m.Files = append(m.Files, found...)
		changed = changed || len(found) > 0
	}

	sv := s.newService(dir, m)
	if changed {
		if err := s.publish(sv); err != nil {
			return err
		}
	}
	s.services[m.ServiceName] = sv

	return nil
}

// reconcile describes parquet files under dir/signal that the manifest does not list.
func reconcile(dir, signal string, listed map[string]bool) ([]manifest.File, error) {
	var found []manifest.File
	var orphans []string
	err := filepath.WalkDir(filepath.Join(dir, signal), func(path string, d fs.DirEntry, err error) error {
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
		// An unlisted merge output is an uncommitted compaction whose sources are still listed, so keeping it would duplicate rows.
		if strings.HasPrefix(d.Name(), sealedPrefix) || strings.HasPrefix(d.Name(), hourPrefix) {
			orphans = append(orphans, path)

			return nil
		}
		mf, err := describe(path, rel, signal)
		if err != nil {
			return err
		}
		found = append(found, mf)

		return nil
	})

	if err != nil {
		return nil, err
	}
	for _, p := range orphans {
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("failed to remove uncommitted merge %s: %w", p, err)
		}
	}

	return found, nil
}

func describe(path, rel, signal string) (manifest.File, error) {
	info, err := os.Stat(path)
	if err != nil {
		return manifest.File{}, fmt.Errorf("failed to stat %s: %w", path, err)
	}
	r, err := file.OpenParquetFile(path, false)
	if err != nil {
		return manifest.File{}, fmt.Errorf("failed to open parquet %s: %w", path, err)
	}
	rows := r.NumRows()
	if err := r.Close(); err != nil {
		return manifest.File{}, fmt.Errorf("failed to close parquet %s: %w", path, err)
	}

	return manifest.File{Path: rel, Signal: signal, Rows: rows, Bytes: info.Size()}, nil
}

func (s *Store) tmpDir() string { return filepath.Join(s.opts.DataDir, ".tmp") }

func (s *Store) serviceDir(name string) string {
	return filepath.Join(s.opts.DataDir, "services", ServiceDir(name))
}

func (s *Store) newService(dir string, m manifest.Manifest) *service {
	return &service{dir: dir, builders: map[string]signalBuilder{}, manifest: m, pending: map[string][]pendingBatch{}}
}

// removeFile unlinks a manifest-relative path; a missing file counts as deleted.
func removeFile(dir, rel string) error {
	if !filepath.IsLocal(rel) {
		return fmt.Errorf("manifest path %q escapes service dir %s", rel, dir)
	}
	if err := os.Remove(filepath.Join(dir, rel)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to remove %s: %w", rel, err)
	}

	return nil
}

// buffered sums buffered and pending bytes across signals, so a failing disk ends in ErrFull rather than unbounded memory.
func (s *Store) buffered() int64 {
	var n int64
	for _, sv := range s.services {
		for _, b := range sv.builders {
			n += b.EstimatedBytes()
		}
		for _, batches := range sv.pending {
			for _, p := range batches {
				n += p.bytes
			}
		}
	}

	return n
}

// admit marks name active, or reports false when the service cap leaves no room for it.
func (s *Store) admit(name string) (*service, bool) {
	if _, ok := s.active[name]; !ok && len(s.active) >= s.opts.MaxServices {
		return nil, false
	}
	s.active[name] = struct{}{}
	sv, ok := s.services[name]
	if !ok {
		sv = s.newService(s.serviceDir(name), manifest.Manifest{SchemaVersion: manifest.Version, ServiceName: name})
		s.services[name] = sv
	}

	return sv, true
}

func (s *Store) AppendTraces(_ context.Context, td ptrace.Traces) (int64, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, "", errors.New("store is closed")
	}
	if s.capExceeded {
		return 0, "", s.diskCapError()
	}
	if s.buffered()+int64((&ptrace.ProtoMarshaler{}).TracesSize(td)) > s.opts.BufferBytes {
		return 0, "", fmt.Errorf("batch does not fit in %d buffered bytes: %w", s.opts.BufferBytes, ErrFull)
	}

	now := s.opts.Now()
	var rejected, accepted int64
	rss := td.ResourceSpans()
	for i := range rss.Len() {
		rs := rss.At(i)
		sv, ok := s.admit(table.ServiceName(rs.Resource()))
		if !ok {
			sss := rs.ScopeSpans()
			for j := range sss.Len() {
				rejected += int64(sss.At(j).Spans().Len())
			}

			continue
		}
		if sv.builders[signalSpans] == nil {
			sv.builders[signalSpans] = table.NewSpansBuilder(s.mem)
		}
		b := sv.builders[signalSpans].(*table.SpansBuilder)
		sss := rs.ScopeSpans()
		for j := range sss.Len() {
			ss := sss.At(j)
			spans := ss.Spans()
			for k := range spans.Len() {
				b.Append(rs.Resource(), ss.Scope(), spans.At(k), now)
				accepted++
			}
		}
	}
	s.stats.SpansAccepted += accepted
	s.stats.SpansRejected += rejected

	msg := ""
	if rejected > 0 {
		msg = limitMsg
	}

	return rejected, msg, nil
}

func (s *Store) AppendLogs(_ context.Context, ld plog.Logs) (int64, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, "", errors.New("store is closed")
	}
	if s.capExceeded {
		return 0, "", s.diskCapError()
	}
	// The wire size over-estimates slightly, which keeps the check all-or-nothing without a rollback path in the builders.
	if s.buffered()+int64((&plog.ProtoMarshaler{}).LogsSize(ld)) > s.opts.BufferBytes {
		return 0, "", fmt.Errorf("batch does not fit in %d buffered bytes: %w", s.opts.BufferBytes, ErrFull)
	}

	now := s.opts.Now()
	var rejected, accepted int64
	rls := ld.ResourceLogs()
	for i := range rls.Len() {
		rl := rls.At(i)
		sv, ok := s.admit(table.ServiceName(rl.Resource()))
		if !ok {
			sls := rl.ScopeLogs()
			for j := range sls.Len() {
				rejected += int64(sls.At(j).LogRecords().Len())
			}

			continue
		}
		if sv.builders[signalLogs] == nil {
			sv.builders[signalLogs] = table.NewLogsBuilder(s.mem)
		}
		b := sv.builders[signalLogs].(*table.LogsBuilder)
		sls := rl.ScopeLogs()
		for j := range sls.Len() {
			sl := sls.At(j)
			recs := sl.LogRecords()
			for k := range recs.Len() {
				b.Append(rl.Resource(), sl.Scope(), recs.At(k), now)
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

// publish bumps the generation and atomically replaces manifest.json.
func (s *Store) publish(sv *service) error {
	next := sv.manifest
	next.Generation++
	if err := os.MkdirAll(sv.dir, 0o700); err != nil {
		return fmt.Errorf("failed to create service dir: %w", err)
	}
	if err := manifest.Write(s.tmpDir(), filepath.Join(sv.dir, manifest.Name), next); err != nil {
		return err
	}
	sv.manifest = next

	return nil
}

func (s *Store) unlock() error {
	return errors.Join(syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN), s.lock.Close())
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.stats
	st.DiskCapExceeded = s.capExceeded
	st.Services = len(s.active)
	for _, sv := range s.services {
		for _, f := range sv.manifest.Files {
			st.BytesOnDisk += f.Bytes
		}
		for _, batches := range sv.pending {
			st.PendingBatches += int64(len(batches))
		}
	}

	return st
}
