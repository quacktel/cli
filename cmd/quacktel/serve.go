package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/quacktel/cli/internal/paths"
	"github.com/quacktel/cli/internal/receiver"
	"github.com/quacktel/cli/internal/redact"
	"github.com/quacktel/cli/internal/store"
)

type serveCmd struct {
	Listen        string        `help:"OTLP/HTTP listen address." default:"${listen}"`
	UnsafeBind    bool          `help:"Allow a non-loopback listen address. Implies --auth."`
	Auth          bool          `help:"Require a bearer token, stored in <data-dir>/otlp-headers."`
	DataDir       string        `help:"Data directory. Defaults to the per-user config dir." type:"path"`
	MaxBody       int64         `help:"Maximum request body size in bytes, before and after decompression." default:"${max_body}"`
	MaxInFlight   int           `name:"max-inflight" help:"Maximum concurrent requests before new ones get 429." default:"${max_inflight}"`
	BufferBytes   int64         `help:"Maximum buffered bytes before requests get 429." default:"67108864"`
	FlushInterval time.Duration `help:"How often buffered logs are written to Parquet." default:"2s"`
	MaxServices   int           `help:"Maximum number of distinct services." default:"256"`

	Retention       time.Duration `help:"Delete data older than this, in whole hourly partitions." default:"720h"`
	MaxDisk         int64         `help:"Maximum bytes of Parquet on disk; the oldest closed hours are deleted first." default:"${max_disk}"`
	CompactInterval time.Duration `help:"How often compaction and retention run." default:"5m"`
	SealMinFiles    int           `help:"Tail files in the open hour that trigger a seal. Must be at least 2." default:"10"`
	RetireGrace     time.Duration `help:"How long a replaced or expired file stays on disk for running queries." default:"60s"`
}

func (c *serveCmd) validate() error {
	switch {
	case c.Retention <= 0:
		return fmt.Errorf("--retention must be greater than 0, got %s", c.Retention)
	case c.MaxDisk <= 0:
		return fmt.Errorf("--max-disk must be greater than 0, got %d", c.MaxDisk)
	case c.CompactInterval <= 0:
		return fmt.Errorf("--compact-interval must be greater than 0, got %s", c.CompactInterval)
	case c.SealMinFiles < 2:
		return fmt.Errorf("--seal-min-files must be at least 2, got %d", c.SealMinFiles)
	case c.RetireGrace <= 0:
		return fmt.Errorf("--retire-grace must be greater than 0, got %s", c.RetireGrace)
	}

	return nil
}

type statsFile struct {
	Requests        int64     `json:"requests"`
	RecordsAccepted int64     `json:"records_accepted"`
	RecordsRejected int64     `json:"records_rejected"`
	ValuesRedacted  int64     `json:"values_redacted"`
	Unauthorized    int64     `json:"unauthorized"`
	Responses429    int64     `json:"responses_429"`
	SpansAccepted   int64     `json:"spans_accepted"`
	SpansRejected   int64     `json:"spans_rejected"`
	MetricsDropped  int64     `json:"metrics_dropped"`
	Flushes         int64     `json:"flushes"`
	FilesWritten    int64     `json:"files_written"`
	BytesWritten    int64     `json:"bytes_written"`
	Services        int       `json:"services"`
	Compactions     int64     `json:"compactions"`
	FilesRetired    int64     `json:"files_retired"`
	BytesOnDisk     int64     `json:"bytes_on_disk"`
	PendingBatches  int64     `json:"pending_batches"`
	WriteErrors     int64     `json:"write_errors"`
	DiskCapExceeded bool      `json:"disk_cap_exceeded"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (c *serveCmd) Run(ctx context.Context, globals *Globals) error {
	if err := c.validate(); err != nil {
		return err
	}

	dir := c.DataDir
	if dir == "" {
		var err error
		if dir, err = paths.DataDir(); err != nil {
			return err
		}
	}

	var token string
	if c.Auth || c.UnsafeBind {
		headers := filepath.Join(dir, otlpHeadersFile)
		var err error
		if token, err = loadOrCreateOTLPHeaders(headers); err != nil {
			return err
		}
		globals.Logger.InfoContext(ctx, "auth enabled", slog.String("headers_file", headers))
	}

	st, err := store.Open(ctx, store.Options{
		DataDir:       dir,
		MaxServices:   c.MaxServices,
		BufferBytes:   c.BufferBytes,
		FlushInterval: c.FlushInterval,
		Logger:        globals.Logger,

		CompactInterval: c.CompactInterval,
		SealMinFiles:    c.SealMinFiles,
		RetireGrace:     c.RetireGrace,
		Retention:       c.Retention,
		MaxDiskBytes:    c.MaxDisk,
	})
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}

	var redacted atomic.Int64
	redactor := redact.Default()
	sink := func(ctx context.Context, ld plog.Logs) (int64, string, error) {
		redacted.Add(int64(redactor.Logs(ld)))
		rejected, msg, err := st.AppendLogs(ctx, ld)
		if errors.Is(err, store.ErrFull) {
			err = fmt.Errorf("%w: %w", receiver.ErrFull, err)
		}

		return rejected, msg, err
	}
	traces := func(ctx context.Context, td ptrace.Traces) (int64, string, error) {
		redacted.Add(int64(redactor.Traces(td)))
		rejected, msg, err := st.AppendTraces(ctx, td)
		if errors.Is(err, store.ErrFull) {
			err = fmt.Errorf("%w: %w", receiver.ErrFull, err)
		}

		return rejected, msg, err
	}

	srv, err := receiver.New(receiver.Options{
		Addr:        c.Listen,
		UnsafeBind:  c.UnsafeBind,
		MaxBody:     c.MaxBody,
		MaxInFlight: c.MaxInFlight,
		Token:       token,
		Logger:      globals.Logger,
	}, receiver.Sinks{Logs: sink, Traces: traces})
	if err != nil {
		return errors.Join(fmt.Errorf("failed to start receiver: %w", err), st.Close())
	}

	globals.Logger.InfoContext(ctx, "serving", slog.String("addr", srv.Addr()), slog.String("data_dir", dir))

	sw := &statsWriter{dir: dir, srv: srv, st: st, redacted: &redacted}
	if err := sw.write(true); err != nil {
		return errors.Join(err, st.Close())
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() { _ = st.Run(runCtx) })
	wg.Go(func() {
		t := time.NewTicker(c.FlushInterval)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				if err := sw.write(false); err != nil {
					globals.Logger.WarnContext(runCtx, "failed to write stats", slog.Any("error", err))
				}
			}
		}
	})

	// The receiver drains in-flight requests before the store's final flush.
	serveErr := srv.Serve(runCtx)
	cancel()
	wg.Wait()

	return errors.Join(serveErr, st.Close(), sw.write(true))
}

// statsWriter skips unchanged stats so an idle daemon doesn't rewrite the file every tick.
type statsWriter struct {
	dir      string
	srv      *receiver.Server
	st       *store.Store
	redacted *atomic.Int64
	last     []byte
}

func (w *statsWriter) write(force bool) error {
	sf := buildStats(w.srv.Stats(), w.st.Stats(), w.redacted.Load())
	cmp, err := json.Marshal(sf)
	if err != nil {
		return fmt.Errorf("failed to encode stats: %w", err)
	}
	if !force && bytes.Equal(cmp, w.last) {
		return nil
	}
	sf.UpdatedAt = time.Now().UTC()
	if err := saveStats(w.dir, sf); err != nil {
		return err
	}
	w.last = cmp

	return nil
}

func buildStats(rs receiver.Stats, ss store.Stats, redacted int64) statsFile {
	return statsFile{
		Requests:        rs.Requests,
		RecordsAccepted: rs.RecordsAccepted,
		RecordsRejected: rs.RecordsRejected,
		ValuesRedacted:  redacted,
		Unauthorized:    rs.Unauthorized,
		Responses429:    rs.Responses429,
		SpansAccepted:   rs.SpansAccepted,
		SpansRejected:   rs.SpansRejected,
		MetricsDropped:  rs.MetricsDropped,
		Flushes:         ss.Flushes,
		FilesWritten:    ss.FilesWritten,
		BytesWritten:    ss.BytesWritten,
		Services:        ss.Services,
		Compactions:     ss.Compactions,
		FilesRetired:    ss.FilesRetired,
		BytesOnDisk:     ss.BytesOnDisk,
		PendingBatches:  ss.PendingBatches,
		WriteErrors:     ss.WriteErrors,
		DiskCapExceeded: ss.DiskCapExceeded,
	}
}

func saveStats(dir string, sf statsFile) (err error) {
	data, err := json.Marshal(sf)
	if err != nil {
		return fmt.Errorf("failed to encode stats: %w", err)
	}

	tmpDir := filepath.Join(dir, ".tmp")
	if err := os.MkdirAll(tmpDir, 0o700); err != nil {
		return fmt.Errorf("failed to create %s: %w", tmpDir, err)
	}
	// CreateTemp makes the file 0600.
	f, err := os.CreateTemp(tmpDir, "stats-*.json")
	if err != nil {
		return fmt.Errorf("failed to create stats file: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.Remove(f.Name()))
		}
	}()
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("failed to write stats file: %w", err)
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, "stats.json")); err != nil {
		return fmt.Errorf("failed to publish stats file: %w", err)
	}

	return nil
}

const otlpHeadersFile = paths.OTLPHeadersFile

var otlpHeadersFormat = paths.OTLPHeadersFormat

// loadOrCreateOTLPHeaders keeps the file across restarts, because the gateway reads it only at startup.
func loadOrCreateOTLPHeaders(path string) (string, error) {
	var raw [32]byte
	_, _ = rand.Read(raw[:])
	token := hex.EncodeToString(raw[:])
	err := writeNewFile(path, []byte("authorization=Bearer "+token+"\n"))
	if err == nil {
		return token, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return "", err
	}

	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("failed to stat %s: %w", path, err)
	}
	if info.Mode().Perm()&^0o600 != 0 {
		return "", fmt.Errorf("%s has mode %04o, want 0600 or stricter; run chmod 600 on it", path, info.Mode().Perm())
	}
	data, err := os.ReadFile(path) //nolint:gosec // fixed name inside the data dir
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", path, err)
	}
	m := otlpHeadersFormat.FindStringSubmatch(string(data))
	if m == nil {
		return "", fmt.Errorf("%s is not in the form authorization=Bearer <64 hex characters>; delete it to create a new one", path)
	}

	return m[1], nil
}

// writeNewFile never leaves a partial file at path: the link fails with ErrExist if path already exists.
func writeNewFile(path string, data []byte) (err error) {
	dir := filepath.Dir(path)
	if err := paths.EnsureDir(dir); err != nil {
		return err
	}
	// CreateTemp makes the file 0600.
	f, err := os.CreateTemp(dir, ".otlp-headers-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() { err = errors.Join(err, os.Remove(f.Name())) }()
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("failed to write %s: %w", path, err)
	}
	if err := os.Link(f.Name(), path); err != nil {
		return fmt.Errorf("failed to create %s: %w", path, err)
	}

	return nil
}
