package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

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
}

type statsFile struct {
	Requests        int64     `json:"requests"`
	RecordsAccepted int64     `json:"records_accepted"`
	RecordsRejected int64     `json:"records_rejected"`
	ValuesRedacted  int64     `json:"values_redacted"`
	Unauthorized    int64     `json:"unauthorized"`
	Responses429    int64     `json:"responses_429"`
	MetricsDropped  int64     `json:"metrics_dropped"`
	Flushes         int64     `json:"flushes"`
	FilesWritten    int64     `json:"files_written"`
	BytesWritten    int64     `json:"bytes_written"`
	Services        int       `json:"services"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (c *serveCmd) Run(ctx context.Context, globals *Globals) error {
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

	srv, err := receiver.New(receiver.Options{
		Addr:        c.Listen,
		UnsafeBind:  c.UnsafeBind,
		MaxBody:     c.MaxBody,
		MaxInFlight: c.MaxInFlight,
		Token:       token,
		Logger:      globals.Logger,
	}, sink)
	if err != nil {
		return errors.Join(fmt.Errorf("failed to start receiver: %w", err), st.Close())
	}

	globals.Logger.InfoContext(ctx, "serving", slog.String("addr", srv.Addr()), slog.String("data_dir", dir))

	writeStats := func() error { return saveStats(dir, srv.Stats(), st.Stats(), redacted.Load()) }

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
				if err := writeStats(); err != nil {
					globals.Logger.WarnContext(runCtx, "failed to write stats", slog.Any("error", err))
				}
			}
		}
	})

	// The receiver drains in-flight requests before the store's final flush.
	serveErr := srv.Serve(runCtx)
	cancel()
	wg.Wait()

	return errors.Join(serveErr, st.Close(), writeStats())
}

func saveStats(dir string, rs receiver.Stats, ss store.Stats, redacted int64) (err error) {
	data, err := json.Marshal(statsFile{
		Requests:        rs.Requests,
		RecordsAccepted: rs.RecordsAccepted,
		RecordsRejected: rs.RecordsRejected,
		ValuesRedacted:  redacted,
		Unauthorized:    rs.Unauthorized,
		Responses429:    rs.Responses429,
		MetricsDropped:  rs.MetricsDropped,
		Flushes:         ss.Flushes,
		FilesWritten:    ss.FilesWritten,
		BytesWritten:    ss.BytesWritten,
		Services:        ss.Services,
		UpdatedAt:       time.Now().UTC(),
	})
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

const otlpHeadersFile = "otlp-headers"

var otlpHeadersFormat = regexp.MustCompile(`^authorization=Bearer ([0-9a-f]{64})\n$`)

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
