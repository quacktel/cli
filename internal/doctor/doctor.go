package doctor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/quacktel/cli/internal/engine"
	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/paths"
	"github.com/quacktel/cli/internal/service"
)

type Status string

const (
	OK   Status = "ok"
	Warn Status = "warn"
	Fail Status = "fail"
)

type Check struct {
	Name   string `json:"name"`
	Status Status `json:"status"`
	Detail string `json:"detail"`
}

type Config struct {
	DataDir   string
	Listen    string
	MaxDisk   int64
	PlistPath string
	Label     string
	Exe       string
	// Launchctl nil skips the launchd checks.
	Launchctl service.Runner
	HTTP      *http.Client
}

const (
	diskWarnPercent = 80
	probeTimeout    = 2 * time.Second
)

// Run never mutates state; the flock probe is released immediately.
func Run(ctx context.Context, cfg Config) []Check {
	stats, statsErr := readStats(cfg.DataDir)

	return []Check{
		checkDataDir(cfg),
		checkAuthHeaders(cfg),
		checkServeLock(cfg),
		checkReceiver(ctx, cfg),
		checkService(ctx, cfg),
		checkStats(stats, statsErr),
		checkDisk(cfg, stats, statsErr),
		checkManifests(cfg),
		checkReadPath(ctx, cfg),
	}
}

func check(name string, status Status, format string, args ...any) Check {
	return Check{Name: name, Status: status, Detail: fmt.Sprintf(format, args...)}
}

func checkDataDir(cfg Config) Check {
	info, err := os.Stat(cfg.DataDir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return check("data_dir", Fail, "%s does not exist; run quacktel serve or quacktel service install", cfg.DataDir)
	case err != nil:
		return check("data_dir", Fail, "%v", err)
	case !info.IsDir():
		return check("data_dir", Fail, "%s is not a directory", cfg.DataDir)
	case info.Mode().Perm() != 0o700:
		return check("data_dir", Fail, "%s has mode %04o, want 0700; run chmod 700 on it", cfg.DataDir, info.Mode().Perm())
	}

	return check("data_dir", OK, "%s (0700)", cfg.DataDir)
}

func checkAuthHeaders(cfg Config) Check {
	path := filepath.Join(cfg.DataDir, paths.OTLPHeadersFile)
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return check("auth_headers", OK, "auth off")
	case err != nil:
		return check("auth_headers", Fail, "%v", err)
	case info.Mode().Perm() != 0o600:
		return check("auth_headers", Fail, "%s has mode %04o, want 0600", path, info.Mode().Perm())
	}
	data, err := os.ReadFile(path) //nolint:gosec // fixed name inside the data dir
	if err != nil {
		return check("auth_headers", Fail, "%v", err)
	}
	if !paths.OTLPHeadersFormat.Match(data) {
		return check("auth_headers", Fail, "%s is not in the form authorization=Bearer <64 hex characters>", path)
	}

	return check("auth_headers", OK, "auth on (0600)")
}

func checkServeLock(cfg Config) Check {
	path := filepath.Join(cfg.DataDir, ".lock")
	// No O_CREATE: the probe must not leave a lock file behind.
	f, err := os.OpenFile(path, os.O_RDWR, 0) //nolint:gosec // fixed name inside the data dir
	if err != nil {
		return check("serve_lock", Fail, "no quacktel serve owns the data dir: %v", err)
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	switch {
	case err == nil:
		unlockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		if unlockErr != nil {
			return check("serve_lock", Fail, "failed to release probe lock: %v", unlockErr)
		}

		return check("serve_lock", Fail, "no quacktel serve owns the data dir")
	case errors.Is(err, syscall.EWOULDBLOCK):
		_ = f.Close()
		return check("serve_lock", OK, "held by a running quacktel serve")
	default:
		_ = f.Close()
		return check("serve_lock", Fail, "failed to probe lock: %v", err)
	}
}

func checkReceiver(ctx context.Context, cfg Config) Check {
	_, port, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return check("receiver", Fail, "invalid listen address %q: %v", cfg.Listen, err)
	}
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cfg.Listen+"/v1/logs", nil)
	if err != nil {
		return check("receiver", Fail, "%v", err)
	}
	// The receiver only answers hosts it knows, so a wildcard or hostname listen still needs a loopback Host.
	req.Host = net.JoinHostPort("127.0.0.1", port)

	client := cfg.HTTP
	if client == nil {
		client = &http.Client{}
	}
	// Copy so the caller's client keeps its own redirect policy.
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := c.Do(req)
	if err != nil {
		return check("receiver", Fail, "no receiver at %s: %v", cfg.Listen, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != http.MethodPost {
		return check("receiver", Fail, "%s answered GET /v1/logs with %d (Allow %q), want 405 with Allow POST; not a quacktel receiver", cfg.Listen, resp.StatusCode, resp.Header.Get("Allow"))
	}

	return check("receiver", OK, "quacktel receiver on %s", cfg.Listen)
}

func checkService(ctx context.Context, cfg Config) Check {
	if _, err := os.Stat(cfg.PlistPath); errors.Is(err, fs.ErrNotExist) {
		return check("service", OK, "no LaunchAgent installed")
	}
	p, err := service.ReadPlist(cfg.PlistPath)
	if err != nil {
		return check("service", Fail, "%v", err)
	}
	if len(p.ProgramArguments) == 0 {
		return check("service", Fail, "%s records no program arguments", cfg.PlistPath)
	}
	recorded := p.ProgramArguments[0]
	if _, err := os.Stat(recorded); err != nil {
		return check("service", Fail, "recorded binary %s is missing; run quacktel service install again", recorded)
	}

	var warning string
	if resolved, err := filepath.EvalSymlinks(recorded); err == nil && cfg.Exe != "" {
		if want, err := filepath.EvalSymlinks(cfg.Exe); err == nil && resolved != want {
			warning = fmt.Sprintf("agent runs %s but this is %s; run quacktel service install to update it", resolved, want)
		}
	}

	if cfg.Launchctl != nil {
		// Status reads the plist from Home, so derive it from the plist's own location.
		home := filepath.Dir(filepath.Dir(filepath.Dir(cfg.PlistPath)))
		m := &service.Manager{Label: cfg.Label, Home: home, UID: os.Getuid(), Run: cfg.Launchctl}
		st, err := m.Status(ctx)
		switch {
		case err != nil:
			return check("service", Fail, "%v", err)
		case !st.Loaded:
			return check("service", Fail, "%s is installed but not loaded; run quacktel service install", cfg.Label)
		case st.State != "running":
			return check("service", Fail, "%s is loaded but %s", cfg.Label, st.State)
		}
	}
	if warning != "" {
		return check("service", Warn, "%s", warning)
	}

	return check("service", OK, "%s running %s", cfg.Label, recorded)
}

type stats struct {
	WriteErrors     int64 `json:"write_errors"`
	DiskCapExceeded bool  `json:"disk_cap_exceeded"`
	PendingBatches  int64 `json:"pending_batches"`
	Responses429    int64 `json:"responses_429"`
	BytesOnDisk     int64 `json:"bytes_on_disk"`
}

func readStats(dir string) (stats, error) {
	data, err := os.ReadFile(filepath.Join(dir, "stats.json")) //nolint:gosec // fixed name inside the data dir
	if err != nil {
		return stats{}, err
	}
	var s stats
	if err := json.Unmarshal(data, &s); err != nil {
		return stats{}, fmt.Errorf("failed to parse stats.json: %w", err)
	}

	return s, nil
}

const resetNote = "counters reset when serve restarts"

func checkStats(s stats, err error) Check {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return check("stats", Warn, "no stats.json yet; %s", resetNote)
	case err != nil:
		return check("stats", Fail, "%v", err)
	case s.WriteErrors > 0 || s.DiskCapExceeded:
		return check("stats", Fail, "write_errors=%d disk_cap_exceeded=%t; %s", s.WriteErrors, s.DiskCapExceeded, resetNote)
	case s.PendingBatches > 0 || s.Responses429 > 0:
		return check("stats", Warn, "pending_batches=%d responses_429=%d; %s", s.PendingBatches, s.Responses429, resetNote)
	}

	return check("stats", OK, "no write errors, no 429s; %s", resetNote)
}

func checkDisk(cfg Config, s stats, err error) Check {
	if err != nil {
		return check("disk", Warn, "no usable stats.json")
	}
	used := fmt.Sprintf("%s of %s", humanBytes(s.BytesOnDisk), humanBytes(cfg.MaxDisk))
	if cfg.MaxDisk > 0 && s.BytesOnDisk*100 >= cfg.MaxDisk*diskWarnPercent {
		return check("disk", Warn, "%s, at least %d%% of the cap", used, diskWarnPercent)
	}

	return check("disk", OK, "%s", used)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func checkManifests(cfg Config) Check {
	loaded, err := manifest.Glob(cfg.DataDir)
	if err != nil {
		return check("manifests", Fail, "%v", err)
	}

	var files int
	var rows int64
	var missing []string
	var unlisted int
	for _, l := range loaded {
		known := map[string]struct{}{}
		for _, f := range l.Manifest.Files {
			known[f.Path] = struct{}{}
			files++
			rows += f.Rows
			if _, err := os.Stat(filepath.Join(l.Dir, f.Path)); err != nil {
				missing = append(missing, filepath.Join(l.Dir, f.Path))
			}
		}
		for _, r := range l.Manifest.Retired {
			known[r.Path] = struct{}{}
		}
		_ = filepath.WalkDir(l.Dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
				return nil //nolint:nilerr // an unreadable entry just isn't counted
			}
			if rel, err := filepath.Rel(l.Dir, path); err == nil {
				if _, ok := known[rel]; !ok {
					unlisted++
				}
			}

			return nil
		})
	}

	summary := fmt.Sprintf("%d services, %d files, %d rows", len(loaded), files, rows)
	switch {
	case len(missing) > 0:
		return check("manifests", Fail, "%d listed files are missing, first %s; %s", len(missing), missing[0], summary)
	case unlisted > 0:
		return check("manifests", Warn, "%d parquet files are not in any manifest; %s", unlisted, summary)
	}

	return check("manifests", OK, "%s", summary)
}

func checkReadPath(ctx context.Context, cfg Config) Check {
	e, err := engine.Open(ctx, engine.Options{AllowedDir: filepath.Join(cfg.DataDir, "services"), DataDir: cfg.DataDir})
	if err != nil {
		return check("read_path", Fail, "failed to open engine: %v", err)
	}
	defer e.Close()

	counts := map[string]any{}
	for _, view := range []string{"logs", "spans"} {
		res, err := e.Query(ctx, "SELECT count(*) FROM "+view, 1)
		if err != nil || len(res.Rows) != 1 {
			return check("read_path", Fail, "failed to count %s: %v", view, err)
		}
		counts[view] = res.Rows[0][0]
	}

	return check("read_path", OK, "logs=%v spans=%v", counts["logs"], counts["spans"])
}
