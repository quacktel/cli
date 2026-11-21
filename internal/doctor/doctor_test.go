package doctor

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/service"
)

func newDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "d")
	require.NoError(t, os.Mkdir(dir, 0o700))

	return dir
}

func write(t *testing.T, path string, data string, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(data), mode))
	require.NoError(t, os.Chmod(path, mode))
}

func TestDoctorDataDir(t *testing.T) {
	dir := newDir(t)
	require.Equal(t, OK, checkDataDir(Config{DataDir: dir}).Status)

	require.NoError(t, os.Chmod(dir, 0o755))
	c := checkDataDir(Config{DataDir: dir})
	require.Equal(t, Fail, c.Status)
	require.Contains(t, c.Detail, "0755")

	c = checkDataDir(Config{DataDir: filepath.Join(dir, "missing")})
	require.Equal(t, Fail, c.Status)
	require.Contains(t, c.Detail, "quacktel service install")
}

func TestDoctorAuthHeaders(t *testing.T) {
	dir := newDir(t)
	cfg := Config{DataDir: dir}
	path := filepath.Join(dir, "otlp-headers")

	c := checkAuthHeaders(cfg)
	require.Equal(t, OK, c.Status)
	require.Equal(t, "auth off", c.Detail)

	good := "authorization=Bearer " + strings.Repeat("ab", 32) + "\n"
	write(t, path, good, 0o600)
	require.Equal(t, OK, checkAuthHeaders(cfg).Status)

	write(t, path, good, 0o644)
	require.Equal(t, Fail, checkAuthHeaders(cfg).Status)

	write(t, path, "authorization=Bearer nothex\n", 0o600)
	require.Equal(t, Fail, checkAuthHeaders(cfg).Status)
}

func TestDoctorServeLock(t *testing.T) {
	dir := newDir(t)
	cfg := Config{DataDir: dir}

	require.Equal(t, Fail, checkServeLock(cfg).Status, "missing lock file")
	_, err := os.Stat(filepath.Join(dir, ".lock"))
	require.ErrorIs(t, err, os.ErrNotExist, "probe must not create the lock file")

	lockPath := filepath.Join(dir, ".lock")
	write(t, lockPath, "", 0o600)
	c := checkServeLock(cfg)
	require.Equal(t, Fail, c.Status, "free lock")
	require.Contains(t, c.Detail, "no quacktel serve")

	f, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	require.Equal(t, OK, checkServeLock(cfg).Status, "held lock")
	require.NoError(t, syscall.Flock(int(f.Fd()), syscall.LOCK_UN))

	require.Equal(t, Fail, checkServeLock(cfg).Status, "released again")
}

func TestDoctorReceiver(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.Host == r.Context().Value(http.LocalAddrContextKey).(net.Addr).String() {
			w.Header().Set("Allow", http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)

			return
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ok.Close()
	c := checkReceiver(t.Context(), Config{Listen: ok.Listener.Addr().String()})
	require.Equal(t, OK, c.Status, c.Detail)

	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer other.Close()
	c = checkReceiver(t.Context(), Config{Listen: other.Listener.Addr().String()})
	require.Equal(t, Fail, c.Status)
	require.Contains(t, c.Detail, "200")

	noAllow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusMethodNotAllowed) }))
	defer noAllow.Close()
	require.Equal(t, Fail, checkReceiver(t.Context(), Config{Listen: noAllow.Listener.Addr().String()}).Status)

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer redirect.Close()
	c = checkReceiver(t.Context(), Config{Listen: redirect.Listener.Addr().String()})
	require.Equal(t, Fail, c.Status)
	require.Contains(t, c.Detail, "302")

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closed := ln.Addr().String()
	require.NoError(t, ln.Close())
	require.Equal(t, Fail, checkReceiver(t.Context(), Config{Listen: closed}).Status)

	require.Equal(t, Fail, checkReceiver(t.Context(), Config{Listen: "nonsense"}).Status)
}

func installPlist(t *testing.T, label string, args []string) string {
	t.Helper()
	home := t.TempDir()
	m := &service.Manager{Label: label, Home: home}
	data, err := service.Render(service.Plist{Label: label, ProgramArguments: args, LogPath: "/dev/null"})
	require.NoError(t, err)
	write(t, m.PlistPath(), string(data), 0o644)

	return m.PlistPath()
}

const running = "gui/501/x = {\n\tstate = running\n\tpid = 42\n}\n"

func fake(out string, err error) service.Runner {
	return func(context.Context, ...string) ([]byte, error) { return []byte(out), err }
}

func TestDoctorService(t *testing.T) {
	exe, err := os.Executable()
	require.NoError(t, err)
	exe, err = filepath.EvalSymlinks(exe)
	require.NoError(t, err)
	label := "dev.quacktel.test"

	c := checkService(t.Context(), Config{PlistPath: filepath.Join(t.TempDir(), "none.plist"), Label: label})
	require.Equal(t, OK, c.Status)
	require.Equal(t, "no LaunchAgent installed", c.Detail)

	plist := installPlist(t, label, []string{exe, "serve"})
	cfg := Config{PlistPath: plist, Label: label, Exe: exe}
	require.Equal(t, OK, checkService(t.Context(), cfg).Status, "launchd checks skipped")

	cfg.Launchctl = fake(running, nil)
	require.Equal(t, OK, checkService(t.Context(), cfg).Status)

	cfg.Launchctl = fake("gui/501/x = {\n\tstate = waiting\n}\n", nil)
	c = checkService(t.Context(), cfg)
	require.Equal(t, Fail, c.Status)
	require.Contains(t, c.Detail, "waiting")

	cfg.Launchctl = fake("Could not find service", errors.New("exit status 113"))
	c = checkService(t.Context(), cfg)
	require.Equal(t, Fail, c.Status)
	require.Contains(t, c.Detail, "not loaded")

	cfg.Launchctl = fake(running, nil)
	cfg.Exe = filepath.Join(t.TempDir(), "other")
	write(t, cfg.Exe, "", 0o755)
	require.Equal(t, Warn, checkService(t.Context(), cfg).Status, "different exe")

	missing := installPlist(t, label, []string{filepath.Join(t.TempDir(), "gone"), "serve"})
	c = checkService(t.Context(), Config{PlistPath: missing, Label: label, Exe: exe, Launchctl: fake(running, nil)})
	require.Equal(t, Fail, c.Status)
	require.Contains(t, c.Detail, "missing")
}

func TestDoctorBrewService(t *testing.T) {
	exe, err := os.Executable()
	require.NoError(t, err)
	exe, err = filepath.EvalSymlinks(exe)
	require.NoError(t, err)

	brewPlist := installPlist(t, service.BrewLabel, []string{exe, "serve", "--auth"})
	ownPlist := filepath.Join(filepath.Dir(brewPlist), "dev.quacktel.test.plist")
	cfg := Config{PlistPath: ownPlist, Label: "dev.quacktel.test", Exe: exe, Launchctl: fake(running, nil)}
	c := checkService(t.Context(), cfg)
	require.Equal(t, OK, c.Status)
	require.Contains(t, c.Detail, service.BrewLabel)

	cfg.Launchctl = fake("Could not find service", errors.New("exit status 113"))
	c = checkService(t.Context(), cfg)
	require.Equal(t, Fail, c.Status)
	require.Contains(t, c.Detail, "brew services restart quacktel")

	data, err := service.Render(service.Plist{Label: cfg.Label, ProgramArguments: []string{exe, "serve"}, LogPath: "/dev/null"})
	require.NoError(t, err)
	write(t, ownPlist, string(data), 0o644)
	c = checkService(t.Context(), cfg)
	require.Equal(t, Warn, c.Status)
	require.Contains(t, c.Detail, "only one serve")
}

func TestDoctorStatsAndDisk(t *testing.T) {
	dir := newDir(t)
	cfg := Config{DataDir: dir, MaxDisk: 1000}
	run := func(json string) (Check, Check) {
		write(t, filepath.Join(dir, "stats.json"), json, 0o600)
		s, err := readStats(dir)
		require.NoError(t, err)

		return checkStats(s, nil), checkDisk(cfg, s, nil)
	}

	s, err := readStats(dir)
	require.Equal(t, Warn, checkStats(s, err).Status, "absent")
	require.Equal(t, Warn, checkDisk(cfg, s, err).Status)

	st, disk := run(`{"bytes_on_disk":100}`)
	require.Equal(t, OK, st.Status)
	require.Contains(t, st.Detail, "reset when serve restarts")
	require.Equal(t, OK, disk.Status)
	require.Contains(t, disk.Detail, "100 B of 1000 B")

	st, _ = run(`{"write_errors":1}`)
	require.Equal(t, Fail, st.Status)
	st, _ = run(`{"disk_cap_exceeded":true}`)
	require.Equal(t, Fail, st.Status)
	st, _ = run(`{"pending_batches":2}`)
	require.Equal(t, Warn, st.Status)
	st, _ = run(`{"responses_429":3}`)
	require.Equal(t, Warn, st.Status)

	_, disk = run(`{"bytes_on_disk":850}`)
	require.Equal(t, Warn, disk.Status)

	write(t, filepath.Join(dir, "stats.json"), "{", 0o600)
	s, err = readStats(dir)
	require.Equal(t, Fail, checkStats(s, err).Status, "corrupt")
}

func seedService(t *testing.T, dir string, listed ...manifest.File) string {
	t.Helper()
	svc := filepath.Join(dir, "services", "svc")
	require.NoError(t, os.MkdirAll(svc, 0o700))
	require.NoError(t, manifest.Write(svc, filepath.Join(svc, manifest.Name), manifest.Manifest{
		SchemaVersion: manifest.Version, ServiceName: "svc", Files: listed,
	}))

	return svc
}

func TestDoctorManifests(t *testing.T) {
	dir := newDir(t)
	cfg := Config{DataDir: dir}
	require.Equal(t, OK, checkManifests(cfg).Status, "no services")

	svc := seedService(t, dir, manifest.File{Path: "a.parquet", Signal: "logs", Rows: 7})
	c := checkManifests(cfg)
	require.Equal(t, Fail, c.Status, "listed file missing")
	require.Contains(t, c.Detail, "a.parquet")

	write(t, filepath.Join(svc, "a.parquet"), "x", 0o600)
	c = checkManifests(cfg)
	require.Equal(t, OK, c.Status, c.Detail)
	require.Equal(t, "1 services, 1 files, 7 rows", c.Detail)

	write(t, filepath.Join(svc, "stray.parquet"), "x", 0o600)
	c = checkManifests(cfg)
	require.Equal(t, Warn, c.Status)
	require.Contains(t, c.Detail, "1 parquet files")

	write(t, filepath.Join(svc, manifest.Name), "{", 0o600)
	require.Equal(t, Fail, checkManifests(cfg).Status, "corrupt manifest")
}

func TestDoctorReadPath(t *testing.T) {
	dir := newDir(t)
	c := checkReadPath(t.Context(), Config{DataDir: dir})
	require.Equal(t, OK, c.Status, c.Detail)
	require.Equal(t, "logs=0 spans=0", c.Detail)

	svc := seedService(t, dir, manifest.File{Path: "bad.parquet", Signal: "logs", Rows: 1})
	write(t, filepath.Join(svc, "bad.parquet"), "not parquet", 0o600)
	c = checkReadPath(t.Context(), Config{DataDir: dir})
	require.Equal(t, Fail, c.Status)
}

func TestDoctorRunOrderAndReadOnly(t *testing.T) {
	dir := newDir(t)
	checks := Run(t.Context(), Config{DataDir: dir, Listen: "127.0.0.1:1", MaxDisk: 1000, PlistPath: filepath.Join(t.TempDir(), "x.plist")})

	names := make([]string, len(checks))
	for i, c := range checks {
		names[i] = c.Name
	}
	require.Equal(t, []string{"data_dir", "auth_headers", "serve_lock", "receiver", "service", "stats", "disk", "manifests", "read_path"}, names)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "doctor must not write to the data dir")
}
