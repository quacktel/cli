package store

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/quacktel/cli/internal/engine"
	"github.com/quacktel/cli/internal/manifest"
)

var fixedNow = time.Date(2026, 11, 20, 13, 30, 0, 123456789, time.UTC)

func newDataDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))

	return dir
}

func openStore(t *testing.T, opts Options) *Store {
	t.Helper()

	t.Setenv("HOME", t.TempDir())
	if opts.DataDir == "" {
		opts.DataDir = newDataDir(t)
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return fixedNow }
	}
	s, err := Open(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	return s
}

func logsFor(services ...string) plog.Logs {
	ld := plog.NewLogs()
	for _, name := range services {
		rl := ld.ResourceLogs().AppendEmpty()
		if name != "" {
			rl.Resource().Attributes().PutStr("service.name", name)
		}
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(fixedNow))
		lr.Body().SetStr("hello")
	}

	return ld
}

func claudeLogs() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "claude-code")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("com.anthropic.claude_code.events")
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(fixedNow))
	lr.SetEventName("claude_code.api_request")
	lr.Attributes().PutStr("session.id", "abc")
	lr.Attributes().PutDouble("cost_usd", 0.01)
	lr.SetTraceID(pcommon.TraceID{0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	lr.Body().SetEmptyMap().PutStr("k", "v")

	return ld
}

func parquetFiles(t *testing.T, dir string) []string {
	t.Helper()

	var out []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			out = append(out, path)
		}

		return err
	})
	require.NoError(t, err)

	return out
}

func readManifest(t *testing.T, dataDir, name string) manifest.Manifest {
	t.Helper()

	m, err := manifest.Read(filepath.Join(dataDir, "services", ServiceDir(name), manifest.Name))
	require.NoError(t, err)

	return m
}

func TestServiceDir(t *testing.T) {
	names := []string{
		"../../etc", "/abs/path", "..", ".", "", "héllo wörld", strings.Repeat("a", 500),
		"Svc", "svc", "s.v.c", "bad\x00name", "claude-code",
	}
	seen := map[string]string{}
	for _, n := range names {
		got := ServiceDir(n)
		require.Equal(t, filepath.Base(got), got, n)
		require.NotEqual(t, ".", got)
		require.NotEqual(t, "..", got)
		require.NotContains(t, got, "/")
		require.NotContains(t, got, "\x00")
		require.LessOrEqual(t, len(got), 73)
		require.NotContains(t, seen, got, "collision between %q and %q", n, seen[got])
		seen[got] = n
	}
	require.Regexp(t, `^claude-code-[0-9a-f]{8}$`, ServiceDir("claude-code"))
	require.Regexp(t, `^unknown_service-[0-9a-f]{8}$`, ServiceDir(""))
	require.Regexp(t, `^_-[0-9a-f]{8}$`, ServiceDir(".."))
	require.Regexp(t, `^_-[0-9a-f]{8}$`, ServiceDir("-"))
}

func TestRoundTrip(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir})

	rejected, msg, err := s.AppendLogs(t.Context(), claudeLogs())
	require.NoError(t, err)
	require.Zero(t, rejected)
	require.Empty(t, msg)
	require.NoError(t, s.Flush(t.Context()))

	e, err := engine.Open(t.Context(), engine.Options{AllowedDir: dataDir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	q := "SELECT time, epoch_ns(time) AS time_ns, typeof(time) AS time_type, observed_time, service_name, event_name, session_id, attrs['session.id'] AS sid, " +
		"attrs['cost_usd'] AS cost, trace_id, span_id, body, severity_text, scope_name, scope_version " +
		"FROM read_parquet('" + dataDir + "/services/*/logs/**/*.parquet')"
	res, err := e.Query(t.Context(), q, 10)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := map[string]any{}
	for i, c := range res.Columns {
		row[c] = res.Rows[0][i]
	}

	// TIMESTAMP_NS scans directly as time.Time with full nanosecond precision.
	require.Equal(t, "TIMESTAMP_NS", row["time_type"])
	require.EqualValues(t, fixedNow.UnixNano(), row["time_ns"])
	require.IsType(t, time.Time{}, row["time"])
	require.Equal(t, fixedNow.UnixNano(), row["time"].(time.Time).UnixNano())
	require.Nil(t, row["observed_time"])
	require.Equal(t, "claude-code", row["service_name"])
	require.Equal(t, "claude_code.api_request", row["event_name"])
	require.Equal(t, "abc", row["session_id"])
	// Map subscript in DuckDB 2.0 returns the value itself, not a list.
	require.Equal(t, "abc", row["sid"])
	require.Equal(t, "0.01", row["cost"])
	require.Equal(t, "deadbeef0102030405060708090a0b0c", row["trace_id"])
	require.Nil(t, row["span_id"])
	require.JSONEq(t, `{"k":"v"}`, row["body"].(string))
	require.Nil(t, row["severity_text"])
	require.Equal(t, "com.anthropic.claude_code.events", row["scope_name"])
	require.Nil(t, row["scope_version"])
}

func TestAtomicWritePath(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir})
	_, _, err := s.AppendLogs(t.Context(), logsFor("a", "b"))
	require.NoError(t, err)
	require.NoError(t, s.Flush(t.Context()))

	files := parquetFiles(t, dataDir)
	require.Len(t, files, 2)
	for _, f := range files {
		require.Regexp(t, `^tail-[0-9A-Z]{26}\.parquet$`, filepath.Base(f))
		require.Contains(t, f, filepath.Join("logs", "date=2026-11-20", "hour=13"))
		entries, err := os.ReadDir(filepath.Dir(f))
		require.NoError(t, err)
		require.Len(t, entries, 1)
		info, err := os.Stat(f)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	tmp, err := os.ReadDir(filepath.Join(dataDir, ".tmp"))
	require.NoError(t, err)
	require.Empty(t, tmp)
}

func TestManifest(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir})
	for range 2 {
		_, _, err := s.AppendLogs(t.Context(), logsFor("svc"))
		require.NoError(t, err)
		require.NoError(t, s.Flush(t.Context()))
	}

	m := readManifest(t, dataDir, "svc")
	require.EqualValues(t, 2, m.Generation)
	require.Equal(t, "svc", m.ServiceName)
	require.Equal(t, 1, m.SchemaVersion)
	require.Len(t, m.Files, 2)

	var listed []string
	for _, f := range m.Files {
		require.Equal(t, "logs", f.Signal)
		require.EqualValues(t, 1, f.Rows)
		require.Positive(t, f.Bytes)
		require.True(t, fixedNow.Equal(f.MinTime))
		listed = append(listed, filepath.Join(dataDir, "services", ServiceDir("svc"), f.Path))
	}
	require.ElementsMatch(t, parquetFiles(t, dataDir), listed)

	st := s.Stats()
	require.EqualValues(t, 2, st.Flushes)
	require.EqualValues(t, 2, st.FilesWritten)
	require.EqualValues(t, 2, st.RecordsAccepted)
	require.Equal(t, 1, st.Services)
}

func TestReconcile(t *testing.T) {
	dataDir := newDataDir(t)
	t.Setenv("HOME", t.TempDir())
	s, err := Open(t.Context(), Options{DataDir: dataDir, Now: func() time.Time { return fixedNow }})
	require.NoError(t, err)
	_, _, err = s.AppendLogs(t.Context(), logsFor("svc"))
	require.NoError(t, err)
	require.NoError(t, s.Flush(t.Context()))
	require.NoError(t, s.Close())

	orig := parquetFiles(t, dataDir)[0]
	src, err := os.Open(orig)
	require.NoError(t, err)
	defer src.Close()
	extra := filepath.Join(filepath.Dir(orig), "tail-01EXTRAEXTRAEXTRAEXTRAEXTR.parquet")
	dst, err := os.OpenFile(extra, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = io.Copy(dst, src)
	require.NoError(t, err)
	require.NoError(t, dst.Close())

	s2, err := Open(t.Context(), Options{DataDir: dataDir})
	require.NoError(t, err)
	require.NoError(t, s2.Close())

	m := readManifest(t, dataDir, "svc")
	require.EqualValues(t, 2, m.Generation)
	require.Len(t, m.Files, 2)
	for _, f := range m.Files {
		require.EqualValues(t, 1, f.Rows)
	}
}

func TestReconcileSkipsRetired(t *testing.T) {
	dataDir := newDataDir(t)
	t.Setenv("HOME", t.TempDir())
	s, err := Open(t.Context(), Options{DataDir: dataDir, Now: func() time.Time { return fixedNow }})
	require.NoError(t, err)
	_, _, err = s.AppendLogs(t.Context(), logsFor("svc"))
	require.NoError(t, err)
	require.NoError(t, s.Flush(t.Context()))
	require.NoError(t, s.Close())

	svcDir := filepath.Join(dataDir, "services", ServiceDir("svc"))
	path := filepath.Join(svcDir, manifest.Name)
	m, err := manifest.Read(path)
	require.NoError(t, err)
	require.Len(t, m.Files, 1)
	m.Retired = []manifest.Retired{{Path: m.Files[0].Path, DeleteAfter: fixedNow}}
	m.Files = nil
	require.NoError(t, manifest.Write(filepath.Join(dataDir, ".tmp"), path, m))

	s2, err := Open(t.Context(), Options{DataDir: dataDir})
	require.NoError(t, err)
	require.NoError(t, s2.Close())

	got := readManifest(t, dataDir, "svc")
	require.Empty(t, got.Files)
	require.Equal(t, m.Retired, got.Retired)
	require.Equal(t, m.Generation, got.Generation)
}

func TestLock(t *testing.T) {
	dataDir := newDataDir(t)
	t.Setenv("HOME", t.TempDir())
	first, err := Open(t.Context(), Options{DataDir: dataDir})
	require.NoError(t, err)

	_, err = Open(t.Context(), Options{DataDir: dataDir})
	require.ErrorContains(t, err, "another quacktel serve")

	require.NoError(t, first.Close())
	require.NoError(t, first.Close())
	second, err := Open(t.Context(), Options{DataDir: dataDir})
	require.NoError(t, err)
	require.NoError(t, second.Close())
}

func TestCloseFlushes(t *testing.T) {
	dataDir := newDataDir(t)
	t.Setenv("HOME", t.TempDir())
	s, err := Open(t.Context(), Options{DataDir: dataDir, Now: func() time.Time { return fixedNow }})
	require.NoError(t, err)
	_, _, err = s.AppendLogs(t.Context(), logsFor("svc"))
	require.NoError(t, err)
	require.Empty(t, parquetFiles(t, dataDir))

	require.NoError(t, s.Close())
	require.Len(t, parquetFiles(t, dataDir), 1)
}

func TestBufferCap(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir, BufferBytes: 10})

	_, _, err := s.AppendLogs(t.Context(), logsFor("svc"))
	require.ErrorIs(t, err, ErrFull)
	require.Zero(t, s.Stats().RecordsAccepted)
	require.Zero(t, s.Stats().Services)
	require.NoError(t, s.Flush(t.Context()))
	require.Empty(t, parquetFiles(t, dataDir))
}

func TestServiceCap(t *testing.T) {
	s := openStore(t, Options{MaxServices: 2})

	rejected, msg, err := s.AppendLogs(t.Context(), logsFor("a", "b", "c", "c"))
	require.NoError(t, err)
	require.EqualValues(t, 2, rejected)
	require.Equal(t, "service limit reached", msg)
	require.EqualValues(t, 2, s.Stats().RecordsAccepted)
	require.EqualValues(t, 2, s.Stats().RecordsRejected)

	rejected, msg, err = s.AppendLogs(t.Context(), logsFor("a"))
	require.NoError(t, err)
	require.Zero(t, rejected)
	require.Empty(t, msg)
}

func TestServiceCapIgnoresOnDiskServices(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir, MaxServices: 2})
	_, _, err := s.AppendLogs(t.Context(), logsFor("a", "b"))
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s = openStore(t, Options{DataDir: dataDir, MaxServices: 2})
	require.Zero(t, s.Stats().Services)

	rejected, _, err := s.AppendLogs(t.Context(), logsFor("c"))
	require.NoError(t, err)
	require.Zero(t, rejected)
	rejected, _, err = s.AppendLogs(t.Context(), logsFor("d", "e"))
	require.NoError(t, err)
	require.EqualValues(t, 1, rejected)
	require.Equal(t, 2, s.Stats().Services)

	rejected, _, err = s.AppendLogs(t.Context(), logsFor("a"))
	require.NoError(t, err)
	require.EqualValues(t, 1, rejected)
}

func TestReceiveTimePartition(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir})

	ld := logsFor("svc")
	ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SetTimestamp(
		pcommon.NewTimestampFromTime(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)))
	_, _, err := s.AppendLogs(t.Context(), ld)
	require.NoError(t, err)
	require.NoError(t, s.Flush(t.Context()))

	files := parquetFiles(t, dataDir)
	require.Len(t, files, 1)
	require.Contains(t, files[0], filepath.Join("date=2026-11-20", "hour=13"))
	require.NotContains(t, files[0], "2001")
}
