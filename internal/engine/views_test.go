package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/store"
	"github.com/quacktel/cli/internal/table"
)

func writeSignalFile(t *testing.T, path, signal, name string) {
	t.Helper()

	recvd := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var rec arrow.RecordBatch
	if signal == "logs" {
		b := table.NewLogsBuilder(memory.NewGoAllocator())
		defer b.Release()
		lr := plog.NewLogRecord()
		lr.Body().SetStr(name)
		b.Append(pcommon.NewResource(), pcommon.NewInstrumentationScope(), lr, recvd)
		rec = b.NewRecord()
	} else {
		b := table.NewSpansBuilder(memory.NewGoAllocator())
		defer b.Release()
		sp := ptrace.NewSpan()
		sp.SetName(name)
		b.Append(pcommon.NewResource(), pcommon.NewInstrumentationScope(), sp, recvd)
		rec = b.NewRecord()
	}
	defer rec.Release()

	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(rec.Schema(), &buf, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
}

// writeService writes one parquet file per name (the file is named after it) and a manifest listing files and retired.
func writeService(t *testing.T, dataDir, svc string, gen int64, files, retired []string) {
	t.Helper()

	dir := filepath.Join(dataDir, "services", svc)
	m := manifest.Manifest{SchemaVersion: manifest.Version, ServiceName: svc, Generation: gen}
	for _, name := range files {
		rel := filepath.Join("logs", name+".parquet")
		writeSignalFile(t, filepath.Join(dir, rel), "logs", name)
		m.Files = append(m.Files, manifest.File{Path: rel, Signal: "logs", Rows: 1})
	}
	for _, name := range retired {
		rel := filepath.Join("logs", name+".parquet")
		writeSignalFile(t, filepath.Join(dir, rel), "logs", name)
		m.Retired = append(m.Retired, manifest.Retired{Path: rel, DeleteAfter: time.Now().Add(time.Hour)})
	}
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, manifest.Write(dir, filepath.Join(dir, manifest.Name), m))
}

func openViews(t *testing.T) (*Engine, string) {
	t.Helper()

	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	e, err := Open(t.Context(), Options{AllowedDir: filepath.Join(dir, "services"), DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	return e, dir
}

func bodies(t *testing.T, e *Engine) []string {
	t.Helper()

	res, err := e.Query(t.Context(), "SELECT body FROM logs ORDER BY body", 100)
	require.NoError(t, err)
	out := []string{}
	for _, r := range res.Rows {
		out = append(out, r[0].(string))
	}

	return out
}

func TestViewsListedFilesOnly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	writeService(t, dir, "a", 1, []string{"a1", "a2"}, []string{"gone"})
	writeService(t, dir, "b", 1, []string{"b1"}, nil)
	// On disk but not listed.
	writeSignalFile(t, filepath.Join(dir, "services", "a", "logs", "orphan.parquet"), "logs", "orphan")
	writeSignalFile(t, filepath.Join(dir, "services", "a", "spans", "s1.parquet"), "spans", "s1")
	require.NoError(t, manifest.Write(filepath.Join(dir, "services", "b"), filepath.Join(dir, "services", "b", manifest.Name), manifest.Manifest{
		SchemaVersion: manifest.Version, ServiceName: "b", Generation: 2,
		Files: []manifest.File{
			{Path: "logs/b1.parquet", Signal: "logs"},
			{Path: "spans/s2.parquet", Signal: "spans"},
		},
	}))
	writeSignalFile(t, filepath.Join(dir, "services", "b", "spans", "s2.parquet"), "spans", "s2")

	e, err := Open(t.Context(), Options{AllowedDir: filepath.Join(dir, "services"), DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	require.Equal(t, []string{"a1", "a2", "b1"}, bodies(t, e))

	res, err := e.Query(t.Context(), "SELECT name FROM spans ORDER BY name", 100)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"s2"}}, res.Rows)
}

func TestViewsEmptyDataDir(t *testing.T) {
	e, _ := openViews(t)

	res, err := e.Query(t.Context(), "SELECT count(*) FROM logs", 10)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(0)}}, res.Rows)

	res, err = e.Query(t.Context(), "SELECT column_name FROM (DESCRIBE spans)", 100)
	require.NoError(t, err)
	var got []string
	for _, r := range res.Rows {
		got = append(got, r[0].(string))
	}
	var want []string
	for _, f := range table.SpansSchema().Fields() {
		want = append(want, f.Name)
	}
	require.Equal(t, want, got)
}

func TestViewsRefreshOnManifestChange(t *testing.T) {
	e, dir := openViews(t)
	require.Empty(t, bodies(t, e))

	writeService(t, dir, "a", 1, []string{"a1"}, nil)
	require.Equal(t, []string{"a1"}, bodies(t, e))

	writeService(t, dir, "a", 2, []string{"a1", "a2"}, nil)
	require.Equal(t, []string{"a1", "a2"}, bodies(t, e))
}

func TestViewsKeepHardening(t *testing.T) {
	e, dir := openViews(t)
	writeService(t, dir, "a", 1, []string{"a1"}, nil)

	_, err := e.Query(t.Context(), "COPY (SELECT * FROM logs) TO '"+filepath.Join(dir, "services", "out.csv")+"'", 10)
	require.ErrorIs(t, err, ErrNotSelect)

	secret := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(secret, []byte("a\n"), 0o600))
	_, err = e.Query(t.Context(), "SELECT * FROM read_text('"+secret+"')", 10)
	require.Error(t, err)
}

func TestOpenRejectsAllowedDirOutsideServices(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	_, err := Open(t.Context(), Options{AllowedDir: t.TempDir(), DataDir: dir})
	require.ErrorContains(t, err, "services dir")
	_, err = Open(t.Context(), Options{AllowedDir: dir, DataDir: dir})
	require.ErrorContains(t, err, "services dir")
}

func TestUnscopedCannotReadDataDirRoot(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	writeStoreServices(t, dir, "alpha")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "otlp-headers"), []byte("authorization=Bearer dummy\n"), 0o600))
	e, err := Open(t.Context(), Options{AllowedDir: filepath.Join(dir, "services"), DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	for _, name := range []string{"otlp-headers", "stats.json"} {
		_, err = e.Query(t.Context(), "SELECT content FROM read_text('"+filepath.Join(dir, name)+"')", 10)
		require.Error(t, err, name)
	}
	res, err := e.Query(t.Context(), "SELECT count(*) FROM logs", 10)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(1)}}, res.Rows)
}

// writeStoreServices writes one real log record per service through the store.
func writeStoreServices(t *testing.T, dataDir string, services ...string) {
	t.Helper()

	s, err := store.Open(t.Context(), store.Options{DataDir: dataDir})
	require.NoError(t, err)
	ld := plog.NewLogs()
	for _, name := range services {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", name)
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(name)
	}
	_, _, err = s.AppendLogs(t.Context(), ld)
	require.NoError(t, err)
	require.NoError(t, s.Flush(t.Context()))
	require.NoError(t, s.Close())
}

func openScoped(t *testing.T, dataDir, service string) *Engine {
	t.Helper()

	e, err := Open(t.Context(), Options{
		AllowedDir: filepath.Join(dataDir, "services", manifest.ServiceDir(service)),
		DataDir:    dataDir,
		Service:    service,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	return e
}

func TestScopedViewsOnlyServiceData(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	writeStoreServices(t, dir, "alpha", "beta")
	e := openScoped(t, dir, "alpha")

	res, err := e.Query(t.Context(), "SELECT DISTINCT service_name FROM logs", 10)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"alpha"}}, res.Rows)

	res, err = e.Query(t.Context(), "SELECT count(*) FROM (DESCRIBE logs)", 10)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	other, err := manifest.Read(filepath.Join(dir, "services", manifest.ServiceDir("beta"), manifest.Name))
	require.NoError(t, err)
	require.NotEmpty(t, other.Files)
	otherFile := filepath.Join(dir, "services", manifest.ServiceDir("beta"), other.Files[0].Path)
	_, err = e.Query(t.Context(), "SELECT * FROM read_parquet('"+otherFile+"')", 10)
	require.ErrorContains(t, err, "Permission")
}

func TestScopedWithoutDataIsEmpty(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	writeStoreServices(t, dir, "beta")
	e := openScoped(t, dir, "alpha")

	res, err := e.Query(t.Context(), "SELECT count(*) FROM logs", 10)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(0)}}, res.Rows)
}
