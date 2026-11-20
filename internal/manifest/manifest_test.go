package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var ts = time.Date(2026, 11, 20, 13, 30, 0, 0, time.UTC)

func sample(name string) Manifest {
	return Manifest{
		SchemaVersion: Version,
		ServiceName:   name,
		Generation:    3,
		Files:         []File{{Path: "logs/a.parquet", Signal: "logs", Rows: 2, Bytes: 10, MinTime: ts, MaxTime: ts.Add(time.Second)}},
	}
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, Name)
	m := sample("svc")
	m.Retired = []Retired{{Path: "logs/old.parquet", DeleteAfter: ts}}

	require.NoError(t, Write(dir, path, m))
	got, err := Read(path)
	require.NoError(t, err)
	require.Equal(t, m, got)
}

func TestWriteLeavesNoTempFiles(t *testing.T) {
	root := t.TempDir()
	tmp := filepath.Join(root, "tmp")
	require.NoError(t, os.Mkdir(tmp, 0o700))
	path := filepath.Join(root, Name)

	require.NoError(t, Write(tmp, path, sample("svc")))
	require.NoError(t, Write(tmp, path, sample("svc")))

	entries, err := os.ReadDir(tmp)
	require.NoError(t, err)
	require.Empty(t, entries)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestWriteFailureRemovesTemp(t *testing.T) {
	root := t.TempDir()
	tmp := filepath.Join(root, "tmp")
	require.NoError(t, os.Mkdir(tmp, 0o700))

	require.Error(t, Write(tmp, filepath.Join(root, "missing", Name), sample("svc")))

	entries, err := os.ReadDir(tmp)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestGlob(t *testing.T) {
	root := t.TempDir()
	got, err := Glob(root)
	require.NoError(t, err)
	require.Empty(t, got)

	for _, name := range []string{"b-2", "a-1", "c-3"} {
		dir := filepath.Join(root, "services", name)
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, Write(root, filepath.Join(dir, Name), sample(name)))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(root, "services", "empty"), 0o700))

	got, err = Glob(root)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for i, name := range []string{"a-1", "b-2", "c-3"} {
		require.Equal(t, filepath.Join(root, "services", name), got[i].Dir)
		require.Equal(t, name, got[i].Manifest.ServiceName)
	}
}

func TestRetiredOmittedWhenEmpty(t *testing.T) {
	data, err := json.Marshal(sample("svc"))
	require.NoError(t, err)
	require.NotContains(t, string(data), "retired")
}
