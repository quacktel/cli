package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func openEngine(t *testing.T, opts Options) (*Engine, string) {
	t.Helper()

	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	opts.AllowedDir = dir
	e, err := Open(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	return e, dir
}

func writeParquet(t *testing.T, dir string) string {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()
	path := filepath.Join(dir, "f.parquet")
	_, err = db.ExecContext(t.Context(), "COPY (SELECT 1 AS a) TO '"+path+"' (FORMAT parquet)")
	require.NoError(t, err)

	return path
}

func TestSelect(t *testing.T) {
	e, _ := openEngine(t, Options{})
	res, err := e.Query(t.Context(), "SELECT 42 AS answer", 10)
	require.NoError(t, err)
	require.Equal(t, []string{"answer"}, res.Columns)
	require.Equal(t, [][]any{{int32(42)}}, res.Rows)
	require.False(t, res.Truncated)
}

func TestReadParquetInAllowedDir(t *testing.T) {
	e, dir := openEngine(t, Options{})
	path := writeParquet(t, dir)

	res, err := e.Query(t.Context(), "SELECT a FROM read_parquet('"+path+"')", 10)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int32(1)}}, res.Rows)
}

func TestGuardRejects(t *testing.T) {
	e, dir := openEngine(t, Options{})

	// Inside the allowed dir DuckDB would permit the write, so only the guard stops these.
	for _, q := range []string{
		"COPY (SELECT 1) TO '" + dir + "/x.csv'",
		"ATTACH ':memory:' AS x",
		"INSTALL httpfs",
		"LOAD httpfs",
		"SET threads=8",
		"PRAGMA threads=8",
		"SELECT 1; COPY (SELECT 1) TO '" + dir + "/y.csv'",
		"COPY (SELECT 1) TO '" + dir + "/z.csv'; SELECT 1",
	} {
		_, err := e.Query(t.Context(), q, 10)
		require.ErrorIs(t, err, ErrNotSelect, q)
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestCopyOutsideAllowedDirFails(t *testing.T) {
	e, _ := openEngine(t, Options{})
	tmp := t.TempDir()

	_, err := e.Query(t.Context(), "COPY (SELECT 1) TO '"+tmp+"/x.csv'", 10)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(tmp, "x.csv"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// PRAGMA version is planned as a SELECT over a table function, so the statement type can't tell it apart.
// query() is a SELECT that takes SQL text, so it must not smuggle other statement types past the guard.
func TestQueryFunctionRunsOnlySelect(t *testing.T) {
	e, dir := openEngine(t, Options{})

	for _, q := range []string{
		"SELECT * FROM query('COPY (SELECT 1) TO ''" + filepath.Join(dir, "q.csv") + "''')",
		"SELECT * FROM query('ATTACH '':memory:'' AS z')",
	} {
		_, err := e.Query(t.Context(), q, 10)
		require.ErrorContains(t, err, "Expected a single SELECT statement", q)
	}
	_, err := os.Stat(filepath.Join(dir, "q.csv"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPragmaVersionIsReadOnlySelect(t *testing.T) {
	e, _ := openEngine(t, Options{})
	res, err := e.Query(t.Context(), "PRAGMA version", 10)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
}

func TestDuckDBDeniesOutsideReads(t *testing.T) {
	e, dir := openEngine(t, Options{})
	other := t.TempDir()
	secret := filepath.Join(other, "secret.txt")
	require.NoError(t, os.WriteFile(secret, []byte("a\n"), 0o600))
	link := filepath.Join(dir, "link.txt")
	require.NoError(t, os.Symlink(secret, link))

	for _, q := range []string{
		"SELECT * FROM read_text('" + secret + "')",
		"SELECT * FROM read_csv('" + secret + "')",
		"SELECT * FROM read_text('" + link + "')",
	} {
		_, err := e.Query(t.Context(), q, 10)
		t.Logf("%s -> %v", q, err)
		require.Error(t, err, q)
		require.NotErrorIs(t, err, ErrNotSelect, q)
	}
}

func TestLockConfigurationHolds(t *testing.T) {
	e, _ := openEngine(t, Options{})
	conn, err := e.db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(t.Context(), "SET threads=8")
	require.Error(t, err)
}

func TestSettingsHoldOnEveryConnection(t *testing.T) {
	e, _ := openEngine(t, Options{})
	other := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(other, []byte("a\n"), 0o600))

	var conns []*sql.Conn
	for range 4 {
		c, err := e.db.Conn(t.Context())
		require.NoError(t, err)
		conns = append(conns, c)
	}
	for _, c := range conns {
		var threads int64
		require.NoError(t, c.QueryRowContext(t.Context(), "SELECT current_setting('threads')").Scan(&threads))
		require.Equal(t, int64(2), threads)

		var ext bool
		require.NoError(t, c.QueryRowContext(t.Context(), "SELECT current_setting('enable_external_access')").Scan(&ext))
		require.False(t, ext)

		_, err := c.ExecContext(t.Context(), "SELECT * FROM read_text('"+other+"')")
		require.Error(t, err)
		_, err = c.ExecContext(t.Context(), "SET threads=8")
		require.Error(t, err)
		require.NoError(t, c.Close())
	}
}

func TestNoExtensionsDownloaded(t *testing.T) {
	e, _ := openEngine(t, Options{})
	home := os.Getenv("HOME")

	_, err := e.Query(t.Context(), "SELECT 1", 10)
	require.NoError(t, err)
	// The hardened engine can't list extensions: duckdb_extensions() scans the extension dir.
	_, err = e.Query(t.Context(), "SELECT * FROM duckdb_extensions()", 10)
	require.ErrorContains(t, err, "file system operations are disabled")

	_, err = os.Stat(filepath.Join(home, ".duckdb"))
	require.ErrorIs(t, err, os.ErrNotExist)

	// Inspect the same binary with an unhardened handle.
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()
	rows, err := db.QueryContext(t.Context(),
		"SELECT extension_name, loaded, installed, install_mode FROM duckdb_extensions()")
	require.NoError(t, err)
	defer rows.Close()
	seen := map[string]string{}
	for rows.Next() {
		var name string
		var loaded, installed bool
		var mode sql.NullString
		require.NoError(t, rows.Scan(&name, &loaded, &installed, &mode))
		t.Logf("%s loaded=%v installed=%v install_mode=%v", name, loaded, installed, mode.String)
		if loaded || installed {
			seen[name] = mode.String
			require.NotEqual(t, "REPOSITORY", mode.String, name)
		}
	}
	require.NoError(t, rows.Err())
	require.Equal(t, "STATICALLY_LINKED", seen["parquet"])
	require.Equal(t, "STATICALLY_LINKED", seen["json"])

	_, err = os.Stat(filepath.Join(home, ".duckdb"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestTruncated(t *testing.T) {
	e, _ := openEngine(t, Options{})
	res, err := e.Query(t.Context(), "SELECT * FROM range(10)", 5)
	require.NoError(t, err)
	require.Len(t, res.Rows, 5)
	require.True(t, res.Truncated)
}

func TestTimeout(t *testing.T) {
	e, _ := openEngine(t, Options{Timeout: 200 * time.Millisecond})

	start := time.Now()
	_, err := e.Query(context.Background(), "SELECT count(*) FROM range(1000000000) a, range(1000) b", 10)
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestViewsCanBeCreatedAfterLock(t *testing.T) {
	e, dir := openEngine(t, Options{})
	path := writeParquet(t, dir)

	_, err := e.db.ExecContext(t.Context(), "CREATE OR REPLACE VIEW v AS SELECT * FROM read_parquet('"+path+"')")
	require.NoError(t, err)
	res, err := e.Query(t.Context(), "SELECT a FROM v", 10)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
}

func TestQueryBindsArgs(t *testing.T) {
	e, _ := openEngine(t, Options{})

	hostile := "x'; DROP TABLE t; --"
	res, err := e.Query(t.Context(), "SELECT ?::VARCHAR AS v", 10, hostile)
	require.NoError(t, err)
	require.Equal(t, [][]any{{hostile}}, res.Rows)

	res, err = e.Query(t.Context(), "SELECT m[?] AS v FROM (SELECT MAP {'a.b': 'c'} AS m)", 10, "a.b")
	require.NoError(t, err)
	require.Equal(t, [][]any{{"c"}}, res.Rows)
}

func TestGuardRejectsWithArgs(t *testing.T) {
	e, dir := openEngine(t, Options{})

	for _, q := range []string{
		"COPY (SELECT ?) TO '" + dir + "/x.csv'",
		"SELECT ?; SELECT 2",
	} {
		_, err := e.Query(t.Context(), q, 10, 1)
		require.ErrorIs(t, err, ErrNotSelect, q)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestScopedAllowedDirMustContainServiceDir(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()

	_, err := Open(t.Context(), Options{AllowedDir: t.TempDir(), DataDir: dir, Service: "a"})
	require.ErrorContains(t, err, "outside allowed dir")
	_, err = Open(t.Context(), Options{AllowedDir: dir, DataDir: dir, Service: "a"})
	require.NoError(t, err)
}
