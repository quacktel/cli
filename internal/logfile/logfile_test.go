package logfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(t.TempDir(), "logs", "q.log")
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(b)
}

func TestRotation(t *testing.T) {
	path := newPath(t)
	w, err := Open(path, 100, 2)
	require.NoError(t, err)

	for i := range 10 {
		n, err := w.Write(fmt.Appendf(nil, "%029d\n", i))
		require.NoError(t, err)
		require.Equal(t, 30, n)
	}
	require.NoError(t, w.Close())

	require.FileExists(t, path)
	require.FileExists(t, path+".1")
	require.FileExists(t, path+".2")
	require.NoFileExists(t, path+".3")

	require.Contains(t, read(t, path), fmt.Sprintf("%029d\n", 9))
	for _, p := range []string{path, path + ".1", path + ".2"} {
		for line := range strings.SplitSeq(strings.TrimSuffix(read(t, p), "\n"), "\n") {
			require.Len(t, line, 29, p)
		}
	}
}

func TestModes(t *testing.T) {
	path := newPath(t)
	w, err := Open(path, 100, 2)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
	di, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), di.Mode().Perm())
}

func TestRejectsWideDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))

	_, err := Open(filepath.Join(dir, "q.log"), 100, 2)
	require.ErrorContains(t, err, "0755")
}

func TestReopenAppends(t *testing.T) {
	path := newPath(t)
	for _, s := range []string{"one\n", "two\n"} {
		w, err := Open(path, 1000, 2)
		require.NoError(t, err)
		_, err = w.Write([]byte(s))
		require.NoError(t, err)
		require.NoError(t, w.Close())
	}

	require.Equal(t, "one\ntwo\n", read(t, path))
}

func TestConcurrentWrites(t *testing.T) {
	path := newPath(t)
	w, err := Open(path, 2000, 10)
	require.NoError(t, err)

	const goroutines, perG = 8, 50
	line := []byte("xxxxxxxxx\n")
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range perG {
				_, err := w.Write(line)
				if err != nil {
					panic(err)
				}
			}
		})
	}
	wg.Wait()
	require.NoError(t, w.Close())

	total := len(read(t, path))
	for i := 1; i <= 10; i++ {
		if b, err := os.ReadFile(fmt.Sprintf("%s.%d", path, i)); err == nil {
			total += len(b)
		}
	}
	require.Equal(t, goroutines*perG*len(line), total)
	require.FileExists(t, path+".1")
}

func TestOversizedWrite(t *testing.T) {
	path := newPath(t)
	w, err := Open(path, 50, 2)
	require.NoError(t, err)

	_, err = w.Write([]byte("small\n"))
	require.NoError(t, err)
	big := strings.Repeat("y", 200)
	_, err = w.Write([]byte(big))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.Equal(t, big, read(t, path))
	require.Equal(t, "small\n", read(t, path+".1"))
}

func TestCloseIdempotent(t *testing.T) {
	w, err := Open(newPath(t), 100, 2)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())
}
