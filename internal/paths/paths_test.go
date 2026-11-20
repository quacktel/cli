package paths

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataDir(t *testing.T) {
	base, err := os.UserConfigDir()
	require.NoError(t, err)

	got, err := DataDir()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(base, "quacktel"), got)
}

func TestEnsureDirCreates(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b")
	require.NoError(t, EnsureDir(dir))

	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestEnsureDirExistingOK(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	require.NoError(t, EnsureDir(dir))
}

func TestEnsureDirRejectsWideMode(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))

	err := EnsureDir(dir)
	require.ErrorContains(t, err, "0755")

	info, statErr := os.Stat(dir)
	require.NoError(t, statErr)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}

func TestEnsureDirRejectsFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(f, nil, 0o600))
	require.Error(t, EnsureDir(f))
}
