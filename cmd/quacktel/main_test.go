package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var binPath string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	dir, err := os.MkdirTemp("", "quacktel-test")
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to create temp dir:", err)
		return 1
	}
	defer os.RemoveAll(dir)

	binPath = filepath.Join(dir, "quacktel")
	if out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build binary: %v\n%s", err, out)
		return 1
	}

	return m.Run()
}

func runBin(t *testing.T, args ...string) (string, error) {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), binPath, args...)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	err := cmd.Run()

	return stdout.String(), err
}

func TestVersion(t *testing.T) {
	stdout, err := runBin(t, "version")
	require.NoError(t, err)
	require.Regexp(t, `^quacktel \S+ \(commit \S+, go[^)]+\)\n$`, stdout)
}

func TestHelp(t *testing.T) {
	stdout, err := runBin(t, "--help")
	require.NoError(t, err)
	require.Contains(t, stdout, "version")
}

func TestQueryJSON(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	stdout, err := runBin(t, "query", "--data-dir", dir, "--format", "json", "SELECT 42 AS answer")
	require.NoError(t, err)
	require.Equal(t, "[{\"answer\":42}]\n", stdout)
}

func TestQueryRejectsCopy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	out := filepath.Join(dir, "x.csv")
	_, err := runBin(t, "query", "--data-dir", dir, "--format", "json", "COPY (SELECT 1) TO '"+out+"'")
	require.Error(t, err)
	require.NoFileExists(t, out)
}

func TestQueryRejectsWideDataDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))
	_, err := runBin(t, "query", "--data-dir", dir, "--format", "json", "SELECT 1")
	require.Error(t, err)
}
