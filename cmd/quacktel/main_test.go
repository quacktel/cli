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
