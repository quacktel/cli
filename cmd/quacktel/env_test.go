package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/quacktel/cli/internal/service"
)

const envDefault = `export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_LOGS_EXPORTER=otlp
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
`

func TestEnvDefault(t *testing.T) {
	out, err := runBin(t, "env")
	require.NoError(t, err)
	require.Equal(t, envDefault, out)
}

func TestEnvListen(t *testing.T) {
	out, err := runBin(t, "env", "--listen", "127.0.0.1:9999")
	require.NoError(t, err)
	require.Contains(t, out, "export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:9999\n")
}

func TestEnvClaudeCode(t *testing.T) {
	out, err := runBin(t, "env", "--claude-code")
	require.NoError(t, err)
	require.Equal(t, envDefault+"export CLAUDE_CODE_ENABLE_TELEMETRY=1\n", out)
}

func TestEnvMCP(t *testing.T) {
	want, err := filepath.EvalSymlinks(binPath)
	require.NoError(t, err)

	out, err := runBin(t, "env", "--mcp")
	require.NoError(t, err)
	var cfg struct {
		MCPServers map[string]struct {
			Command string   `json:"command"`
			Args    []string `json:"args"`
		} `json:"mcpServers"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &cfg))
	srv := cfg.MCPServers["quacktel"]
	require.True(t, filepath.IsAbs(srv.Command))
	require.Equal(t, want, srv.Command)
	require.Equal(t, []string{"mcp"}, srv.Args)

	out, err = runBin(t, "env", "--mcp", "--service", "otel-go-app")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(out), &cfg))
	require.Equal(t, []string{"mcp", "--service", "otel-go-app"}, cfg.MCPServers["quacktel"].Args)
}

func TestEnvAuth(t *testing.T) {
	dir := dataDir(t)
	_, err := runBin(t, "env", "--auth", "--data-dir", dir)
	require.Error(t, err)

	p := startServe(t, freeAddr(t), dir, "--auth")
	token := readToken(t, dir)
	require.NoError(t, p.cmd.Process.Signal(os.Interrupt))
	require.NoError(t, p.wait(t), p.stderr.String())

	out, err := runBin(t, "env", "--auth", "--data-dir", dir)
	require.NoError(t, err)
	require.Equal(t, envDefault+"export OTEL_EXPORTER_OTLP_HEADERS='authorization=Bearer "+token+"'\n", out)
}

func TestEnvAuthReadsInstalledServiceDataDir(t *testing.T) {
	home := t.TempDir()
	dir := dataDir(t)
	other := dataDir(t)
	label := randomLabel(t)
	const tokenA = "authorization=Bearer aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const tokenB = "authorization=Bearer bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	require.NoError(t, os.WriteFile(filepath.Join(dir, otlpHeadersFile), []byte(tokenA+"\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(other, otlpHeadersFile), []byte(tokenB+"\n"), 0o600))

	m := &service.Manager{Label: label, Home: home}
	data, err := service.Render(service.Plist{
		Label:            label,
		ProgramArguments: []string{"/nonexistent/quacktel", "serve", "--auth", "--data-dir", dir},
		LogPath:          filepath.Join(home, "launchd.log"),
	})
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(m.PlistPath()), 0o755))
	require.NoError(t, os.WriteFile(m.PlistPath(), data, 0o644))

	out, stderr, err := runBinHome(t, home, "env", "--auth", "--label", label)
	require.NoError(t, err, stderr)
	require.Contains(t, out, "OTEL_EXPORTER_OTLP_HEADERS='"+tokenA+"'")

	out, stderr, err = runBinHome(t, home, "env", "--auth", "--label", label, "--data-dir", other)
	require.NoError(t, err, stderr)
	require.Contains(t, out, "OTEL_EXPORTER_OTLP_HEADERS='"+tokenB+"'")
}
