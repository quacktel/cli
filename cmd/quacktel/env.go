package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/quacktel/cli/internal/paths"
)

type envCmd struct {
	Listen     string `help:"OTLP/HTTP address the receiver listens on." default:"${listen}"`
	ClaudeCode bool   `name:"claude-code" help:"Also enable Claude Code telemetry."`
	Auth       bool   `help:"Also export the bearer token from <data-dir>/otlp-headers."`
	DataDir    string `help:"Data directory. Defaults to the per-user config dir." type:"path"`
	MCP        bool   `name:"mcp" help:"Print only an MCP server config for quacktel mcp, as JSON."`
	Service    string `help:"With --mcp, restrict the MCP server to one service's data."`
}

func (c *envCmd) Run(w io.Writer) error {
	if c.MCP {
		return c.printMCP(w)
	}

	lines := []string{
		"export OTEL_EXPORTER_OTLP_ENDPOINT=http://" + c.Listen,
		"export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"export OTEL_LOGS_EXPORTER=otlp",
		"export OTEL_TRACES_EXPORTER=otlp",
		"export OTEL_METRICS_EXPORTER=none",
	}
	if c.ClaudeCode {
		lines = append(lines, "export CLAUDE_CODE_ENABLE_TELEMETRY=1")
	}
	if c.Auth {
		header, err := c.authHeader()
		if err != nil {
			return err
		}
		lines = append(lines, "export OTEL_EXPORTER_OTLP_HEADERS='"+header+"'")
	}
	_, err := fmt.Fprintln(w, strings.Join(lines, "\n"))

	return err
}

func (c *envCmd) authHeader() (string, error) {
	dir := c.DataDir
	if dir == "" {
		var err error
		if dir, err = paths.DataDir(); err != nil {
			return "", err
		}
	}
	path := filepath.Join(dir, otlpHeadersFile)
	data, err := os.ReadFile(path) //nolint:gosec // fixed name inside the data dir
	if errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("no %s; run quacktel serve --auth first", path)
	}
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", path, err)
	}
	if !otlpHeadersFormat.Match(data) {
		return "", fmt.Errorf("%s is not in the form authorization=Bearer <64 hex characters>", path)
	}

	return strings.TrimSpace(string(data)), nil
}

func (c *envCmd) printMCP(w io.Writer) error {
	exe, err := executablePath()
	if err != nil {
		return err
	}

	args := []string{"mcp"}
	if c.Service != "" {
		args = append(args, "--service", c.Service)
	}
	type server struct {
		Command string   `json:"command"`
		Args    []string `json:"args"`
	}
	cfg := struct {
		MCPServers map[string]server `json:"mcpServers"`
	}{map[string]server{"quacktel": {Command: exe, Args: args}}}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(cfg)
}

func executablePath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to find the quacktel binary: %w", err)
	}
	if exe, err = filepath.EvalSymlinks(exe); err != nil {
		return "", fmt.Errorf("failed to resolve the quacktel binary: %w", err)
	}

	return exe, nil
}
