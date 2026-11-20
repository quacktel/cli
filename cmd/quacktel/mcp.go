package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/quacktel/cli/internal/engine"
	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/mcpserver"
	"github.com/quacktel/cli/internal/paths"
	"github.com/quacktel/cli/internal/tools"
)

type mcpCmd struct {
	DataDir string `help:"Data directory. Defaults to the per-user config dir." type:"path"`
	Service string `help:"Restrict the session to one service's data."`
	NoSQL   bool   `name:"no-sql" help:"Do not expose the raw sql tool."`
}

// Run serves MCP over stdio, so stdout carries protocol only and logs go to stderr.
func (c *mcpCmd) Run(ctx context.Context, globals *Globals) error {
	dir := c.DataDir
	if dir == "" {
		var err error
		if dir, err = paths.DataDir(); err != nil {
			return err
		}
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("no data dir at %s; run quacktel serve first", dir)
	}
	if err := paths.EnsureDir(dir); err != nil {
		return err
	}

	opts := engine.Options{AllowedDir: dir, DataDir: dir}
	if c.Service != "" {
		opts.Service = c.Service
		opts.AllowedDir = filepath.Join(dir, "services", manifest.ServiceDir(c.Service))
	}
	e, err := engine.Open(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to open engine: %w", err)
	}
	defer e.Close()

	v, _ := buildInfo()
	if version != "" {
		v = version
	}
	srv := mcpserver.New(&tools.Runner{Eng: e, Now: time.Now, Service: c.Service}, mcpserver.Options{
		SQL: !c.NoSQL, Version: v, Logger: globals.Logger,
	})
	if err := srv.Run(ctx, &mcp.StdioTransport{}); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("mcp server failed: %w", err)
	}

	return nil
}
