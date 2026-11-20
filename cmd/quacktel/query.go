package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"text/tabwriter"

	"golang.org/x/term"

	"github.com/quacktel/cli/internal/engine"
	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/paths"
)

type queryCmd struct {
	SQL string `arg:"" help:"A single SELECT statement."`

	DataDir string `help:"Data directory. Defaults to the per-user config dir." type:"path"`
	Format  string `help:"Output format: auto is table on a terminal, JSON otherwise." enum:"auto,table,json" default:"auto"`
	MaxRows int    `help:"Maximum rows to return." default:"1000"`
	Service string `help:"Restrict the session to one service's data."`
}

func (c *queryCmd) Run(ctx context.Context, globals *Globals, w io.Writer) error {
	dir := c.DataDir
	if dir == "" {
		var err error
		if dir, err = paths.DataDir(); err != nil {
			return err
		}
	}
	if err := paths.EnsureDir(dir); err != nil {
		return err
	}

	e, err := engine.Open(ctx, engineOptions(dir, c.Service))
	if err != nil {
		return fmt.Errorf("failed to open engine: %w", err)
	}
	defer e.Close()

	res, err := e.Query(ctx, c.SQL, c.MaxRows)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	if res.Truncated {
		globals.Logger.Warn("result truncated", slog.Int("max_rows", c.MaxRows))
	}

	format := c.Format
	if format == "auto" {
		format = "json"
		if term.IsTerminal(int(os.Stdout.Fd())) {
			format = "table"
		}
	}
	if format == "table" {
		return writeTable(w, res)
	}

	return writeJSON(w, res)
}

// engineOptions keeps DuckDB's reach to the services dir, so the headers file, stats and manifests stay out of it.
func engineOptions(dir, service string) engine.Options {
	opts := engine.Options{AllowedDir: filepath.Join(dir, "services"), DataDir: dir}
	if service != "" {
		opts.Service = service
		opts.AllowedDir = filepath.Join(dir, "services", manifest.ServiceDir(service))
	}

	return opts
}

func writeJSON(w io.Writer, res engine.Result) error {
	out := make([]map[string]any, 0, len(res.Rows))
	for _, row := range res.Rows {
		obj := make(map[string]any, len(res.Columns))
		for i, col := range res.Columns {
			obj[col] = row[i]
		}
		out = append(out, obj)
	}

	if err := json.NewEncoder(w).Encode(out); err != nil {
		return fmt.Errorf("failed to write json: %w", err)
	}

	return nil
}

func writeTable(w io.Writer, res engine.Result) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for i, col := range res.Columns {
		if i > 0 {
			fmt.Fprint(tw, "\t")
		}
		fmt.Fprint(tw, col)
	}
	fmt.Fprintln(tw)
	for _, row := range res.Rows {
		for i, v := range row {
			if i > 0 {
				fmt.Fprint(tw, "\t")
			}
			if v == nil {
				continue
			}
			fmt.Fprint(tw, v)
		}
		fmt.Fprintln(tw)
	}

	if err := tw.Flush(); err != nil {
		return fmt.Errorf("failed to write table: %w", err)
	}

	return nil
}
