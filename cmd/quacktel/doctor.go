package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"text/tabwriter"

	"github.com/alecthomas/kong"

	"github.com/quacktel/cli/internal/doctor"
	"github.com/quacktel/cli/internal/paths"
	"github.com/quacktel/cli/internal/service"
)

const defaultMaxDisk = 10 << 30

// errChecksFailed makes main exit 1 without logging, because the report already says what failed.
var errChecksFailed = errors.New("doctor found failing checks")

type doctorCmd struct {
	serviceLabel

	DataDir string `help:"Data directory. Defaults to the installed agent's, then the per-user config dir." type:"path"`
	Listen  string `help:"OTLP/HTTP address to probe. Defaults to the installed agent's, then ${listen}." default:"${listen}"`
	MaxDisk int64  `help:"Disk cap to compare against. Defaults to the installed agent's, then the serve default." default:"${max_disk}"`
	JSON    bool   `name:"json" help:"Print checks as JSON."`
}

func (c *doctorCmd) Run(ctx context.Context, kctx *kong.Context, w io.Writer) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to find the home directory: %w", err)
	}
	m := &service.Manager{Label: c.Label, Home: home, UID: os.Getuid()}

	explicit := map[string]bool{}
	for _, p := range kctx.Path {
		if p.Flag != nil {
			explicit[p.Flag.Name] = true
		}
	}
	if p, err := service.ReadPlist(m.PlistPath()); err == nil {
		// Recorded args are validated the same way install validated them.
		i := slices.Index(p.ProgramArguments, "serve")
		if i < 0 {
			return fmt.Errorf("%s records no serve command", m.PlistPath())
		}
		rec, err := parseServeArgs(p.ProgramArguments[i+1:])
		if err != nil {
			return fmt.Errorf("invalid recorded arguments in %s: %w", m.PlistPath(), err)
		}
		if !explicit["data-dir"] && rec.DataDir != "" {
			c.DataDir = rec.DataDir
		}
		if !explicit["listen"] {
			c.Listen = rec.Listen
		}
		if !explicit["max-disk"] {
			c.MaxDisk = rec.MaxDisk
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if c.DataDir == "" {
		if c.DataDir, err = paths.DataDir(); err != nil {
			return err
		}
	}

	exe, err := executablePath()
	if err != nil {
		return err
	}
	checks := doctor.Run(ctx, doctor.Config{
		DataDir:   c.DataDir,
		Listen:    c.Listen,
		MaxDisk:   c.MaxDisk,
		PlistPath: m.PlistPath(),
		Label:     c.Label,
		Exe:       exe,
		Launchctl: service.DefaultRunner,
	})

	if err := writeChecks(w, checks, c.JSON); err != nil {
		return err
	}
	for _, ck := range checks {
		if ck.Status == doctor.Fail {
			return errChecksFailed
		}
	}

	return nil
}

func writeChecks(w io.Writer, checks []doctor.Check, asJSON bool) error {
	if asJSON {
		if err := json.NewEncoder(w).Encode(checks); err != nil {
			return fmt.Errorf("failed to write json: %w", err)
		}

		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STATUS\tCHECK\tDETAIL")
	for _, ck := range checks {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", ck.Status, ck.Name, ck.Detail)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("failed to write table: %w", err)
	}

	return nil
}
