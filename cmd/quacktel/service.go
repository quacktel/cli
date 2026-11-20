package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/alecthomas/kong"

	"github.com/quacktel/cli/internal/receiver"
	"github.com/quacktel/cli/internal/service"
)

type serviceCmd struct {
	Install   serviceInstallCmd `cmd:"" help:"Install and start the launchd agent. Pass serve flags after --."`
	Uninstall serviceLabelCmd   `cmd:"" help:"Stop and remove the launchd agent."`
	Restart   serviceRestartCmd `cmd:"" help:"Restart the launchd agent."`
	Status    serviceStatusCmd  `cmd:"" help:"Show the launchd agent's state and recorded command."`
}

type serviceLabel struct {
	Label string `hidden:"" help:"launchd label, overridden by tests." default:"${service_label}"`
}

func (l serviceLabel) manager(globals *Globals) (*service.Manager, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to find the home directory: %w", err)
	}

	return &service.Manager{Label: l.Label, Home: home, UID: os.Getuid(), Logger: globals.Logger}, nil
}

type serviceInstallCmd struct {
	serviceLabel

	Args []string `arg:"" optional:"" passthrough:"" help:"Flags for quacktel serve, after --. The agent always runs with --auth."`
}

// serveProbe is parsed only to reuse serve's own flag parsing and validation.
type serveProbe struct {
	Serve serveCmd `cmd:""`
}

func validateServeArgs(args []string) error {
	var probe serveProbe
	parser, err := kong.New(&probe, kong.Name("quacktel"), kong.NoDefaultHelp(), kong.Writers(io.Discard, io.Discard), kongVars())
	if err != nil {
		return fmt.Errorf("failed to build serve parser: %w", err)
	}
	if _, err := parser.Parse(append([]string{"serve", "--auth"}, args...)); err != nil {
		return fmt.Errorf("invalid serve arguments: %w", err)
	}
	if err := probe.Serve.validate(); err != nil {
		return err
	}
	if _, err := receiver.CheckAddr(probe.Serve.Listen, probe.Serve.UnsafeBind); err != nil {
		return err
	}

	return nil
}

func (c *serviceInstallCmd) Run(ctx context.Context, globals *Globals, w io.Writer) error {
	// Kong keeps the separator in a passthrough argument.
	passthrough := c.Args
	if len(passthrough) > 0 && passthrough[0] == "--" {
		passthrough = passthrough[1:]
	}
	if err := validateServeArgs(passthrough); err != nil {
		return err
	}
	exe, err := executablePath()
	if err != nil {
		return err
	}
	m, err := c.manager(globals)
	if err != nil {
		return err
	}

	logPath := filepath.Join(m.LogDir(), "serve.log")
	args := []string{exe, "--log-file", logPath, "serve"}
	// The agent has no terminal to prompt on, so it always authenticates.
	if !slices.Contains(passthrough, "--auth") {
		args = append(args, "--auth")
	}
	args = append(args, passthrough...)

	if err := m.Install(ctx, args); err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "plist: %s\ncommand: %s\nlog: %s\n", m.PlistPath(), shellJoin(args), logPath)

	return err
}

type serviceLabelCmd struct {
	serviceLabel
}

func (c *serviceLabelCmd) Run(ctx context.Context, globals *Globals) error {
	m, err := c.manager(globals)
	if err != nil {
		return err
	}

	return m.Uninstall(ctx)
}

type serviceRestartCmd struct {
	serviceLabel
}

func (c *serviceRestartCmd) Run(ctx context.Context, globals *Globals) error {
	m, err := c.manager(globals)
	if err != nil {
		return err
	}

	return m.Restart(ctx)
}

type serviceStatusCmd struct {
	serviceLabel
}

func (c *serviceStatusCmd) Run(ctx context.Context, globals *Globals, w io.Writer) error {
	m, err := c.manager(globals)
	if err != nil {
		return err
	}
	st, err := m.Status(ctx)
	if err != nil {
		return err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "label: %s\nloaded: %t\n", m.Label, st.Loaded)
	if st.Loaded {
		fmt.Fprintf(&b, "state: %s\n", st.State)
		if st.PID > 0 {
			fmt.Fprintf(&b, "pid: %d\n", st.PID)
		}
		if st.LastExit != nil {
			fmt.Fprintf(&b, "last exit code: %d\n", *st.LastExit)
		}
	}
	if len(st.Args) > 0 {
		fmt.Fprintf(&b, "command: %s\n", shellJoin(st.Args))
	}
	_, err = io.WriteString(w, b.String())

	return err
}

var shellSafe = regexp.MustCompile(`^[A-Za-z0-9_@%+=:,./-]+$`)

func shellJoin(args []string) string {
	q := make([]string, len(args))
	for i, a := range args {
		if shellSafe.MatchString(a) {
			q[i] = a
		} else {
			q[i] = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
		}
	}

	return strings.Join(q, " ")
}
