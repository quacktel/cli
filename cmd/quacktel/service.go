package main

import (
	"context"
	"errors"
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

	AllowTempBinary bool `hidden:"" help:"Install even when the binary is in a temp dir; for tests."`

	Args []string `arg:"" optional:"" passthrough:"" help:"Flags for quacktel serve, after --. The agent always runs with --auth."`
}

// serveProbe is parsed only to reuse serve's own flag parsing and validation.
type serveProbe struct {
	Serve serveCmd `cmd:""`
}

// parseServeArgs applies serve's own parsing and validation to flags that will run unattended.
func parseServeArgs(args []string) (serveCmd, error) {
	var probe serveProbe
	parser, err := kong.New(&probe, kong.Name("quacktel"), kong.NoDefaultHelp(), kong.Writers(io.Discard, io.Discard), kongVars())
	if err != nil {
		return serveCmd{}, fmt.Errorf("failed to build serve parser: %w", err)
	}
	if _, err := parser.Parse(append([]string{"serve", "--auth"}, args...)); err != nil {
		return serveCmd{}, fmt.Errorf("invalid serve arguments: %w", err)
	}
	if err := probe.Serve.validate(); err != nil {
		return serveCmd{}, err
	}
	if _, err := receiver.CheckAddr(probe.Serve.Listen, probe.Serve.UnsafeBind); err != nil {
		return serveCmd{}, err
	}

	return probe.Serve, nil
}

func explicitFlags(kctx *kong.Context) map[string]bool {
	explicit := map[string]bool{}
	for _, p := range kctx.Path {
		if p.Flag != nil {
			explicit[p.Flag.Name] = true
		}
	}

	return explicit
}

// recordedServe parses the serve flags recorded in the installed plist; ok is false when none is installed.
func recordedServe(m *service.Manager) (rec serveCmd, ok bool, err error) {
	p, err := service.ReadPlist(m.PlistPath())
	if errors.Is(err, os.ErrNotExist) {
		return serveCmd{}, false, nil
	}
	if err != nil {
		return serveCmd{}, false, err
	}
	// Recorded args are validated the same way install validated them.
	i := slices.Index(p.ProgramArguments, "serve")
	if i < 0 {
		return serveCmd{}, false, fmt.Errorf("%s records no serve command", m.PlistPath())
	}
	rec, err = parseServeArgs(p.ProgramArguments[i+1:])
	if err != nil {
		return serveCmd{}, false, fmt.Errorf("invalid recorded arguments in %s: %w", m.PlistPath(), err)
	}

	return rec, true, nil
}

// isTempBinary reports whether exe lives where the OS or go run will delete it.
func isTempBinary(exe, tmp string) bool {
	exe = filepath.Clean(exe)
	if r, err := filepath.EvalSymlinks(tmp); err == nil {
		tmp = r
	}
	tmp = filepath.Clean(tmp)
	if rel, err := filepath.Rel(tmp, exe); err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
		return true
	}

	return strings.Contains(exe, "/go-build")
}

func validateServeArgs(args []string) error {
	_, err := parseServeArgs(args)

	return err
}

// withDataDir swaps the value of --data-dir for abs, because launchd runs with cwd / and a relative path would move the store.
func withDataDir(args []string, abs string) []string {
	out := slices.Clone(args)
	for i, a := range out {
		switch {
		case a == "--data-dir" && i+1 < len(out):
			out[i+1] = abs
		case strings.HasPrefix(a, "--data-dir="):
			out[i] = "--data-dir=" + abs
		}
	}

	return out
}

func (c *serviceInstallCmd) Run(ctx context.Context, globals *Globals, w io.Writer) error {
	// Kong keeps the separator in a passthrough argument.
	passthrough := c.Args
	if len(passthrough) > 0 && passthrough[0] == "--" {
		passthrough = passthrough[1:]
	}
	probe, err := parseServeArgs(passthrough)
	if err != nil {
		return err
	}
	if probe.DataDir != "" {
		passthrough = withDataDir(passthrough, probe.DataDir)
	}
	exe, err := executablePath()
	if err != nil {
		return err
	}
	if !c.AllowTempBinary && isTempBinary(exe, os.TempDir()) {
		return fmt.Errorf("refusing to install a temporary binary (%s); install quacktel first, e.g. mise run install", exe)
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
