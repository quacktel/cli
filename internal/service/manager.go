package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Runner runs launchctl with args and returns its combined output.
type Runner func(ctx context.Context, args ...string) ([]byte, error)

func defaultRunner(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "/bin/launchctl", args...).CombinedOutput() //nolint:gosec // fixed binary; args are built by Manager, not user input
}

// Manager controls one launchd agent in the user's GUI domain.
type Manager struct {
	Label  string
	Home   string
	UID    int
	Run    Runner
	Logger *slog.Logger
}

// Status is what launchctl reports for the agent.
type Status struct {
	Loaded   bool
	State    string
	PID      int
	LastExit *int
	Args     []string
}

func (m *Manager) PlistPath() string {
	return filepath.Join(m.Home, "Library", "LaunchAgents", m.Label+".plist")
}

// LogDir holds both quacktel's own log file and launchd's capture of stdout and stderr.
func (m *Manager) LogDir() string {
	return filepath.Join(m.Home, "Library", "Logs", "quacktel")
}

func (m *Manager) target() string { return "gui/" + strconv.Itoa(m.UID) + "/" + m.Label }

func (m *Manager) run(ctx context.Context, args ...string) ([]byte, error) {
	if m.Run == nil {
		return defaultRunner(ctx, args...)
	}

	return m.Run(ctx, args...)
}

// Install writes the plist for args and (re)loads the agent.
func (m *Manager) Install(ctx context.Context, args []string) error {
	if err := os.MkdirAll(filepath.Dir(m.PlistPath()), 0o755); err != nil { //nolint:gosec // launchd must be able to traverse LaunchAgents
		return fmt.Errorf("failed to create LaunchAgents dir: %w", err)
	}
	if err := os.MkdirAll(m.LogDir(), 0o700); err != nil {
		return fmt.Errorf("failed to create log dir: %w", err)
	}
	data, err := Render(Plist{
		Label:            m.Label,
		ProgramArguments: args,
		LogPath:          filepath.Join(m.LogDir(), "launchd.log"),
	})
	if err != nil {
		return err
	}
	if err := writeAtomic(m.PlistPath(), data); err != nil {
		return err
	}

	m.bootout(ctx)
	if out, err := m.run(ctx, "bootstrap", "gui/"+strconv.Itoa(m.UID), m.PlistPath()); err != nil {
		return fmt.Errorf("failed to bootstrap %s: %w: %s", m.Label, err, strings.TrimSpace(string(out)))
	}

	return nil
}

// Uninstall unloads the agent and removes its plist.
func (m *Manager) Uninstall(ctx context.Context) error {
	m.bootout(ctx)
	if err := os.Remove(m.PlistPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove plist: %w", err)
	}

	return nil
}

// bootout failing just means nothing was loaded; a stale loaded copy would otherwise make bootstrap fail.
func (m *Manager) bootout(ctx context.Context) {
	out, err := m.run(ctx, "bootout", m.target())
	if err != nil {
		m.logger().DebugContext(ctx, "bootout failed",
			slog.Any("error", err),
			slog.Bool("not_loaded", notLoaded.Match(out)),
			slog.String("output", strings.TrimSpace(string(out))))
	}
}

func (m *Manager) Restart(ctx context.Context) error {
	if out, err := m.run(ctx, "kickstart", "-k", m.target()); err != nil {
		return fmt.Errorf("failed to restart %s: %w: %s", m.Label, err, strings.TrimSpace(string(out)))
	}

	return nil
}

var (
	notLoaded  = regexp.MustCompile(`No such process|Could not find service`)
	stateRe    = regexp.MustCompile(`(?m)^\s*state = (\S.*?)\s*$`)
	pidRe      = regexp.MustCompile(`(?m)^\s*pid = (\d+)\s*$`)
	lastExitRe = regexp.MustCompile(`(?m)^\s*last exit code = (-?\d+)\s*$`)
)

func (m *Manager) Status(ctx context.Context) (Status, error) {
	var st Status
	if p, err := ReadPlist(m.PlistPath()); err == nil {
		st.Args = p.ProgramArguments
	} else if !errors.Is(err, os.ErrNotExist) {
		return Status{}, err
	}

	out, err := m.run(ctx, "print", m.target())
	if err != nil {
		if notLoaded.Match(out) {
			return st, nil
		}

		return Status{}, fmt.Errorf("failed to query %s: %w: %s", m.Label, err, strings.TrimSpace(string(out)))
	}

	st.Loaded = true
	// The first match is the job's own; later ones belong to nested dicts.
	if sm := stateRe.FindSubmatch(out); sm != nil {
		st.State = string(sm[1])
	}
	if pm := pidRe.FindSubmatch(out); pm != nil {
		st.PID, _ = strconv.Atoi(string(pm[1]))
	}
	if lm := lastExitRe.FindSubmatch(out); lm != nil {
		code, _ := strconv.Atoi(string(lm[1]))
		st.LastExit = &code
	}

	return st, nil
}

func (m *Manager) logger() *slog.Logger {
	if m.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}

	return m.Logger
}

func writeAtomic(path string, data []byte) (err error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".plist-*")
	if err != nil {
		return fmt.Errorf("failed to create temp plist: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.Remove(f.Name()))
		}
	}()
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		// CreateTemp makes 0600, but launchd expects a world-readable plist.
		err = f.Chmod(0o644)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("failed to write plist: %w", err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("failed to publish plist: %w", err)
	}

	return nil
}
