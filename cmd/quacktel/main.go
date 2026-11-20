package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/lmittmann/tint"

	"github.com/quacktel/cli/internal/receiver"
)

// Set by ldflags at release time.
var (
	version = ""
	commit  = ""
	date    = ""
)

type Globals struct {
	Debug bool `help:"Enable debug logging on stderr."`

	Logger *slog.Logger `kong:"-"`
}

type cli struct {
	Globals

	Version versionCmd `cmd:"" help:"Print version information."`
	Serve   serveCmd   `cmd:"" help:"Run the OTLP receiver and write logs to Parquet."`
	Query   queryCmd   `cmd:"" help:"Run a read-only SQL query over stored telemetry."`
	Env     envCmd     `cmd:"" help:"Print shell exports or MCP config for pointing apps at quacktel."`
	MCP     mcpCmd     `cmd:"" name:"mcp" help:"Serve stored telemetry to agents over MCP on stdio."`
}

type versionCmd struct{}

func (*versionCmd) Run(globals *Globals, w io.Writer) error {
	globals.Logger.Debug("printing version", slog.String("date", date))

	_, err := fmt.Fprintln(w, versionString())

	return err
}

func versionString() string {
	v, c := version, commit
	if v == "" || c == "" {
		bv, bc := buildInfo()
		if v == "" {
			v = bv
		}
		if c == "" {
			c = bc
		}
	}

	return fmt.Sprintf("quacktel %s (commit %s, %s)", v, c, runtime.Version())
}

func buildInfo() (string, string) {
	v, c := "dev", "unknown"

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return v, c
	}
	// Main.Version is "(devel)" for local builds.
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		v = info.Main.Version
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && s.Value != "" {
			c = s.Value
		}
	}

	return v, c
}

func main() {
	if err := run(); err != nil {
		slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
			NoColor: os.Getenv("NO_COLOR") != "",
		})).Error("quacktel failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var args cli
	parser, err := kong.New(&args,
		kong.Name("quacktel"),
		kong.Description("Local OTLP to Parquet store with MCP query. Results go to stdout; logs go to stderr."),
		kong.Writers(os.Stdout, os.Stderr),
		kong.Vars{
			"listen":       receiver.DefaultAddr,
			"max_body":     strconv.Itoa(receiver.DefaultMaxBody),
			"max_inflight": strconv.Itoa(receiver.DefaultMaxInFlight),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to build CLI: %w", err)
	}

	kctx, err := parser.Parse(os.Args[1:])
	if err != nil {
		return err
	}

	level := slog.LevelInfo
	if args.Debug {
		level = slog.LevelDebug
	}
	args.Logger = slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
		Level:   level,
		NoColor: os.Getenv("NO_COLOR") != "",
	}))

	kctx.BindTo(ctx, (*context.Context)(nil))
	kctx.Bind(&args.Globals)
	kctx.BindTo(os.Stdout, (*io.Writer)(nil))

	return kctx.Run()
}
