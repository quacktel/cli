# quacktel

A local OpenTelemetry sink for coding agents. `quacktel` receives OTLP logs and
traces on `127.0.0.1`, stores them as Parquet on your machine, and lets agents
query them over MCP. Telemetry never leaves the machine.

## Contents

- [About](#about)
- [Install](#install)
- [Quickstart](#quickstart)
- [Running in the background](#running-in-the-background)
- [Claude Code](#claude-code)
- [OpenShell](#openshell)
- [Your own apps](#your-own-apps)
- [Querying](#querying)
- [Storage locations](#storage-locations)
- [Security notes](#security-notes)
- [Development](#development)
- [Built with AI](#built-with-ai)
- [License](#license)

## About

Coding agents like Claude Code already emit OpenTelemetry: cost, tokens, tool
calls and, if you opt in, prompts. Most of the time that data goes nowhere, or
to a hosted backend you'd rather not send your prompts and file paths to.
quacktel keeps it on your machine, in plain Parquet files, and hands it back to
the agent through MCP so it can answer questions about its own sessions.

Use it to:

- let Claude Code analyse its own sessions: cost, tokens, slow tools, errors;
- debug the apps you're building from their logs and traces;
- collect [OpenShell](#openshell) sandbox network events.

> **Status:** pre-1.0. Supported on macOS on Apple silicon. Linux release
> archives are published but untested, and the background service is macOS
> only; full Linux support is planned.

### Features

- **OTLP/HTTP receiver**: logs and traces, protobuf or JSON, gzip. Metrics are
  accepted and dropped.
- **Locked down by default**: listens on loopback only, rejects browser requests
  (`Origin`) and DNS-rebinding `Host` headers, caps body size and in-flight
  requests, and can require a bearer token (`--auth`).
- **Redaction before storage**: values of secret-looking attribute keys
  (`authorization`, `cookie`, `*_token`, `password`, `api_key`, …) are replaced
  with `[REDACTED]`. Token *counts* such as `input_tokens` are kept. Log bodies
  aren't scanned.
- **Parquet storage**: per-service directories, compacted into hourly files, with
  30-day retention and a 10 GiB disk cap by default.
- **SQL over your telemetry**: `quacktel query` runs read-only DuckDB SQL over
  `logs` and `spans` views.
- **MCP server for agents**: `quacktel mcp` exposes six read-only tools:
  `list_services`, `describe_schema`, `search_logs`, `find_spans`, `get_trace`
  and `sql`. Sessions can be scoped to one service.
- **Background service**: `quacktel service install` runs the receiver as a
  per-user launchd agent that survives logout and reboot.
- **Health checks**: `quacktel doctor` checks the receiver, the service, the
  data dir and the read path.

## Install

### Homebrew

```sh
brew install quacktel/tap/quacktel
```

### Install script

Downloads the release archive for your platform, checks its SHA-256 and installs
to `~/.local/bin`. Read [install.sh](install.sh) before piping it to a shell.

```sh
curl -fsSL https://raw.githubusercontent.com/quacktel/cli/main/install.sh | sh
```

Pass options after `sh -s --`, for example `sh -s -- --version v0.1.0`, or
`--prefix DIR` to install to `DIR/bin`. Make sure that directory is on your
`PATH`.

### From source

You need the Xcode command-line tools, for cgo, and
[mise](https://mise.jdx.dev), which pins Go and the other tools.

```sh
git clone https://github.com/quacktel/cli.git
cd cli
mise install
mise run install        # installs to ~/.local/bin/quacktel
```

## Quickstart

Start the receiver in the background and check it's healthy:

```sh
quacktel service install    # runs quacktel serve --auth under launchd
quacktel doctor             # every check should be ok
```

Send Claude Code's telemetry to it, for the current shell:

```sh
eval "$(quacktel env --claude-code --auth)"
```

Give Claude Code the MCP tools, then start a session:

```sh
claude mcp add quacktel -- "$(command -v quacktel)" mcp
claude
```

Use Claude Code for a while, then ask it *"what did my API requests cost
today?"* or *"which tools failed most this week?"*. You can also query directly:

```sh
quacktel query "SELECT event_name, count(*) FROM logs GROUP BY 1 ORDER BY 2 DESC"
```

To make the telemetry settings permanent, see
[Send Claude Code's telemetry to quacktel](#send-claude-codes-telemetry-to-quacktel).

## Running in the background

`service install` runs `quacktel serve --auth` under launchd. To change the
receiver's settings, pass `serve` flags after `--`. They're validated before
anything is written:

```sh
quacktel service install -- --retention 168h --max-disk 5368709120
```

Running `service install` again upgrades in place. After upgrading or
rebuilding the binary, run `quacktel service restart`. Use
`quacktel service status` to see what's running and `quacktel service uninstall`
to remove it.

Homebrew users can run `brew services start quacktel` instead. Use one or the
other, not both: only one receiver can hold the data dir.

To run the receiver in the foreground instead, use `quacktel serve`. Add
`--auth` if anything outside your shell will send to it.

## Claude Code

### Send Claude Code's telemetry to quacktel

`quacktel env` prints the environment variables an OTel exporter needs. For
Claude Code, add `--claude-code`. Add `--auth` if the receiver requires a token,
which is the default for `service install`.

```sh
quacktel env --claude-code --auth
```

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_LOGS_EXPORTER=otlp
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_EXPORTER_OTLP_HEADERS='authorization=Bearer <token>'
```

For one shell, run `eval "$(quacktel env --claude-code --auth)"` before starting
`claude`. To make it permanent, add the variables to the `env` block of
`~/.claude/settings.json`. `--json` prints them in that shape, and `jq` merges
them into your existing settings while keeping other keys:

```sh
quacktel env --claude-code --auth --json
jq -s '.[0] * .[1]' ~/.claude/settings.json <(quacktel env --claude-code --auth --json) \
  > /tmp/settings.json && mv /tmp/settings.json ~/.claude/settings.json
```

The token is a local secret, so keep `settings.json` private.

Claude Code sends its usage data (cost, tokens, tool results, prompts if you
enable `OTEL_LOG_USER_PROMPTS`) as log events. You'll find them in `logs`, with
`event_name` values such as `api_request`, `tool_result` and `user_prompt`
(Claude Code puts the short name in the `event.name` attribute and
`claude_code.api_request` in the body). Claude Code's traces are
a beta feature; enable them with `CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1`.

### Let Claude Code query quacktel over MCP

```sh
claude mcp add quacktel -- "$(command -v quacktel)" mcp
```

Or print a config to paste into any MCP client:

```sh
quacktel env --mcp
```

Then ask things like *"what did my API requests cost today?"* or *"show the
slowest spans from my app in the last hour"*.

- The tools default to the last 24 hours and 100 rows (max 500), and cut long
  strings at 2,000 characters.
- Every result is labelled as untrusted data. Telemetry is written by local
  processes, and agents shouldn't follow instructions found in it.
- `quacktel mcp --service <name>` limits a session to one service's data, and
  `--no-sql` removes the raw `sql` tool.
- `quacktel serve` (or `service install`) must have run at least once, so the
  data dir exists. If Claude Code reports the server as disconnected, the reason
  is on the server's stderr: check Claude Code's MCP logs or run
  `claude --debug`.

## OpenShell

The OpenShell gateway can export sandbox network events (OCSF) as OTLP logs.
With quacktel running with `--auth` (the default for `service install`), add
this to the gateway's `gateway.toml`:

```toml
[openshell.gateway.otlp.ocsf]
enabled      = true
endpoint     = "http://127.0.0.1:4318"
protocol     = "http/protobuf"
headers_file = "/Users/<you>/Library/Application Support/quacktel/otlp-headers"
```

- `headers_file` is quacktel's token file, in the format the gateway expects.
  It's created on the first `serve --auth` and reused across restarts.
- Restart the gateway after the file is first created, because the gateway reads
  it only at startup.
- The gateway retries batches, so the same event can be stored twice.
  Deduplicate on `ocsf.metadata.uid` in queries.
- OCSF bodies include command lines and process ancestry, and **are not
  redacted**.
- Gap records (`event_name = 'openshell.ocsf.gap'`) report events the exporter
  dropped.

```sql
SELECT DISTINCT attrs['ocsf.metadata.uid'] AS uid,
       json_extract_string(body, '$.action') AS action,
       json_extract_string(body, '$.dst_endpoint.hostname') AS host,
       coalesce(attrs['openshell.sandbox.id'], resource_attrs['openshell.sandbox.id']) AS sandbox
FROM logs
WHERE attrs['ocsf.class_uid'] = '4001'
ORDER BY uid;
```

## Your own apps

Point any OpenTelemetry SDK at quacktel with the same variables, without
`--claude-code`:

```sh
eval "$(quacktel env --auth)"
```

Set `service.name` in your app's resource. Each service gets its own directory,
and `quacktel mcp --service <name>` scopes to it. `examples/otel-go-app` is a
small Go app that sends a sample trace:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 go run -C examples/otel-go-app .
```

## Querying

```sh
quacktel query "SELECT event_name, count(*) FROM logs GROUP BY 1 ORDER BY 2 DESC"
quacktel query --format json "SELECT name, duration_ns FROM spans ORDER BY duration_ns DESC LIMIT 10"
quacktel query --service claude-code "SELECT sum(CAST(attrs['cost_usd'] AS DOUBLE)) FROM logs"
```

- Query the `logs` and `spans` views. They're built from the manifests, so they
  only include committed files.
- Don't `read_parquet` the files directly: during compaction a glob can return
  rows twice.
- Attribute values are stored as strings, so `CAST` them for numeric
  comparisons.
- Only single `SELECT` statements run, and DuckDB can only read the stored
  telemetry, not other files.

## Storage locations

| What | Where |
|---|---|
| Binary | `~/.local/bin/quacktel` (install script, source) or `$(brew --prefix)/bin/quacktel` |
| Data dir | `~/Library/Application Support/quacktel/` (mode `0700`; override with `--data-dir`) |
| Telemetry | `<data dir>/services/<service>-<hash>/{logs,spans}/date=YYYY-MM-DD/hour=HH/*.parquet` |
| Per-service file index | `<data dir>/services/<service>-<hash>/manifest.json` |
| Auth token | `<data dir>/otlp-headers` (mode `0600`; created by `serve --auth`) |
| Receiver counters | `<data dir>/stats.json` (reset when the receiver restarts) |
| Service log | `~/Library/Logs/quacktel/serve.log` (rotated at 10 MiB, 3 kept) |
| launchd crash log | `~/Library/Logs/quacktel/launchd.log` |
| LaunchAgent | `~/Library/LaunchAgents/dev.quacktel.serve.plist` |

Partitions use the time quacktel received the data (UTC), not the record's
timestamp. Old hours are deleted whole by retention or the disk cap; the current
hour is never deleted. If the current hour alone exceeds the disk cap, new
writes are refused with 429 until space frees up.

## Security notes

- The receiver only listens on loopback unless you pass `--unsafe-bind`, which
  also turns on `--auth`.
- Any local process can send telemetry and pick its own `service.name`. Treat
  stored content, and anything an agent reads from it, as untrusted.
- Redaction only looks at attribute keys. Log bodies (Claude Code prompts if you
  enable them, OCSF command lines) are stored as received.
- `quacktel env --auth` prints the token on purpose. Don't paste its output
  anywhere public.

## Development

```sh
mise exec -- go test -race ./...
mise exec -- golangci-lint run ./...
mise run check:tidy
```

[DESIGN.md](DESIGN.md) has the design and its decisions. [AGENTS.md](AGENTS.md)
has the conventions and invariants contributors and agents follow.

## Built with AI

This project is built with AI-assisted development workflows using tools including
[OpenAI Codex](https://developers.openai.com/codex/), [Claude Code](https://claude.ai/code), and
[Cursor](https://cursor.com).

## License

Apache-2.0. See [LICENSE](LICENSE).
