# quacktel

An agent-first [OpenTelemetry](https://opentelemetry.io) backend for local
development and coding agents. quacktel receives OTLP logs and traces on
`127.0.0.1` so agents can check that the software they're building actually
works before it's released. Telemetry never leaves the machine.

Incoming telemetry streams through [Apache Arrow](https://arrow.apache.org) into
compacted [Apache Parquet](https://parquet.apache.org) files and is queried by
a statically linked [DuckDB](https://duckdb.org) engine over the
[Model Context Protocol (MCP)](https://modelcontextprotocol.io). It works with
any app instrumented with an OpenTelemetry SDK.

## Contents

- [About](#about)
- [Install](#install)
- [Quickstart](#quickstart)
- [Running in the background](#running-in-the-background)
- [Claude Code](#claude-code)
- [Your own apps](#your-own-apps)
- [OpenShell](#openshell)
- [Querying](#querying)
- [Storage locations](#storage-locations)
- [Security notes](#security-notes)
- [Development](#development)
- [Built with AI](#built-with-ai)
- [License](#license)

## About

Passing tests don't show what your software does when it runs: which requests
it makes, where it's slow, or what it logs when something fails. quacktel gives
coding agents that runtime view locally. Point your app's OpenTelemetry SDK at
`127.0.0.1:4318`, run it, and let the agent inspect the resulting traces and
logs before you ship. The same store also captures coding-agent telemetry
(cost, tokens, tool calls) and [NVIDIA OpenShell](https://www.nvidia.com/en-us/ai/openshell/)
sandbox events.

> Status: pre-1.0. Supported on macOS (Apple Silicon). Linux release archives
> are published but untested, and the background service is macOS only; full
> Linux support is planned.

### How it works

```text
App / Agent (OTLP/HTTP)
  → quacktel serve       redact → Apache Arrow → compacted Parquet + manifests
  → quacktel mcp / query locked-down DuckDB over committed views
  → Coding agent         typed, context-budgeted MCP tools + read-only SQL
```

The write path and query engine stay decoupled. `quacktel serve` decodes
OTLP/HTTP logs and traces (protobuf or JSON, with gzip; metrics return `200`
and are dropped), redacts secret-looking attribute keys, and writes compacted
Parquet files via Apache Arrow with zero database lock contention. Each
`quacktel mcp` or `quacktel query` invocation opens an in-memory, statically
linked DuckDB directly over the committed Parquet manifests, giving you
read-only SQL over `logs` and `spans` with no separate database or runtime
extensions.

### Built for agents

MCP is the primary interface, not an add-on. `quacktel mcp` exposes six
read-only tools (`list_services`, `describe_schema`, `search_logs`,
`find_spans`, `get_trace` and `sql`) designed around an agent's loop: results
stay within a context budget, telemetry is labelled as untrusted data, typed
tools bind every value as a parameter, and sessions can be scoped to a single
service or locked down with `--no-sql`.

## Install

### Homebrew

```sh
brew install quacktel/tap/quacktel
```

### Install script

Downloads the release archive for your platform, checks its SHA-256 and installs
to `~/.local/bin`. If `gh` is installed it also checks GitHub's release
attestation, and if `cosign` is installed it verifies the build provenance (from
v0.2.0). Read [install.sh](install.sh) before piping it to a shell.

```sh
curl -fsSL https://raw.githubusercontent.com/quacktel/cli/main/install.sh | sh
```

Pass options after `sh -s --`, for example `sh -s -- --version v0.1.0`, or
`--prefix DIR` to install to `DIR/bin`. Make sure that directory is on your
`PATH`.

### From source

You need the Xcode Command Line Tools (for cgo) and
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

Claude Code is the fastest way to see live data. Point its telemetry at
quacktel for the current shell:

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

To make Claude Code's telemetry settings permanent, see
[Send Claude Code's telemetry to quacktel](#send-claude-codes-telemetry-to-quacktel).
To send traces and logs from your own services so an agent can verify them, see
[Your own apps](#your-own-apps).

## Running in the background

`service install` runs `quacktel serve --auth` under launchd. To change the
receiver's settings, pass `serve` flags after `--`. They're validated before
anything is written:

```sh
quacktel service install -- --retention 168h --max-disk 5368709120
```

Running `service install` again upgrades in place. After upgrading or
rebuilding the binary, run `quacktel service restart`. `quacktel --version`
shows which version you have. Use
`quacktel service status` to see what's running and `quacktel service uninstall`
to remove it.

Homebrew users can run `brew services start quacktel` instead. Use one or the
other, not both: only one receiver can hold the data dir. Under `brew services`,
logs go to `$(brew --prefix)/var/log/quacktel.log`.

To run the receiver in the foreground instead, use `quacktel serve`. Add
`--auth` to require a bearer token (recommended so other local processes can't
write freely, and enabled automatically with `--unsafe-bind`).

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
tmp="$(mktemp)"
jq -s '.[0] * .[1]' ~/.claude/settings.json <(quacktel env --claude-code --auth --json) > "$tmp" \
  && chmod 600 "$tmp" && mv "$tmp" ~/.claude/settings.json
```

The token is a local secret, so keep `settings.json` private (mode `0600`). If
the file doesn't exist yet, write the `--json` output straight to it.

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

- Typed tools default to the last 24 hours and 100 rows (20 for `search_logs`,
  max 500). Responses are capped at 40,000 JSON characters to fit within an
  agent's context budget, and long strings are cut at 2,000 characters.
- Every result is labelled as untrusted data. Telemetry is written by local
  processes, and agents shouldn't follow instructions found in it.
- `quacktel mcp --service <name>` limits a session to one service's data, and
  `--no-sql` removes the raw `sql` tool.
- `quacktel serve` (or `service install`) must have run at least once, so the
  data dir exists. If Claude Code reports the server as disconnected, the reason
  is on the server's stderr: check Claude Code's MCP logs or run
  `claude --debug`.

## Your own apps

Point any OpenTelemetry SDK at quacktel with the same variables, without
`--claude-code`:

```sh
eval "$(quacktel env --auth)"
```

Set `service.name` in your app's resource. Each service gets its own directory,
and `quacktel mcp --service <name>` scopes to it. From a clone of this repo,
`examples/otel-go-app` is a small Go app that sends a sample trace:

```sh
eval "$(quacktel env --auth)"
go run -C examples/otel-go-app .
```

## OpenShell

The [OpenShell](https://www.nvidia.com/en-us/ai/openshell/) gateway and
supervisor can export sandbox OCSF events (network, auth and state changes) as
OTLP logs. With quacktel running with `--auth` (the default for
`service install`), add this to the gateway's `gateway.toml`:

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
       json_extract_string(body, '$.activity_name') AS activity,
       coalesce(
         json_extract_string(body, '$.dst_endpoint.hostname'),
         json_extract_string(body, '$.dst_endpoint.domain'),
         json_extract_string(body, '$.dst_endpoint.ip')
       ) AS host,
       json_extract_string(body, '$.dst_endpoint.port') AS port,
       coalesce(attrs['openshell.sandbox.id'], resource_attrs['openshell.sandbox.id']) AS sandbox
FROM logs
WHERE attrs['ocsf.class_uid'] = '4001'
ORDER BY uid;
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
timestamp. Old hours are deleted whole by retention (30 days by default) or the
disk cap (10 GiB by default); the current hour is never deleted. If the current
hour alone exceeds the disk cap, new writes are refused with 429 until space
frees up.

## Security notes

- The receiver only listens on loopback unless you pass `--unsafe-bind`, which
  also turns on `--auth`. It rejects browser `Origin` and DNS-rebinding `Host`
  headers and caps request body size and in-flight requests.
- Any local process can send telemetry and pick its own `service.name`. Treat
  stored content, and anything an agent reads from it, as untrusted.
- Redaction runs before storage and replaces values of secret-looking attribute
  keys (`authorization`, `cookie`, `*_token`, `password`, `api_key`, …) with
  `[REDACTED]` while keeping token counts such as `input_tokens`. Log bodies
  (Claude Code prompts if you enable them, OCSF command lines) are stored as
  received.
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
