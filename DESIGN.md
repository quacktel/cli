# quacktel design

Status: draft v0.1 design · 2026-11-20

## Summary

quacktel is a single Go binary that runs a local OpenTelemetry (OTLP) receiver,
stores logs and traces as Parquet, and lets coding agents query them over stdio
MCP. DuckDB is the query engine, statically linked. The goal is to let an agent
debug the apps you are developing, and analyse its own behaviour, using
telemetry that never leaves the machine.

```
quacktel serve              # background service: OTLP/HTTP 127.0.0.1:4318 → Arrow → Parquet
quacktel mcp                # stdio MCP server, spawned by the agent; read-only DuckDB
quacktel query "SELECT …"   # human CLI, same engine and guardrails as mcp
quacktel env                # print OTEL_* exports and the MCP config snippet
quacktel service install|uninstall|status
quacktel doctor             # port, permissions, service state, disk use, dropped data
quacktel version
```

## Goals

- **Easy to install:** one self-contained binary with no runtime downloads.
- **Simple to run in the background:** a per-user service on macOS and Linux.
- **Secure by default:** one write-only listener bound to localhost, a locked-down
  read path, redaction, and bounded retention.
- **Useful to agents:** typed MCP tools for common questions, plus a
  read-only SQL escape hatch.

## Non-goals (v0.1)

- Metrics storage. Metrics are deferred because their data model (temporality,
  histograms) is the hardest to map to Parquet.
- OTLP/gRPC.
- Windows and macOS amd64 (Intel).
- Remote or shared storage, multi-user, or a query listener on the network.
- Writing to other tools' config. The only onboarding command is `quacktel env`.
- Substrait. Typed tools compile directly to parameterised DuckDB SQL.

## Decisions

| Area | Decision | Rationale |
|---|---|---|
| Name | Binary, paths and service are all named `quacktel` | Matches the repo and org. |
| First users | Apps under development, and coding agents' own telemetry | Agent debugging plus agent self-analysis. |
| Signals | Logs and traces | Highest value for debugging; metrics deferred. |
| Platforms | M0–M6: macOS arm64 only. M7: add Linux amd64 and arm64. macOS amd64 is out of scope | Keeps the first phases tight: one runner and one service manager. Platform-specific code (paths, service) sits behind a small interface so Linux can be added without touching the rest. |
| Transport | OTLP/HTTP only: protobuf and JSON, gzip accepted | One endpoint to harden. Users set `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`. |
| Engine | DuckDB via `duckdb-go` v2.20000.0 (DuckDB 2.0.0 LTS), statically linked, used only on the read path | Full SQL with no runtime extensions. |
| Writer | arrow-go `pqarrow` | Reuses the parquet-arrow-search pattern and keeps the daemon free of DuckDB. |
| Write path | Tail → seal → compact, with a manifest | ≤ ~1–5s staleness without DuckDB lock contention. |
| Attributes | Promoted columns plus `MAP(VARCHAR, VARCHAR)` | Fast filters on hot keys; stable schema. |
| Query path | Typed MCP tools → prepared SQL with bound parameters, plus a read-only `sql` tool | No injection surface in typed tools. |
| Scope | One store per user, physically partitioned by service. `mcp` sees all services by default; `mcp --service` is enforced by DuckDB's `allowed_directories` | Allows cross-service queries by default; scoping later needs no data migration. |
| Redaction | Key-pattern masking, on by default, configurable | Cheap and predictable. |
| Retention | 30 days or 10 GB, whichever is hit first | Enough history for agents to analyse usage and cost trends. |
| Onboarding | Docs plus `quacktel env` | Nothing writes to another tool's config. |
| Distribution | Until 1.0: build from source (`go install`). Pre-1.0 release (M6): Homebrew and `install.sh` with attestation check; .deb/.rpm with Linux (M7) | Personal use until 1.0, so no release pipeline yet. | See [Distribution](#distribution). |

## Architecture

```
 OTel SDK / Claude Code
        │ POST /v1/logs, /v1/traces   (http/protobuf | http/json, gzip)
        ▼
 ┌──────────── quacktel serve ─────────────┐
 │ receiver: Origin/Host/Content-Type gate │
 │           body + decompressed caps, 429 │
 │ decode → redact → Arrow record builders │
 │ flusher: tail files (atomic rename)     │
 │ compactor: tail → sealed → hourly       │
 │ retention: drop partitions, size cap    │
 │ manifest: per-service list of live files│
 └───────────────┬─────────────────────────┘
                 │ services/<svc>/… Parquet + manifest.json (0700)
                 ▼
 quacktel mcp / quacktel query
   read manifest → read_parquet([files]) in a locked-down DuckDB
```

The daemon and the readers share nothing except the data directory. `mcp` works
when the daemon is stopped; it just sees no new data.

## Storage

### Paths

| | macOS (M0–M6) | Linux (M7) |
|---|---|---|
| Data | `~/Library/Application Support/quacktel/` | `$XDG_DATA_HOME/quacktel/` (`~/.local/share/quacktel`) |
| Logs | `~/Library/Logs/quacktel/` | journald (systemd user unit) |
| Config | `$XDG_CONFIG_HOME/quacktel/config.toml` (`~/.config/…`) | same |

The data directory is created `0700`, and its files `0600`. `doctor` warns about
looser permissions.

### Layout

```
data/
  services/
    <svc-dir>/                        # one directory per service.name
      manifest.json                   # generation N, live files for this service
      logs/date=2026-11-20/hour=13/
        tail-<ulid>.parquet           # flushed every 1–5s, only when there is data
        sealed-<ulid>.parquet         # tails merged every ~5 min
        hour.parquet                  # final merge once the hour is closed
      spans/date=…/hour=…/…
  .tmp/                               # in-progress writes; never read
```

**Service directories.** `service.name` is untrusted input, so it is never used
as a path as-is:

- `<svc-dir>` is a slug of the name (lowercase `[a-z0-9._-]`, max 64 chars) plus
  an 8-char hash of the full name, for example `checkout-api-1a2b3c4d`. This
  blocks path traversal and avoids collisions between names that slug the same.
- The original name is kept in the manifest and in the `service_name` column.
- A missing `service.name` maps to `unknown_service`.
- New service directories are capped (default 256). Past the cap, records
  are rejected via OTLP `partial_success`, so a misbehaving sender can't
  create unbounded directories.
- Each service is flushed and compacted independently, so many quiet services
  produce more small files. Flushes only happen when there is data, and
  compaction runs per service.

### Write, seal, compact

1. **Flush:** the in-memory Arrow builder is written to `.tmp/x.parquet`,
   fsynced, then renamed into its partition.
2. **Publish:** that service's new `manifest.json` is written to `.tmp` and
   renamed over the old one. Readers use only the files in the manifest, never a glob, so
   compaction can never produce duplicate or missing rows.
3. **Compact:** tails are merged into a sealed file, the manifest is published
   with the sealed file replacing its sources, and the sources are unlinked
   after a grace period (default 60s) so in-flight queries can finish.
4. **Shutdown:** on SIGTERM/SIGINT the buffer is flushed, the manifest
   published, then the process exits.

The compaction targets (sealed every ~5 min, hourly after the hour closes,
~128 MB row groups) are starting points to tune.

### Schema v1

Timestamps are `TIMESTAMP_NS` UTC; IDs are lowercase hex `VARCHAR`. Attribute
values are stringified. Arrays and maps in attributes are JSON-encoded strings.

**logs**

| Column | Type | Source |
|---|---|---|
| `time`, `observed_time` | TIMESTAMP_NS | LogRecord |
| `service_name` | VARCHAR | resource `service.name` |
| `severity_number`, `severity_text` | INT32, VARCHAR | LogRecord |
| `event_name` | VARCHAR | LogRecord `event_name` (Claude Code events) |
| `body` | VARCHAR | AnyValue; JSON-encoded if not a string |
| `trace_id`, `span_id` | VARCHAR | LogRecord |
| `scope_name`, `scope_version` | VARCHAR | InstrumentationScope |
| `resource_attrs`, `attrs` | MAP(VARCHAR, VARCHAR) | remaining attributes |

**spans**

| Column | Type | Source |
|---|---|---|
| `trace_id`, `span_id`, `parent_span_id` | VARCHAR | Span |
| `name`, `kind` | VARCHAR | Span |
| `start_time`, `end_time` | TIMESTAMP_NS | Span |
| `duration_ns` | INT64 | computed |
| `status_code`, `status_message` | VARCHAR | Span status |
| `service_name` | VARCHAR | resource `service.name` |
| `scope_name`, `scope_version` | VARCHAR | InstrumentationScope |
| `resource_attrs`, `attrs` | MAP(VARCHAR, VARCHAR) | remaining attributes |
| `events` | LIST(STRUCT(time, name, attrs MAP)) | Span events (exceptions) |
| `links` | LIST(STRUCT(trace_id, span_id, attrs MAP)) | Span links |

**Promoted keys.** A small fixed set (for example `http.request.method`,
`http.response.status_code`, `gen_ai.request.model`, `user.id`/`session.id` for
agent sessions) is to be finalised in M1 from real Claude Code and SDK samples.
Any change to the promoted set bumps the schema version, recorded in Parquet
key-value metadata and in the manifest. Readers union schema versions by column
name (`union_by_name`).

## Receiver hardening

- Bind to `127.0.0.1:4318` only. The address is configurable, but
  non-loopback addresses are refused unless `--unsafe-bind` is given.
- Reject any request with an `Origin` header. OTel SDKs don't send one; browsers do.
- Require `Host` to be `localhost:4318` or `127.0.0.1:4318` (DNS-rebinding defence).
- Require `Content-Type` to be `application/x-protobuf` or `application/json`.
  Browsers must preflight these, and preflights are never answered.
- Accept only `POST /v1/logs` and `/v1/traces`. `/v1/metrics` returns
  `200` and drops the data, counted in `doctor`, so exporters that
  also send metrics (Claude Code) don't log errors. **Open:** 200-and-drop vs 404.
- Cap the request body (default 8 MiB) and the decompressed size (gzip-bomb
  defence).
- Bound the buffer. When full, return `429` with `Retry-After`, which OTLP
  exporters honour.
- Return OTLP `partial_success` when records are dropped (for example a
  malformed batch).

## Redaction

- Runs on the write path before anything is buffered or written.
- Masks values of resource, scope, log, span and event attributes whose
  **keys** match case-insensitive globs: `*authorization*`, `*cookie*`,
  `*token*`, `*secret*`, `*password*`, `*api_key*`, `*apikey*`, `*credential*`.
  The value becomes `[REDACTED]`.
- Patterns are extendable and can be allow-listed in `config.toml`.
- **Limitation:** log bodies and free-text values are not scanned. Prompts,
  tool inputs and file contents sent by agents (for example Claude Code with
  `OTEL_LOG_USER_PROMPTS=1`) are stored as received. The docs must say this
  plainly. Value scanning is a v0.2 candidate.

## Read path

### DuckDB connection

Each `mcp`/`query` process opens an in-memory DuckDB and applies these settings,
locking configuration last:

```sql
SET autoinstall_known_extensions = false;
SET autoload_known_extensions = false;
SET allow_community_extensions = false;
SET allowed_directories = ['<data dir>/services/']; -- or ['<data dir>/services/<svc-dir>/'] when scoped
SET enable_external_access = false;
SET memory_limit = '512MB';
SET threads = 2;
SET lock_configuration = true;
```

Views over the current files in the manifests are created **before** locking.
Each query refreshes them when a manifest generation has changed.

**Scoped sessions.** `quacktel mcp --service <name>` (and `query --service`)
resolve the name to `<svc-dir>` and set `allowed_directories` to that directory
only. The restriction is enforced by DuckDB, not by the tools, so the `sql` tool
can't read other services' files with `read_parquet` either. In a scoped session,
`list_services` returns only that service, and the `service` argument is
ignored. Unscoped sessions read every service's manifest and union the files.

**Verified in M0 (DuckDB 2.0.0, duckdb-go v2.20000.0):**

- The settings are database-wide. `Open` applies them once on one connection
  (via `duckdb.NewConnector` and `sql.OpenDB`), ending with
  `lock_configuration`, and tests confirm every pooled connection sees them
  and rejects `SET`. No per-connection init or `SetMaxOpenConns(1)` is needed.
- `allowed_directories` with `enable_external_access = false` permits
  `read_parquet` inside the data dir and denies other paths, including a
  symlink inside the data dir that points outside it (`read_text` and
  `read_csv` fail with a permission error).
- The guard uses `(*duckdb.Stmt).StatementType()` and requires
  `duckdb.STATEMENT_TYPE_SELECT`. It prepares with `(*duckdb.Conn).Prepare`,
  not `PrepareContext`: `PrepareContext` runs every statement except the last,
  so `COPY ...; SELECT 1` would execute the `COPY`. `Prepare` rejects
  multi-statement input without running anything.
- `PRAGMA version` prepares as a `SELECT` (a table function), so it is allowed.
  It is read-only. Setting pragmas such as `PRAGMA threads=8` are rejected.
- `COPY ... TO` outside the allowed dir fails at prepare time with a
  permission error, before the guard's type check. The guard is still what
  stops a `COPY` into the allowed dir.
- `duckdb_extensions()` fails on a hardened connection, because it scans the
  extension directory. On an unhardened handle, `parquet`, `json`, `icu`,
  `core_functions`, `autocomplete`, `tpch` and `tpcds` are loaded and
  `STATICALLY_LINKED`; every other extension is `NOT_INSTALLED`. Nothing is
  written to `~/.duckdb`.
- `CREATE OR REPLACE VIEW` over `read_parquet` in the data dir works after
  `lock_configuration`, so no connection rebuild is needed on manifest change.

### `sql` tool / `quacktel query`

- Prepare the statement, then check its statement type is `SELECT` before
  executing. This rejects `COPY`, `ATTACH`, `INSTALL`, `SET`, `PRAGMA` and
  multi-statement input. The check uses `(*duckdb.Stmt).StatementType()`.
- A per-query timeout (default 10s) and a row cap (default 1,000) apply.
  Wide text columns are truncated in MCP output.

### MCP tools

All tools take optional `service`, `since` and `until` (`since` defaults to 1h).
Results are capped and truncated for token budget; responses say when
truncation happened.

| Tool | Inputs | Returns |
|---|---|---|
| `list_services` | `since` | Services with log and span counts and first/last seen. |
| `describe_schema` | `service?` | Columns per table, plus the top attribute keys with example values. |
| `search_logs` | `service`, `min_severity`, `text` (ILIKE), `regex`, `event_name`, `trace_id`, `attrs` (key=value), `limit` | Matching log rows, newest first. |
| `find_spans` | `service`, `name`, `min_duration_ms`, `status=error`, `attrs`, `limit` | Matching spans with duration and status. |
| `get_trace` | `trace_id` | The span tree (indented or nested) plus correlated logs. |
| `sql` | `query` | Rows from the read-only connection; see guardrails above. |

Typed tools build SQL from fixed templates. All user values are bound
parameters; column and attribute names are validated against the schema.

### `quacktel query`

Prints a table on a TTY and JSON otherwise (`--format table|json|csv`).
It uses the same connection and guardrails as `sql`.

## Background service

- **macOS:** `service install` writes a per-user LaunchAgent at
  `~/Library/LaunchAgents/dev.quacktel.serve.plist` (never a root
  LaunchDaemon) and loads it with `launchctl bootstrap gui/$UID`. The plist
  records the absolute path of the running binary (e.g. `~/go/bin/quacktel`),
  so a source install works. After rebuilding, run `service restart`.
  Once released, Homebrew users can use `brew services` instead.
- **Linux (M7):** `service install` writes a `systemd --user` unit to
  `~/.config/systemd/user/quacktel.service` and enables it. The .deb/.rpm
  packages ship the unit in `/usr/lib/systemd/user/` but don't enable it.
- `service status` and `doctor` check: the service is loaded, the port is bound
  by quacktel, data-dir permissions, disk use against the cap, manifest
  health, and dropped, 429 and metrics-drop counters (from a small `stats.json`
  written by the daemon).

## Onboarding

`quacktel env` prints:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_LOGS_EXPORTER=otlp
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
```

`quacktel env --claude-code` adds `CLAUDE_CODE_ENABLE_TELEMETRY=1`.
`quacktel env --mcp` prints the MCP server JSON snippet
(`{"command": "quacktel", "args": ["mcp"]}`). The docs cover Claude Code and
common SDKs. Claude Code's cost and token data arrives as log events
(`event_name`), so v0.1 covers agent self-analysis without metrics.
(Verify the current event names and attributes against the Claude Code
monitoring docs in M1.)

## Distribution

**Until 1.0 (personal use):** build and install from source with
`go install ./cmd/quacktel`, wrapped as a mise task. The only requirement is the
Xcode command-line tools for CGO: `duckdb-go` ships prebuilt static DuckDB
libraries, so DuckDB isn't compiled locally. CI lints and tests on a macOS
arm64 runner and doesn't publish anything. Everything below is the pre-1.0
release plan (M6/M7).

DuckDB needs CGO, so every target is built natively. There is no cross-compiling.

| Target | Runner | Notes |
|---|---|---|
| darwin/arm64 | macOS arm64 runner | Ad-hoc signed by the linker (macOS arm64 requires a signature). Not notarised in v0.1; see below. |
| linux/amd64, linux/arm64 (M7) | Native Linux runners (e.g. `ubuntu-24.04-arm`) | Built on the oldest supported glibc base image so it runs on older distros. |

- **Homebrew:** a tap at `quacktel/homebrew-tap` (macOS). The formula includes a
  `service` block so `brew services` works.
- **Apple notarisation is deferred.** It needs a paid Apple Developer Program
  membership. v0.1 relies on the fact that Gatekeeper only checks files carrying
  the `com.apple.quarantine` attribute, which browsers set but Homebrew formula
  installs and `curl` in `install.sh` do not (verify in M6). Integrity and
  provenance come from checksums and GitHub attestations instead. A binary
  downloaded through a browser needs `xattr -d com.apple.quarantine`, which the
  docs cover. If you join the programme later, goreleaser can add signing and
  notarisation (via quill) without changing anything else.
- **Linux packages (M7):** .deb and .rpm via goreleaser `nfpms`, shipping the
  systemd user unit.
- **`install.sh` (in the repo):** detects OS and architecture, downloads the
  release archive, and verifies it before installing:
  1. If `gh` is present, run
     `gh attestation verify <archive> --repo quacktel/cli` (build provenance from
     `actions/attest-build-provenance`). If the release is immutable,
     `gh release verify-asset` also applies.
  2. Otherwise, fall back to checking the SHA-256 against `checksums.txt`, and
     print that provenance was not verified (`--require-attestation` makes this
     fatal).
  3. Install to `~/.local/bin` by default (no sudo); `--prefix` overrides.
- **Supply chain:** pinned DuckDB and `duckdb-go` versions, an SBOM per artifact
  (goreleaser `sboms`), build-provenance attestations, immutable releases, and
  pinned tool versions in `mise.toml` and CI.

## Code layout (proposed)

```
cmd/quacktel/            # Kong CLI, following parquet-arrow-search
internal/receiver/       # HTTP handler, gates, OTLP decode
internal/redact/
internal/arrowconv/      # OTLP → Arrow builders (logs, spans)
internal/store/          # flush, manifest, compaction, retention, paths
internal/engine/         # DuckDB open + hardening, view refresh, SELECT guard
internal/tools/          # typed tool → SQL templates (shared by mcp + query)
internal/mcpserver/      # stdio MCP
internal/service/        # launchd / systemd user unit
internal/doctor/
```

Reused from parquet-arrow-search: Kong CLI conventions, Arrow batch handling,
schema-validation ideas (`internal/schema`), and test-data patterns.
Not reused: Substrait, Participle SQL, and the custom executor.

## Milestones

| | Scope | Exit criteria |
|---|---|---|
| M0 | Repo scaffold, Kong CLI skeleton, CI (lint + test on macOS arm64), DuckDB static-link spike | `go install ./cmd/quacktel` on a Mac gives a binary that runs `SELECT 42` through DuckDB, with no runtime extension downloads; hardening settings verified by tests. |
| M1 | Receiver + redaction + logs writer + per-service layout + manifest | Claude Code logs land in Parquet; hostile `service.name` values (traversal, unicode, overflow past the cap) are handled; the hardening tests pass (Origin, Host, Content-Type, caps, 429). |
| M2 | Spans writer | A Go/Node sample app's traces land and can be queried. |
| M3 | Compaction + retention + size cap | A 24h soak shows a bounded file count, no duplicate rows, and the cap enforced. |
| M4 | `engine`, `tools`, `mcp`, `query` | All six tools work against fixtures; the SELECT guard rejects the bypass corpus; a scoped session can't read another service's files through `sql`/`read_parquet`. |
| M5 | `service` (launchd), `doctor`, `env` | Install, reboot, data still flowing, on macOS. |
| M6 | Pre-1.0 release: goreleaser, GitHub attestations, SBOM, Homebrew tap, `install.sh` | A release binary, installed via the tap formula and via `install.sh` on a clean Mac, runs with no Gatekeeper prompt and passes `gh attestation verify`. Decide on notarisation. |
| M7 | Linux amd64/arm64: XDG paths, systemd user unit, .deb/.rpm, `install.sh` Linux support | Native builds on the oldest supported glibc; install, reboot, data still flowing, on Ubuntu and Fedora. |

## Risks and open questions

- **Cross-service visibility:** an unscoped `mcp` session can read every
  project's telemetry, including prompts. This is the accepted default for v0.1.
  Use `quacktel mcp --service <name>` for untrusted repos or client separation.
  Scoping is a real boundary only for the MCP path: an agent with unrestricted
  shell or file access can read the data directory directly, because `0700`
  only protects against other OS users.
- **Telemetry poisoning:** `service.name` is self-reported. Any local process
  can write records labelled as another service, including text crafted to act
  as instructions to an agent. Tool output labels log and span content as
  untrusted data, and the docs warn about it. Sender authentication (for
  example a per-service token) is a v0.2 candidate.
- **Body content isn't redacted:** agent prompts and tool I/O are stored as
  received. Mitigations are clear docs, `0700` permissions and retention.
  Value scanning is a v0.2 candidate.
- **DuckDB hardening semantics vary by version.** They are verified in M0 and
  covered by tests that must fail if a setting stops taking effect.
- **Release problems surface late:** no release pipeline until M6. This is low risk with a single target built natively; M0 still proves the static DuckDB link.
- **CGO release cost:** one native build in M6. M7 adds two
  Linux builds and the choice of oldest supported glibc. M0 de-risks the macOS
  path before any feature work.
- **Binary size** is 79 MB unstripped and 62 MB with `-ldflags='-s -w'` (darwin/arm64, measured in M0); acceptable.
- **Stringified attribute values** lose types (numeric comparisons need a
  `CAST`). Typed tools hide this; the `sql` tool docs must mention it.
- **`/v1/metrics` behaviour:** 200-and-drop vs 404 is still open.
- **The promoted-key set** is finalised in M1 from real samples.
- **No notarisation:** browser downloads trigger a Gatekeeper block, and
  there's no Apple-backed identity on the binary. Mitigations: the Homebrew
  and `install.sh` paths, plus GitHub attestations. Revisit if users hit this
  often.
