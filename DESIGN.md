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
| Transport | OTLP/HTTP only: protobuf and JSON, gzip accepted, both decoded with `go.opentelemetry.io/collector/pdata` v1.71.0 | One endpoint to harden. Users set `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`. |
| Engine | DuckDB via `duckdb-go` v2.20000.0 (DuckDB 2.0.0 LTS), statically linked, used only on the read path | Full SQL with no runtime extensions. |
| Writer | arrow-go `pqarrow` | Reuses the parquet-arrow-search pattern and keeps the daemon free of DuckDB. |
| Write path | Tail → seal → compact, with a manifest | ≤ ~1–5s staleness without DuckDB lock contention. |
| Attributes | Promoted columns plus `MAP(VARCHAR, VARCHAR)` | Fast filters on hot keys; stable schema. |
| Query path | Typed MCP tools → prepared SQL with bound parameters, plus a read-only `sql` tool | No injection surface in typed tools. |
| Scope | One store per user, physically partitioned by service. `mcp` sees all services by default; `mcp --service` is enforced by DuckDB's `allowed_directories` | Allows cross-service queries by default; scoping later needs no data migration. |
| Redaction | Key-pattern masking, on by default; extra patterns come with `config.toml` (deferred) | Cheap and predictable. |
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
   manifests → logs/spans views over read_parquet([files]) in a locked-down DuckDB
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
        hour-<ulid>.parquet           # final merge once the hour is closed
      spans/date=…/hour=…/…
  .tmp/                               # in-progress writes; never read
  .lock                               # flock held by the running serve
  stats.json                          # counters for doctor, rewritten atomically
```

**Partitions.** `date=`/`hour=` use the **receive time** (UTC), never the record
time, so skewed or hostile timestamps can't write into old partitions. The
manifest keeps each file's min/max record time for readers.

**One serve per data dir.** `serve` takes an exclusive flock on `.lock` and
refuses to start if it is held. On startup it deletes retired files, adds
unlisted `tail-*` files (a flush that crashed between rename and publish), and
deletes unlisted `sealed-*`/`hour-*` files: those are compactions that never
published, whose sources are still listed, so keeping them would duplicate rows.

**Service directories.** `service.name` is untrusted input, so it is never used
as a path as-is:

- `<svc-dir>` is a slug of the name (lowercase `[a-z0-9._-]`, max 64 chars) plus
  an 8-char hash of the full name, for example `checkout-api-1a2b3c4d`. This
  blocks path traversal and avoids collisions between names that slug the same.
- The original name is kept in the manifest and in the `service_name` column.
- A missing `service.name` maps to `unknown_service`.
- Services are capped (default 256) per process run, counting only services
  that have received records since startup. Past the cap, records are rejected
  via OTLP `partial_success`, which bounds memory. Directories already on disk
  don't count until they receive data. On-disk service directories are bounded
  by M3 retention, which must remove expired service directories.
- Each service is flushed and compacted independently, so many quiet services
  produce more small files. Flushes only happen when there is data, and
  compaction runs per service.

### Write, seal, compact

`Run` calls `Maintain` every `--flush-interval`. One pass runs, in order:

1. **Flush.** Each non-empty builder is swapped into the service's pending
   queue under the store lock. The lock is then released, and each pending batch
   is written to `.tmp/x.parquet`, fsynced and renamed into the partition of its
   **receive** time as `tail-<ulid>.parquet`. The lock is retaken to publish the
   service's new `manifest.json` (written to `.tmp`, renamed over the old one).
   Requests never wait on disk.
2. **Retry.** A batch whose write fails stays pending and is retried next pass.
   Files written but not published (a failed publish) stay in `unpublished` and
   are listed by the next publish. Pending bytes count toward the buffer cap, so
   a failing disk ends in `429`, and `write_errors` counts the failures.
3. **Compaction**, every `--compact-interval` (default 5m):
   - **Open hour:** one merge per partition per pass. Once a partition holds
     `--seal-min-files` sealed files (default 10), every file in it (sealed and
     tails) is merged into one new `sealed-<ulid>.parquet`. Otherwise, once it
     holds that many tails, the tails are merged into one `sealed-<ulid>.parquet`.
     The sealed check comes first so steady traffic can't starve it. After a
     pass the open hour holds at most `--seal-min-files` sealed files, plus the
     tails flushed since.
   - **Closed hour:** once `hourEnd + 2×FlushInterval` has passed, every file in
     the partition is merged into one `hour-<ulid>.parquet`, so late tails are
     included.
   - A merge streams the sources through a `pqarrow` writer with `WriteBuffered`
     and 128Ki-row row groups, so memory stays at a batch plus one row group
     (tens of MB for typical rows). It runs outside the store lock.
     The manifest is published with the merged file replacing its sources.
     Sources whose schema metadata differs are skipped with a warning.
4. **Retention**, in the same housekeeping step as compaction; see below.
5. **Cleanup.** Files a manifest stops listing move to its `Retired` list with a
   delete-after time (`--retire-grace`, default 60s) so in-flight queries can
   finish. Cleanup unlinks the due ones, prunes empty hour and date
   directories, republishes the manifest, and removes service directories with
   nothing left and no records since startup.

**Shutdown:** on SIGTERM/SIGINT the receiver drains, the buffer is flushed and
the manifest published, then the process exits.

**Startup:** `serve` deletes retired files and any unlisted `sealed-*`/`hour-*`
files (see Storage above), and reconciles only unlisted `tail-*` files into the
manifest.

### Retention and size cap

Housekeeping retires files whose hour partition ended more than `--retention`
ago (default 720h). It then enforces `--max-disk` (default 10 GiB): while the
listed bytes exceed the cap, the oldest whole (service, signal, hour) partition
goes first. The open hour is never deleted. If only the open hour is left over
the cap, the store refuses new writes (`429`, and `stats.json` shows
`disk_cap_exceeded`) until the hour closes and retention frees space. Retired files follow the same grace
period as compaction. A service with no files and no activity since startup has
its directory removed.

### Schema v1

Timestamps are tz-less `TIMESTAMP_NS`, UTC by convention, because duckdb-go
v2.20000.0 can't scan `TIMESTAMPTZ_NS`; IDs are lowercase hex `VARCHAR`. Attribute
values are stringified. Arrays and maps in attributes are JSON-encoded strings.

**logs**

| Column | Type | Source |
|---|---|---|
| `time`, `observed_time` | TIMESTAMP_NS | LogRecord |
| `service_name` | VARCHAR | resource `service.name` |
| `severity_number`, `severity_text` | INT32, VARCHAR | LogRecord |
| `event_name` | VARCHAR | LogRecord event name, falling back to attribute `event.name` |
| `session_id` | VARCHAR | attribute `session.id` |
| `body` | VARCHAR | AnyValue; JSON-encoded if not a string |
| `trace_id`, `span_id` | VARCHAR | LogRecord |
| `scope_name`, `scope_version` | VARCHAR | InstrumentationScope |
| `resource_attrs`, `attrs` | MAP(VARCHAR, VARCHAR) | all attributes (promoted keys are copied, not removed) |

**spans**

| Column | Type | Source |
|---|---|---|
| `trace_id`, `span_id` | VARCHAR | Span |
| `parent_span_id` | VARCHAR | Span; null for a root span |
| `trace_state`, `name` | VARCHAR | Span |
| `kind` | VARCHAR | Span kind, lowercase: `unspecified`, `internal`, `server`, `client`, `producer`, `consumer` |
| `start_time`, `end_time` | TIMESTAMP_NS | Span |
| `duration_ns` | INT64 | `end - start`; null when either time is zero or the span ends before it starts |
| `status_code` | VARCHAR | Span status, lowercase: `unset`, `ok`, `error` |
| `status_message` | VARCHAR | Span status |
| `service_name` | VARCHAR | resource `service.name` |
| `scope_name`, `scope_version` | VARCHAR | InstrumentationScope |
| `session_id` | VARCHAR | attribute `session.id` |
| `http_request_method`, `http_route` | VARCHAR | attributes `http.request.method`, `http.route` |
| `http_response_status_code` | INT64 | attribute `http.response.status_code` (a numeric string is accepted) |
| `gen_ai_request_model` | VARCHAR | attribute `gen_ai.request.model` |
| `gen_ai_usage_input_tokens`, `gen_ai_usage_output_tokens` | INT64 | attributes `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens` |
| `resource_attrs`, `attrs` | MAP(VARCHAR, VARCHAR) | all attributes (promoted keys are copied, not removed) |
| `events` | LIST(STRUCT(time, name, attrs MAP)) | Span events (exceptions) |
| `links` | LIST(STRUCT(trace_id, span_id, trace_state, attrs MAP)) | Span links |

**Promoted keys.** Logs promote `event_name` and `session_id`, copied from the
record and left in `attrs`. Spans promote `session_id`, the HTTP keys and the `gen_ai` keys listed above, also copied. Any change to the promoted set bumps the schema version, recorded in Parquet
key-value metadata and in the manifest. Readers union schema versions by column
name (`union_by_name`).

### `serve` flags

| Flag | Default | Meaning |
|---|---|---|
| `--flush-interval` | `2s` | Maintenance pass interval; buffered records are written each pass. |
| `--retention` | `720h` | Delete whole hour partitions older than this. |
| `--max-disk` | `10737418240` | Parquet bytes cap; oldest closed hours are deleted first. |
| `--compact-interval` | `5m` | How often compaction and retention run. |
| `--seal-min-files` | `10` | Open-hour tails that trigger a seal. At least 2. |
| `--retire-grace` | `60s` | How long retired files stay on disk for running queries. |

`serve` rejects a non-positive value, or `--seal-min-files` below 2, before it
opens the store or listens.

`stats.json` also carries `compactions`, `files_retired`, `bytes_on_disk`,
`pending_batches`, `write_errors` and `disk_cap_exceeded`. It is rewritten only when a counter
changes (never for `updated_at` alone), once at startup and once at shutdown.

## Receiver hardening

- Bind to `127.0.0.1:4318` only. The address is configurable, but
  non-loopback addresses are refused unless `--unsafe-bind` is given.
- Reject any request with an `Origin` header. OTel SDKs don't send one; browsers do.
- Require `Host` to be `localhost:4318` or `127.0.0.1:4318` (DNS-rebinding defence).
- Optionally require `Authorization: Bearer <token>` (`--auth`, implied by
  `--unsafe-bind`), compared in constant time. The token lives in
  `<data-dir>/otlp-headers` in the OpenShell headers-file format
  (`authorization=Bearer <64 hex>`), created `0600` and reused across restarts.
  A malformed file or a mode wider than `0600` fails startup. A missing or wrong
  header returns `401` before the body is read. Gate order: method and path,
  `Origin`, `Host`, `Authorization`, `Content-Type`, in-flight slot, body.
- Require `Content-Type` to be `application/x-protobuf` or `application/json`.
  Browsers must preflight these, and preflights are never answered.
- Accept only `POST /v1/logs` and `POST /v1/traces`. `/v1/metrics` returns `200` and drops the data, counted in `doctor`, so
  exporters that also send metrics (Claude Code) don't log errors.
- With `--unsafe-bind` on a non-loopback address, the bound `host:port` is also
  an allowed `Host`, since remote clients send it. No other gate changes.
- Cap the request body (default 8 MiB) and the decompressed size (gzip-bomb
  defence).
- Cap in-flight requests (default 16, `--max-inflight`). Excess requests get
  `429` with `Retry-After` before their body is read, so buffered bodies stay bounded.
- Bound the buffer. When full, return `429` with `Retry-After`, which OTLP
  exporters honour.
- Return OTLP `partial_success` when records are dropped (for example a
  malformed batch).

## Redaction

- Runs on the write path before anything is buffered or written.
- Masks values of resource, scope, log, span and event attributes whose
  **keys** match case-insensitive globs: `*authorization*`, `*cookie*`,
  `*token`, `*token.*`, `*_token_*`, `*bearer*`, `*secret*`, `*password*`,
  `*api_key*`, `*apikey*`, `*credential*`. The value becomes `[REDACTED]`.
  The token globs match credential-shaped keys (`access_token`, `x-auth-token`)
  but not usage counts such as Claude Code's `input_tokens` or `max_tokens`.
- Keys are case-folded and `-` is folded to `_`, so `X-API-KEY` matches `*api_key*`.
- `config.toml` is deferred. M1 uses flags plus the built-in globs.
- **Limitation:** log bodies and free-text values are not scanned. Prompts,
  tool inputs and file contents sent by agents (for example Claude Code with
  `OTEL_LOG_USER_PROMPTS=1`) are stored as received. Claude Code also sends
  `user.email` on events, and it isn't masked. The docs must say this
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

The `logs` and `spans` views list exactly the files in the manifests and are
created **before** locking. Each query refreshes them when a manifest has
changed. A signal with no files gets a typed empty view, so queries against it
still work.

**Globbing Parquet is unsupported.** `read_parquet('…/*.parquet')` sees files a
manifest has retired or not yet published, and compaction makes it return
duplicate rows. Query `FROM logs` or `FROM spans`.

**Scoped sessions.** `quacktel mcp --service <name>` (and `query --service`)
resolve the name to `<svc-dir>` and set `allowed_directories` to that directory
only. The restriction is enforced by DuckDB, not by the tools, so the `sql` tool
can't read other services' files with `read_parquet` either. In a scoped session,
`list_services` returns only that service, and the `service` argument is
ignored. Unscoped sessions read every service's manifest and union the files, and
their `allowed_directories` is `<data dir>/services/` only. `otlp-headers`,
`stats.json`, `.lock` and the manifests are outside DuckDB's reach (the engine
reads manifests with Go, and `Open` rejects an unscoped `AllowedDir` outside
`<data dir>/services`).

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
- A per-query timeout (default 10s) applies. `query` caps rows at 1,000 by
  default; the `sql` tool caps them at 500 (see below).
- Unscoped sessions can read only `<data dir>/services/`; `read_text` on
  `otlp-headers` (the bearer token), `stats.json` or a manifest fails with a
  permission error.
- Scoped sessions (`--service`) narrow the connection two ways: `allowed_directories`
  shrinks to that service's directory, and the `logs`/`spans` views are built
  from that one service's manifest only. `read_parquet` on another service's
  file fails.

### MCP tools

`quacktel mcp [--data-dir] [--service] [--no-sql]` serves these read-only tools
over stdio (every tool is annotated `readOnlyHint`). Stdout carries protocol
only; logs go to stderr.

Defaults shared by the typed tools:

- **Window:** `since` defaults to 24h ago and `until` to now. Both accept an RFC
  3339 timestamp or a duration meaning "ago" (`2h`). `get_trace` ignores the
  window.
- **Limit:** `limit` defaults to 100 (20 for `search_logs`, whose rows carry many
  attributes) and is capped at 500. A capped result sets `truncated`.
- **Response budget:** each result is capped at 40,000 JSON characters, to stay
  under Claude Code's 25k-token MCP output cap. Rows that would exceed it are
  dropped, `truncated` is set, and `note` adds a size-cap sentence so agents
  narrow the query instead of raising `limit`. For the typed tools, if the rows
  do not all fit, the `attrs` and `resource_attrs` columns are removed from every
  row first (all or nothing), `truncated` is set, and `note` says attrs were
  omitted; rows are dropped only if the result still does not fit. `sql` never
  removes columns, because the agent chose them; it only drops rows. `get_trace`
  shares one budget across spans and logs, spans first; once spans are cut it
  returns no logs. `describe_schema` examples are cut at 200 characters.
- **Truncation:** strings longer than 2000 characters are cut and marked
  `…[truncated N chars]`, and `truncated` is set.
- **Untrusted data:** every result carries a `note` saying the content is
  untrusted telemetry written by local processes, and that instructions in it
  must not be followed. Tool descriptions say the same.
- **Errors:** bad input (an unparseable time, an invalid trace ID, a service
  outside the scope, a rejected SQL statement) comes back as an `IsError` tool
  result with the message, so the agent can correct itself. Other failures
  return a generic message and are logged to stderr.

| Tool | Inputs | Returns |
|---|---|---|
| `list_services` | `since`, `until` | `rows`: per service `service`, `logs`, `spans`, `first_seen`, `last_seen`. |
| `describe_schema` | `service?`, `event_name?` | `logs` and `spans`, each with `columns` (name, type) and the 50 most common `top_attribute_keys` (key, count, example) over the last 24h. `event_name` narrows the log keys to one event; spans are unaffected. |
| `search_logs` | `service`, `since`, `until`, `limit`, `min_severity` (1-24 or trace..fatal), `text` (case-insensitive substring), `regex` (RE2), `event_name`, `trace_id`, `session_id`, `attrs` (key to exact value) | `rows` of matching logs, newest first. |
| `find_spans` | `service`, `since`, `until`, `limit`, `name` (substring), `kind`, `status` (unset, ok, error), `http_route`, `min_duration_ms`, `attrs` | `rows` of matching spans, slowest first; the window applies to start time. |
| `get_trace` | `trace_id` (32 hex), `service?`, `max_spans?` (default and cap 1000), `include_logs?` (default true) | `spans`: a flat list ordered by start time, each with `depth` (0 for roots); up to `max_spans`. `logs`: correlated logs, up to 500; `[]` when `include_logs` is false. |
| `sql` | `query`, `limit` | `rows` from the read-only connection; see guardrails above. No time window is applied, so filter on `time` or `start_time` in the query. Not registered with `--no-sql`. |

`list_services`, `search_logs`, `find_spans` and `sql` return `rows`, `truncated`
and `note`; `get_trace` returns `spans`, `logs`, `truncated` and `note`.

Typed tools build SQL from fixed templates. Every user value, including
attribute keys, is a bound parameter; nothing user-supplied is interpolated
into SQL. The `sql` tool is the only path that runs caller-written SQL, and it
passes the SELECT guard first.

### `quacktel query`

Prints a table on a TTY and JSON otherwise (`--format table|json|csv`).
It uses the same connection and guardrails as `sql`.

## Background service

- **macOS:** `service install|uninstall|restart|status` manage a per-user
  LaunchAgent at `~/Library/LaunchAgents/dev.quacktel.serve.plist` (never a root
  LaunchDaemon; `--label` is hidden and used by tests). The plist is rendered
  through `encoding/xml` and written atomically with mode 0644. Its keys:
  - `Label`
  - `ProgramArguments`: `[<bin>, "--log-file", "<home>/Library/Logs/quacktel/serve.log", "serve", "--auth", <passthrough...>]`,
    with the absolute path of the running binary (e.g. `~/go/bin/quacktel`)
  - `RunAtLoad` is `<true/>`
  - `KeepAlive` is `{SuccessfulExit: <false/>}`, so a crash restarts but a clean
    exit doesn't
  - `ThrottleInterval` 30
  - `ProcessType` `Background`
  - `StandardOutPath` and `StandardErrorPath`: `<home>/Library/Logs/quacktel/launchd.log`,
    the launchd crash log
- launchd needs self-closing `<true/>` and `<false/>`. `<true></true>` makes
  `bootstrap` fail with "Bootstrap failed: 5", and `plutil` doesn't catch it, so
  `Render` rewrites them.
- **Domain:** the agent loads into `gui/<uid>`.
  - `install` writes the plist, runs `bootout` (ignoring failure, so a stale
    loaded copy doesn't block it), waits until `launchctl print` reports the job
    gone (polling every 100ms, up to 10s), then runs `bootstrap`. launchd fails
    `bootstrap` with status 5 while it is still tearing a running job down, so
    `bootstrap` is also retried up to 5 times with backoff, only on status 5.
    Running it again upgrades the agent in place.
  - `install` refuses a binary under the OS temp dir or a `go-build` path (for
    example from `go run`), because the agent would crash-loop once it is
    deleted. The hidden `--allow-temp-binary` flag is for tests.
  - `restart` runs `kickstart -k`, which is needed after rebuilding the binary.
  - `uninstall` runs `bootout`, waits for the job to be gone like `install`, and
    removes the plist.
  - `status` parses `launchctl print`.
- **Passthrough:** flags after `--` go to `serve`. They are checked up front by
  parsing them into `serveCmd`, running `validate()` and `receiver.CheckAddr`,
  so a typo fails at install, not at the first launchd restart. The agent always
  runs with `--auth`, because it has no terminal to prompt on. A relative
  `--data-dir` is recorded as an absolute path, because launchd runs with cwd `/`.
- **Logs:** `--log-file` writes `serve.log` rotated at 10 MiB, keeping 3 old
  files. launchd's own capture (`launchd.log`) holds crashes that happen before
  the logger starts. If rotation can't reopen the file, writes fall back to
  stderr (so into `launchd.log`) and the next write retries the file.
- **Linux (M7):** `service install` writes a `systemd --user` unit to
  `~/.config/systemd/user/quacktel.service` and enables it. The .deb/.rpm
  packages ship the unit in `/usr/lib/systemd/user/` but don't enable it.

### Doctor

`quacktel doctor [--data-dir] [--listen] [--max-disk] [--json]` prints a
`STATUS CHECK DETAIL` table (or a JSON array of `{name, status, detail}`) and
exits 1 if any check fails. Without flags it uses the installed agent's
recorded `--data-dir`, `--listen` and `--max-disk`; flags given explicitly win.
It never mutates state; the flock probe releases immediately. Checks, in order:

1. `data_dir`: exists with mode 0700.
2. `auth_headers`: `otlp-headers`, if present, is 0600 and well formed; absent
   means auth is off.
3. `serve_lock`: the flock on `.lock` is held, so a `serve` owns the data dir.
4. `receiver`: `GET /v1/logs` returns 405 with `Allow: POST`.
5. `service`: the plist's binary exists and matches the running one (warn if
   not), and launchd reports the agent loaded and running.
6. `stats`: `write_errors` or `disk_cap_exceeded` fail; `pending_batches` or
   `responses_429` warn. The counters reset when `serve` restarts.
7. `disk`: warns at 80% of `--max-disk`.
8. `manifests`: every manifest parses and lists only existing files; unlisted
   Parquet files warn.
9. `read_path`: the engine opens and counts rows in `logs` and `spans`.

## Onboarding

`quacktel env [--listen] [--claude-code] [--auth] [--data-dir] [--json | --mcp] [--service]`
prints shell exports to stdout, one per line. The endpoint comes from `--listen`
(default `127.0.0.1:4318`):

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_LOGS_EXPORTER=otlp
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
```

- `--claude-code` adds `export CLAUDE_CODE_ENABLE_TELEMETRY=1`.
- `--auth` adds `export OTEL_EXPORTER_OTLP_HEADERS='authorization=Bearer <token>'`,
  read from `<data-dir>/otlp-headers` (so it prints the token, on request). Like
  `doctor`, it uses the installed agent's recorded `--data-dir` unless
  `--data-dir` is given. It fails if the file doesn't exist; run
  `quacktel service install` (or `quacktel serve --auth`) first.
- `--json` prints the same variables as `{"env": {...}}`, the shape of
  `~/.claude/settings.json`, with the header value unquoted. It can't be
  combined with `--mcp`.
- `--mcp` prints only an MCP server config, pretty-printed, with the absolute
  path of the running binary (symlinks resolved). `--service X` adds
  `"--service", "X"` to the args:

  ```json
  {"mcpServers": {"quacktel": {"command": "/abs/path/quacktel", "args": ["mcp"]}}}
  ```

Onboarding on macOS is four commands:

```sh
quacktel service install
eval "$(quacktel env --claude-code --auth)"
claude mcp add quacktel -- "$(command -v quacktel)" mcp
quacktel doctor
```

Run `quacktel serve` once before `quacktel mcp`, because `serve` creates the data
dir and `mcp` exits if it is missing. `mcp` logs at Warn by default (`--debug`
enables Debug). If the client reports the MCP server as disconnected, the reason
is on the server's stderr, which Claude Code shows in its MCP logs or with
`claude --debug`.

Register the server with Claude Code with:

```sh
claude mcp add quacktel -- "$(command -v quacktel)" mcp
```

or paste the `env --mcp` JSON into the client's MCP config. The docs cover
Claude Code and common SDKs. Claude Code's cost and token data arrives as log
events (`event_name`), so v0.1 covers agent self-analysis without metrics.
(Verify the current event names and attributes against the Claude Code
monitoring docs in M1.)

### Example app

`examples/otel-go-app` is a separate Go module that sends one synthetic trace
(a server, a client `gen_ai` and an internal error span):

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 go run -C examples/otel-go-app .
quacktel query "SELECT name, kind, duration_ns FROM spans ORDER BY start_time"
```

### OpenShell

The OpenShell gateway runs on the same host and exports sandbox network events
(OCSF) as OTLP/HTTP protobuf logs over loopback. Start `quacktel serve --auth`
and add this to the gateway's `gateway.toml`:

```toml
[openshell.gateway.otlp.ocsf]
enabled = true
endpoint = "http://127.0.0.1:4318"
protocol = "http/protobuf"
headers_file = "<data-dir>/otlp-headers"
```

- The gateway reads the headers file only at startup, so restart it after the
  file is first created. quacktel reuses the same token across restarts.
- The gateway retries batches and quacktel doesn't dedupe on write, so
  duplicates are possible. Deduplicate in queries with `ocsf.metadata.uid`.
- OCSF bodies include command lines and process ancestry, and are **not** redacted.
- Gap records (`event_name = 'openshell.ocsf.gap'`) report dropped events.

```sql
SELECT json_extract_string(body, '$.action') AS action,
       json_extract_string(body, '$.dst_endpoint.hostname') AS host,
       attrs['openshell.sandbox.id'] AS sandbox
FROM logs
WHERE attrs['ocsf.class_uid'] = '4001';
```

`quacktel env --openshell`, which would print this `gateway.toml` snippet, remains future work.

## Distribution

**Until 1.0 (personal use):** build and install from source with
`go install ./cmd/quacktel`, wrapped as a mise task. The only requirement is the
Xcode command-line tools for CGO: `duckdb-go` ships prebuilt static DuckDB
libraries, so DuckDB isn't compiled locally. CI lints and tests on a macOS
arm64 runner, runs a goreleaser snapshot build (`goreleaser build --snapshot
--single-target`) natively on macOS arm64, Linux amd64 and Linux arm64 runners
to prove the CGO link early, and doesn't publish anything. Everything below is
the pre-1.0 release plan (M6/M7).

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
| M2 (done) | Spans writer | A Go/Node sample app's traces land and can be queried. |
| M3 (done) | Compaction + retention (including removal of expired service directories) + size cap | A 24h soak shows a bounded file count, no duplicate rows, and the cap enforced. |
| M4 (done) | `engine`, `tools`, `mcp`, `query` | All six tools work against fixtures; the SELECT guard rejects the bypass corpus; a scoped session can't read another service's files through `sql`/`read_parquet`. |
| M5 (done) | `service` (launchd), `doctor` | Install, reboot, data still flowing, on macOS. |
| M6 | Pre-1.0 release: goreleaser, GitHub attestations, SBOM, Homebrew tap, `install.sh` | A release binary, installed via the tap formula and via `install.sh` on a clean Mac, runs with no Gatekeeper prompt and passes `gh attestation verify`. Decide on notarisation. |
| M7 | Linux amd64/arm64: XDG paths, systemd user unit, .deb/.rpm, `install.sh` Linux support | Native builds on the oldest supported glibc; install, reboot, data still flowing, on Ubuntu and Fedora. |

### Manual soak

M3 exit check, run by hand: run `quacktel serve` for 24h with Claude Code
telemetry pointed at it, then check:

- Counters in `stats.json` reset when `serve` restarts, so read it before
  restarting.
- `stats.json`: `compactions` and `files_retired` are growing, `bytes_on_disk`
  is under the cap, and `write_errors` is 0.
- Each closed hour holds exactly one file per (service, signal).
- `SELECT count(*) FROM logs` is the same before and after a `serve` restart.

### M5 exit check

Run by hand on macOS: `quacktel service install`, reboot, then confirm
`quacktel doctor` passes and `quacktel query "SELECT count(*) FROM logs"` grows
after using Claude Code.

## Risks and open questions

- **launchd domain:** the agent loads into `gui/<uid>`, which needs a login
  session. A headless `user/<uid>` mode is future work.
- **`brew services`** arrives in M6; until then `service install` is the only
  way to run at login.
- **Doctor counters reset on restart:** `stats` and `disk` reflect only the
  current `serve` process.

- **Cross-service visibility:** an unscoped `mcp` session can read every
  project's telemetry, including prompts. This is the accepted default for v0.1.
  Use `quacktel mcp --service <name>` for untrusted repos or client separation.
  Scoping (`allowed_directories` plus per-service views) is a real boundary
  only for the MCP path: an agent with unrestricted shell or file access can
  read the data directory directly, because `0700` only protects against other
  OS users.
- **Telemetry poisoning:** `service.name` is self-reported. Any local process
  can write records labelled as another service, including text crafted to act
  as instructions to an agent. Tool output labels log and span content as
  untrusted data, and the docs warn about it. Sender authentication (for
  example a per-service token) is a v0.2 candidate.
- **No dedup on write:** a gateway retry (OpenShell) stores duplicate rows.
  Deduplicate in queries, for example on `ocsf.metadata.uid`.
- **Body content isn't redacted:** agent prompts and tool I/O are stored as
  received, so agents using the MCP tools see unredacted bodies. Mitigations
  are clear docs, `0700` permissions, retention and `mcp --service`. Value
  scanning is a v0.2 candidate.
- **`text` filters treat `%` and `_` as wildcards:** `search_logs` `text` and
  `find_spans` `name` use `ILIKE`, so those characters match more than
  themselves. This widens a match but can't reach other data. Escaping is
  future work.
- **`env --auth` prints the bearer token** to stdout, so avoid pasting its
  output into logs or shared terminals.
- **Tool output can still be large:** strings are cut at 2000 characters and
  rows at 500, but one result can carry many long fields; agents should narrow
  the window rather than raise limits.
- **DuckDB hardening semantics vary by version.** They are verified in M0 and
  covered by tests that must fail if a setting stops taking effect.
- **Release problems surface late:** no release pipeline until M6. This is low risk with a single target built natively; M0 still proves the static DuckDB link.
- **CGO release cost:** one native build in M6. M7 adds two
  Linux builds and the choice of oldest supported glibc. M0 de-risks the macOS
  path before any feature work.
- **Binary size** is 121 MB unstripped and 90 MB with `-ldflags='-s -w'` (darwin/arm64, measured in M1; M0 was 79 MB and 62 MB); acceptable.
- **The open hour holds at most `SealMinFiles` sealed files plus the tails
  flushed since the last pass** per (service, signal). With defaults that is
  ≤ 10 sealed plus at most `CompactInterval/FlushInterval` (150) tails just
  before a pass. Queries over it open more files than a closed hour.
- **A merge reads and rewrites a whole closed hour**, so I/O spikes at each hour
  boundary for busy services, and a merge that fails is retried every
  housekeeping pass.
- **Skipped merges stay skipped:** partitions whose files have different schema
  metadata are never compacted, and are only logged.
- **The disk cap ignores `.tmp` and can't delete the open hour**, so
  `bytes_on_disk` can exceed `--max-disk` until the hour closes. While the open
  hour alone is over the cap, writes get `429` rather than growing the disk.
- **A failed final flush loses rows:** if the flush in `Close` fails (for
  example a failing disk), the rows still pending are lost, and an error is
  returned and logged.
- **Stats counters are per process:** `stats.json` counters reset when `serve`
  restarts. Cumulative counters, or persisting them across restarts, are future
  work.
- **A query racing retirement** can fail if it outlives `--retire-grace`.
- **Stringified attribute values** lose types (numeric comparisons need a
  `CAST`). Typed tools hide this; the `sql` tool docs must mention it.
- **The span promoted-key set** was chosen in M2 from the OpenTelemetry HTTP and GenAI conventions, not from real samples. Revisit it against real agent traces; a change bumps the schema version.
- **Span attribute keys follow the current semantic conventions only.** Senders on older names (for example `http.method`, `http.status_code`) get no promoted columns; the values stay in `attrs`.
- **The example module pins OpenTelemetry Go.** `mise run check:tidy` and the e2e build cover it, but bumping the SDK is a manual change.
- **No notarisation:** browser downloads trigger a Gatekeeper block, and
  there's no Apple-backed identity on the binary. Mitigations: the Homebrew
  and `install.sh` paths, plus GitHub attestations. Revisit if users hit this
  often.
