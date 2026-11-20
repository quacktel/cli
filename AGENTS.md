`DESIGN.md` is the source of truth for scope and decisions. When a change departs from it, update `DESIGN.md` in the same change.

## Treat this repository as public

This repository is private for now but will be made public. Assume everything committed, including history, will be published.

Keep secrets and private details out of commits, code, tests, issues and pull requests. That includes API keys, 1Password references (vault, item or field names), account details and private paths. Use placeholders such as `op://<vault>/<item>/<field>`.

When something needs a credential, pass it only to the one command that uses it. Never print, write or commit it. Tool errors can echo a secret reference, so silence or redact their stderr.

Test fixtures use synthetic telemetry only. Never commit captured real telemetry: it can contain prompts, file contents and tokens.

## Workflow

Until 1.0, this repo uses trunk-based development: commit directly to `main`, with no pull requests.

- Use Conventional Commits. Squash noisy local work before pushing, because history will be public.
- The pre-commit hooks and CI on `main` are the gate. When CI fails on `main`, fix it before adding anything else.
- Never force-push `main`.
- Subagent worktree branches are short-lived. Review them, merge them into `main` locally, and delete them.

## Tooling

- Use the tools pinned in `mise.toml` and `mise.lock`, and run them through `mise exec --`. Run `mise install` if any are missing.
- Fix lint findings instead of weakening `.golangci.yml` or adding blanket suppressions. Every `//nolint` names the linter and gives a reason.
- Pin new tools and dependencies to explicit versions, never `latest`. That includes DuckDB and `duckdb-go`.

## Verification

Fast loop: `go test ./...`

Before finishing, after your last edit:

```sh
go vet ./... && go test -race -count=1 ./...
mise exec -- golangci-lint run ./...
mise run check:tidy
```

The `cmd/quacktel` tests build and run the real binary.

## Guiding principles

- **Validate upfront.** `serve` checks its config, bind address, data-dir permissions and limits before it listens, and exits on failure. `doctor` reuses the same checks. A problem should appear at startup, not when the first request arrives.
- **Local and least-privilege by default.** A new feature must not widen the network surface, the read path's file access, or what reaches disk unredacted, unless `DESIGN.md` is updated to say so.

## Invariants

- Only the OTLP receiver listens, and only on loopback, unless `--unsafe-bind` is given. Never add a query listener; MCP is stdio only.
- The receiver rejects requests that carry `Origin`, a `Host` other than `localhost` or `127.0.0.1` on the receiver's port, or a content type other than `application/x-protobuf` or `application/json`. Body and decompressed sizes are capped. A full buffer returns `429`, and nothing grows without bound.
- Redaction runs before anything is buffered or written.
- `service.name` is untrusted. It reaches the filesystem only as the slug and hash directory name, never as a path itself.
- Every file is written to `.tmp`, fsynced, then renamed into place. Nothing is written in place, and that includes manifests.
- Readers open only the files a manifest lists, never a glob. A file a manifest stops listing is unlinked only after the grace period.
- The daemon never opens DuckDB. DuckDB is on the read path only, and it never installs or loads extensions at runtime.
- Every reader connection applies the hardening settings in `DESIGN.md` and sets `lock_configuration = true` last. Scoped sessions restrict `allowed_directories` to that service's directory.
- Raw SQL runs only after the prepared statement's type is confirmed as `SELECT`.
- Typed tools bind every user value as a parameter. Column and attribute names are validated against the schema, never interpolated unchecked.
- MCP tool output marks telemetry content as untrusted data, because any local process can write it.
- Data files and directories are created `0600` and `0700`.
- Logs go to stderr and results go to stdout.

## Code conventions

### Errors

Return errors up the stack and log only at the top level: the command's `Run` function or the HTTP handler boundary. Never log an error and then return it, because it will be logged twice.

Wrap errors from other packages with `%w` and context. Errors from the same package may be returned as they are when wrapping adds nothing:

```go
return fmt.Errorf("failed to open manifest: %w", err)
```

Check close errors on files you write, keeping the earlier error if there was one. Don't add handling for impossible cases; trust the compiler and internal invariants.

### Logging

Use `slog` throughout. Pass the logger via `Globals.Logger` rather than calling `slog.Default()` in business logic. Inside HTTP handlers, use the `InfoContext` / `ErrorContext` variants.

### Contexts

Pass `context.Context` as the first argument to every function that does I/O or should respect cancellation. Don't store contexts in structs.

### Package names

Use lowercase, single-word names that describe the domain, not the layer: `receiver`, `redact`, `store`, `engine`. Avoid `util`, `helpers` and `common`.

### Comments

Comment only the **why**, never the **what**. Don't write docstrings for obvious functions. One line is the maximum.

## Testing

- Use `github.com/stretchr/testify/require`, not `assert`, so tests fail fast.
- Use `t.Context()` in tests. Keep `os.Exit` out of functions that have deferred cleanup.
- Every new CLI flag or subcommand needs a real-binary test. Real-binary tests share one binary, built by `TestMain`, and use a temporary `HOME` and data directory.
- Every invariant above needs a test that fails if it breaks: receiver gates, hostile `service.name` values, SELECT-guard bypasses, and scoped-session file access.
- When a test fails, fix the code. Change the test's data only if the test is wrong.
