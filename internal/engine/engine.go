package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/quacktel/cli/internal/manifest"
)

const (
	defaultMemoryLimit = "512MB"
	defaultThreads     = 2
	defaultTimeout     = 10 * time.Second
)

const multiStmtMsg = "missing context for multi-statement query"

var ErrNotSelect = errors.New("only a single SELECT statement is allowed")

type Options struct {
	AllowedDir string
	// DataDir, when set, builds the logs and spans views from its manifests; unscoped, AllowedDir must lie within <DataDir>/services.
	DataDir string
	// Service scopes the views to one service; AllowedDir must then contain that service's directory.
	Service     string
	MemoryLimit string
	Threads     int
	// Timeout bounds each query.
	Timeout time.Duration
}

type Engine struct {
	db      *sql.DB
	timeout time.Duration
	dataDir string
	service string
	// scopedDir is the one service directory the views may read; empty when unscoped.
	scopedDir string

	mu          sync.Mutex
	fingerprint string
	built       bool
}

type Result struct {
	Columns   []string
	Rows      [][]any
	Truncated bool
}

func Open(ctx context.Context, opts Options) (*Engine, error) {
	if opts.MemoryLimit == "" {
		opts.MemoryLimit = defaultMemoryLimit
	}
	if opts.Threads == 0 {
		opts.Threads = defaultThreads
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultTimeout
	}

	var scopedDir string
	switch {
	case opts.Service != "":
		if opts.DataDir == "" {
			return nil, errors.New("a scoped engine needs a data dir")
		}
		scopedDir = filepath.Join(opts.DataDir, "services", manifest.ServiceDir(opts.Service))
		// DataDir may sit outside AllowedDir because manifests are read with Go, not DuckDB.
		if !within(opts.AllowedDir, scopedDir) {
			return nil, fmt.Errorf("service dir %s is outside allowed dir %s", scopedDir, opts.AllowedDir)
		}
	case opts.DataDir != "" && !within(filepath.Join(opts.DataDir, "services"), opts.AllowedDir):
		return nil, fmt.Errorf("allowed dir %s must be the services dir under %s or lie within it", opts.AllowedDir, opts.DataDir)
	}

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create duckdb connector: %w", err)
	}
	db := sql.OpenDB(connector)

	// These are database-wide, so applying once covers every pooled connection, and lock_configuration stops later SETs.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to get connection: %w", err), db.Close())
	}
	stmts := []string{
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
		"SET allow_community_extensions=false",
		fmt.Sprintf("SET allowed_directories=['%s']", quote(opts.AllowedDir)),
		"SET enable_external_access=false",
		fmt.Sprintf("SET memory_limit='%s'", quote(opts.MemoryLimit)),
		fmt.Sprintf("SET threads=%d", opts.Threads),
		"SET lock_configuration=true",
	}
	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			return nil, errors.Join(fmt.Errorf("failed to apply %q: %w", s, err), conn.Close(), db.Close())
		}
	}
	if err := conn.Close(); err != nil {
		return nil, errors.Join(fmt.Errorf("failed to release connection: %w", err), db.Close())
	}

	e := &Engine{db: db, timeout: opts.Timeout, dataDir: opts.DataDir, service: opts.Service, scopedDir: scopedDir}
	if err := e.refresh(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}

	return e, nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func (e *Engine) Query(ctx context.Context, query string, maxRows int, args ...any) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	if err := e.refresh(ctx); err != nil {
		return Result{}, err
	}

	conn, err := e.db.Conn(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	if err := requireSelect(conn, query); err != nil {
		return Result{}, err
	}

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return Result{}, fmt.Errorf("failed to run query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return Result{}, fmt.Errorf("failed to read columns: %w", err)
	}

	res := Result{Columns: cols, Rows: [][]any{}}
	for rows.Next() {
		if len(res.Rows) >= maxRows {
			res.Truncated = true

			break
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return Result{}, fmt.Errorf("failed to scan row: %w", err)
		}
		res.Rows = append(res.Rows, vals)
	}
	if err := rows.Err(); err != nil {
		return Result{}, fmt.Errorf("failed to read rows: %w", err)
	}

	return res, nil
}

// requireSelect uses the non-context Prepare because PrepareContext executes all but the last statement of multi-statement input.
func requireSelect(conn *sql.Conn, query string) error {
	var typ duckdb.StmtType
	err := conn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %T", driverConn)
		}
		stmt, err := c.Prepare(query)
		if err != nil {
			return err
		}
		ds, ok := stmt.(*duckdb.Stmt)
		if !ok {
			return errors.Join(fmt.Errorf("unexpected driver statement %T", stmt), stmt.Close())
		}
		typ, err = ds.StatementType()

		return errors.Join(err, ds.Close())
	})
	if err != nil {
		// Prepare rejects multi-statement input without running any of it; the driver exports no sentinel, so match its text.
		if strings.Contains(err.Error(), multiStmtMsg) {
			return fmt.Errorf("%w: multiple statements", ErrNotSelect)
		}

		return fmt.Errorf("failed to prepare query: %w", err)
	}
	if typ != duckdb.STATEMENT_TYPE_SELECT {
		return ErrNotSelect
	}

	return nil
}

func quote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
