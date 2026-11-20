package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

const (
	defaultMemoryLimit = "512MB"
	defaultThreads     = 2
	defaultTimeout     = 10 * time.Second
)

const multiStmtMsg = "missing context for multi-statement query"

var ErrNotSelect = errors.New("only a single SELECT statement is allowed")

type Options struct {
	AllowedDir  string
	MemoryLimit string
	Threads     int
	// Timeout bounds each query.
	Timeout time.Duration
}

type Engine struct {
	db      *sql.DB
	timeout time.Duration
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

	return &Engine{db: db, timeout: opts.Timeout}, nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func (e *Engine) Query(ctx context.Context, query string, maxRows int) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	conn, err := e.db.Conn(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	if err := requireSelect(conn, query); err != nil {
		return Result{}, err
	}

	rows, err := conn.QueryContext(ctx, query)
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
