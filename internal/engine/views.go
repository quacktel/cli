package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/table"
)

type signalView struct {
	name   string
	schema func() *arrow.Schema
}

var views = []signalView{
	{"logs", table.LogsSchema},
	{"spans", table.SpansSchema},
}

// within reports whether dir is root or lies beneath it.
func within(root, dir string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(dir))
	if err != nil {
		return false
	}

	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// scoped keeps only the service's own manifest; the name check guards against a manifest copied into the wrong directory.
func scoped(loaded []manifest.Loaded, dir, service string) []manifest.Loaded {
	var out []manifest.Loaded
	for _, l := range loaded {
		if l.Dir == dir && l.Manifest.ServiceName == service {
			out = append(out, l)
		}
	}

	return out
}

// fingerprint changes whenever a manifest is added, removed or rewritten.
// Retention can remove a service between Glob and Stat, so a vanished manifest drops that service rather than failing the query.
func fingerprint(loaded []manifest.Loaded) (string, []manifest.Loaded, error) {
	var sb strings.Builder
	kept := loaded[:0:0]
	for _, l := range loaded {
		fi, err := os.Stat(filepath.Join(l.Dir, manifest.Name))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", nil, fmt.Errorf("failed to stat manifest: %w", err)
		}
		fmt.Fprintf(&sb, "%s|%d|%d\n", l.Dir, l.Manifest.Generation, fi.ModTime().UnixNano())
		kept = append(kept, l)
	}

	return sb.String(), kept, nil
}

// refresh rebuilds the views when the manifests changed since the last build.
func (e *Engine) refresh(ctx context.Context) error {
	if e.dataDir == "" {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	loaded, err := manifest.Glob(e.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read manifests: %w", err)
	}
	if e.scopedDir != "" {
		loaded = scoped(loaded, e.scopedDir, e.service)
	}
	fp, loaded, err := fingerprint(loaded)
	if err != nil {
		return err
	}
	if e.built && fp == e.fingerprint {
		return nil
	}
	if err := buildViews(ctx, e.db, loaded); err != nil {
		return err
	}
	e.fingerprint, e.built = fp, true

	return nil
}

// buildViews lists only manifest files, never Retired entries or anything else on disk.
func buildViews(ctx context.Context, db *sql.DB, loaded []manifest.Loaded) error {
	for _, v := range views {
		var paths []string
		for _, l := range loaded {
			for _, f := range l.Manifest.Files {
				if f.Signal == v.name {
					paths = append(paths, filepath.Join(l.Dir, f.Path))
				}
			}
		}
		slices.Sort(paths)

		var stmt string
		if len(paths) == 0 {
			cols, err := table.DuckDBColumns(v.schema())
			if err != nil {
				return fmt.Errorf("failed to build %s columns: %w", v.name, err)
			}
			stmt = fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT %s LIMIT 0", v.name, cols)
		} else {
			quoted := make([]string, len(paths))
			for i, p := range paths {
				quoted[i] = "'" + quote(p) + "'"
			}
			stmt = fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet([%s], union_by_name = true)", v.name, strings.Join(quoted, ", "))
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to create view %s: %w", v.name, err)
		}
	}

	return nil
}
