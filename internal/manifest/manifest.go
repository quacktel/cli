package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	Name    = "manifest.json"
	Version = 1
)

type File struct {
	Path    string    `json:"path"`
	Signal  string    `json:"signal"`
	Rows    int64     `json:"rows"`
	Bytes   int64     `json:"bytes"`
	MinTime time.Time `json:"min_time,omitzero"`
	MaxTime time.Time `json:"max_time,omitzero"`
}

type Retired struct {
	Path        string    `json:"path"`
	DeleteAfter time.Time `json:"delete_after"`
}

type Manifest struct {
	SchemaVersion int       `json:"schema_version"`
	ServiceName   string    `json:"service_name"`
	Generation    int64     `json:"generation"`
	Files         []File    `json:"files"`
	Retired       []Retired `json:"retired,omitempty"`
}

type Loaded struct {
	Dir      string
	Manifest Manifest
}

func Read(path string) (Manifest, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is derived from the data dir, never from request input
	if err != nil {
		return Manifest{}, fmt.Errorf("failed to read manifest %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("failed to parse manifest %s: %w", path, err)
	}

	return m, nil
}

// Write renames a fully synced copy over path, so readers never see a partial manifest.
func Write(tmpDir, path string, m Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode manifest: %w", err)
	}

	tmpPath := filepath.Join(tmpDir, "manifest-"+ulid.Make().String()+".json")
	if err := writeSynced(tmpPath, data); err != nil {
		return errors.Join(err, os.Remove(tmpPath))
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.Join(fmt.Errorf("failed to publish manifest: %w", err), os.Remove(tmpPath))
	}

	return SyncDir(filepath.Dir(path))
}

// Glob reads every service manifest under dataDir; a missing services dir means none.
func Glob(dataDir string) ([]Loaded, error) {
	root := filepath.Join(dataDir, "services")
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	var out []Loaded
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		m, err := Read(filepath.Join(dir, Name))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, Loaded{Dir: dir, Manifest: m})
	}
	slices.SortFunc(out, func(a, b Loaded) int { return strings.Compare(a.Dir, b.Dir) })

	return out, nil
}

func writeSynced(path string, data []byte) (err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: path is derived from the data dir, never from request input
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close %s: %w", path, cerr)
		}
	}()
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync %s: %w", path, err)
	}

	return nil
}

func SyncDir(path string) (err error) {
	d, err := os.Open(path) //nolint:gosec // G304: path is derived from the data dir, never from request input
	if err != nil {
		return fmt.Errorf("failed to open dir %s: %w", path, err)
	}
	defer func() {
		if cerr := d.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close dir %s: %w", path, cerr)
		}
	}()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("failed to sync dir %s: %w", path, err)
	}

	return nil
}
