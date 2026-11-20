package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/oklog/ulid/v2"

	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/table"
)

const (
	mergeBatchRows    = 64 << 10
	mergeRowGroupLen  = 128 << 10
	tailPrefix        = "tail-"
	sealedPrefix      = "sealed-"
	hourPrefix        = "hour-"
	closeGraceFlushes = 2
)

var errSchemaMismatch = errors.New("schema metadata differs between files")

type mergePlan struct {
	sv      *service
	signal  string
	dir     string
	prefix  string
	sources []manifest.File
}

// compact seals open-hour partitions and merges whole partitions once their hour is closed.
func (s *Store) compact(ctx context.Context) error {
	var errs []error
	for _, p := range s.planMerges(s.opts.Now()) {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		merged, err := s.merge(ctx, p)
		if errors.Is(err, errSchemaMismatch) {
			s.log.Warn("skipping compaction of partition", "service", p.sv.manifest.ServiceName, "dir", p.dir, "error", err)

			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to compact %s for service %q: %w", p.dir, p.sv.manifest.ServiceName, err))

			continue
		}
		if err := s.commitMerge(p, merged); err != nil {
			errs = append(errs, fmt.Errorf("failed to publish compaction of %s for service %q: %w", p.dir, p.sv.manifest.ServiceName, err))
		}
	}

	return errors.Join(errs...)
}

func (s *Store) planMerges(now time.Time) []mergePlan {
	s.mu.Lock()
	defer s.mu.Unlock()

	var plans []mergePlan
	for _, sv := range s.services {
		groups := map[string][]manifest.File{}
		for _, f := range sv.manifest.Files {
			groups[filepath.Dir(f.Path)] = append(groups[filepath.Dir(f.Path)], f)
		}
		for dir, files := range groups {
			slices.SortFunc(files, func(a, b manifest.File) int { return strings.Compare(a.Path, b.Path) })
			if sources, prefix := s.pickSources(dir, files, now); len(sources) > 0 {
				plans = append(plans, mergePlan{sv: sv, signal: files[0].Signal, dir: dir, prefix: prefix, sources: sources})
			}
		}
	}

	return plans
}

// pickSources chooses one merge per partition: in the open hour, everything once sealed files pile up, else the tails; everything once the hour is closed.
func (s *Store) pickSources(dir string, files []manifest.File, now time.Time) ([]manifest.File, string) {
	hour, ok := partitionHour(dir + "/x")
	if !ok {
		return nil, ""
	}
	if hour.Equal(now.UTC().Truncate(time.Hour)) {
		var tails []manifest.File
		for _, f := range files {
			if strings.HasPrefix(filepath.Base(f.Path), tailPrefix) {
				tails = append(tails, f)
			}
		}
		// Checked first: with steady traffic the tail condition is true every pass and would starve this merge.
		if len(files)-len(tails) >= s.opts.SealMinFiles {
			return files, sealedPrefix
		}
		if len(tails) >= s.opts.SealMinFiles {
			return tails, sealedPrefix
		}

		return nil, ""
	}
	if !now.Before(hour.Add(time.Hour+closeGraceFlushes*s.opts.FlushInterval)) && len(files) > 1 {
		return files, hourPrefix
	}

	return nil, ""
}

type mergeSource struct {
	pf *file.Reader
	fr *pqarrow.FileReader
}

func sameSignalSchema(a, b *arrow.Schema) bool {
	for _, k := range []string{table.SchemaVersionKey, table.SignalKey} {
		av, aok := a.Metadata().GetValue(k)
		bv, bok := b.Metadata().GetValue(k)
		if av != bv || aok != bok {
			return false
		}
	}

	return true
}

// openSources opens every source and checks that its schema metadata matches the first's.
func (s *Store) openSources(p mergePlan) (srcs []mergeSource, schema *arrow.Schema, err error) {
	for _, f := range p.sources {
		path := filepath.Join(p.sv.dir, f.Path)
		pf, oerr := file.OpenParquetFile(path, false)
		if oerr != nil {
			return srcs, nil, fmt.Errorf("failed to open parquet %s: %w", path, oerr)
		}
		srcs = append(srcs, mergeSource{pf: pf})
		fr, rerr := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: mergeBatchRows}, s.mem)
		if rerr != nil {
			return srcs, nil, fmt.Errorf("failed to read parquet %s: %w", path, rerr)
		}
		srcs[len(srcs)-1].fr = fr
		sch, serr := fr.Schema()
		if serr != nil {
			return srcs, nil, fmt.Errorf("failed to read schema of %s: %w", path, serr)
		}
		if schema == nil {
			schema = sch
		} else if !sameSignalSchema(schema, sch) {
			return srcs, nil, fmt.Errorf("%s: %w", f.Path, errSchemaMismatch)
		}
	}

	return srcs, schema, nil
}

// merge streams the sources through one writer, so memory stays at a batch plus one row group.
func (s *Store) merge(ctx context.Context, p mergePlan) (_ manifest.File, err error) {
	srcs, schema, err := s.openSources(p)
	defer func() {
		for _, src := range srcs {
			if cerr := src.pf.Close(); cerr != nil {
				err = errors.Join(err, fmt.Errorf("failed to close source parquet: %w", cerr))
			}
		}
	}()
	if err != nil {
		return manifest.File{}, err
	}

	tmpPath := filepath.Join(s.tmpDir(), ulid.Make().String()+".parquet")
	rows, size, err := s.writeMerged(ctx, tmpPath, schema, srcs)
	if err != nil {
		return manifest.File{}, errors.Join(err, removeIfExists(tmpPath))
	}

	part := filepath.Join(p.sv.dir, p.dir)
	final := filepath.Join(part, p.prefix+ulid.Make().String()+".parquet")
	if err := os.Rename(tmpPath, final); err != nil {
		return manifest.File{}, errors.Join(fmt.Errorf("failed to rename merged parquet into place: %w", err), removeIfExists(tmpPath))
	}
	if err := manifest.SyncDir(part); err != nil {
		return manifest.File{}, errors.Join(err, removeIfExists(final))
	}

	merged := manifest.File{Path: filepath.Join(p.dir, filepath.Base(final)), Signal: p.signal, Rows: rows, Bytes: size}
	for _, f := range p.sources {
		if !f.MinTime.IsZero() && (merged.MinTime.IsZero() || f.MinTime.Before(merged.MinTime)) {
			merged.MinTime = f.MinTime
		}
		if f.MaxTime.After(merged.MaxTime) {
			merged.MaxTime = f.MaxTime
		}
	}

	return merged, nil
}

func (s *Store) writeMerged(ctx context.Context, path string, schema *arrow.Schema, srcs []mergeSource) (rows, size int64, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: path is derived from the data dir, never from request input
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close %s: %w", path, cerr)
		}
	}()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd), parquet.WithMaxRowGroupLength(mergeRowGroupLen))
	w, err := pqarrow.NewFileWriter(schema, nopCloser{f}, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create parquet writer: %w", err)
	}
	for _, src := range srcs {
		rr, err := src.fr.GetRecordReader(ctx, nil, nil)
		if err != nil {
			return 0, 0, errors.Join(fmt.Errorf("failed to open record reader: %w", err), w.Close())
		}
		for rr.Next() {
			rec := rr.RecordBatch()
			if err := w.WriteBuffered(rec); err != nil {
				rr.Release()

				return 0, 0, errors.Join(fmt.Errorf("failed to write merged rows: %w", err), w.Close())
			}
			rows += rec.NumRows()
		}
		rerr := rr.Err()
		rr.Release()
		if rerr != nil {
			return 0, 0, errors.Join(fmt.Errorf("failed to read source rows: %w", rerr), w.Close())
		}
	}
	if err := w.Close(); err != nil {
		return 0, 0, fmt.Errorf("failed to finish merged parquet: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, 0, fmt.Errorf("failed to sync %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to stat %s: %w", path, err)
	}

	return rows, info.Size(), nil
}

// commitMerge swaps the sources for the merged file in one publish, or abandons the merge if the manifest moved.
func (s *Store) commitMerge(p mergePlan, merged manifest.File) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	mergedPath := filepath.Join(p.sv.dir, merged.Path)
	listed := map[string]bool{}
	for _, f := range p.sv.manifest.Files {
		listed[f.Path] = true
	}
	srcSet := map[string]bool{}
	for _, f := range p.sources {
		srcSet[f.Path] = true
		if !listed[f.Path] {
			s.log.Warn("abandoning compaction: manifest changed", "service", p.sv.manifest.ServiceName, "dir", p.dir)

			return removeIfExists(mergedPath)
		}
	}

	deleteAfter := s.opts.Now().Add(s.opts.RetireGrace)
	prev := p.sv.manifest
	files := slices.DeleteFunc(slices.Clone(prev.Files), func(f manifest.File) bool { return srcSet[f.Path] })
	retired := slices.Clone(prev.Retired)
	for _, f := range p.sources {
		retired = append(retired, manifest.Retired{Path: f.Path, DeleteAfter: deleteAfter})
	}
	files = append(files, merged)
	p.sv.manifest.Files = files
	p.sv.manifest.Retired = retired
	if err := s.publish(p.sv); err != nil {
		p.sv.manifest = prev

		return errors.Join(err, removeIfExists(mergedPath))
	}
	s.stats.Compactions++

	return nil
}
