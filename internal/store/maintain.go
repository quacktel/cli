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
	"github.com/oklog/ulid/v2"

	"github.com/quacktel/cli/internal/manifest"
)

const partitionLayout = "date=2006-01-02/hour=15"

// pendingBatch is a swapped-out record whose partition is fixed at receivedAt, so retries stay honest about receive time.
type pendingBatch struct {
	rec        arrow.RecordBatch
	bytes      int64
	minT, maxT time.Time
	receivedAt time.Time
}

type flushJob struct {
	sv      *service
	signal  string
	batches []pendingBatch
}

func (s *Store) Run(ctx context.Context) error {
	t := time.NewTicker(s.opts.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.Maintain(ctx); err != nil && ctx.Err() == nil {
				s.log.ErrorContext(ctx, "maintenance failed", "error", err)
			}
		}
	}
}

// Maintain runs one pass: flush, compaction and retention when due, then retired-file cleanup.
func (s *Store) Maintain(ctx context.Context) error {
	s.mm.Lock()
	defer s.mm.Unlock()

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil
	}

	errs := []error{s.flush(ctx)}
	now := s.opts.Now()
	if s.lastHousekeeping.IsZero() || now.Sub(s.lastHousekeeping) >= s.opts.CompactInterval {
		s.lastHousekeeping = now
		errs = append(errs, s.compact(ctx), s.retain(now))
	}
	errs = append(errs, s.cleanup(now))

	return errors.Join(errs...)
}

func (s *Store) Flush(ctx context.Context) error {
	s.mm.Lock()
	defer s.mm.Unlock()

	return s.flush(ctx)
}

// flush swaps builders out under the lock, writes outside it, then retakes it to publish.
func (s *Store) flush(ctx context.Context) error {
	jobs, touched := s.swapOut()

	var errs []error
	var writeErrs int64
	written := map[*service][]manifest.File{}
	failed := map[*flushJob][]pendingBatch{}
	for _, j := range jobs {
		for i, p := range j.batches {
			if err := ctx.Err(); err != nil {
				failed[j] = append(failed[j], j.batches[i:]...)
				errs = append(errs, err)

				break
			}
			f, err := s.writeBatch(j.sv, j.signal, p)
			if err != nil {
				writeErrs++
				failed[j] = append(failed[j], p)
				errs = append(errs, fmt.Errorf("failed to flush %s for service %q: %w", j.signal, j.sv.manifest.ServiceName, err))

				continue
			}
			written[j.sv] = append(written[j.sv], f)
			p.rec.Release()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.WriteErrors += writeErrs
	for _, j := range jobs {
		j.sv.pending[j.signal] = failed[j]
	}
	for sv := range touched {
		if err := s.publishFlushed(sv, written[sv]); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// swapOut moves each non-empty builder into its service's pending queue and lists the work to write.
func (s *Store) swapOut() ([]*flushJob, map[*service]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.opts.Now()
	var jobs []*flushJob
	touched := map[*service]bool{}
	for _, sv := range s.services {
		for _, signal := range signals {
			if b := sv.builders[signal]; b != nil && b.Len() > 0 {
				minT, maxT := b.MinMaxTime()
				bytes := b.EstimatedBytes()
				sv.pending[signal] = append(sv.pending[signal], pendingBatch{rec: b.NewRecord(), bytes: bytes, minT: minT, maxT: maxT, receivedAt: now})
			}
			if len(sv.pending[signal]) > 0 {
				jobs = append(jobs, &flushJob{sv: sv, signal: signal, batches: sv.pending[signal]})
				touched[sv] = true
			}
		}
		if len(sv.unpublished) > 0 {
			touched[sv] = true
		}
	}

	return jobs, touched
}

// publishFlushed lists newly written files plus any left unpublished by an earlier failure; must run under s.mu.
func (s *Store) publishFlushed(sv *service, written []manifest.File) error {
	sv.unpublished = append(sv.unpublished, written...)
	for _, f := range written {
		s.stats.Flushes++
		s.stats.FilesWritten++
		s.stats.BytesWritten += f.Bytes
	}
	if len(sv.unpublished) == 0 {
		return nil
	}
	prev := sv.manifest
	sv.manifest.Files = append(slices.Clone(prev.Files), sv.unpublished...)
	if err := s.publish(sv); err != nil {
		sv.manifest = prev

		return fmt.Errorf("failed to publish manifest for service %q: %w", prev.ServiceName, err)
	}
	sv.unpublished = nil

	return nil
}

// writeBatch writes one pending batch into the partition of its receive time, without holding s.mu.
func (s *Store) writeBatch(sv *service, signal string, p pendingBatch) (manifest.File, error) {
	tmpPath := filepath.Join(s.tmpDir(), ulid.Make().String()+".parquet")
	size, err := s.writeParquet(tmpPath, p.rec)
	if err != nil {
		return manifest.File{}, errors.Join(err, removeIfExists(tmpPath))
	}

	part := filepath.Join(sv.dir, signal, p.receivedAt.UTC().Format(partitionLayout))
	if err := os.MkdirAll(part, 0o700); err != nil {
		return manifest.File{}, errors.Join(fmt.Errorf("failed to create partition dir: %w", err), removeIfExists(tmpPath))
	}
	final := filepath.Join(part, "tail-"+ulid.Make().String()+".parquet")
	if err := os.Rename(tmpPath, final); err != nil {
		return manifest.File{}, errors.Join(fmt.Errorf("failed to rename parquet into place: %w", err), removeIfExists(tmpPath))
	}
	rel, err := filepath.Rel(sv.dir, final)
	if err != nil {
		return manifest.File{}, fmt.Errorf("failed to relativise %s: %w", final, err)
	}
	// The file is already in place, so a failed dir sync must not queue the batch for a second write.
	f := manifest.File{Path: rel, Signal: signal, Rows: p.rec.NumRows(), Bytes: size, MinTime: p.minT.UTC(), MaxTime: p.maxT.UTC()}
	if err := manifest.SyncDir(part); err != nil {
		s.log.Warn("failed to sync partition dir", "error", err)
	}

	return f, nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// partitionHour parses the date/hour directories of a manifest-relative path.
func partitionHour(rel string) (time.Time, bool) {
	parts := strings.Split(filepath.ToSlash(filepath.Dir(rel)), "/")
	if len(parts) < 3 {
		return time.Time{}, false
	}
	t, err := time.Parse(partitionLayout, strings.Join(parts[len(parts)-2:], "/"))

	return t, err == nil
}

// retirer moves files from Files to Retired, remembering each service's prior manifest for rollback.
type retirer struct {
	deleteAfter time.Time
	prev        map[*service]manifest.Manifest
}

func (r *retirer) retire(sv *service, drop func(f manifest.File) bool) (bytes int64) {
	var keep []manifest.File
	var gone []manifest.Retired
	for _, f := range sv.manifest.Files {
		if drop(f) {
			gone = append(gone, manifest.Retired{Path: f.Path, DeleteAfter: r.deleteAfter})
			bytes += f.Bytes

			continue
		}
		keep = append(keep, f)
	}
	if len(gone) == 0 {
		return 0
	}
	if _, ok := r.prev[sv]; !ok {
		r.prev[sv] = sv.manifest
	}
	sv.manifest.Files = keep
	sv.manifest.Retired = append(slices.Clone(sv.manifest.Retired), gone...)

	return bytes
}

// retain retires files past the age limit, then the oldest closed partitions while the disk cap is exceeded; if only the open hour is left over it, writes are refused.
func (s *Store) retain(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := &retirer{deleteAfter: now.Add(s.opts.RetireGrace), prev: map[*service]manifest.Manifest{}}
	cutoff := now.Add(-s.opts.Retention)
	var total int64
	for _, sv := range s.services {
		r.retire(sv, func(f manifest.File) bool {
			h, ok := partitionHour(f.Path)

			return ok && !h.Add(time.Hour).After(cutoff)
		})
		for _, f := range sv.manifest.Files {
			total += f.Bytes
		}
	}

	openHour := now.UTC().Truncate(time.Hour)
	s.capExceeded = false
	for total > s.opts.MaxDiskBytes {
		sv, dir := s.oldestClosedPartition(openHour)
		if sv == nil {
			s.capExceeded = true
			s.log.Warn("disk cap exceeded by the open hour alone, refusing writes", "bytes", total, "max_bytes", s.opts.MaxDiskBytes)

			break
		}
		total -= r.retire(sv, func(f manifest.File) bool { return filepath.Dir(f.Path) == dir })
	}

	var errs []error
	for sv, m := range r.prev {
		if err := s.publish(sv); err != nil {
			sv.manifest = m
			errs = append(errs, fmt.Errorf("failed to publish retention for service %q: %w", m.ServiceName, err))
		}
	}

	return errors.Join(errs...)
}

// oldestClosedPartition picks by hour, then service dir, then partition dir, so the choice is deterministic; must run under s.mu.
func (s *Store) oldestClosedPartition(openHour time.Time) (*service, string) {
	var best *service
	var bestHour time.Time
	var bestDir string
	for _, sv := range s.services {
		for _, f := range sv.manifest.Files {
			h, ok := partitionHour(f.Path)
			if !ok || !h.Before(openHour) {
				continue
			}
			dir := filepath.Dir(f.Path)
			if best == nil || h.Before(bestHour) || (h.Equal(bestHour) && (sv.dir < best.dir || (sv.dir == best.dir && dir < bestDir))) {
				best, bestHour, bestDir = sv, h, dir
			}
		}
	}

	return best, bestDir
}

// cleanup unlinks retired files past their grace period, then drops services with nothing left.
func (s *Store) cleanup(now time.Time) error {
	type due struct {
		sv    *service
		paths []string
	}
	s.mu.Lock()
	var dues []due
	for _, sv := range s.services {
		var d due
		for _, r := range sv.manifest.Retired {
			if !r.DeleteAfter.After(now) {
				d.paths = append(d.paths, r.Path)
			}
		}
		if len(d.paths) > 0 {
			d.sv = sv
			dues = append(dues, d)
		}
	}
	s.mu.Unlock()

	var errs []error
	deleted := map[*service]map[string]bool{}
	var n int64
	for _, d := range dues {
		deleted[d.sv] = map[string]bool{}
		for _, p := range d.paths {
			if err := removeFile(d.sv.dir, p); err != nil {
				errs = append(errs, err)

				continue
			}
			pruneEmptyDirs(d.sv.dir, p)
			deleted[d.sv][p] = true
			n++
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.FilesRetired += n
	for sv, gone := range deleted {
		if len(gone) == 0 {
			continue
		}
		prev := sv.manifest
		sv.manifest.Retired = slices.DeleteFunc(slices.Clone(prev.Retired), func(r manifest.Retired) bool { return gone[r.Path] })
		if err := s.publish(sv); err != nil {
			sv.manifest = prev
			errs = append(errs, fmt.Errorf("failed to publish cleanup for service %q: %w", prev.ServiceName, err))
		}
	}

	return errors.Join(append(errs, s.removeEmptyServices())...)
}

// pruneEmptyDirs removes the hour and date directories left behind, never the signal directory.
func pruneEmptyDirs(svcDir, rel string) {
	dir := filepath.Dir(rel)
	for range 2 {
		if os.Remove(filepath.Join(svcDir, dir)) != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// removeEmptyServices must run under s.mu.
func (s *Store) removeEmptyServices() error {
	var errs []error
	for name, sv := range s.services {
		if _, active := s.active[name]; active || !sv.empty() {
			continue
		}
		// The path is rebuilt from the name, so a service can never point RemoveAll outside services/.
		if sv.dir != s.serviceDir(name) {
			continue
		}
		if err := os.RemoveAll(sv.dir); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove service dir %s: %w", sv.dir, err))

			continue
		}
		for _, b := range sv.builders {
			b.Release()
		}
		delete(s.services, name)
	}

	return errors.Join(errs...)
}

func (sv *service) empty() bool {
	if len(sv.manifest.Files) > 0 || len(sv.manifest.Retired) > 0 || len(sv.unpublished) > 0 {
		return false
	}
	for _, b := range sv.builders {
		if b.Len() > 0 {
			return false
		}
	}
	for _, p := range sv.pending {
		if len(p) > 0 {
			return false
		}
	}

	return true
}

func (s *Store) Close() error {
	s.mm.Lock()
	defer s.mm.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		return nil
	}
	s.closed = true
	s.mu.Unlock()

	err := s.flush(context.Background())

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sv := range s.services {
		for _, b := range sv.builders {
			b.Release()
		}
		for _, batches := range sv.pending {
			for _, p := range batches {
				p.rec.Release()
			}
		}
	}

	return errors.Join(err, s.unlock())
}
