package store

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/quacktel/cli/internal/engine"
	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/table"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock(t time.Time) *clock { return &clock{t: t} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

var baseTime = time.Date(2026, 11, 20, 13, 30, 0, 0, time.UTC)

func seqLogs(service string, at time.Time, seqs ...int64) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	recs := rl.ScopeLogs().AppendEmpty().LogRecords()
	for i, seq := range seqs {
		lr := recs.AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(i) * time.Second)))
		lr.Body().SetStr("hello")
		lr.Attributes().PutInt("seq", seq)
	}

	return ld
}

func seqSpans(service string, at time.Time, seqs ...int64) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", service)
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i, seq := range seqs {
		sp := spans.AppendEmpty()
		sp.SetTraceID(testTraceID)
		sp.SetSpanID(rootSpanID)
		sp.SetName("op")
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(i) * time.Second)))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(i)*time.Second + time.Millisecond)))
		sp.Attributes().PutInt("seq", seq)
	}

	return td
}

// appendFlush appends one log record per seq and flushes, producing one tail per call.
func appendFlush(t *testing.T, s *Store, service string, at time.Time, seqs ...int64) {
	t.Helper()

	_, _, err := s.AppendLogs(t.Context(), seqLogs(service, at, seqs...))
	require.NoError(t, err)
	require.NoError(t, s.Flush(t.Context()))
}

func listedFiles(t *testing.T, dataDir, service, signal string) []manifest.File {
	t.Helper()

	var out []manifest.File
	for _, f := range readManifest(t, dataDir, service).Files {
		if f.Signal == signal {
			out = append(out, f)
		}
	}

	return out
}

// listedSeqs reads seq values through DuckDB over exactly the manifest's files for one service and signal.
func listedSeqs(t *testing.T, e *engine.Engine, dataDir, service, signal string) []int64 {
	t.Helper()

	var quoted []string
	for _, f := range listedFiles(t, dataDir, service, signal) {
		quoted = append(quoted, "'"+filepath.Join(dataDir, "services", ServiceDir(service), f.Path)+"'")
	}
	if len(quoted) == 0 {
		return nil
	}
	res, err := e.Query(t.Context(), "SELECT CAST(attrs['seq'] AS BIGINT) AS seq FROM read_parquet(["+strings.Join(quoted, ",")+"]) ORDER BY 1", 1<<22)
	require.NoError(t, err)
	require.False(t, res.Truncated)
	out := make([]int64, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, r[0].(int64))
	}

	return out
}

func totalRows(files []manifest.File) int64 {
	var n int64
	for _, f := range files {
		n += f.Rows
	}

	return n
}

func TestNonBlockingFlush(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir})

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s.writeParquet = func(path string, rec arrow.RecordBatch) (int64, error) {
		once.Do(func() {
			close(entered)
			<-release
		})

		return writeParquet(path, rec)
	}

	_, _, err := s.AppendLogs(t.Context(), seqLogs("svc", baseTime, 1))
	require.NoError(t, err)

	flushed := make(chan error, 1)
	go func() { flushed <- s.Flush(t.Context()) }()
	<-entered

	appended := make(chan error, 1)
	go func() {
		_, _, err := s.AppendLogs(t.Context(), seqLogs("svc", baseTime, 2))
		appended <- err
	}()
	select {
	case err := <-appended:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "AppendLogs blocked behind an in-flight flush")
	}

	close(release)
	require.NoError(t, <-flushed)
	require.NoError(t, s.Flush(t.Context()))

	e := spansEngine(t, dataDir)
	require.Equal(t, []int64{1, 2}, listedSeqs(t, e, dataDir, "svc", signalLogs))
}

func TestWriteFailureRetry(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now})

	failed := false
	s.writeParquet = func(path string, rec arrow.RecordBatch) (int64, error) {
		if !failed {
			failed = true

			return 0, os.ErrInvalid
		}

		return writeParquet(path, rec)
	}

	_, _, err := s.AppendLogs(t.Context(), seqLogs("svc", baseTime, 1, 2, 3))
	require.NoError(t, err)
	require.ErrorIs(t, s.Maintain(t.Context()), os.ErrInvalid)
	require.EqualValues(t, 1, s.Stats().WriteErrors)
	require.EqualValues(t, 1, s.Stats().PendingBatches)
	require.Empty(t, parquetFiles(t, dataDir))

	clk.Advance(40 * time.Minute)
	require.NoError(t, s.Maintain(t.Context()))
	st := s.Stats()
	require.EqualValues(t, 1, st.WriteErrors)
	require.Zero(t, st.PendingBatches)

	files := listedFiles(t, dataDir, "svc", signalLogs)
	require.Len(t, files, 1)
	require.EqualValues(t, 3, totalRows(files))
	require.Contains(t, files[0].Path, filepath.Join("date=2026-11-20", "hour=13"))
	e := spansEngine(t, dataDir)
	require.Equal(t, []int64{1, 2, 3}, listedSeqs(t, e, dataDir, "svc", signalLogs))
}

func TestPersistentWriteFailureEndsInErrFull(t *testing.T) {
	s := openStore(t, Options{BufferBytes: 4096})
	s.writeParquet = func(string, arrow.RecordBatch) (int64, error) { return 0, os.ErrInvalid }
	// Registered after openStore's Close, so it runs first and lets the final flush drain.
	t.Cleanup(func() { s.writeParquet = writeParquet })

	var full error
	for i := range 500 {
		if _, _, full = s.AppendLogs(t.Context(), seqLogs("svc", baseTime, int64(i))); full != nil {
			break
		}
		require.Error(t, s.Maintain(t.Context()))
	}
	require.ErrorIs(t, full, ErrFull)
	require.Positive(t, s.Stats().PendingBatches)
}

func TestPublishFailureRetry(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir})

	appendFlush(t, s, "svc", baseTime, 1)
	svcDir := filepath.Join(dataDir, "services", ServiceDir("svc"))
	// A read-only service dir makes the manifest rename fail while the partition dir stays writable.
	require.NoError(t, os.Chmod(svcDir, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(svcDir, 0o700)) })

	_, _, err := s.AppendLogs(t.Context(), seqLogs("svc", baseTime, 2))
	require.NoError(t, err)
	require.Error(t, s.Maintain(t.Context()))
	require.Len(t, readManifest(t, dataDir, "svc").Files, 1)
	require.Len(t, parquetFiles(t, dataDir), 2)
	require.Zero(t, s.Stats().PendingBatches)

	require.NoError(t, os.Chmod(svcDir, 0o700))
	require.NoError(t, s.Maintain(t.Context()))
	require.Len(t, readManifest(t, dataDir, "svc").Files, 2)
	e := spansEngine(t, dataDir)
	require.Equal(t, []int64{1, 2}, listedSeqs(t, e, dataDir, "svc", signalLogs))
}

// sealFixture writes 12 single-row tails in the open hour and runs one compaction pass.
func sealFixture(t *testing.T, dataDir string, clk *clock) (*Store, int64) {
	t.Helper()

	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now, SealMinFiles: 10})
	for i := range 12 {
		appendFlush(t, s, "svc", baseTime.Add(time.Duration(i)*time.Second), int64(i))
	}
	require.Len(t, readManifest(t, dataDir, "svc").Files, 12)
	gen := readManifest(t, dataDir, "svc").Generation

	clk.Advance(5 * time.Minute)
	require.NoError(t, s.Maintain(t.Context()))

	return s, gen
}

func TestSealOpenHourTails(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s, gen := sealFixture(t, dataDir, clk)

	m := readManifest(t, dataDir, "svc")
	require.Len(t, m.Files, 1)
	require.True(t, strings.HasPrefix(filepath.Base(m.Files[0].Path), "sealed-"), m.Files[0].Path)
	require.EqualValues(t, 12, m.Files[0].Rows)
	require.True(t, baseTime.Equal(m.Files[0].MinTime), m.Files[0].MinTime)
	require.True(t, baseTime.Add(11*time.Second).Equal(m.Files[0].MaxTime), m.Files[0].MaxTime)
	require.Len(t, m.Retired, 12)
	require.Equal(t, gen+1, m.Generation)
	require.EqualValues(t, 1, s.Stats().Compactions)

	e := spansEngine(t, dataDir)
	seqs := listedSeqs(t, e, dataDir, "svc", signalLogs)
	require.Len(t, seqs, 12)
	require.Equal(t, int64(0), seqs[0])
	require.Equal(t, int64(11), seqs[11])
	require.Equal(t, m.Files[0].Rows, int64(len(seqs)))
}

func TestSealOpenHourBoundsSealedFiles(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	const sealMin = 3
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now, SealMinFiles: sealMin, CompactInterval: time.Second})

	var next int64
	var want []int64
	for pass := range 12 {
		for range sealMin {
			appendFlush(t, s, "svc", clk.Now(), next)
			want = append(want, next)
			next++
		}
		clk.Advance(2 * time.Second)
		require.NoError(t, s.Maintain(t.Context()))

		sealed := 0
		for _, f := range readManifest(t, dataDir, "svc").Files {
			if strings.HasPrefix(filepath.Base(f.Path), sealedPrefix) {
				sealed++
			}
		}
		require.LessOrEqual(t, sealed, sealMin, "pass %d", pass)
	}

	files := readManifest(t, dataDir, "svc").Files
	require.Equal(t, int64(len(want)), totalRows(files))
	require.Equal(t, want, listedSeqs(t, spansEngine(t, dataDir), dataDir, "svc", signalLogs))
}

func TestMergeRowGroupLen(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now, SealMinFiles: 3})

	const perFile = 50_000
	for i := range 3 {
		seqs := make([]int64, perFile)
		for j := range seqs {
			seqs[j] = int64(i*perFile + j)
		}
		appendFlush(t, s, "svc", clk.Now(), seqs...)
	}
	require.Greater(t, 3*perFile, mergeRowGroupLen)

	clk.Advance(5 * time.Minute)
	require.NoError(t, s.Maintain(t.Context()))

	files := readManifest(t, dataDir, "svc").Files
	require.Len(t, files, 1)
	require.EqualValues(t, 3*perFile, files[0].Rows)
	pf, err := file.OpenParquetFile(filepath.Join(dataDir, "services", ServiceDir("svc"), files[0].Path), false)
	require.NoError(t, err)
	defer func() { require.NoError(t, pf.Close()) }()
	require.Greater(t, pf.NumRowGroups(), 1)
	require.EqualValues(t, 3*perFile, pf.NumRows())
}

func TestDiskCapRefusesWritesFromOpenHour(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now, MaxDiskBytes: 1})

	appendFlush(t, s, "svc", clk.Now(), 1)
	require.NoError(t, s.Maintain(t.Context()))
	require.True(t, s.Stats().DiskCapExceeded)

	_, _, err := s.AppendLogs(t.Context(), seqLogs("svc", clk.Now(), 2))
	require.ErrorIs(t, err, ErrFull)
	require.ErrorContains(t, err, "disk cap")
	_, _, err = s.AppendTraces(t.Context(), seqSpans("svc", clk.Now(), 2))
	require.ErrorIs(t, err, ErrFull)
	require.EqualValues(t, 1, s.Stats().RecordsAccepted)

	// Once the hour closes the cap retires it, which frees the space.
	clk.Advance(time.Hour)
	require.NoError(t, s.Maintain(t.Context()))
	require.False(t, s.Stats().DiskCapExceeded)
	_, _, err = s.AppendLogs(t.Context(), seqLogs("svc", clk.Now(), 3))
	require.NoError(t, err)
}

func TestHourlyCompaction(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now, CompactInterval: time.Second, FlushInterval: 2 * time.Second})
	for i := range 5 {
		appendFlush(t, s, "svc", baseTime, int64(i))
	}

	clk.Set(time.Date(2026, 11, 20, 14, 0, 1, 0, time.UTC))
	require.NoError(t, s.Maintain(t.Context()))
	require.Len(t, readManifest(t, dataDir, "svc").Files, 5)

	clk.Set(time.Date(2026, 11, 20, 14, 0, 5, 0, time.UTC))
	require.NoError(t, s.Maintain(t.Context()))
	files := readManifest(t, dataDir, "svc").Files
	require.Len(t, files, 1)
	require.True(t, strings.HasPrefix(filepath.Base(files[0].Path), "hour-"), files[0].Path)
	require.EqualValues(t, 5, files[0].Rows)
	e := spansEngine(t, dataDir)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, listedSeqs(t, e, dataDir, "svc", signalLogs))
}

func TestCompactSkipsSchemaMismatch(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now, SealMinFiles: 3})
	for i := range 3 {
		appendFlush(t, s, "svc", baseTime, int64(i))
	}
	before := readManifest(t, dataDir, "svc")

	b := table.NewLogsBuilder(memory.DefaultAllocator)
	lr := seqLogs("svc", baseTime, 99).ResourceLogs().At(0)
	b.Append(lr.Resource(), lr.ScopeLogs().At(0).Scope(), lr.ScopeLogs().At(0).LogRecords().At(0), baseTime)
	rec := b.NewRecord()
	defer rec.Release()
	meta := arrow.NewMetadata([]string{table.SchemaVersionKey, table.SignalKey}, []string{"999", "logs"})
	odd := array.NewRecordBatch(arrow.NewSchema(rec.Schema().Fields(), &meta), rec.Columns(), rec.NumRows())
	defer odd.Release()
	target := filepath.Join(dataDir, "services", ServiceDir("svc"), before.Files[1].Path)
	_, err := writeParquet(target+".new", odd)
	require.NoError(t, err)
	require.NoError(t, os.Rename(target+".new", target))

	clk.Advance(5 * time.Minute)
	require.NoError(t, s.Maintain(t.Context()))
	after := readManifest(t, dataDir, "svc")
	require.Equal(t, before.Files, after.Files)
	require.Empty(t, after.Retired)
	require.Zero(t, s.Stats().Compactions)
	require.Len(t, parquetFiles(t, dataDir), 3)
}

func TestCompactionCrashSafety(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s, _ := sealFixture(t, dataDir, clk)
	require.Len(t, readManifest(t, dataDir, "svc").Retired, 12)
	require.Len(t, parquetFiles(t, dataDir), 13)
	require.NoError(t, s.Close())

	s = openStore(t, Options{DataDir: dataDir, Now: clk.Now})
	m := readManifest(t, dataDir, "svc")
	require.Empty(t, m.Retired)
	require.Len(t, m.Files, 1)
	require.Len(t, parquetFiles(t, dataDir), 1)
	require.EqualValues(t, 12, s.Stats().FilesRetired)

	e := spansEngine(t, dataDir)
	seqs := listedSeqs(t, e, dataDir, "svc", signalLogs)
	require.Len(t, seqs, 12)
	require.Len(t, slices.Compact(slices.Clone(seqs)), 12)
}

func TestUncommittedMergeDiscardedOnRestart(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now})
	for i := range 3 {
		appendFlush(t, s, "svc", clk.Now(), int64(i))
	}
	require.NoError(t, s.Close())

	// Simulate a crash after the merged file was renamed into place but before the manifest publish.
	files := listedFiles(t, dataDir, "svc", signalLogs)
	require.Len(t, files, 3)
	svcDir := filepath.Join(dataDir, "services", ServiceDir("svc"))
	src, err := os.ReadFile(filepath.Join(svcDir, files[0].Path))
	require.NoError(t, err)
	orphan := filepath.Join(svcDir, filepath.Dir(files[0].Path), sealedPrefix+"orphan.parquet")
	require.NoError(t, os.WriteFile(orphan, src, 0o600))

	s = openStore(t, Options{DataDir: dataDir, Now: clk.Now})
	require.NoFileExists(t, orphan)
	require.Len(t, listedFiles(t, dataDir, "svc", signalLogs), 3)

	e := spansEngine(t, dataDir)
	seqs := listedSeqs(t, e, dataDir, "svc", signalLogs)
	require.ElementsMatch(t, []int64{0, 1, 2}, seqs)
	require.NoError(t, s.Close())
}

func TestRetiredGrace(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s, _ := sealFixture(t, dataDir, clk)
	require.Len(t, parquetFiles(t, dataDir), 13)

	clk.Advance(59 * time.Second)
	require.NoError(t, s.Maintain(t.Context()))
	require.Len(t, parquetFiles(t, dataDir), 13)
	require.Len(t, readManifest(t, dataDir, "svc").Retired, 12)

	clk.Advance(time.Second)
	require.NoError(t, s.Maintain(t.Context()))
	require.Len(t, parquetFiles(t, dataDir), 1)
	require.Empty(t, readManifest(t, dataDir, "svc").Retired)
	require.EqualValues(t, 12, s.Stats().FilesRetired)
}

func TestRetainByAge(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now, Retention: 2 * time.Hour})
	for i := range 3 {
		appendFlush(t, s, "svc", clk.Now(), int64(i))
		clk.Advance(time.Hour)
	}
	require.Len(t, readManifest(t, dataDir, "svc").Files, 3)

	clk.Set(time.Date(2026, 11, 20, 17, 30, 0, 0, time.UTC))
	require.NoError(t, s.Maintain(t.Context()))
	m := readManifest(t, dataDir, "svc")
	require.Len(t, m.Files, 1)
	require.Contains(t, m.Files[0].Path, "hour=15")
	require.Len(t, m.Retired, 2)
	require.Equal(t, clk.Now().Add(60*time.Second), m.Retired[0].DeleteAfter)
}

func TestRetainSizeCap(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now})
	for i := range 3 {
		appendFlush(t, s, "a", clk.Now(), int64(i))
		appendFlush(t, s, "b", clk.Now(), int64(i))
		if i < 2 {
			clk.Advance(time.Hour)
		}
	}
	hours := func(name string) []string {
		var out []string
		for _, f := range readManifest(t, dataDir, name).Files {
			out = append(out, filepath.Base(filepath.Dir(f.Path)))
		}

		return out
	}
	total := s.Stats().BytesOnDisk
	require.Positive(t, total)
	a13 := readManifest(t, dataDir, "a").Files[0].Bytes

	// Dropping only the oldest partition of the first service is enough, so the other service keeps its own.
	s.opts.MaxDiskBytes = total - a13
	require.NoError(t, s.Maintain(t.Context()))
	require.Equal(t, []string{"hour=14", "hour=15"}, hours("a"))
	require.Equal(t, []string{"hour=13", "hour=14", "hour=15"}, hours("b"))

	// The current open hour (15) is never retired, even over the cap.
	clk.Advance(6 * time.Minute)
	s.opts.MaxDiskBytes = 1
	require.NoError(t, s.Maintain(t.Context()))
	require.Equal(t, []string{"hour=15"}, hours("a"))
	require.Equal(t, []string{"hour=15"}, hours("b"))
}

func TestServiceRemoval(t *testing.T) {
	dataDir := newDataDir(t)
	clk := newClock(baseTime)
	s := openStore(t, Options{DataDir: dataDir, Now: clk.Now, MaxServices: 1})
	appendFlush(t, s, "gone", baseTime, 1)
	require.NoError(t, s.Close())

	clk.Set(time.Date(2026, 11, 20, 20, 0, 0, 0, time.UTC))
	s = openStore(t, Options{DataDir: dataDir, Now: clk.Now, MaxServices: 1, Retention: time.Hour})
	svcDir := filepath.Join(dataDir, "services", ServiceDir("gone"))
	require.NoError(t, s.Maintain(t.Context()))
	require.DirExists(t, svcDir)
	require.Empty(t, readManifest(t, dataDir, "gone").Files)

	clk.Advance(time.Minute)
	require.NoError(t, s.Maintain(t.Context()))
	require.NoDirExists(t, svcDir)
	require.Zero(t, s.Stats().Services)

	rejected, _, err := s.AppendLogs(t.Context(), seqLogs("fresh", clk.Now(), 1))
	require.NoError(t, err)
	require.Zero(t, rejected)
	require.Equal(t, 1, s.Stats().Services)
}

const (
	soakHours = 48
	// A step is 30 simulated minutes: real fsyncs cost ~5ms each on macOS, so per-minute steps would run for minutes.
	soakStep = 30 * time.Minute
	// The first hours step at one compact interval with enough tails per pass to seal every time, which is what bounds the open hour's sealed files.
	soakFineHours  = 2
	soakCheckEvery = 6 * time.Hour
	soakSealMin    = 2
	soakRetention  = 12 * time.Hour
	soakFlush      = time.Second
	soakCompact    = 5 * time.Minute
	soakGrace      = 60 * time.Second
	soakRecords    = 3
	// Measured at ~60 KiB per closed hour across all services and signals, so the cap binds near 5 hours, well before the 12h retention does.
	soakMaxDisk = 300 << 10
)

type soakSeq struct {
	seq  int64
	hour time.Time
}

func TestSoakSimulated(t *testing.T) {
	dataDir := newDataDir(t)
	start := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	clk := newClock(start)
	s := openStore(t, Options{
		DataDir: dataDir, Now: clk.Now, Retention: soakRetention, MaxDiskBytes: soakMaxDisk, SealMinFiles: soakSealMin,
		FlushInterval: soakFlush, CompactInterval: soakCompact, RetireGrace: soakGrace,
	})
	e := spansEngine(t, dataDir)

	services := []string{"alpha", "beta", "gamma"}
	sent := map[string][]soakSeq{}
	var next int64
	began := time.Now()
	nextCheck := start.Add(soakCheckEvery)
	for clk.Now().Before(start.Add(soakHours * time.Hour)) {
		now := clk.Now()
		step, rounds := soakStep, 1
		if now.Before(start.Add(soakFineHours * time.Hour)) {
			step, rounds = soakCompact, soakSealMin
		}
		for round := range rounds {
			soakAppend(t, s, services, sent, &next, now)
			// The last round's tails are flushed by Maintain itself, just before the pass.
			if round < rounds-1 {
				require.NoError(t, s.Flush(t.Context()))
			}
		}
		require.NoError(t, s.Maintain(t.Context()))
		for _, name := range services {
			for _, sig := range signals {
				checkPartitions(t, listedFiles(t, dataDir, name, sig), now, rounds)
			}
		}
		if !now.Before(nextCheck) {
			checkSoak(t, s, e, dataDir, services, sent, now, rounds)
			nextCheck = nextCheck.Add(soakCheckEvery)
		}
		clk.Advance(step)
	}

	st := s.Stats()
	var files, rows int
	for _, name := range services {
		m := readManifest(t, dataDir, name)
		files += len(m.Files)
		for _, f := range m.Files {
			rows += int(f.Rows)
		}
	}
	t.Logf("soak runtime=%s files=%d bytes=%d rows=%d compactions=%d retired=%d flushes=%d", time.Since(began).Round(time.Millisecond), files, st.BytesOnDisk, rows, st.Compactions, st.FilesRetired, st.Flushes)
	require.Zero(t, st.WriteErrors)
	require.Zero(t, st.PendingBatches)
}

func soakAppend(t *testing.T, s *Store, services []string, sent map[string][]soakSeq, next *int64, now time.Time) {
	t.Helper()

	for _, name := range services {
		for _, sig := range signals {
			seqs := make([]int64, soakRecords)
			for i := range seqs {
				*next++
				seqs[i] = *next
				sent[name+"/"+sig] = append(sent[name+"/"+sig], soakSeq{seq: *next, hour: now.Truncate(time.Hour)})
			}
			var err error
			if sig == signalLogs {
				_, _, err = s.AppendLogs(t.Context(), seqLogs(name, now, seqs...))
			} else {
				_, _, err = s.AppendTraces(t.Context(), seqSpans(name, now, seqs...))
			}
			require.NoError(t, err)
		}
	}
}

func checkSoak(t *testing.T, s *Store, e *engine.Engine, dataDir string, services []string, sent map[string][]soakSeq, now time.Time, flushedSincePass int) {
	t.Helper()

	var totalFiles int
	var totalBytes int64
	hourBytes := map[time.Time]int64{}
	onDisk := map[string]bool{}
	for _, name := range services {
		m := readManifest(t, dataDir, name)
		totalFiles += len(m.Files)
		svcDir := filepath.Join(dataDir, "services", ServiceDir(name))
		for _, f := range m.Files {
			totalBytes += f.Bytes
			h, ok := partitionHour(f.Path)
			require.True(t, ok, f.Path)
			hourBytes[h] += f.Bytes
			onDisk[filepath.Join(svcDir, f.Path)] = true
		}
		for _, r := range m.Retired {
			onDisk[filepath.Join(svcDir, r.Path)] = true
		}
		for _, sig := range signals {
			oldest := checkPartitions(t, listedFiles(t, dataDir, name, sig), now, flushedSincePass)
			// Retention and the size cap drop whole hours, so every row at or after the oldest kept hour must be present exactly once.
			var want []int64
			for _, r := range sent[name+"/"+sig] {
				if !r.hour.Before(oldest) {
					want = append(want, r.seq)
				}
			}
			require.Equal(t, want, listedSeqs(t, e, dataDir, name, sig), "%s/%s at %s", name, sig, now)
		}
	}

	hoursRetained := int(soakRetention/time.Hour) + 1
	require.LessOrEqual(t, totalFiles, (hoursRetained+soakSealMin)*len(services)*len(signals), "at %s", now)

	var largest int64
	for _, b := range hourBytes {
		largest = max(largest, b)
	}
	require.LessOrEqual(t, totalBytes, int64(soakMaxDisk)+largest, "at %s", now)

	for _, p := range parquetFiles(t, dataDir) {
		require.True(t, onDisk[p], "orphan parquet %s", p)
	}
	require.Equal(t, totalBytes, s.Stats().BytesOnDisk)
}

// checkPartitions asserts the file-count shape of one service and signal and returns its oldest listed hour.
// A pass merges the open hour into at most soakSealMin sealed files, so only the tails flushed since the last pass (plus fewer than soakSealMin it left) can be above that.
func checkPartitions(t *testing.T, files []manifest.File, now time.Time, flushedSincePass int) time.Time {
	t.Helper()

	open := now.Truncate(time.Hour)
	perPart := map[string][]string{}
	for _, f := range files {
		perPart[filepath.Dir(f.Path)] = append(perPart[filepath.Dir(f.Path)], filepath.Base(f.Path))
	}
	oldest := open
	for dir, names := range perPart {
		h, ok := partitionHour(dir + "/x")
		require.True(t, ok, dir)
		oldest = minTime(oldest, h)
		require.True(t, h.Add(time.Hour).After(now.Add(-soakRetention)), "%s outlived retention at %s", dir, now)
		switch {
		case h.Equal(open):
			tails, sealed := 0, 0
			for _, n := range names {
				switch {
				case strings.HasPrefix(n, tailPrefix):
					tails++
				case strings.HasPrefix(n, sealedPrefix):
					sealed++
				}
			}
			require.LessOrEqual(t, sealed, soakSealMin, "%s: %v", dir, names)
			require.LessOrEqual(t, tails, flushedSincePass+soakSealMin, "%s: %v", dir, names)
		case !now.Before(h.Add(time.Hour + 2*soakFlush + soakStep)):
			require.Len(t, names, 1, "%s: %v", dir, names)
		}
	}

	return oldest
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}

	return a
}
