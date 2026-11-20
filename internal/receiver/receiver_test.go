package receiver_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/quacktel/cli/internal/receiver"
)

type fixture struct {
	srv  *receiver.Server
	mu   sync.Mutex
	got  []plog.Logs
	sink receiver.Sink
}

func (f *fixture) calls() []plog.Logs {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got
}

func start(t *testing.T, maxBody int64, sink receiver.Sink) *fixture {
	t.Helper()
	return startWith(t, receiver.Options{Addr: "127.0.0.1:0", MaxBody: maxBody}, sink)
}

func startWith(t *testing.T, opts receiver.Options, sink receiver.Sink) *fixture {
	t.Helper()
	f := &fixture{}
	f.sink = func(ctx context.Context, logs plog.Logs) (int64, string, error) {
		f.mu.Lock()
		f.got = append(f.got, logs)
		f.mu.Unlock()
		if sink != nil {
			return sink(ctx, logs)
		}
		return 0, "", nil
	}
	srv, err := receiver.New(opts, f.sink)
	require.NoError(t, err)
	f.srv = srv
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	return f
}

type req struct {
	method, path, host, ctype, encoding, origin string
	body                                        []byte
}

type result struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func (f *fixture) do(t *testing.T, r req) result {
	t.Helper()
	if r.method == "" {
		r.method = http.MethodPost
	}
	if r.path == "" {
		r.path = "/v1/logs"
	}
	if r.ctype == "" {
		r.ctype = "application/x-protobuf"
	}
	hr, err := http.NewRequestWithContext(t.Context(), r.method, "http://"+f.srv.Addr()+r.path, bytes.NewReader(r.body))
	require.NoError(t, err)
	hr.Header.Set("Content-Type", r.ctype)
	if r.encoding != "" {
		hr.Header.Set("Content-Encoding", r.encoding)
	}
	if r.origin != "" {
		hr.Header.Set("Origin", r.origin)
	}
	if r.host != "" {
		hr.Host = r.host
	}
	resp, err := http.DefaultClient.Do(hr)
	require.NoError(t, err)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return result{StatusCode: resp.StatusCode, Header: resp.Header, Body: b}
}

func port(f *fixture) string {
	_, p, _ := net.SplitHostPort(f.srv.Addr())
	return p
}

func sampleProto(t *testing.T, n int) []byte {
	t.Helper()
	req := plogotlp.NewExportRequest()
	recs := req.Logs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for range n {
		recs.AppendEmpty().Body().SetStr("hello")
	}
	b, err := req.MarshalProto()
	require.NoError(t, err)
	return b
}

func gz(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(b)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestGates(t *testing.T) {
	f := start(t, 0, nil)
	p := port(f)
	body := sampleProto(t, 1)
	tests := []struct {
		name string
		req  req
		want int
	}{
		{"origin", req{origin: "http://evil.example", body: body}, http.StatusForbidden},
		{"wrong host", req{host: "evil.example:" + p, body: body}, http.StatusForbidden},
		{"wrong port", req{host: "127.0.0.1:1", body: body}, http.StatusForbidden},
		{"no port", req{host: "localhost", body: body}, http.StatusForbidden},
		{"text/plain", req{ctype: "text/plain", body: body}, http.StatusUnsupportedMediaType},
		{"br", req{encoding: "br", body: body}, http.StatusUnsupportedMediaType},
		{"get", req{method: http.MethodGet}, http.StatusMethodNotAllowed},
		{"unknown path", req{path: "/nope", body: body}, http.StatusNotFound},
		{"traces", req{path: "/v1/traces", body: body}, http.StatusNotFound},
		{"localhost host ok", req{host: "localhost:" + p, body: body}, http.StatusOK},
		{"malformed proto", req{body: []byte{0xff, 0xff, 0xff}}, http.StatusBadRequest},
		{"malformed json", req{ctype: "application/json", body: []byte("{nope")}, http.StatusBadRequest},
		{"json charset ok", req{ctype: "application/json; charset=utf-8", body: []byte("{}")}, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := f.do(t, tt.req)
			require.Equal(t, tt.want, resp.StatusCode)
			if tt.want == http.StatusMethodNotAllowed {
				require.Equal(t, "POST", resp.Header.Get("Allow"))
			}
		})
	}
	// Gates must reject before the sink sees anything.
	require.Len(t, f.calls(), 2)
}

func TestSizeCaps(t *testing.T) {
	f := start(t, 1024, nil)
	big := bytes.Repeat([]byte{0}, 64<<10)
	bomb := gz(t, big)
	require.Less(t, len(bomb), 1024)

	require.Equal(t, http.StatusRequestEntityTooLarge, f.do(t, req{body: big}).StatusCode)
	require.Equal(t, http.StatusRequestEntityTooLarge, f.do(t, req{encoding: "gzip", body: bomb}).StatusCode)
	require.Equal(t, http.StatusOK, f.do(t, req{encoding: "gzip", body: gz(t, sampleProto(t, 1))}).StatusCode)
	require.Empty(t, f.calls()[1:])
}

func TestJSONDecoding(t *testing.T) {
	f := start(t, 0, nil)
	body := `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"svc"}}]},
	"scopeLogs":[{"logRecords":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174",
	"severityNumber":9,"eventName":"my.event","body":{"stringValue":"hi"}}]}]}]}`
	resp := f.do(t, req{ctype: "application/json", body: []byte(body)})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	calls := f.calls()
	require.Len(t, calls, 1)
	rl := calls[0].ResourceLogs().At(0)
	svc, ok := rl.Resource().Attributes().Get("service.name")
	require.True(t, ok)
	require.Equal(t, "svc", svc.Str())
	lr := rl.ScopeLogs().At(0).LogRecords().At(0)
	require.Equal(t, "5b8efff798038103d269b633813fc60c", lr.TraceID().String())
	require.Equal(t, "eee19b7ec3c1b174", lr.SpanID().String())
	require.EqualValues(t, 9, lr.SeverityNumber())
	require.Equal(t, "my.event", lr.EventName())
}

func TestResponseEncoding(t *testing.T) {
	f := start(t, 0, nil)
	resp := f.do(t, req{body: sampleProto(t, 1)})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/x-protobuf", resp.Header.Get("Content-Type"))
	require.NoError(t, plogotlp.NewExportResponse().UnmarshalProto(resp.Body))

	resp = f.do(t, req{ctype: "application/json", body: []byte("{}")})
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	require.NoError(t, plogotlp.NewExportResponse().UnmarshalJSON(resp.Body))
}

func TestSinkOutcomes(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		f := start(t, 0, func(context.Context, plog.Logs) (int64, string, error) {
			return 0, "", fmt.Errorf("wrapped: %w", receiver.ErrFull)
		})
		resp := f.do(t, req{body: sampleProto(t, 1)})
		require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		require.Equal(t, "1", resp.Header.Get("Retry-After"))
		require.EqualValues(t, 1, f.srv.Stats().Responses429)
	})
	t.Run("error", func(t *testing.T) {
		f := start(t, 0, func(context.Context, plog.Logs) (int64, string, error) {
			return 0, "", io.ErrUnexpectedEOF
		})
		require.Equal(t, http.StatusServiceUnavailable, f.do(t, req{body: sampleProto(t, 1)}).StatusCode)
	})
	t.Run("partial", func(t *testing.T) {
		f := start(t, 0, func(context.Context, plog.Logs) (int64, string, error) {
			return 2, "bad records", nil
		})
		resp := f.do(t, req{body: sampleProto(t, 5)})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		out := plogotlp.NewExportResponse()
		require.NoError(t, out.UnmarshalProto(resp.Body))
		require.EqualValues(t, 2, out.PartialSuccess().RejectedLogRecords())
		require.Equal(t, "bad records", out.PartialSuccess().ErrorMessage())
		st := f.srv.Stats()
		require.EqualValues(t, 3, st.RecordsAccepted)
		require.EqualValues(t, 2, st.RecordsRejected)
		require.EqualValues(t, 1, st.Requests)
	})
}

func TestInFlightCap(t *testing.T) {
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	f := startWith(t, receiver.Options{Addr: "127.0.0.1:0", MaxInFlight: 2}, func(context.Context, plog.Logs) (int64, string, error) {
		arrived <- struct{}{}
		<-release
		return 0, "", nil
	})

	body := sampleProto(t, 1)
	statuses := make(chan int, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			hr, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+f.srv.Addr()+"/v1/logs", bytes.NewReader(body))
			if err != nil {
				errs <- err
				return
			}
			hr.Header.Set("Content-Type", "application/x-protobuf")
			resp, err := http.DefaultClient.Do(hr)
			if err != nil {
				errs <- err
				return
			}
			statuses <- resp.StatusCode
			errs <- resp.Body.Close()
		}()
	}
	<-arrived
	<-arrived

	resp := f.do(t, req{body: sampleProto(t, 1)})
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	require.Equal(t, "1", resp.Header.Get("Retry-After"))
	require.EqualValues(t, 1, f.srv.Stats().Responses429)

	close(release)
	for range 2 {
		require.Equal(t, http.StatusOK, <-statuses)
		require.NoError(t, <-errs)
	}
	// Slots are released as handlers finish, so a later request gets through.
	require.Equal(t, http.StatusOK, f.do(t, req{body: sampleProto(t, 1)}).StatusCode)
}

func TestMetricsDropped(t *testing.T) {
	f := start(t, 0, nil)
	resp := f.do(t, req{path: "/v1/metrics", body: []byte("anything")})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 1, f.srv.Stats().MetricsDropped)
	require.Empty(t, f.calls())

	resp = f.do(t, req{path: "/v1/metrics", origin: "http://x", body: []byte("x")})
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.EqualValues(t, 1, f.srv.Stats().MetricsDropped)
}

func TestBind(t *testing.T) {
	sink := func(context.Context, plog.Logs) (int64, string, error) { return 0, "", nil }

	for _, addr := range []string{"0.0.0.0:0", "example.com:4318", ":0"} {
		_, err := receiver.New(receiver.Options{Addr: addr}, sink)
		require.Error(t, err, addr)
	}

	s, err := receiver.New(receiver.Options{Addr: "0.0.0.0:0", UnsafeBind: true}, sink)
	require.NoError(t, err)
	_, p, err := net.SplitHostPort(s.Addr())
	require.NoError(t, err)
	require.NotEqual(t, "0", p)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, s.Serve(ctx))

	s, err = receiver.New(receiver.Options{Addr: "localhost:0"}, sink)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(s.Addr(), "127.0.0.1:") || strings.HasPrefix(s.Addr(), "[::1]:"))
	require.NoError(t, s.Serve(ctx))
}

func TestUnsafeBindHost(t *testing.T) {
	// macOS has no 127.0.0.2 alias, so fall back to the IPv6 loopback, which is also outside the default Host set.
	host := ""
	for _, h := range []string{"127.0.0.2", "::1"} {
		if ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", net.JoinHostPort(h, "0")); err == nil {
			require.NoError(t, ln.Close())
			host = h
			break
		}
	}
	if host == "" {
		t.Skip("no alternate loopback address available")
	}

	for _, unsafe := range []bool{true, false} {
		sink := func(context.Context, plog.Logs) (int64, string, error) { return 0, "", nil }
		srv, err := receiver.New(receiver.Options{Addr: net.JoinHostPort(host, "0"), UnsafeBind: unsafe}, sink)
		require.NoError(t, err)
		f := &fixture{srv: srv}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- srv.Serve(ctx) }()

		_, p, _ := net.SplitHostPort(srv.Addr())
		resp := f.do(t, req{host: net.JoinHostPort(host, p), body: sampleProto(t, 1)})
		if unsafe {
			require.Equal(t, http.StatusOK, resp.StatusCode)
		} else {
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
		}
		resp = f.do(t, req{host: "evil.example:" + p, body: sampleProto(t, 1)})
		require.Equal(t, http.StatusForbidden, resp.StatusCode)

		cancel()
		require.NoError(t, <-done)
	}
}
