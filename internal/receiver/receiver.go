package receiver

import (
	"compress/gzip"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

const (
	DefaultAddr        = "127.0.0.1:4318"
	DefaultMaxBody     = 8 << 20
	DefaultMaxInFlight = 16

	mimeProto = "application/x-protobuf"
	mimeJSON  = "application/json"
)

// ErrFull is wrapped by a sink to signal backpressure.
var ErrFull = errors.New("buffer full")

// LogsSink stores a decoded batch and reports how many records it rejected.
type LogsSink func(ctx context.Context, ld plog.Logs) (rejected int64, msg string, err error)

// TracesSink stores a decoded batch and reports how many spans it rejected.
type TracesSink func(ctx context.Context, td ptrace.Traces) (rejected int64, msg string, err error)

// Sinks holds one sink per signal; a nil sink makes that signal's route return 404.
type Sinks struct {
	Logs   LogsSink
	Traces TracesSink
}

// codec is the decode surface shared by the OTLP export requests.
type codec interface {
	UnmarshalJSON([]byte) error
	UnmarshalProto([]byte) error
}

// encoder is the encode surface shared by the OTLP export responses.
type encoder interface {
	MarshalJSON() ([]byte, error)
	MarshalProto() ([]byte, error)
}

type Options struct {
	Addr        string
	UnsafeBind  bool
	MaxBody     int64
	MaxInFlight int
	Token       string
	Logger      *slog.Logger
}

type Stats struct {
	Requests        int64
	RecordsAccepted int64
	RecordsRejected int64
	SpansAccepted   int64
	SpansRejected   int64
	Responses429    int64
	MetricsDropped  int64
	Unauthorized    int64
}

type Server struct {
	srv     *http.Server
	ln      net.Listener
	sinks   Sinks
	log     *slog.Logger
	maxBody int64
	slots   chan struct{}
	hosts   map[string]struct{}
	auth    []byte

	requests, accepted, rejected, spansAccepted, spansRejected, full, metrics, unauthorized atomic.Int64
}

// New validates the bind address and binds the listener, so a bad address fails before serving.
func New(opts Options, sinks Sinks) (*Server, error) {
	if opts.Addr == "" {
		opts.Addr = DefaultAddr
	}
	if opts.MaxBody <= 0 {
		opts.MaxBody = DefaultMaxBody
	}
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = DefaultMaxInFlight
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	host, err := CheckAddr(opts.Addr, opts.UnsafeBind)
	if err != nil {
		return nil, err
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %q: %w", opts.Addr, err)
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	s := &Server{
		ln:      ln,
		sinks:   sinks,
		log:     opts.Logger,
		maxBody: opts.MaxBody,
		slots:   make(chan struct{}, opts.MaxInFlight),
		hosts: map[string]struct{}{
			net.JoinHostPort("localhost", port): {},
			net.JoinHostPort("127.0.0.1", port): {},
		},
	}
	if opts.Token != "" {
		s.auth = []byte("Bearer " + opts.Token)
	}
	// Remote clients send the server's own address as Host, so an opted-in unsafe bind must allow it.
	if opts.UnsafeBind {
		s.hosts[net.JoinHostPort(host, port)] = struct{}{}
	}
	s.srv = &http.Server{
		Handler:           http.HandlerFunc(s.handle),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	return s, nil
}

// CheckAddr lets callers such as service install reject a bad bind before serve ever runs.
func CheckAddr(addr string, unsafeBind bool) (host string, err error) {
	host, _, err = net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid receiver address %q: %w", addr, err)
	}
	if !unsafeBind && !isLoopback(host) {
		return "", fmt.Errorf("refusing non-loopback address %q without unsafe bind", addr)
	}

	return host, nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) Addr() string { return s.ln.Addr().String() }

// Serve blocks until ctx is done, then shuts down gracefully.
func (s *Server) Serve(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() { errc <- s.srv.Serve(s.ln) }()
	select {
	case err := <-errc:
		return fmt.Errorf("receiver stopped: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("failed to shut down receiver: %w", err)
	}
	return nil
}

func (s *Server) Stats() Stats {
	return Stats{
		Requests:        s.requests.Load(),
		RecordsAccepted: s.accepted.Load(),
		RecordsRejected: s.rejected.Load(),
		SpansAccepted:   s.spansAccepted.Load(),
		SpansRejected:   s.spansRejected.Load(),
		Responses429:    s.full.Load(),
		MetricsDropped:  s.metrics.Load(),
		Unauthorized:    s.unauthorized.Load(),
	}
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	if !s.routed(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := r.Header["Origin"]; ok {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if _, ok := s.hosts[r.Host]; !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if s.auth != nil && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), s.auth) != 1 {
		s.unauthorized.Add(1)
		// RFC 9110 requires a challenge on every 401.
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt != mimeProto && mt != mimeJSON {
		http.Error(w, "content type must be application/x-protobuf or application/json", http.StatusUnsupportedMediaType)
		return
	}
	// Bodies are buffered in memory, so concurrency is capped before reading.
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		s.full.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many requests in flight", http.StatusTooManyRequests)
		return
	}
	body, status, err := s.readBody(w, r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	switch r.URL.Path {
	case "/v1/metrics":
		s.metrics.Add(1)
		s.respond(w, mt, pmetricotlp.NewExportResponse())
	case "/v1/logs":
		s.logs(w, r, mt, body)
	default:
		s.traces(w, r, mt, body)
	}
}

func (s *Server) routed(p string) bool {
	switch p {
	case "/v1/metrics":
		return true
	case "/v1/logs":
		return s.sinks.Logs != nil
	case "/v1/traces":
		return s.sinks.Traces != nil
	default:
		return false
	}
}

// decode answers 400 itself on failure so callers only handle success.
func decode(w http.ResponseWriter, mt string, body []byte, req codec) bool {
	var err error
	if mt == mimeJSON {
		err = req.UnmarshalJSON(body)
	} else {
		err = req.UnmarshalProto(body)
	}
	if err != nil {
		http.Error(w, "malformed request", http.StatusBadRequest)
		return false
	}
	return true
}

// sinkFailed answers 429 or 503 itself and reports whether the sink failed.
func (s *Server) sinkFailed(w http.ResponseWriter, r *http.Request, err error, what string) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrFull) {
		s.full.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "buffer full", http.StatusTooManyRequests)
		return true
	}
	s.log.ErrorContext(r.Context(), "failed to store "+what, "error", err)
	http.Error(w, "unavailable", http.StatusServiceUnavailable)
	return true
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request, mt string, body []byte) {
	req := plogotlp.NewExportRequest()
	if !decode(w, mt, body, req) {
		return
	}
	logs := req.Logs()
	total := int64(logs.LogRecordCount())
	rejected, msg, err := s.sinks.Logs(r.Context(), logs)
	if s.sinkFailed(w, r, err, "logs") {
		return
	}
	s.accepted.Add(total - rejected)
	s.rejected.Add(rejected)
	resp := plogotlp.NewExportResponse()
	if rejected > 0 {
		resp.PartialSuccess().SetRejectedLogRecords(rejected)
		resp.PartialSuccess().SetErrorMessage(msg)
	}
	s.respond(w, mt, resp)
}

func (s *Server) traces(w http.ResponseWriter, r *http.Request, mt string, body []byte) {
	req := ptraceotlp.NewExportRequest()
	if !decode(w, mt, body, req) {
		return
	}
	td := req.Traces()
	total := int64(td.SpanCount())
	rejected, msg, err := s.sinks.Traces(r.Context(), td)
	if s.sinkFailed(w, r, err, "traces") {
		return
	}
	s.spansAccepted.Add(total - rejected)
	s.spansRejected.Add(rejected)
	resp := ptraceotlp.NewExportResponse()
	if rejected > 0 {
		resp.PartialSuccess().SetRejectedSpans(rejected)
		resp.PartialSuccess().SetErrorMessage(msg)
	}
	s.respond(w, mt, resp)
}

func (s *Server) respond(w http.ResponseWriter, mt string, resp encoder) {
	var out []byte
	var err error
	if mt == mimeJSON {
		out, err = resp.MarshalJSON()
	} else {
		out, err = resp.MarshalProto()
	}
	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", mt)
	_, _ = w.Write(out)
}

func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, int, error) {
	var src io.Reader = http.MaxBytesReader(w, r.Body, s.maxBody)
	switch strings.ToLower(r.Header.Get("Content-Encoding")) {
	case "", "identity":
	case "gzip":
		zr, err := gzip.NewReader(src)
		if err != nil {
			return nil, http.StatusBadRequest, errors.New("malformed gzip body")
		}
		defer zr.Close()
		// The cap applies after decompression, so a small gzip bomb can't expand past it.
		src = http.MaxBytesReader(w, zr, s.maxBody)
	default:
		return nil, http.StatusUnsupportedMediaType, errors.New("unsupported content encoding")
	}
	b, err := io.ReadAll(src)
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return nil, http.StatusRequestEntityTooLarge, errors.New("request too large")
		}
		return nil, http.StatusBadRequest, errors.New("failed to read body")
	}
	return b, 0, nil
}
