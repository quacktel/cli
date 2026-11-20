package receiver

import (
	"compress/gzip"
	"context"
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
)

const (
	DefaultAddr    = "127.0.0.1:4318"
	DefaultMaxBody = 8 << 20

	mimeProto = "application/x-protobuf"
	mimeJSON  = "application/json"
)

// ErrFull is wrapped by a sink to signal backpressure.
var ErrFull = errors.New("buffer full")

// Sink stores a decoded batch and reports how many records it rejected.
type Sink func(ctx context.Context, logs plog.Logs) (rejected int64, msg string, err error)

type Options struct {
	Addr       string
	UnsafeBind bool
	MaxBody    int64
	Logger     *slog.Logger
}

type Stats struct {
	Requests        int64
	RecordsAccepted int64
	RecordsRejected int64
	Responses429    int64
	MetricsDropped  int64
}

type Server struct {
	srv     *http.Server
	ln      net.Listener
	sink    Sink
	log     *slog.Logger
	maxBody int64
	hosts   map[string]struct{}

	requests, accepted, rejected, full, metrics atomic.Int64
}

// New validates the bind address and binds the listener, so a bad address fails before serving.
func New(opts Options, sink Sink) (*Server, error) {
	if opts.Addr == "" {
		opts.Addr = DefaultAddr
	}
	if opts.MaxBody <= 0 {
		opts.MaxBody = DefaultMaxBody
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	host, _, err := net.SplitHostPort(opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("invalid receiver address %q: %w", opts.Addr, err)
	}
	if !opts.UnsafeBind && !isLoopback(host) {
		return nil, fmt.Errorf("refusing non-loopback address %q without unsafe bind", opts.Addr)
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %q: %w", opts.Addr, err)
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	s := &Server{
		ln:      ln,
		sink:    sink,
		log:     opts.Logger,
		maxBody: opts.MaxBody,
		hosts: map[string]struct{}{
			net.JoinHostPort("localhost", port): {},
			net.JoinHostPort("127.0.0.1", port): {},
		},
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
		Responses429:    s.full.Load(),
		MetricsDropped:  s.metrics.Load(),
	}
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	if r.URL.Path != "/v1/logs" && r.URL.Path != "/v1/metrics" {
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
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt != mimeProto && mt != mimeJSON {
		http.Error(w, "content type must be application/x-protobuf or application/json", http.StatusUnsupportedMediaType)
		return
	}
	body, status, err := s.readBody(w, r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	if r.URL.Path == "/v1/metrics" {
		s.metrics.Add(1)
		s.respond(w, mt, plogotlp.NewExportResponse())
		return
	}
	s.logs(w, r, mt, body)
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request, mt string, body []byte) {
	req := plogotlp.NewExportRequest()
	var err error
	if mt == mimeJSON {
		err = req.UnmarshalJSON(body)
	} else {
		err = req.UnmarshalProto(body)
	}
	if err != nil {
		http.Error(w, "malformed request", http.StatusBadRequest)
		return
	}
	logs := req.Logs()
	total := int64(logs.LogRecordCount())
	rejected, msg, err := s.sink(r.Context(), logs)
	if err != nil {
		if errors.Is(err, ErrFull) {
			s.full.Add(1)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "buffer full", http.StatusTooManyRequests)
			return
		}
		s.log.ErrorContext(r.Context(), "failed to store logs", "error", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
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

func (s *Server) respond(w http.ResponseWriter, mt string, resp plogotlp.ExportResponse) {
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
