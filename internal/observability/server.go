package observability

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// SnapshotFunc gathers local in-memory state. It must not perform network or
// disk I/O. ReadyFunc has the same constraint.
type SnapshotFunc func() NodeSnapshot
type ReadyFunc func() (bool, string)

// Server owns the optional observability HTTP listener. Health means this
// process is serving; readiness is supplied by the node and is independent of
// leadership. Close deterministically joins the HTTP serving goroutine.
type Server struct {
	ln        net.Listener
	http      *http.Server
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func Listen(addr string, metrics *Metrics, snapshot SnapshotFunc, ready ReadyFunc) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	s := &Server{ln: ln, done: make(chan struct{})}
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := metrics.WritePrometheus(w, snapshot()); err != nil {
			return
		}
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		ok, reason := ready()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = fmt.Fprintf(w, "%s\n", reason)
	})
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	go func() { defer close(s.done); _ = s.http.Serve(ln) }()
	return s, nil
}

func (s *Server) Addr() string { return s.ln.Addr().String() }

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.http.Shutdown(ctx)
		if err != nil {
			_ = s.http.Close()
		}
		<-s.done
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.closeErr = err
		}
	})
	return s.closeErr
}
