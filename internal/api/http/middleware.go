package http

import (
	"fmt"
	"net/http"
	"time"
)

const serviceName = "searchsurge"

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(ww, r)
		elapsed := time.Since(start).Milliseconds()
		fmt.Printf("[%s] %s %s -> %d (%dms)\n", serviceName, r.Method, r.URL.Path, ww.status, elapsed)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}