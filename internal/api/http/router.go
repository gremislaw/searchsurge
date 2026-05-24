package http

import (
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

func NewRouter(gwMux *runtime.ServeMux, role string) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/", gwMux)
	mux.HandleFunc("/health", Health)

	return LoggingMiddleware(mux, role)
}
