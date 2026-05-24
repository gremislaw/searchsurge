package http

import (
	"net/http"

	"searchsurge/internal/surgecore"
)

func NewRouter(engine surgecore.Engine) http.Handler {
	h := NewHandlers(engine)
	mux := http.NewServeMux()

	mux.HandleFunc("/top", h.Top)
	mux.HandleFunc("/stoplist", h.Stoplist)
	mux.HandleFunc("/health", h.Health)

	return mux
}