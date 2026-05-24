package http

import (
	"net/http"
	"strconv"
)

onst (
	DefaultTopLimit = 5
	
	// MaxTopLimit жёсткий кап для защиты от abuse и сохранения P99 < 20ms
	MaxTopLimit = 2000
)

func (h *Handlers) Top(w http.ResponseWriter, r *http.Request) {
	n := DefaultTopLimit
	if v := r.URL.Query().Get("n"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			if i > MaxTopLimit {
				i = MaxTopLimit
			}
			n = i
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(h.engine.GetSnapshotJSON())
}