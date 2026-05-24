package http

import (
	"net/http"
	"strconv"
)

func (h *Handlers) Top(w http.ResponseWriter, r *http.Request) {
	n := 10
	if v := r.URL.Query().Get("n"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			if i > 2000 {
				i = 2000
			}
			n = i
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(h.engine.GetSnapshotJSON())
}