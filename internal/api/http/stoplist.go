package http

import (
	"encoding/json"
	"net/http"
)

func (h *Handlers) Stoplist(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Words []string `json:"words"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.engine.UpdateStopList(req.Words)
	w.WriteHeader(http.StatusNoContent)
}