package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
)

type Handler struct {
	service TrendingServiceHandler
}

func New(service TrendingServiceHandler) *Handler {
	return &Handler{service: service}
}

func (h *Handler) GetTop(w http.ResponseWriter, r *http.Request) {
	nStr := r.URL.Query().Get("n")
    n, err := strconv.Atoi(nStr)
	if err != nil || n <= 0 {
        n = 10
    }

	top := h.service.GetTop(r.Context(), n)

	w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(top)
}