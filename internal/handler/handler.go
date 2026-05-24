package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
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

func (h *Handler) AddWord(w http.ResponseWriter, r *http.Request) {
	word := chi.URLParam(r, "word")

	h.service.AddWord(r.Context(), word)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) DeleteWord(w http.ResponseWriter, r *http.Request) {
	word := chi.URLParam(r, "word")

	h.service.DeleteWord(r.Context(), word)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) GetBlackList(w http.ResponseWriter, r *http.Request) {
	blacklist := h.service.GetBlackList(r.Context())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(blacklist)
}