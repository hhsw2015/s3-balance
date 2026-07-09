package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/DullJZ/s3-balance/internal/offline"
	"github.com/gorilla/mux"
)

// OfflineHandler exposes admin endpoints to submit and query offline-download tasks.
// PoC scope: single R2 bucket target, no balancer wiring, no virtual mapping write.
type OfflineHandler struct {
	executor *offline.Executor
	store    *offline.Store
}

func NewOfflineHandler(executor *offline.Executor, store *offline.Store) *OfflineHandler {
	return &OfflineHandler{executor: executor, store: store}
}

// RegisterRoutes attaches endpoints under the already-mounted /api subrouter.
func (h *OfflineHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/offline/submit", h.submit).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/offline/tasks", h.list).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/offline/tasks/{id}", h.get).Methods(http.MethodGet, http.MethodOptions)
}

type submitRequest struct {
	SourceURL string `json:"source_url"`
	R2Bucket  string `json:"r2_bucket"`
	Key       string `json:"key"`
}

func (h *OfflineHandler) submit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.SourceURL == "" || req.R2Bucket == "" || req.Key == "" {
		writeErr(w, http.StatusBadRequest, "source_url, r2_bucket, key are required")
		return
	}

	t := &offline.Task{
		ID:        newTaskID(),
		SourceURL: req.SourceURL,
		R2Bucket:  req.R2Bucket,
		Key:       strings.TrimPrefix(req.Key, "/"),
	}
	if _, err := h.executor.Submit(t); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *OfflineHandler) list(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"tasks": h.store.List()})
}

func (h *OfflineHandler) get(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	t, ok := h.store.Get(id)
	if !ok {
		writeErr(w, http.StatusNotFound, "task not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func newTaskID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
