package server

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// APIHandler returns a chi router with the pipeline control plane REST API.
//
//	GET    /api/v1/pipelines            — list all pipelines
//	GET    /api/v1/pipelines/:id        — get pipeline detail
//	POST   /api/v1/pipelines            — create pipeline from JSON spec
//	DELETE /api/v1/pipelines/:id        — remove pipeline (must be stopped)
//	POST   /api/v1/pipelines/:id/start  — start pipeline
//	POST   /api/v1/pipelines/:id/stop   — stop pipeline
//	POST   /api/v1/pipelines/:id/pause  — pause a running pipeline
//	POST   /api/v1/pipelines/:id/resume — resume a paused pipeline
func APIHandler(mgr *Manager) http.Handler {
	r := chi.NewRouter()

	r.Get("/api/v1/pipelines", listPipelines(mgr))
	r.Post("/api/v1/pipelines", createPipeline(mgr))
	r.Get("/api/v1/pipelines/{id}", getPipeline(mgr))
	r.Delete("/api/v1/pipelines/{id}", deletePipeline(mgr))
	r.Post("/api/v1/pipelines/{id}/start", startPipeline(mgr))
	r.Post("/api/v1/pipelines/{id}/stop", stopPipeline(mgr))
	r.Post("/api/v1/pipelines/{id}/pause", pausePipeline(mgr))
	r.Post("/api/v1/pipelines/{id}/resume", resumePipeline(mgr))

	return r
}

func listPipelines(mgr *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		infos := mgr.List()
		writeJSON(w, http.StatusOK, map[string]any{
			"pipelines": infos,
			"count":     len(infos),
		})
	}
}

func getPipeline(mgr *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		info, err := mgr.Get(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, info)
	}
}

func createPipeline(mgr *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var cfg PipelineConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}

		if err := mgr.Add(cfg); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"status": "created", "pipeline": cfg.Name})
	}
}

func deletePipeline(mgr *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if err := mgr.Remove(id); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "removed", "pipeline": id})
	}
}

func startPipeline(mgr *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if err := mgr.Start(r.Context(), id); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "started", "pipeline": id})
	}
}

func stopPipeline(mgr *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if err := mgr.Stop(id); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "stopped", "pipeline": id})
	}
}

func pausePipeline(mgr *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if err := mgr.Pause(id); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "paused", "pipeline": id})
	}
}

func resumePipeline(mgr *Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if err := mgr.Resume(r.Context(), id); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "resumed", "pipeline": id})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
