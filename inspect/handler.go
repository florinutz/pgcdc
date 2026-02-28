package inspect

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// Handler returns an HTTP handler that serves inspector data.
// GET /inspect?point=post-detector&limit=10
func Handler(insp *Inspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		point := TapPoint(r.URL.Query().Get("point"))
		if point == "" {
			point = TapPostDetector
		}

		limitStr := r.URL.Query().Get("limit")
		limit := 10
		if limitStr != "" {
			if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
				limit = n
			}
		}

		events := insp.Snapshot(point, limit)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"point":  string(point),
			"count":  len(events),
			"events": events,
		})
	}
}

// SSEHandler returns an HTTP handler that streams events via Server-Sent Events.
// GET /inspect/stream?point=post-detector
func SSEHandler(insp *Inspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		point := TapPoint(r.URL.Query().Get("point"))
		if point == "" {
			point = TapPostDetector
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher.Flush()

		events, cancel := insp.Subscribe(point)
		defer cancel()

		enc := json.NewEncoder(w)
		for {
			select {
			case <-r.Context().Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				w.Write([]byte("data: "))
				enc.Encode(ev)
				w.Write([]byte("\n"))
				flusher.Flush()
			}
		}
	}
}
