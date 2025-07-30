package microqueuego

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type SubmitRequest struct {
	Payload    json.RawMessage `json:"payload"`
	TargetURL  string          `json:"target_url"`
	MaxRetries *int            `json:"max_retries"`
}

func NewHTTPServer(port string, q *Queue) *http.Server {
	router := http.NewServeMux()
	router.HandleFunc("POST /submit", func(w http.ResponseWriter, r *http.Request) {
		var req SubmitRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, fmt.Sprintf("error decoding request: %v", err), http.StatusBadRequest)
			return
		}

		if len(req.Payload) == 0 {
			http.Error(w, "payload cannot be empty", http.StatusBadRequest)
			return
		}

		jobID := uuid.New().String()
		maxRetries := 3
		if req.MaxRetries != nil {
			maxRetries = *req.MaxRetries
		}

		newJob := Job{
			ID:            jobID,
			Payload:       []byte(req.Payload),
			TargetURL:     req.TargetURL,
			MaxRetries:    maxRetries,
			Retries:       0,
			Status:        StatusPending,
			CreatedAt:     time.Now(),
			NextAttemptAt: time.Now(),
		}

		q.SubmitJob(newJob)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "Job submitted", "job_id": jobID})
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	return srv
}
