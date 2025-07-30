package microqueuego

import "time"

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusRetry      JobStatus = "retrying"
)

type Job struct {
	ID            string
	Payload       []byte
	TargetURL     string
	MaxRetries    int
	Retries       int
	Status        JobStatus
	LastError     string
	CreatedAt     time.Time
	NextAttemptAt time.Time
}
