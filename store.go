package microqueuego

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/glebarez/sqlite"
)

type Store struct {
	DB *sql.DB
}

func NewSQLiteStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	query := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		payload BLOB,
		target_url TEXT,
		max_retries INTEGER,
		retries INTEGER,
		status TEXT,
		last_error TEXT,
		created_at INTEGER,
		next_attempt_at INTEGER
	);`

	_, err = db.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create jobs table: %w", err)
	}

	return &Store{DB: db}, nil
}

func (s *Store) SaveJob(job Job) error {
	stmt, err := s.DB.Prepare(`
		INSERT INTO jobs (id, payload, target_url, max_retries, retries, status, created_at, next_attempt_at, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            payload = excluded.payload,
            target_url = excluded.target_url,
            max_retries = excluded.max_retries,
            retries = excluded.retries,
            status = excluded.status,
            created_at = excluded.created_at,
            next_attempt_at = excluded.next_attempt_at,
            last_error = excluded.last_error;
	`)

	if err != nil {
		return fmt.Errorf("prepare save job statement: %w", err)
	}

	defer stmt.Close()

	_, err = stmt.Exec(
		job.ID,
		job.Payload,
		job.TargetURL,
		job.MaxRetries,
		job.Retries,
		string(job.Status),
		job.LastError,
		job.CreatedAt.Unix(),
		job.NextAttemptAt.Unix(),
	)

	if err != nil {
		return fmt.Errorf("execute save job: %w", err)
	}

	return nil
}

func (s *Store) GetPendingJobs() ([]Job, error) {
	query := `
	SELECT 
		id, 
		payload, 
		target_url, 
		max_retries, 
		retries, 
		status, 
		last_error, 
		created_at, 
		next_attempt_at 
	FROM jobs 
	WHERE status IN (?, ?)`
	rows, err := s.DB.Query(query, StatusPending, StatusRetry)
	if err != nil {
		return nil, fmt.Errorf("query pending jobs: %w", err)
	}

	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var job Job
		var createdAtUnix, nextAttemptAtUnix int64
		var statusStr string

		err := rows.Scan(
			&job.ID,
			&job.Payload,
			&job.TargetURL,
			&job.MaxRetries,
			&job.Retries,
			&statusStr,
			&job.LastError,
			&createdAtUnix,
			&nextAttemptAtUnix,
		)

		if err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}

		job.Status = JobStatus(statusStr)
		job.CreatedAt = time.Unix(createdAtUnix, 0)
		job.NextAttemptAt = time.Unix(nextAttemptAtUnix, 0)
		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error after scan: %w", err)
	}

	return jobs, nil
}

func (s *Store) DeleteJob(jobID string) error {
	_, err := s.DB.Exec("DELETE FROM jobs WHERE id = ?", jobID)
	if err != nil {
		return fmt.Errorf("delete job %s: %w", jobID, err)
	}

	return nil
}
