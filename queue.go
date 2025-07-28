package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

type Queue struct {
	jobCh        chan Job
	retryCh      chan Job
	workerCount  int
	shutdownCh   chan struct{}
	wg           sync.WaitGroup
	jobsInFlight map[string]Job
	mu           sync.Mutex
}

func NewQueue(workerCount int) *Queue {
	return &Queue{
		jobCh:        make(chan Job),
		retryCh:      make(chan Job),
		workerCount:  workerCount,
		shutdownCh:   make(chan struct{}),
		jobsInFlight: make(map[string]Job),
	}
}

func (q *Queue) Start() {
	for i := 0; i < q.workerCount; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}
	q.wg.Add(1)
	go q.scheduler()
	fmt.Printf("Queue started with %d workers and 1 scheduler.\n", q.workerCount)
}

func (q *Queue) SubmitJob(job Job) {
	q.jobCh <- job
	q.mu.Lock()
	q.jobsInFlight[job.ID] = job
	q.mu.Unlock()
	fmt.Printf("Work %s sended to the queue. (URL: %s)\n", job.ID, job.TargetURL)
}

func (q *Queue) Stop() {
	close(q.shutdownCh)
	q.wg.Wait()
	close(q.jobCh)
	fmt.Println("Queue stopped.")
}

func (q *Queue) worker(id int) {
	defer q.wg.Done()
	fmt.Printf("Worker %d intialized.\n", id)

	for {
		select {
		case job, ok := <-q.jobCh:
			if !ok {
				fmt.Printf("Worker %d: Work Channel Closed, exit...\n", id)
			}

			q.mu.Lock()
			currentJob := q.jobsInFlight[job.ID]
			currentJob.Status = StatusProcessing
			q.jobsInFlight[job.ID] = currentJob
			q.mu.Unlock()
			fmt.Printf("Worker %d proccessing work %s (Tries: %d)\n", id, job.ID, job.Retries)

			// llamar al handeJob
			err := q.handleJob(job)

			q.mu.Lock()
			if err != nil {
				job.LastError = err.Error()
				job.Retries++
				if job.Retries < job.MaxRetries {
					job.Status = StatusRetry
					job.NextAttemptAt = time.Now().Add(exponentialBackoff(job.Retries))
					fmt.Printf("Worker %d: Job %s failed, retrying in %s (Retries %d/%d). Error: %s\n",
						id, job.ID, time.Until(job.NextAttemptAt).Round(time.Second), job.Retries, job.MaxRetries, err)
					q.jobsInFlight[job.ID] = job // Actualizar en el mapa
					q.retryCh <- job
				} else {
					job.Status = StatusFailed
					fmt.Printf("Worker %d: Job %s failed totally after %d retries. Error: %s\n",
						id, job.ID, job.Retries, err)
					q.jobsInFlight[job.ID] = job // Actualizar en el mapa
					// Aquí podrías loguear a un "dead-letter log" o un archivo de fallos.
				}
			} else {
				job.Status = StatusCompleted
				fmt.Printf("Worker %d: Trabajo %s completado con éxito.\n", id, job.ID)
				delete(q.jobsInFlight, job.ID) // Eliminar del mapa de trabajos en vuelo si se completó
			}
			q.mu.Unlock()
		case <-q.shutdownCh:
			fmt.Printf("Worker %d: Shutdown signal received, exit...\n", id)
			return
		}
	}
}

func (q *Queue) scheduler() {
	defer q.wg.Done() // Asegura que el scheduler se registre con WaitGroup
	fmt.Println("Scheduler started.")

	// Mapa interno del scheduler para los trabajos a reintentar
	// Este mapa es accedido SOLO por esta goroutine, por lo que NO necesita mutex
	scheduledJobs := make(map[string]Job)
	ticker := time.NewTicker(1 * time.Second) // Revisa cada segundo
	defer ticker.Stop()

	for {
		select {
		case job := <-q.retryCh: // Un trabajo fallido que necesita ser programado
			scheduledJobs[job.ID] = job
			fmt.Printf("Scheduler: Job %s programmed to retry in %s.\n", job.ID, job.NextAttemptAt.Format("15:04:05"))

		case <-ticker.C: // Cada segundo, revisa los trabajos a reintentar
			now := time.Now()
			for id, job := range scheduledJobs {
				if now.After(job.NextAttemptAt) {
					fmt.Printf("Scheduler: sending job %s to retry (retry %d).\n", job.ID, job.Retries)
					q.jobCh <- job            // Enviar de vuelta a la cola principal
					delete(scheduledJobs, id) // Quitar del mapa del scheduler
				}
			}
		case <-q.shutdownCh:
			fmt.Println("Scheduler: Shutdown signal received, exit...")
			return
		}
	}
}

func (q *Queue) handleJob(job Job) error {
	if job.TargetURL != "" {
		return dispatchWebhook(job)
	} else {
		dispatchStdout(job)
		return nil
	}
}

func dispatchWebhook(job Job) error {
	fmt.Printf("  -> Sending webhook for job %s a %s\n", job.ID, job.TargetURL)
	if rand.IntN(100) < 50 {
		return errors.New("simulated webhooj failure (e.g., HTTP 500)")
	}

	return nil
}

func dispatchStdout(job Job) {
	fmt.Printf("  -> Exit to STDOUT for job %s: %s\n", job.ID, string(job.Payload))
}

func exponentialBackoff(retryNum int) time.Duration {
	duration := time.Duration(math.Pow(2, float64(retryNum))) * time.Second
	if duration > 30*time.Second {
		return 30 * time.Second
	}

	return duration
}
