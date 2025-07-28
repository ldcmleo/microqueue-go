package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

func main() {
	rand.Seed(time.Now().UnixNano()) // Inicializar el generador de números aleatorios

	q := NewQueue(5) // 5 workers
	q.Start()

	// Enviar algunos trabajos de ejemplo
	for i := range 10 {
		jobID := uuid.New().String()
		job := Job{
			ID:         jobID,
			Payload:    fmt.Appendf(nil, `{"message": "Hola desde Go, trabajo número %d!"}`, i+1),
			TargetURL:  fmt.Sprintf("http://localhost:8080/webhook/%s", jobID),
			MaxRetries: 3,
			Retries:    0,
			Status:     StatusPending,
			CreatedAt:  time.Now(),
		}
		q.SubmitJob(job)
		time.Sleep(100 * time.Millisecond) // Pequeña pausa para ver los mensajes
	}

	// Dar tiempo para que algunos trabajos se procesen
	time.Sleep(3 * time.Second)

	fmt.Println("\n Stopping queue...")
	q.Stop()
	fmt.Println("\n Application ended.")

	// Opcional: imprimir el estado final de los trabajos
	fmt.Println("\nResults:")
	q.mu.Lock()
	for _, job := range q.jobsInFlight {
		fmt.Printf("  ID: %s, URL: %s, Estado: %s, Intentos: %d\n", job.ID, job.TargetURL, job.Status, job.Retries)
	}
	q.mu.Unlock()
}
