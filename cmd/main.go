package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	microqueuego "github.com/ldcmleo/microqueue-go"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	dbPath := "queue_data.db"
	store, err := microqueuego.NewSQLiteStore(dbPath)
	if err != nil {
		log.Fatalf("error starting DB: %v", err)
	}

	defer store.DB.Close()
	q := microqueuego.NewQueue(5, store)
	q.Start()

	httpPort := "8080"
	httpSrv := microqueuego.NewHTTPServer(httpPort, q)

	// launch HTTP SERVER
	go func() {
		fmt.Printf("Server HTTP Listen on http://localhost:%s\n", httpPort)
		if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("Error starting HTTP Server: %v\n", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	fmt.Println("\nInterruption signal received, starting gracefull shutdown...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("Error shutting down HTTP server: %v\n", err)
	}

	q.Stop()
	fmt.Println("\n Application ended.")
	q.PrintLog()
}
