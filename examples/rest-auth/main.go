package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	shrest "github.com/FarelRA/storhub/rest"
	"github.com/FarelRA/storhub/storhub"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN environment variable not set")
	}
	password := os.Getenv("STORHUB_REST_ADMIN_PASSWORD")
	if password == "" {
		log.Fatal("STORHUB_REST_ADMIN_PASSWORD environment variable not set")
	}
	key := os.Getenv("STORHUB_REST_SIGNING_KEY")
	if key == "" {
		log.Fatal("STORHUB_REST_SIGNING_KEY environment variable not set")
	}
	hub, err := storhub.NewStorHub(token)
	if err != nil {
		log.Fatal(err)
	}
	hash, err := shrest.HashPassword(password)
	if err != nil {
		log.Fatal(err)
	}
	listen := os.Getenv("STORHUB_REST_LISTEN")
	if listen == "" {
		listen = ":8080"
	}
	opts := shrest.DefaultOptions()
	opts.Auth = &shrest.AuthOptions{
		TokenSigningKey: []byte(key),
		Users:           []shrest.User{{Username: "admin", PasswordHash: hash, UID: 0, PrimaryGID: 0, Admin: true}},
	}
	handler, err := shrest.New(hub, opts)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("serving authenticated REST API on %s%s", listen, opts.BasePath)
	srv := &http.Server{
		Addr:              listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveUntilSignal(srv, hub)
}

// serveUntilSignal runs the server and drains it cleanly on SIGINT/SIGTERM,
// flushing pending metadata before exit.
func serveUntilSignal(srv *http.Server, hub *storhub.StorHub) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
	if err := hub.Shutdown(shutdownCtx); err != nil {
		log.Printf("metadata flush failed: %v", err)
	}
}
