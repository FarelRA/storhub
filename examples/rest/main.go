package main

import (
	"log"
	"net/http"
	"os"

	shrest "github.com/FarelRA/storhub/rest"
	"github.com/FarelRA/storhub/storhub"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN environment variable not set")
	}
	hub, err := storhub.NewStorHub(token)
	if err != nil {
		log.Fatal(err)
	}
	listen := os.Getenv("STORHUB_REST_LISTEN")
	if listen == "" {
		listen = ":8080"
	}
	opts := shrest.DefaultOptions()
	handler, err := shrest.New(hub, opts)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("serving unauthenticated REST API on %s%s", listen, opts.BasePath)
	log.Fatal(http.ListenAndServe(listen, handler))
}
