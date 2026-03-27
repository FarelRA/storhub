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
	log.Fatal(http.ListenAndServe(listen, handler))
}
