# Authenticated REST Example

This example starts the StorHub REST API with bearer authentication enabled.

What it teaches:

- how to turn on auth through `rest.Options.Auth`
- how to hash a password with `rest.HashPassword`
- how UNIX-style authorization is layered onto the same REST handler
- how to sign in from the built-in Alpine.js console at `/ui`

How to use it:

```bash
GITHUB_TOKEN=your_token \
STORHUB_REST_ADMIN_PASSWORD=change-me \
STORHUB_REST_SIGNING_KEY=signing-secret \
go run ./examples/rest-auth
```

Optional environment:

- `STORHUB_REST_LISTEN` changes the bind address; default is `:8080`
