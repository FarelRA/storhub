# REST Example

This example starts the StorHub REST API without authentication.

What it teaches:

- how to expose StorHub over HTTP with the public `rest` package
- how to keep the handler unauthenticated by leaving `Options.Auth` unset
- how to use the default `/api/v1` route layout from a standalone process
- how to use the built-in Alpine.js console at `/ui`

How to use it:

```bash
GITHUB_TOKEN=your_token go run ./examples/rest
```

Optional environment:

- `STORHUB_REST_LISTEN` changes the bind address; default is `:8080`
