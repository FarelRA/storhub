package rest

import (
	"context"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	"github.com/go-chi/chi/v5"
	"net/http"
	"strings"
)

// handleLogin serves POST /auth/login: decode credentials, mint an auth JWT.
func (h *restHandler) handleLogin(auth *restAuthenticator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req restLoginRequest
		if err := h.decodeJSON(r, &req, false); err != nil {
			h.writeMappedError(w, err)
			return
		}
		principal, token, ttl, err := auth.login(req.Username, req.Password)
		if err != nil {
			// No username, password, or token is ever logged: the reason
			// is a fixed string, and the path carries no credentials.
			logging.Warn(h.logger, "auth login rejected", "path", logging.RedactSensitivePath(r.URL.Path), "reason", "invalid credentials")
			h.writeError(w, http.StatusUnauthorized, "invalid_credentials", "invalid username or password")
			return
		}
		h.writeJSON(w, http.StatusOK, restLoginResponse{Token: token, TokenType: "Bearer", ExpiresIn: int64(ttl.Seconds()), Principal: principal})
	}
}

// requestBearerToken extracts the Authorization: Bearer *** token from a
// request, tolerating inline whitespace. Empty unless the scheme is Bearer.
// Callers keep their own header-vs-query precedence on top of it.
func requestBearerToken(r *http.Request) string {
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
}

func (h *restHandler) authMiddleware(auth *restAuthenticator, basePath string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, fromQuery := bearerOrQueryToken(r)
			if token == "" {
				h.writeUnauthorized(w, r, auth, "missing bearer token")
				return
			}
			// authPrincipal rejects auth-JWT-via-query explicitly so the
			// token falls through to the share lane below before failing.
			if ctx, ok := h.authPrincipal(r, auth, token, fromQuery); ok {
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			if ctx, ok, fatal := h.sharePrincipal(r, auth, basePath, token, w); fatal {
				return
			} else if ok {
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			h.writeUnauthorized(w, r, auth, "invalid bearer token")
		})
	}
}

// bearerOrQueryToken extracts the bearer credential, reporting whether it
// came from the query string (share-capability lane) or the header.
func bearerOrQueryToken(r *http.Request) (token string, fromQuery bool) {
	if token = requestBearerToken(r); token != "" {
		return token, false
	}
	if token = strings.TrimSpace(queryFirstParam(r.URL.RawQuery, "token")); token != "" {
		return token, true
	}
	return "", false
}

// authPrincipal verifies an auth JWT and returns the request context
// carrying the authorized client. ok=false means "not a usable auth token,
// try the share lane"; a nil context with ok=false after a query-token
// rejection still falls through to shares.
func (h *restHandler) authPrincipal(r *http.Request, auth *restAuthenticator, token string, fromQuery bool) (context.Context, bool) {
	principal, err := auth.parseToken(token)
	if err == nil && fromQuery {
		// Auth JWTs must travel in the Authorization header: query
		// strings land in intermediaries, browser history and
		// Referer headers. Query-token acceptance is reserved for
		// share capabilities (handled below), not the whole
		// authenticated surface.
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	// Re-read the user record behind the token: auth JWTs are
	// otherwise irrevocable, so a disabled, demoted, or removed
	// account must not keep full access until expiry.
	fresh, live := auth.currentPrincipal(principal)
	if !live {
		return nil, false
	}
	// Attach the caller's identity for the storage layers below:
	// downstream permission checks must see the authenticated
	// principal, never the server process's own credentials.
	identity := shfs.WithIdentity(r.Context(), shfs.Identity{
		UID:    fresh.UID,
		GID:    fresh.PrimaryGID,
		Groups: fresh.Groups,
		Admin:  fresh.Admin,
		Umask:  defaultRESTUmask,
	})
	return context.WithValue(identity, clientCtxKey, &authorizedClient{base: h.client, principal: fresh}), true
}

// sharePrincipal verifies a share JWT and returns the nobody-visitor
// context scoped to the shared path. fatal=true means the handler already
// answered (share-management forbidden); ok=false means "not a share token".
func (h *restHandler) sharePrincipal(r *http.Request, auth *restAuthenticator, basePath, token string, w http.ResponseWriter) (context.Context, bool, bool) {
	claims, err := h.parseShareToken(token)
	if err != nil {
		return nil, false, false
	}
	if h.isRevoked(claims.ID) {
		h.writeUnauthorized(w, r, auth, "invalid bearer token")
		return nil, false, true
	}
	project := chi.URLParam(r, "project")
	if project == claims.Project && strings.HasPrefix(r.URL.Path, basePath+"/projects/"+project+"/shares") {
		h.writeMappedError(w, errForbidden("share links cannot manage shares"))
		return nil, false, true
	}
	// Share links act as an unauthenticated read-only visitor.
	identity := shfs.WithIdentity(r.Context(), shfs.Identity{UID: nobodyUID, GID: nobodyGID})
	return context.WithValue(identity, clientCtxKey, newRestrictedClient(h.client, claims.Project, claims.Path)), true, false
}

func (h *restHandler) writeUnauthorized(w http.ResponseWriter, r *http.Request, auth *restAuthenticator, message string) {
	// Auth rejections log at Warn with project-scoped context only: the
	// message is a fixed reason string, the path is redacted, and the
	// bearer token itself is never logged (it may arrive via query on
	// the share lane).
	logging.Warn(h.logger, "auth rejected", "path", logging.RedactSensitivePath(r.URL.Path), "reason", message)
	w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q`, auth.realm))
	h.writeError(w, http.StatusUnauthorized, "unauthorized", message)
}
