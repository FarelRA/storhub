package logging

import (
	"net/url"
	"regexp"
	"strings"
)

const redactedPlaceholder = "REDACTED"

// safeQueryKeys lists query parameters whose values are operational and can
// never carry credentials. Everything else is masked before logging, because
// query strings are attacker- or user-controlled and credential-in-URL flows
// (bearer tokens, share capabilities) are common.
var safeQueryKeys = map[string]struct{}{
	"path":        {},
	"op":          {},
	"offset":      {},
	"size":        {},
	"delete_size": {},
	"recursive":   {},
	"name":        {},
	"target":      {},
}

// shareIDPattern matches share identifiers embedded in URL paths. Share
// routes authenticate on the identifier alone, so the identifier is a bearer
// capability and must not reach logs.
var shareIDPattern = regexp.MustCompile(`(/shares/)[^/?]+`)

// RedactQueryValues masks every query-string value except keys on the safe
// list. An unparseable query is masked wholesale.
func RedactQueryValues(rawQuery string) string {
	rawQuery = strings.TrimSpace(rawQuery)
	if rawQuery == "" {
		return ""
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return redactedPlaceholder
	}
	out := url.Values{}
	for key, vals := range values {
		if _, safe := safeQueryKeys[key]; safe {
			out[key] = vals
			continue
		}
		out[key] = []string{redactedPlaceholder}
	}
	return out.Encode()
}

// RedactSensitivePath masks share capability identifiers in a URL path.
func RedactSensitivePath(path string) string {
	if !strings.Contains(path, "/shares/") {
		return path
	}
	return shareIDPattern.ReplaceAllString(path, "${1}"+redactedPlaceholder)
}

// RedactRequestURI applies path and query redaction to a request URI such as
// http.Request.RequestURI ("path?query").
func RedactRequestURI(requestURI string) string {
	path, query, hasQuery := strings.Cut(requestURI, "?")
	redacted := RedactSensitivePath(path)
	if !hasQuery {
		return redacted
	}
	if q := RedactQueryValues(query); q != "" {
		return redacted + "?" + q
	}
	return redacted
}
