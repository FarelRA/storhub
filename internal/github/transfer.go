package github

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/logging"
)

type cachedAssetURL struct {
	url     string
	expires time.Time
}

// UploadAsset uploads one release asset to the fully-qualified uploadURL
// (taken from the release's UploadURL template, with the asset name set
// as the ?name= query parameter). The endpoint alone determines the
// destination: there are no owner/project/releaseTag parameters.
func (c *Client) UploadAsset(ctx context.Context, uploadURL, assetName string, reader io.ReadSeeker, size int64) (int64, error) {
	cleanURL := strings.Split(uploadURL, "{")[0]
	parsed, err := url.Parse(cleanURL)
	if err != nil {
		return 0, fmt.Errorf("parse upload url: %w", err)
	}
	query := parsed.Query()
	query.Set("name", assetName)
	parsed.RawQuery = query.Encode()
	endpoint := parsed.String()

	started := time.Now().UTC()
	assetID, err := c.uploadAssetAttempt(ctx, endpoint, assetName, reader, size)
	if err == nil {
		logging.Info(c.logger, "upload asset complete", "asset", assetName, "size", size, "elapsed", time.Now().UTC().Sub(started))
		return assetID, nil
	}
	// Fail loudly: silently reusing a pre-existing asset with the same name
	// would hand the caller an unverified ID (possibly stale or partial
	// content) as if the fresh bytes had been stored. Callers that want
	// name-based reuse can compose FindAssetIDByName themselves.
	logging.Warn(c.logger, "upload asset failed", "asset", assetName, "size", size, "elapsed", time.Now().UTC().Sub(started), "body", uploadErrorBody(err), "err", err)
	return 0, fmt.Errorf("upload asset: %w", err)
}

// DownloadAssetStream fetches [start,end] bytes of an asset. The returned
// size is the server-declared Content-Length and may be -1 when the server
// does not declare a length; readers must therefore rely on their own byte
// accounting rather than on the reported size.
//
// The octet-stream GET is a redirect to a short-lived signed CDN URL. That
// resolution is cached per asset, so reading a large file in many range
// requests costs one core-API call per TTL window instead of one per read;
// range fetches themselves hit the CDN directly without auth headers.
func (c *Client) DownloadAssetStream(ctx context.Context, owner, project string, assetID, start, end int64) (io.ReadCloser, int64, error) {
	rangeHeader := ""
	switch {
	case start >= 0 && end >= start:
		rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
	default:
		// No silent full-asset fallback: an invalid or unset range is a
		// caller bug and must surface as an error.
		return nil, 0, fmt.Errorf("download asset %d: invalid byte range [%d,%d]", assetID, start, end)
	}
	started := time.Now().UTC()
	for attempt := 0; attempt < 2; attempt++ {
		cdnURL, cached := c.cachedAssetURL(assetID)
		if cached {
			body, size, status, err := c.fetchCDNRange(ctx, cdnURL.url, rangeHeader, end-start+1)
			if err == nil {
				logging.Debug(c.logger, "download asset complete", "asset", assetID, "size", size, "elapsed", time.Now().UTC().Sub(started))
				return body, size, nil
			}
			if !isCDNRejection(status) {
				return nil, 0, err
			}
			// Signed URL expired or revoked: drop it and re-resolve once.
			c.invalidateAssetURL(assetID)
			logging.Warn(c.logger, "cached asset URL rejected; re-resolving via API", "asset", assetID, "status", status)
		}
		resp, err := c.doCDNResolve(ctx, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/assets/%d", owner, project, assetID)), "application/octet-stream", rangeHeader)
		if err != nil {
			return nil, 0, fmt.Errorf("download asset: %w", err)
		}
		if isAssetRedirect(resp.StatusCode) {
			location := resp.Header.Get("Location")
			_ = resp.Body.Close()
			if location == "" {
				return nil, 0, fmt.Errorf("download asset %d: redirect missing location", assetID)
			}
			if resolved, err := resp.Request.URL.Parse(location); err == nil {
				location = resolved.String()
			}
			c.storeAssetURL(assetID, location)
			body, size, status, fetchErr := c.fetchCDNRange(ctx, location, rangeHeader, end-start+1)
			if fetchErr == nil {
				logging.Debug(c.logger, "download asset complete", "asset", assetID, "size", size, "elapsed", time.Now().UTC().Sub(started))
				return body, size, nil
			}
			if !isCDNRejection(status) || attempt > 0 {
				return nil, 0, fetchErr
			}
			c.invalidateAssetURL(assetID)
			logging.Warn(c.logger, "fresh asset URL rejected; retrying resolution", "asset", assetID, "status", status)
			continue
		}
		// Non-redirect response: treat it as the final answer (also keeps
		// test servers that stream bytes directly working unchanged). Error
		// statuses never reach here: doRequest already converted every
		// >=400 response into an *APIError.
		logging.Debug(c.logger, "download asset complete", "asset", assetID, "size", resp.ContentLength, "elapsed", time.Now().UTC().Sub(started))
		return resp.Body, resp.ContentLength, nil
	}
	return nil, 0, fmt.Errorf("download asset %d: exhausted cdn re-resolution attempts", assetID)
}

// fetchCDNRange performs one unauthenticated range GET against a signed
// CDN URL. The request is bounded by a deadline scaled to the expected
// byte count, so a stalled connection surfaces as a retryable timeout
// instead of hanging the reader forever. The status code is returned
// alongside so callers can tell expired-signature rejections apart from
// other failures.
//
// The deadline's cancel is bound to the RETURNED BODY's lifetime, not to
// this function: callers stream the body long after we return, and a
// defer cancel() here amputated every transfer mid-read ("context
// canceled" after whatever bytes had already buffered - the intermittent
// EIO plague on mounted reads). Closing the body releases the resources;
// the deadline itself still aborts genuinely stalled streams.
func (c *Client) fetchCDNRange(ctx context.Context, url, rangeHeader string, length int64) (io.ReadCloser, int64, int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.transferDeadline(length))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		return nil, 0, 0, fmt.Errorf("create cdn request: %w", err)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := c.cdn.Do(req)
	if err != nil {
		cancel()
		logging.Debug(c.logger, "cdn range fetch failed", "url", c.redactSignedURL(url), "range", rangeHeader, "err", err)
		return nil, 0, 0, fmt.Errorf("cdn request: %w", err)
	}
	if resp.StatusCode >= 400 {
		defer func() { _ = resp.Body.Close() }()
		logging.Debug(c.logger, "cdn range fetch rejected", "url", c.redactSignedURL(url), "range", rangeHeader, "status", resp.StatusCode)
		cancel()
		return nil, 0, resp.StatusCode, &CDNError{StatusCode: resp.StatusCode}
	}
	// Ownership of cancel transfers to the returned body.
	_ = cancel
	return &deadlineBoundBody{ReadCloser: resp.Body, cancel: cancel}, resp.ContentLength, resp.StatusCode, nil
}

// deadlineBoundBody keeps a response context alive until the streamed
// body is closed, so readers may consume it after the request function
// has returned.
//
// CONTRACT: Close() owns the cancel. fetchCDNRange deliberately does NOT
// defer cancel() - doing so amputates every live stream the moment the
// function returns - and transfers the transfer-deadline CancelFunc to
// this body instead. Every caller MUST Close() the returned reader on
// ALL paths (defer Close is the idiom). An abandoned, never-closed
// reader pins its connection and timer until the size-scaled transfer
// deadline fires: minutes for a large range. That deadline is the
// deliberate outer bound - a shorter watchdog would kill legitimate
// slow-but-live streams, so no tighter fallback exists. Close is
// idempotent and safe to call alongside a read loop.
type deadlineBoundBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (b *deadlineBoundBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.cancel)
	return err
}

// isAssetRedirect reports whether a status carries the signed asset URL
// in Location: the API answers the octet-stream GET with any of these
// depending on endpoint and edge, so all of them resolve like 302 does.
func isAssetRedirect(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

// isCDNRejection reports whether a CDN status indicates the signed URL is
// no longer usable and should be re-resolved through the API. That includes
// the usual 4xx (forbidden / not found / bad request / gone) and GitHub's
// non-standard 618, which means the front-door JWT on the URL has expired.
func isCDNRejection(status int) bool {
	return status == http.StatusForbidden || status == http.StatusNotFound ||
		status == http.StatusBadRequest || status == http.StatusGone ||
		status == StatusSignedURLExpired
}

// cachedAssetURL is the signed-URL cache read path. It takes the shared
// read lock so concurrent range reads proceed in parallel; stores and
// invalidations (the rare paths) take the exclusive lock. An expired
// entry returns ok=false but is left in place: the insert-time prune in
// storeAssetURL bounds physical retention.
func (c *Client) cachedAssetURL(assetID int64) (cachedAssetURL, bool) {
	c.assetMu.RLock()
	defer c.assetMu.RUnlock()
	cached, ok := c.assetURLs[assetID]
	return cached, ok && c.now().Before(cached.expires)
}

const (
	// assetURLSafetyMargin is subtracted from a signed URL's authoritative
	// expiry so the cache lapses just before the token does, forcing a
	// proactive re-resolution instead of a reactive 618.
	assetURLSafetyMargin = 6 * storcfg.PatienceUnit
	// assetURLFallbackTTL is assumed when a signed URL carries no parseable
	// expiry. GitHub's front-door token is ~30 minutes, so re-resolve just
	// before that; a too-long guess still self-heals via the 618 re-resolution
	// path.
	assetURLFallbackTTL = 360*storcfg.PatienceUnit - assetURLSafetyMargin
	// assetURLCacheCap bounds the signed-URL cache. Entries live ~30 min,
	// so the useful working set is small by construction; the cap exists
	// only so a long-lived mount touching millions of distinct chunks
	// cannot retain one dead URL string (~0.5-1.5 KB) per asset forever.
	// 10k entries cost a few MB at worst.
	assetURLCacheCap = 10000
)

func (c *Client) storeAssetURL(assetID int64, rawURL string) {
	expires := c.now().Add(assetURLFallbackTTL)
	if parsed, err := url.Parse(rawURL); err == nil {
		// A release-assets URL carries TWO independent expiries: the front-door
		// JWT (validated by release-assets.githubusercontent.com; exceeding it
		// yields the non-standard 618 "jwt:expired") and the backing Azure SAS
		// 'se'. They differ — the JWT is ~30 min while 'se' can run ~10 min
		// longer — so the EARLIER one governs whether the URL still works.
		// Trusting 'se' alone kept a JWT-dead URL cached for minutes,
		// guaranteeing a 618 storm on deep reads.
		if exp, ok := signedURLExpiry(parsed); ok {
			expires = exp.Add(-assetURLSafetyMargin)
		}
	}
	c.assetMu.Lock()
	defer c.assetMu.Unlock()
	c.assetURLs[assetID] = cachedAssetURL{url: rawURL, expires: expires}
	c.pruneAssetURLLocked()
}

// pruneAssetURLLocked bounds the cache after an insert. Expired entries
// are swept out first: cachedAssetURL only logically expires them, so
// without this sweep the map keeps one signed URL per asset ID ever
// read. If the map is still over the cap - a burst of distinct fresh
// assets inside one TTL window - the soonest-to-expire entries, a proxy
// for the oldest, are evicted until it fits. Dropping a still-live entry
// costs its next reader one API re-resolution, never correctness.
// Callers must hold c.assetMu exclusively.
func (c *Client) pruneAssetURLLocked() {
	if len(c.assetURLs) <= assetURLCacheCap {
		return
	}
	now := c.now()
	for id, cached := range c.assetURLs {
		if !now.Before(cached.expires) {
			delete(c.assetURLs, id)
		}
	}
	for len(c.assetURLs) > assetURLCacheCap {
		var victim int64
		var earliest time.Time
		found := false
		for id, cached := range c.assetURLs {
			if !found || cached.expires.Before(earliest) {
				victim, earliest, found = id, cached.expires, true
			}
		}
		if !found {
			return
		}
		delete(c.assetURLs, victim)
	}
}

// signedURLExpiry returns the earliest authoritative expiry carried by a
// signed asset URL: the minimum of the front-door JWT's 'exp' claim and the
// backing Azure SAS 'se' parameter. ok is false when neither is parseable.
func signedURLExpiry(parsed *url.URL) (time.Time, bool) {
	values := parsed.Query()
	var earliest time.Time
	found := false
	consider := func(t time.Time, ok bool) {
		if ok && (!found || t.Before(earliest)) {
			earliest, found = t, true
		}
	}
	consider(jwtExpiry(values.Get("jwt")))
	consider(sasExpiry(values.Get("se")))
	return earliest, found
}

// sasExpiry parses the Azure SAS 'se' expiry. GitHub mints it with
// fractional seconds (RFC3339Nano, e.g. ...00.000Z); parsing RFC3339
// only silently failed on those and fell back to the JWT/fallback TTL,
// causing extra re-resolves. Try Nano first, then plain RFC3339.
func sasExpiry(se string) (time.Time, bool) {
	if se == "" {
		return time.Time{}, false
	}
	if exp, err := time.Parse(time.RFC3339Nano, se); err == nil {
		return exp, true
	}
	exp, err := time.Parse(time.RFC3339, se)
	if err != nil {
		return time.Time{}, false
	}
	return exp, true
}

// jwtExpiry decodes the 'exp' claim from a compact JWS (header.payload.sig)
// WITHOUT verifying the signature: the payload is base64url JSON and the
// expiry is public metadata, not a bearer secret. Returns ok=false for any
// malformed token or a missing/non-positive exp, so callers fall back safely.
func jwtExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp <= 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Exp, 0).UTC(), true
}

func (c *Client) invalidateAssetURL(assetID int64) {
	c.assetMu.Lock()
	defer c.assetMu.Unlock()
	delete(c.assetURLs, assetID)
}

// redactSignedURL strips the query from a signed CDN URL for logging:
// the signature is a bearer credential and must never reach logs.
func (c *Client) redactSignedURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "(asset url)"
	}
	parsed.RawQuery = ""
	return parsed.String()
}

// doStream performs a sized octet-stream release-asset upload: POST with
// the upload class (both per-minute windows, no hourly pacing), a
// size-scaled transfer deadline, and a timeout-free transport. Retries
// rewind via bodyFactory (see uploadAssetAttempt).
func (c *Client) doStream(ctx context.Context, endpoint string, bodyFactory func() (io.Reader, error), size int64) (*http.Response, error) {
	return c.doRequest(ctx, http.MethodPost, endpoint, bodyFactory, requestUpload, true, "application/octet-stream", "application/vnd.github+json", "", size, false)
}

func (c *Client) uploadAssetAttempt(ctx context.Context, endpoint, assetName string, reader io.ReadSeeker, size int64) (int64, error) {
	resp, err := c.doStream(ctx, endpoint, func() (io.Reader, error) {
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind upload reader: %w", err)
		}
		return reader, nil
		// Retries are safe: a release-asset upload is one atomic PUT - no
		// asset exists unless the full body lands - and the bodyFactory
		// rewinds the reader for every attempt. A lost response after
		// finalize can leave an orphan under this attempt's random name;
		// unreferenced assets are exactly what the assets purge scope removes.
	}, size)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	var asset struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&asset); err != nil {
		return 0, fmt.Errorf("decode asset response: %w", err)
	}
	if asset.ID == 0 {
		return 0, fmt.Errorf("asset response missing id for %s", assetName)
	}
	return asset.ID, nil
}
