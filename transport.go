package storhub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const maxAPIErrorBodyBytes = 64 << 10

var (
	ErrFileNotFound    = errors.New("file not found")
	ErrProjectNotFound = errors.New("project not found")
)

type APIError struct {
	StatusCode     int
	Message        string
	Body           string
	Headers        http.Header
	RetryAfter     time.Duration
	RateLimitReset time.Time
	RateLimited    bool
}

func (e *APIError) Error() string {
	message := e.Message
	if message == "" {
		message = strings.TrimSpace(e.Body)
	}
	if message == "" {
		message = http.StatusText(e.StatusCode)
	}
	return fmt.Sprintf("github API error (%d): %s", e.StatusCode, message)
}

func (e *APIError) NotFound() bool { return e != nil && e.StatusCode == http.StatusNotFound }

func (e *APIError) IsRetryable() bool {
	if e == nil {
		return false
	}
	if e.RateLimited {
		return true
	}
	switch e.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	case http.StatusForbidden:
		return e.RateLimited
	case http.StatusInternalServerError:
		return true
	default:
		return false
	}
}

func (h *StorHub) doJSON(ctx context.Context, method, endpoint string, body any) (*http.Response, error) {
	return h.doJSONWithRetryable(ctx, method, endpoint, body, isRetrySafeMethod(method))
}

func (h *StorHub) doJSONWithRetryable(ctx context.Context, method, endpoint string, body any, retryable bool) (*http.Response, error) {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
	}
	return h.doRequest(ctx, method, endpoint, func() (io.Reader, error) {
		if payload == nil {
			return nil, nil
		}
		return bytes.NewReader(payload), nil
	}, requestOptions{contentType: "application/json", accept: "application/vnd.github+json", contentSize: int64(len(payload)), retryable: retryable})
}

func (h *StorHub) getJSON(ctx context.Context, endpoint string, out any) error {
	resp, err := h.doJSON(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (h *StorHub) doRequest(ctx context.Context, method, endpoint string, bodyFactory func() (io.Reader, error), opts requestOptions) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt <= h.config.MaxRetries; attempt++ {
		reader, err := bodyFactory()
		if err != nil {
			return nil, err
		}
		if opts.contentSize == 0 && reader != nil {
			reader = http.NoBody
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		h.applyHeaders(req, opts)
		if opts.contentSize >= 0 {
			req.ContentLength = opts.contentSize
		}

		resp, err := h.client.Do(req)
		if err == nil {
			apiErr := decodeAPIError(resp)
			if apiErr == nil {
				return resp, nil
			}
			resp.Body.Close()
			lastErr = apiErr
			if attempt == h.config.MaxRetries || !opts.retryable || !apiErr.IsRetryable() {
				return nil, apiErr
			}
			if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, apiErr)); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}

		lastErr = fmt.Errorf("perform request: %w", err)
		if attempt == h.config.MaxRetries || !opts.retryable || !isRetryableNetworkError(err) {
			return nil, lastErr
		}
		if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, nil)); sleepErr != nil {
			return nil, sleepErr
		}
	}
	return nil, lastErr
}

type requestOptions struct {
	contentType string
	accept      string
	contentSize int64
	rangeHeader string
	retryable   bool
}

func (h *StorHub) applyHeaders(req *http.Request, opts requestOptions) {
	accept := opts.accept
	if accept == "" {
		accept = "application/vnd.github+json"
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("X-GitHub-Api-Version", h.apiVersion)
	if opts.contentType != "" {
		req.Header.Set("Content-Type", opts.contentType)
	}
	if opts.rangeHeader != "" {
		req.Header.Set("Range", opts.rangeHeader)
	}
}

func decodeAPIError(resp *http.Response) *APIError {
	if resp.StatusCode < http.StatusBadRequest {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxAPIErrorBodyBytes))
	var payload struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(body, &payload)
	err := &APIError{
		StatusCode: resp.StatusCode,
		Message:    payload.Message,
		Body:       string(body),
		Headers:    resp.Header.Clone(),
	}
	if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); retryAfter > 0 {
		err.RetryAfter = retryAfter
	}
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		err.RateLimited = true
		if reset, ok := parseUnixTime(resp.Header.Get("X-RateLimit-Reset")); ok {
			err.RateLimitReset = reset
		}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		err.RateLimited = true
	}
	return err
}

func (h *StorHub) retryDelay(attempt int, apiErr *APIError) time.Duration {
	if apiErr != nil {
		if apiErr.RetryAfter > 0 {
			return nonNegativeDelay(apiErr.RetryAfter)
		}
		if apiErr.RateLimited && !apiErr.RateLimitReset.IsZero() {
			delay := time.Until(apiErr.RateLimitReset)
			return nonNegativeDelay(delay)
		}
	}
	base := float64(h.config.BaseRetryDelay)
	delay := time.Duration(base * math.Pow(2, float64(attempt)))
	if delay > h.config.MaxRetryDelay {
		delay = h.config.MaxRetryDelay
	}
	if delay <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int63n(int64(delay/4 + 1)))
	return delay + jitter
}

func isRetrySafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func nonNegativeDelay(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	return delay
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if strings.TrimSpace(value) == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		return when.Sub(now)
	}
	return 0
}

func parseUnixTime(value string) (time.Time, bool) {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}

func isRetryableNetworkError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF)
}
