package github

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Commit is one GitHub commit with message and SHA.
type Commit struct {
	SHA         string
	Message     string
	CommittedAt time.Time
}

type putContentResponse struct {
	Content struct {
		SHA string `json:"sha"`
	} `json:"content"`
	Commit struct {
		SHA string `json:"sha"`
	} `json:"commit"`
}

type commitResponse struct {
	SHA    string `json:"sha"`
	Commit struct {
		Message string `json:"message"`
		Author  struct {
			Date time.Time `json:"date"`
		} `json:"author"`
	} `json:"commit"`
}

// putFileRequest is the GitHub contents-API create/update payload.
type putFileRequest struct {
	Message string `json:"message"`
	SHA     string `json:"sha,omitempty"`
	Content string `json:"content"` // base64-encoded file bytes
}

type deleteFileRequest struct {
	Message string `json:"message"`
	SHA     string `json:"sha"`
}

// ContentEntry is one item in a contents-API directory listing.
type ContentEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Type string `json:"type"` // "file" or "dir"
	SHA  string `json:"sha"`
}

// GetFileContent reads the file at filePath from ref with its SHA.
func (c *Client) GetFileContent(ctx context.Context, owner, project, filePath, ref string) ([]byte, string, error) {
	endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", owner, project, escapeContentPath(filePath)))
	if ref != "" {
		endpoint += "?ref=" + url.QueryEscape(ref)
	}
	resp, err := c.doCDNResolve(ctx, endpoint, "application/vnd.github.raw", "")
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read content: %w", err)
	}
	sha := computeGitBlobSHA(data)
	return data, sha, nil
}

func computeGitBlobSHA(data []byte) string {
	header := fmt.Sprintf("blob %d\x00", len(data))
	h := sha1.New()
	h.Write([]byte(header))
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// PutFileContent creates or updates filePath with payload, using previousSHA
// as an optimistic-concurrency precondition (empty means create). It returns
// the commit SHA and the new blob content SHA.
//
// The PUT stays retryable: a lost-response create retry now
// lands on GitHub's 422 create-collision, which is NOT retried here
// (422 is not a retryable status) and is resolved by the caller's
// content-addressed collision check (writeObjects verifies the upstream
// bytes and treats a match as success). A lost-response update retry
// carries the now-stale previousSHA and 409s - survivable: the commit
// loop rebases. Disabling retries for creates instead would forfeit the
// 429/5xx retry that rate-limited metadata commits depend on.
func (c *Client) PutFileContent(ctx context.Context, owner, project, filePath string, payload []byte, previousSHA, message string) (string, string, error) {
	endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", owner, project, escapeContentPath(filePath)))
	resp, err := c.doJSONWithRetryable(ctx, http.MethodPut, endpoint, putFileRequest{
		Message: message,
		SHA:     previousSHA,
		Content: base64.StdEncoding.EncodeToString(payload),
	}, true)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var result putContentResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", fmt.Errorf("decode content response: %w", err)
	}
	return result.Commit.SHA, result.Content.SHA, nil
}

// ListFileCommits lists the commits touching filePath.
func (c *Client) ListFileCommits(ctx context.Context, owner, project, filePath string) ([]Commit, error) {
	escapedPath := url.QueryEscape(filePath)
	batchCommits, err := paginateGET[commitResponse](ctx, c, func(page int) string {
		return c.apiURL(fmt.Sprintf("/repos/%s/%s/commits?path=%s&per_page=%d&page=%d", owner, project, escapedPath, pageSize, page))
	})
	if err != nil {
		return nil, err
	}
	commits := make([]Commit, 0, len(batchCommits))
	for _, item := range batchCommits {
		commits = append(commits, Commit{SHA: item.SHA, Message: item.Commit.Message, CommittedAt: item.Commit.Author.Date.UTC()})
	}
	return commits, nil
}

// ListDir returns the entries of a directory path via the contents API. A
// missing directory surfaces as a 404 APIError so callers can distinguish an
// absent tree from a transport failure.
func (c *Client) ListDir(ctx context.Context, owner, project, dirPath string) ([]ContentEntry, error) {
	endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", owner, project, escapeContentPath(dirPath)))
	var entries []ContentEntry
	if err := c.getJSON(ctx, endpoint, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// DeleteFileContent removes filePath, using sha as the optimistic-concurrency
// precondition (GitHub requires the current blob sha). Returns the commit SHA.
func (c *Client) DeleteFileContent(ctx context.Context, owner, project, filePath, sha, message string) (string, error) {
	endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", owner, project, escapeContentPath(filePath)))
	resp, err := c.doJSONWithRetryable(ctx, http.MethodDelete, endpoint, deleteFileRequest{Message: message, SHA: sha}, true)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode delete response: %w", err)
	}
	return result.Commit.SHA, nil
}

// escapeContentPath URL-escapes each path segment; raw '#', '?' or '%'
// characters in filenames would otherwise truncate or rewrite the request.
//
// The name stays (callers and tests use it); the work splits into
// cleanSegments (normalize: "" and "." drop out, ".." pops the previous
// segment clamped at the repo root so it can never escape into the URL
// the way path.Clean's leading ".." did) plus escapeSegments (escape
// each survivor and join). An empty result means the input named nothing
// addressable.
func escapeContentPath(filePath string) string {
	return escapeSegments(cleanSegments(filePath))
}

// cleanSegments splits a content path into raw segments, dropping ""
// and ".", resolving ".." against the survivors clamped at the repo
// root. Leading slashes never produce a leading empty segment.
func cleanSegments(filePath string) []string {
	var kept []string
	for _, seg := range strings.Split(strings.TrimLeft(filePath, "/"), "/") {
		switch seg {
		case "", ".":
			continue
		case "..":
			if len(kept) > 0 {
				kept = kept[:len(kept)-1]
			}
		default:
			kept = append(kept, seg)
		}
	}
	return kept
}

// escapeSegments URL-escapes each segment and joins them back into a
// request path.
func escapeSegments(kept []string) string {
	escaped := make([]string, 0, len(kept))
	for _, seg := range kept {
		escaped = append(escaped, url.PathEscape(seg))
	}
	return strings.Join(escaped, "/")
}
