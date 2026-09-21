package rest

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestRESTShareCreateAndAccess(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte("abcdef0123456789abcdef0123456789")})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hello world"), nil, http.StatusCreated)
	shareResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "hello.txt"}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, shareResp, &share)
	if share.ID == "" || !strings.Contains(share.URL, "share=") || share.DownloadURL == "" {
		t.Fatalf("unexpected share response: %+v", share)
	}
	info := mustRequest(t, handler, http.MethodGet, "/api/v1/shares/"+share.Token, nil, nil, http.StatusOK)
	var public shareResponse
	decodeJSONBody(t, info, &public)
	if public.Path != "hello.txt" || public.ID != share.ID || public.Token != share.Token {
		t.Fatalf("unexpected public share response: %+v", public)
	}
	shared := mustRequest(t, handler, http.MethodGet, share.DownloadURL, nil, nil, http.StatusOK)
	if body := string(readBody(t, shared)); body != "hello world" {
		t.Fatalf("unexpected shared body: %q", body)
	}
}

func TestRESTShareDownloadCanBeDisabled(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte("abcdef0123456789abcdef0123456789")})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hello world"), nil, http.StatusCreated)
	shareResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "hello.txt"}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, shareResp, &share)
	// Shares are always download-capable after normalization.
	resp := mustRequest(t, handler, http.MethodGet,
		"/api/v1/shares/"+share.ID+"/download?token="+url.QueryEscape(share.Token), nil, nil, http.StatusOK)
	if body := string(readBody(t, resp)); body != "hello world" {
		t.Fatalf("unexpected body: %q", body)
	}
}

func TestRESTShareCanonicalizesPath(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte("abcdef0123456789abcdef0123456789")})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hello world"), nil, http.StatusCreated)
	shareResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "docs/../hello.txt"}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, shareResp, &share)
	if share.Path != "hello.txt" {
		t.Fatalf("expected canonical share path, got %q", share.Path)
	}
	shared := mustRequest(t, handler, http.MethodGet, share.DownloadURL, nil, nil, http.StatusOK)
	if body := string(readBody(t, shared)); body != "hello world" {
		t.Fatalf("unexpected shared body: %q", body)
	}
}

func TestRESTProjectShareListAndDelete(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte("abcdef0123456789abcdef0123456789")})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hello world"), nil, http.StatusCreated)
	shareResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "hello.txt"}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, shareResp, &share)
	listResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/shares", nil, nil, http.StatusOK)
	var listing sharesResponse
	decodeJSONBody(t, listResp, &listing)
	if len(listing.Shares) != 1 || listing.Shares[0].ID != share.ID {
		t.Fatalf("unexpected share listing: %+v", listing)
	}
	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/shares/"+share.ID, nil, nil, http.StatusNoContent)
	// Revocation is by ID in the management plane; redemption dies too.
	missing := mustRequest(t, handler, http.MethodGet, "/api/v1/shares/"+share.Token, nil, nil, http.StatusNotFound)
	assertErrorCode(t, missing, "not_found")
}
