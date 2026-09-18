package rest

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func posixBenchRESTPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}
	return payload
}

func posixBenchRESTHandler(b *testing.B) http.Handler {
	b.Helper()
	handler, err := newHandlerForClient(newFakeRESTClient(), Options{AllowAnonymous: true})
	if err != nil {
		b.Fatalf("new handler: %v", err)
	}
	return handler
}

func posixBenchRESTSeed(b *testing.B, handler http.Handler, benchPath string, payload []byte) {
	b.Helper()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/demo/content?path="+benchPath, bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		b.Fatalf("seed PUT %s: status=%d body=%s", benchPath, resp.StatusCode, body)
	}
}

func BenchmarkPosixRESTGetSmall(b *testing.B) {
	b.ReportAllocs()
	handler := posixBenchRESTHandler(b)
	const benchPath = "pb-get-small.txt"
	payload := posixBenchRESTPayload(4096)
	posixBenchRESTSeed(b, handler, benchPath, payload)
	target := "/api/v1/projects/demo/content?path=" + benchPath
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		resp := rec.Result()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			b.Fatalf("read body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("status=%d want %d", resp.StatusCode, http.StatusOK)
		}
		if len(body) != len(payload) {
			b.Fatalf("unexpected body: got %d want %d", len(body), len(payload))
		}
	}
}

func BenchmarkPosixRESTRangedGet(b *testing.B) {
	b.ReportAllocs()
	handler := posixBenchRESTHandler(b)
	const benchPath = "pb-ranged-get.txt"
	payload := posixBenchRESTPayload(4096)
	posixBenchRESTSeed(b, handler, benchPath, payload)
	target := "/api/v1/projects/demo/content?path=" + benchPath
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("Range", "bytes=0-1023")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		resp := rec.Result()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			b.Fatalf("read body: %v", err)
		}
		if resp.StatusCode != http.StatusPartialContent {
			b.Fatalf("status=%d want %d", resp.StatusCode, http.StatusPartialContent)
		}
		if len(body) != 1024 {
			b.Fatalf("unexpected body: got %d want 1024", len(body))
		}
	}
}

func BenchmarkPosixRESTPutSmall(b *testing.B) {
	b.ReportAllocs()
	handler := posixBenchRESTHandler(b)
	const benchPath = "pb-put-small.txt"
	payload := posixBenchRESTPayload(4096)
	posixBenchRESTSeed(b, handler, benchPath, payload)
	target := "/api/v1/projects/demo/content?path=" + benchPath
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPut, target, bytes.NewReader(payload))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		resp := rec.Result()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			b.Fatalf("read body: %v", err)
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			b.Fatalf("status=%d body=%s", resp.StatusCode, body)
		}
	}
}

func BenchmarkPosixRESTStat(b *testing.B) {
	b.ReportAllocs()
	handler := posixBenchRESTHandler(b)
	const benchPath = "pb-stat.txt"
	payload := posixBenchRESTPayload(4096)
	posixBenchRESTSeed(b, handler, benchPath, payload)
	target := "/api/v1/projects/demo/nodes?path=" + benchPath
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		resp := rec.Result()
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			b.Fatalf("read body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("status=%d want %d", resp.StatusCode, http.StatusOK)
		}
		if len(body) == 0 {
			b.Fatal("empty stat body")
		}
	}
}
