package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// panickingClient fails the test the moment any call reaches it: a share
// client must deny every mutation without delegating.
type panickingClient struct {
	Client
}

func (panickingClient) methodCalled() {}

// TestRestrictedClientDeniesEverything exercises the chokepoint contract:
// every method of the Client interface, invoked with zero arguments, must
// return an access-denied error and never delegate to the underlying
// client. A future Client method that "forgets" to deny (and instead
// forwards) fails here with a nil-pointer panic from the embedded
// interface, and a read method whose zero-argument path escapes the share
// scope likewise trips the panicking underlying.
func TestRestrictedClientDeniesEverything(t *testing.T) {
	client := newRestrictedClient(panickingClient{}, "unshared-project", "shared")
	if client.allowedPath == "" {
		t.Fatal("test expects a non-root allowed path so zero args cannot match")
	}

	ifaceType := reflect.TypeOf((*Client)(nil)).Elem()
	restricted := reflect.ValueOf(client)
	for i := 0; i < ifaceType.NumMethod(); i++ {
		method := ifaceType.Method(i)
		m := restricted.MethodByName(method.Name)
		if !m.IsValid() {
			t.Fatalf("restrictedClient does not implement Client.%s", method.Name)
		}
		mt := m.Type()
		args := make([]reflect.Value, mt.NumIn())
		for j := 0; j < mt.NumIn(); j++ {
			argType := mt.In(j)
			if argType == reflect.TypeOf((*context.Context)(nil)).Elem() {
				args[j] = reflect.ValueOf(context.Background())
				continue
			}
			args[j] = reflect.Zero(argType)
		}
		results := m.Call(args)
		denied := false
		for _, res := range results {
			if err, ok := res.Interface().(error); ok && err != nil && strings.Contains(err.Error(), "access denied") {
				denied = true
			}
		}
		if !denied {
			t.Fatalf("Client.%s did not deny a zero-argument call: %v", method.Name, results)
		}
	}
}

// TestShareCreateLocationHeader pins the 201 creation convention.
func TestShareCreateLocationHeader(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/projects/demo/shares", bytes.NewReader(mustJSONMarshal(t, shareRequest{Path: "shared"})))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+login.Token)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("share create: got %d want 201 body=%s", rec.Code, rec.Body.String())
	}
	var share shareResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &share); err != nil {
		t.Fatalf("decode share: %v", err)
	}
	want := "/api/v1/projects/demo/shares/" + share.ID
	if got := rec.Header().Get("Location"); got != want {
		t.Fatalf("Location header: got %q want %q", got, want)
	}
}
