package firebase_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fb-tunnel/fb-tunnel-go/firebase"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// newTestClient creates a FirebaseClient pointed at the test HTTP server.
func newTestClient(t *testing.T, srv *httptest.Server) *firebase.Client {
	t.Helper()
	return firebase.NewClient(srv.URL, "test-secret", 2)
}

// ── Get tests ─────────────────────────────────────────────────────────────────

func TestGetReturnsValue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method: %s", r.Method)
		}
		if !strings.Contains(r.URL.RawQuery, "auth=test-secret") {
			t.Errorf("missing auth query param: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"firebase","value":42}`)
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	var result map[string]interface{}
	found, err := client.Get(context.Background(), "some/path", &result)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if result["name"] != "firebase" {
		t.Errorf("name: got %v", result["name"])
	}
}

func TestGetReturnsNullAsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "null")
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	var result map[string]interface{}
	found, err := client.Get(context.Background(), "some/path", &result)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if found {
		t.Error("expected found=false for null response")
	}
}

func TestGetRetriesOnServerError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			// Fail first two attempts with 503.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `"ok"`)
	}))
	defer srv.Close()

	client := firebase.NewClient(srv.URL, "s", 3)
	var result string
	found, err := client.Get(context.Background(), "p", &result)
	if err != nil {
		t.Fatalf("Get error after retries: %v", err)
	}
	if !found {
		t.Fatal("expected found=true after retry")
	}
	if result != "ok" {
		t.Errorf("result: got %q, want %q", result, "ok")
	}
	if calls.Load() != 3 {
		t.Errorf("expected 3 calls, got %d", calls.Load())
	}
}

func TestGetFailsAfterRetryLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	// retryLimit=1 means 2 total attempts.
	client := firebase.NewClient(srv.URL, "s", 1)
	var result interface{}
	_, err := client.Get(context.Background(), "p", &result)
	if err == nil {
		t.Fatal("expected error after exhausted retries")
	}
}

func TestGetContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate slow response.
		time.Sleep(5 * time.Second)
		fmt.Fprint(w, "null")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client := newTestClient(t, srv)
	var result interface{}
	_, err := client.Get(ctx, "p", &result)
	if err == nil {
		t.Fatal("expected error when context cancelled")
	}
}

// ── Put tests ─────────────────────────────────────────────────────────────────

func TestPutSendsCorrectBody(t *testing.T) {
	var received map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type: got %q", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{}`)
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	payload := map[string]interface{}{"key": "value", "num": float64(42)}
	if err := client.Put(context.Background(), "some/path", payload); err != nil {
		t.Fatalf("Put error: %v", err)
	}
	if received["key"] != "value" {
		t.Errorf("key: got %v", received["key"])
	}
}

func TestPutRetriesOnServerError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{}`)
	}))
	defer srv.Close()

	client := firebase.NewClient(srv.URL, "s", 3)
	if err := client.Put(context.Background(), "p", "value"); err != nil {
		t.Fatalf("Put error: %v", err)
	}
}

func TestPutFailsOnNonRetryableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden) // 403 = not retryable
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	err := client.Put(context.Background(), "p", "value")
	if err == nil {
		t.Fatal("expected error for 403, got nil")
	}
}

// ── Delete tests ──────────────────────────────────────────────────────────────

func TestDeleteSucceeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	if err := client.Delete(context.Background(), "some/path"); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
}

func TestDeleteTreats404AsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	// 404 should be treated as already-deleted = success.
	if err := client.Delete(context.Background(), "some/path"); err != nil {
		t.Fatalf("Delete should succeed on 404, got: %v", err)
	}
}

func TestDeleteRetriesOnServerError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := firebase.NewClient(srv.URL, "s", 3)
	if err := client.Delete(context.Background(), "p"); err != nil {
		t.Fatalf("Delete error after retry: %v", err)
	}
}

// ── Patch tests ───────────────────────────────────────────────────────────────

func TestPatchSendsCorrectBody(t *testing.T) {
	var received map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{}`)
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	payload := map[string]interface{}{"state": "active"}
	if err := client.Patch(context.Background(), "some/path", payload); err != nil {
		t.Fatalf("Patch error: %v", err)
	}
	if received["state"] != "active" {
		t.Errorf("state: got %v", received["state"])
	}
}

// ── SSE / Listen tests ────────────────────────────────────────────────────────

func TestListenReceivesEvents(t *testing.T) {
	sseBody := "event: put\ndata: {\"path\":\"/\",\"data\":\"hello\"}\n\nevent: put\ndata: {\"path\":\"/x\",\"data\":42}\n\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, sseBody)
		if flusher != nil {
			flusher.Flush()
		}
		// Block until client disconnects.
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := client.Listen(ctx, "sessions")

	var events []firebase.SseEvent
	for i := 0; i < 2; i++ {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("channel closed before receiving 2 events")
			}
			events = append(events, ev)
		case <-ctx.Done():
			t.Fatal("timed out waiting for SSE events")
		}
	}
	cancel()

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Event != "put" {
		t.Errorf("event[0].Event: got %q, want %q", events[0].Event, "put")
	}
	if !strings.Contains(events[0].Data, "hello") {
		t.Errorf("event[0].Data: got %q", events[0].Data)
	}
}

func TestListenChannelClosedOnContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := newTestClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch := client.Listen(ctx, "sessions")
	cancel()

	// Channel should close eventually after context cancel.
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // channel closed as expected
			}
		case <-timeout.C:
			t.Fatal("timed out waiting for channel to close")
		}
	}
}
