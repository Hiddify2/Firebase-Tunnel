// Package firebase provides an async client for Firebase Realtime Database using
// its REST API. It handles:
//
//   - Reading and writing JSON nodes.
//   - Deleting nodes (used to clean up acknowledged chunks).
//   - Server-Sent Events (SSE) streaming listener so the server can be pushed
//     new data without polling every node individually.
//   - Retry logic with exponential back-off for transient HTTP errors.
//
// # Authentication
//
// A Database Secret is appended as "?auth=<secret>" to every request. This is
// the simplest approach for a proof-of-concept. In production you would replace
// this with short-lived JWTs issued by a service account.
//
// # Long-polling / Streaming
//
// Firebase Realtime Database supports the Server-Sent Events (SSE) protocol:
// issue a GET with "Accept: text/event-stream" and the server will push
// "put" / "patch" events as JSON as data changes. This is used in
// Client.Listen to avoid polling individual nodes.
//
// # Concurrency
//
// Client is safe for concurrent use. All methods may be called from multiple
// goroutines simultaneously.
package firebase

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// SseEvent is a parsed Server-Sent Event received from Firebase's streaming endpoint.
type SseEvent struct {
	// Event is the event type string as sent by Firebase ("put", "patch", "cancel", …).
	Event string

	// Data is the raw JSON data field of the event.
	Data string
}

// FirebaseEventPayload is the deserialized payload carried inside a Firebase SSE
// "put" or "patch" event.
type FirebaseEventPayload struct {
	// Path is the database path that changed, relative to the listener URL.
	Path string `json:"path"`

	// Data is the new value at that path. nil means the node was deleted.
	Data interface{} `json:"data"`
}

// Client is an async Firebase Realtime Database client.
//
// It is safe for concurrent use from multiple goroutines.
type Client struct {
	baseURL    string
	secret     string
	http       *http.Client
	retryLimit uint32
}

// NewClient creates a new Firebase client.
//
//   - baseURL: Firebase project URL without trailing slash.
//   - secret: Database secret for authentication.
//   - retryLimit: How many times to retry failed idempotent requests.
func NewClient(baseURL, secret string, retryLimit uint32) *Client {
	transport := &http.Transport{
		MaxIdleConns:       100,
		IdleConnTimeout:    90 * time.Second,
		DisableCompression: false,
	}
	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		secret:     secret,
		http:       httpClient,
		retryLimit: retryLimit,
	}
}

// ── URL construction ──────────────────────────────────────────────────────────

// url builds a full Firebase REST URL for path, appending the auth secret.
func (c *Client) url(path string) string {
	p := strings.TrimPrefix(path, "/")
	return fmt.Sprintf("%s/%s.json?auth=%s", c.baseURL, p, c.secret)
}

// streamURL builds the SSE streaming URL for path.
func (c *Client) streamURL(path string) string {
	p := strings.TrimPrefix(path, "/")
	return fmt.Sprintf(`%s/%s.json?auth=%s&orderBy="$key"`, c.baseURL, p, c.secret)
}

// ── Public API ────────────────────────────────────────────────────────────────

// Get reads a JSON node and deserializes it into v (must be a pointer).
//
// Returns (false, nil) if the node is null (i.e., does not exist).
// Returns (true, nil) on success with v populated.
func (c *Client) Get(ctx context.Context, path string, v interface{}) (bool, error) {
	u := c.url(path)
	resp, err := c.getWithRetry(ctx, u)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("reading GET response body: %w", err)
	}

	text := strings.TrimSpace(string(body))
	if text == "null" {
		return false, nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return false, fmt.Errorf("deserializing GET response from %s: %w (body: %s)", path, err, text)
	}
	return true, nil
}

// Put overwrites a JSON node with v.
func (c *Client) Put(ctx context.Context, path string, v interface{}) error {
	u := c.url(path)
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("serializing PUT body: %w", err)
	}
	return c.putRawWithRetry(ctx, u, body)
}

// Patch performs a partial update (merge) on a JSON node.
// Only the provided keys are updated.
func (c *Client) Patch(ctx context.Context, path string, v interface{}) error {
	u := c.url(path)
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("serializing PATCH body: %w", err)
	}
	return c.patchRawWithRetry(ctx, u, body)
}

// Delete removes a JSON node.
func (c *Client) Delete(ctx context.Context, path string) error {
	u := c.url(path)
	return c.deleteWithRetry(ctx, u)
}

// ── Retry wrappers ────────────────────────────────────────────────────────────

func (c *Client) getWithRetry(ctx context.Context, url string) (*http.Response, error) {
	delay := 200 * time.Millisecond
	var lastErr error
	for attempt := uint32(0); attempt <= c.retryLimit; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			slog.Warn("Firebase GET attempt error", "attempt", attempt, "err", err)
		} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		} else {
			_ = resp.Body.Close()
			if !shouldRetry(resp.StatusCode) {
				return nil, fmt.Errorf("Firebase GET failed with status %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("Firebase GET status %d", resp.StatusCode)
			slog.Warn("Firebase GET attempt failed", "attempt", attempt, "status", resp.StatusCode)
		}
		if attempt < c.retryLimit {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
			if delay < 10*time.Second {
				delay *= 2
			}
		}
	}
	return nil, fmt.Errorf("Firebase GET failed after %d attempts: %w", c.retryLimit+1, lastErr)
}

func (c *Client) putRawWithRetry(ctx context.Context, url string, body []byte) error {
	delay := 200 * time.Millisecond
	var lastErr error
	for attempt := uint32(0); attempt <= c.retryLimit; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			slog.Warn("Firebase PUT attempt error", "attempt", attempt, "err", err)
		} else {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if !shouldRetry(resp.StatusCode) {
				return fmt.Errorf("Firebase PUT failed with status %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("Firebase PUT status %d", resp.StatusCode)
			slog.Warn("Firebase PUT attempt failed", "attempt", attempt, "status", resp.StatusCode)
		}
		if attempt < c.retryLimit {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			if delay < 10*time.Second {
				delay *= 2
			}
		}
	}
	return fmt.Errorf("Firebase PUT failed after %d attempts: %w", c.retryLimit+1, lastErr)
}

func (c *Client) patchRawWithRetry(ctx context.Context, url string, body []byte) error {
	delay := 200 * time.Millisecond
	var lastErr error
	for attempt := uint32(0); attempt <= c.retryLimit; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			slog.Warn("Firebase PATCH attempt error", "attempt", attempt, "err", err)
		} else {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if !shouldRetry(resp.StatusCode) {
				return fmt.Errorf("Firebase PATCH failed with status %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("Firebase PATCH status %d", resp.StatusCode)
			slog.Warn("Firebase PATCH attempt failed", "attempt", attempt, "status", resp.StatusCode)
		}
		if attempt < c.retryLimit {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			if delay < 10*time.Second {
				delay *= 2
			}
		}
	}
	return fmt.Errorf("Firebase PATCH failed after %d attempts: %w", c.retryLimit+1, lastErr)
}

func (c *Client) deleteWithRetry(ctx context.Context, url string) error {
	delay := 200 * time.Millisecond
	var lastErr error
	for attempt := uint32(0); attempt <= c.retryLimit; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			slog.Warn("Firebase DELETE attempt error", "attempt", attempt, "err", err)
		} else {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			// 404 means already deleted – treat as success.
			if resp.StatusCode == http.StatusNotFound {
				return nil
			}
			if !shouldRetry(resp.StatusCode) {
				return fmt.Errorf("Firebase DELETE failed with status %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("Firebase DELETE status %d", resp.StatusCode)
			slog.Warn("Firebase DELETE attempt failed", "attempt", attempt, "status", resp.StatusCode)
		}
		if attempt < c.retryLimit {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			if delay < 10*time.Second {
				delay *= 2
			}
		}
	}
	return fmt.Errorf("Firebase DELETE failed after %d attempts: %w", c.retryLimit+1, lastErr)
}

// ── SSE streaming listener ────────────────────────────────────────────────────

// Listen opens a Server-Sent Events stream for path and forwards parsed events
// onto the returned channel.
//
// The returned channel is closed when ctx is cancelled or the connection cannot
// be re-established after an error. The goroutine reconnects automatically on
// transient errors.
//
// Cancel ctx to stop the listener.
func (c *Client) Listen(ctx context.Context, path string) <-chan SseEvent {
	ch := make(chan SseEvent, 256)
	go c.listenLoop(ctx, path, ch)
	return ch
}

func (c *Client) listenLoop(ctx context.Context, path string, ch chan<- SseEvent) {
	defer close(ch)
	u := c.streamURL(path)
	for {
		slog.Debug("SSE: connecting", "url", u)
		err := c.runSSELoop(ctx, u, ch)
		if err == nil || ctx.Err() != nil {
			// ctx cancelled or receiver done.
			slog.Debug("SSE: stopping listener")
			return
		}
		slog.Error("SSE: stream error, reconnecting in 2s", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// runSSELoop streams SSE events. Returns nil when ctx is cancelled or the
// channel is full and receiver appears gone; returns an error on connection loss.
func (c *Client) runSSELoop(ctx context.Context, url string, ch chan<- SseEvent) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("SSE connect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("SSE connect failed with %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	var currentEvent, currentData string

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			// Empty line = dispatch accumulated event.
			if currentEvent != "" && currentData != "" {
				event := SseEvent{Event: currentEvent, Data: currentData}
				slog.Debug("SSE received event", "event", event.Event)
				select {
				case ch <- event:
				case <-ctx.Done():
					return nil
				}
				currentEvent = ""
				currentData = ""
			}
		} else if after, ok := strings.CutPrefix(line, "event:"); ok {
			currentEvent = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "data:"); ok {
			currentData = strings.TrimSpace(after)
		}
		// We ignore "id:" and "retry:" lines.
	}

	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("SSE stream read error: %w", err)
	}
	if ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("SSE stream ended unexpectedly")
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// shouldRetry returns true for HTTP status codes that are worth retrying.
func shouldRetry(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}
