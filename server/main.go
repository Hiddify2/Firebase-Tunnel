// Command fb-tunnel-server is the server binary for the fb-tunnel proof-of-concept.
//
// The server monitors Firebase Realtime Database for new tunnel sessions and
// forwards traffic between Firebase and the real destination TCP servers.
//
// Responsibilities:
//
//  1. Session discovery: polls sessions/ in Firebase for new entries with
//     state = Pending. For each new session it spawns a handler goroutine.
//
//  2. TCP connection: the handler opens an outbound TCP connection to the
//     destination requested in the session metadata.
//
//  3. c2s → TCP: reads chunks from sessions/{id}/c2s, reassembles them in
//     order, decompresses them, and writes the bytes to the TCP socket.
//
//  4. TCP → s2c: reads bytes from the TCP socket, batches / compresses /
//     base64-encodes them, and writes chunks to sessions/{id}/s2c.
//
//  5. Acknowledgements: reads the client's ack pointer from
//     sessions/{id}/acks/c2s_ack and deletes acknowledged s2c chunks.
//
//  6. Session cleanup: when the local TCP socket closes (or the client marks
//     the session as Closing/Closed) the handler marks the session as Closed
//     and deletes all remaining queue nodes.
//
// Usage:
//
//	fb-tunnel-server [path/to/server.toml]
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/fb-tunnel/fb-tunnel-go/config"
	"github.com/fb-tunnel/fb-tunnel-go/firebase"
	"github.com/fb-tunnel/fb-tunnel-go/protocol"
	"github.com/fb-tunnel/fb-tunnel-go/tunnel"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	configPath := "server.toml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	slog.Info("loading config", "path", configPath)
	cfg, err := config.LoadServerConfig(configPath)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	fb := firebase.NewClient(cfg.FirebaseURL, cfg.FirebaseSecret, cfg.RetryLimit)

	slog.Info("server started",
		"poll_interval_ms", cfg.PollIntervalMS,
		"session_timeout_s", cfg.SessionTimeoutS,
	)

	// Set of session IDs that already have a handler spawned, to avoid
	// duplicating handlers for the same session.
	var mu sync.Mutex
	known := make(map[string]struct{})

	pollInterval := time.Duration(cfg.PollIntervalMS) * time.Millisecond
	ctx := context.Background()

	for {
		sessions, err := discoverPendingSessions(ctx, fb)
		if err != nil {
			slog.Warn("session discovery error", "err", err)
		} else {
			for _, meta := range sessions {
				mu.Lock()
				_, exists := known[meta.SessionID]
				if exists {
					mu.Unlock()
					continue
				}
				known[meta.SessionID] = struct{}{}
				mu.Unlock()

				slog.Info("new session",
					"session_id", meta.SessionID,
					"target_host", meta.TargetHost,
					"target_port", meta.TargetPort,
				)

				m := meta
				go func() {
					if err := tunnel.HandleSession(ctx, m, fb, cfg); err != nil {
						slog.Error("session handler error", "session_id", m.SessionID, "err", err)
					}
					mu.Lock()
					delete(known, m.SessionID)
					mu.Unlock()
				}()
			}
		}
		time.Sleep(pollInterval)
	}
}

// discoverPendingSessions fetches all sessions from Firebase that have state == Pending.
func discoverPendingSessions(ctx context.Context, fb *firebase.Client) ([]protocol.SessionMetadata, error) {
	root := protocol.PathSessionsRoot()
	var all map[string]interface{}
	found, err := fb.Get(ctx, root, &all)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	var pending []protocol.SessionMetadata
	for _, value := range all {
		obj, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		metaVal, ok := obj["metadata"]
		if !ok {
			continue
		}
		// Round-trip through JSON to deserialize.
		data, err := json.Marshal(metaVal)
		if err != nil {
			continue
		}
		var meta protocol.SessionMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			slog.Debug("failed to parse session metadata", "err", err)
			continue
		}
		if meta.State == protocol.SessionStatePending {
			pending = append(pending, meta)
		}
	}
	return pending, nil
}
