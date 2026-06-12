package tunnel

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/fb-tunnel/fb-tunnel-go/config"
	"github.com/fb-tunnel/fb-tunnel-go/firebase"
	"github.com/fb-tunnel/fb-tunnel-go/protocol"
	"github.com/fb-tunnel/fb-tunnel-go/session"
)

// serverC2SPollInterval is how often the server checks the c2s queue for new data.
const serverC2SPollInterval = 150 * time.Millisecond

// ──────────────────────────────────────────────────────────────────────────────
// HandleSession – server-side session handler
// ──────────────────────────────────────────────────────────────────────────────

// HandleSession handles a tunnel session from start to finish on the server side.
//
// This is the top-level function called for each Pending session.
//
// Flow:
//  1. Open outbound TCP connection to the destination declared in meta.
//  2. Mark the session Active in Firebase.
//  3. Run the bidirectional relay (c2s: Firebase→TCP, s2c: TCP→Firebase).
//  4. Mark the session Closed and delete all queue nodes.
func HandleSession(ctx context.Context, meta protocol.SessionMetadata, fb *firebase.Client, cfg *config.ServerConfig) error {
	sessionID := meta.SessionID
	target := fmt.Sprintf("%s:%d", meta.TargetHost, meta.TargetPort)

	// ── 1. Open TCP connection to destination ──────────────────────────────────
	slog.Info("session: connecting to target", "session_id", sessionID, "target", target)
	tcp, err := net.DialTimeout("tcp", target, 30*time.Second)
	if err != nil {
		return fmt.Errorf("cannot connect to %s: %w", target, err)
	}
	slog.Info("session: connected to target", "session_id", sessionID, "target", target)

	// ── 2. Mark session Active ────────────────────────────────────────────────
	{
		updated := meta
		updated.State = protocol.SessionStateActive
		if err := fb.Put(ctx, protocol.PathMetadata(sessionID), &updated); err != nil {
			_ = tcp.Close()
			return fmt.Errorf("marking session Active: %w", err)
		}
	}

	// ── 3. Run bidirectional relay ─────────────────────────────────────────────
	relayErr := runServerRelay(ctx, sessionID, tcp, fb, cfg)

	// ── 4. Mark session Closed ────────────────────────────────────────────────
	{
		var currentMeta protocol.SessionMetadata
		found, _ := fb.Get(context.Background(), protocol.PathMetadata(sessionID), &currentMeta)
		if found {
			currentMeta.State = protocol.SessionStateClosed
			_ = fb.Put(context.Background(), protocol.PathMetadata(sessionID), &currentMeta)
		}
	}

	// ── 5. Clean up all queue nodes ───────────────────────────────────────────
	cleanupSession(context.Background(), fb, sessionID)

	slog.Info("session: handler exiting", "session_id", sessionID)
	return relayErr
}

// ──────────────────────────────────────────────────────────────────────────────
// Relay
// ──────────────────────────────────────────────────────────────────────────────

func runServerRelay(ctx context.Context, sessionID string, tcp net.Conn, fb *firebase.Client, cfg *config.ServerConfig) error {
	c2sPath := protocol.PathC2S(sessionID)
	s2cPath := protocol.PathS2C(sessionID)
	acksC2sPath := protocol.PathAcks(sessionID) + "/c2s_ack"
	sessionTimeout := time.Duration(cfg.SessionTimeoutS) * time.Second

	relayCtx, relayCancel := context.WithCancel(ctx)
	defer relayCancel()
	defer tcp.Close()

	// ── c2s task: Firebase → TCP ──────────────────────────────────────────────
	c2sDone := make(chan struct{})
	go func() {
		defer close(c2sDone)
		receiver, byteRx := session.NewChunkReceiver()
		var deliveredUpTo *uint64
		lastActivity := time.Now()

		for {
			// Timeout check.
			if time.Since(lastActivity) > sessionTimeout {
				slog.Warn("c2s: session timed out", "session_id", sessionID)
				return
			}

			// Check if client closed the session.
			var meta protocol.SessionMetadata
			found, err := fb.Get(relayCtx, protocol.PathMetadata(sessionID), &meta)
			if err == nil && found {
				if meta.State == protocol.SessionStateClosing || meta.State == protocol.SessionStateClosed {
					slog.Debug("c2s: client closed session", "session_id", sessionID)
					return
				}
			}

			// Fetch new chunks.
			chunks, err := fetchNewChunks(relayCtx, fb, c2sPath, deliveredUpTo)
			if err != nil {
				slog.Warn("c2s: fetch error", "session_id", sessionID, "err", err)
				select {
				case <-relayCtx.Done():
					return
				case <-time.After(serverC2SPollInterval):
					continue
				}
			}

			if len(chunks) > 0 {
				lastActivity = time.Now()
			}

			for _, chunk := range chunks {
				newAck, err := receiver.Ingest(relayCtx, chunk)
				if err != nil {
					slog.Warn("c2s: ingest error", "session_id", sessionID, "err", err)
					return
				}
				if newAck != nil {
					deliveredUpTo = newAck

					// Write reassembled bytes to TCP socket.
				drainLoop:
					for {
						select {
						case bytes := <-byteRx:
							if _, writeErr := tcp.Write(bytes); writeErr != nil {
								slog.Debug("c2s: TCP write failed", "session_id", sessionID)
								return
							}
						default:
							break drainLoop
						}
					}

					// Acknowledge and clean up.
					if ackErr := session.UpdateAckAndCleanup(
						relayCtx,
						fb,
						sessionID,
						acksC2sPath,
						c2sPath,
						*newAck,
					); ackErr != nil {
						slog.Warn("c2s: ack update failed", "session_id", sessionID, "err", ackErr)
					}
				}
			}

			select {
			case <-relayCtx.Done():
				return
			case <-time.After(serverC2SPollInterval):
			}
		}
	}()

	// ── s2c task: TCP → Firebase ──────────────────────────────────────────────
	s2cDone := make(chan struct{})
	go func() {
		defer close(s2cDone)

		batchInterval := time.Duration(min64(cfg.PollIntervalMS, 50)) * time.Millisecond
		flusherCtx, flusherCancel := context.WithCancel(relayCtx)
		defer flusherCancel()

		sender := session.NewChunkSender(
			flusherCtx,
			sessionID,
			s2cPath,
			fb,
			batchInterval,
			readBufSize,
		)

		buf := make([]byte, readBufSize)
		for {
			n, err := tcp.Read(buf)
			if n > 0 {
				if feedErr := sender.Feed(flusherCtx, buf[:n]); feedErr != nil {
					slog.Warn("s2c: feed error", "session_id", sessionID, "err", feedErr)
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					slog.Warn("s2c: TCP read error", "session_id", sessionID, "err", err)
				} else {
					slog.Debug("s2c: TCP EOF", "session_id", sessionID)
				}
				return
			}
		}
	}()

	select {
	case <-c2sDone:
		slog.Debug("session: c2s task finished", "session_id", sessionID)
	case <-s2cDone:
		slog.Debug("session: s2c task finished", "session_id", sessionID)
	}

	return nil
}

// cleanupSession deletes all data for a session from Firebase (best-effort).
func cleanupSession(ctx context.Context, fb *firebase.Client, sessionID string) {
	root := "sessions/" + sessionID
	if err := fb.Delete(ctx, root); err != nil {
		slog.Warn("cleanup: failed to delete session", "session_id", sessionID, "err", err)
	} else {
		slog.Debug("cleanup: deleted session from Firebase", "session_id", sessionID)
	}
}

// min64 returns the smaller of a and b.
func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
