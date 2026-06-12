// Package tunnel provides the client-side and server-side tunnel session logic
// for the fb-tunnel library.
//
// ClientSession manages the Firebase side of a single SOCKS5 connection:
//  1. Session creation: writes SessionMetadata (state = Pending) to Firebase
//     and waits for the server to update it to Active.
//  2. Client-to-server forwarding: reads bytes from the local SOCKS5 net.Conn,
//     feeds them into a ChunkSender which batches, compresses, and writes
//     chunks to sessions/{id}/c2s.
//  3. Server-to-client forwarding: polls sessions/{id}/s2c for new chunks,
//     reassembles them in order via ChunkReceiver, and writes the bytes to
//     the local SOCKS5 net.Conn.
//  4. Acknowledgements: after delivering chunks to the local conn, writes the
//     new ack pointer to sessions/{id}/acks/s2c_ack and deletes those chunks.
//  5. Cleanup: on session close, deletes all remaining database nodes for the
//     session.
package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/fb-tunnel/fb-tunnel-go/firebase"
	"github.com/fb-tunnel/fb-tunnel-go/protocol"
	"github.com/fb-tunnel/fb-tunnel-go/session"
	"github.com/google/uuid"
)

// activationTimeout is how long to wait for the server to set the session
// state to Active.
const activationTimeout = 30 * time.Second

// s2cPollInterval is how frequently the client polls the s2c queue for new chunks.
const s2cPollInterval = 150 * time.Millisecond

// readBufSize is the maximum number of bytes to read from the SOCKS5 conn in one call.
const readBufSize = 32 * 1024

// ──────────────────────────────────────────────────────────────────────────────
// ClientSession
// ──────────────────────────────────────────────────────────────────────────────

// ClientSession is one end-to-end Firebase tunnel session managed by the client.
type ClientSession struct {
	sessionID string
	fb        *firebase.Client
	sender    *session.ChunkSender
	s2cPath   string
	// cancelFlusher cancels the background flusher goroutine.
	cancelFlusher context.CancelFunc
}

// NewClientSession creates a new session: writes metadata to Firebase and waits
// for server activation.
//
// Parameters:
//   - ctx: parent context; cancellation aborts the activation wait.
//   - targetHost: the SOCKS5 requested host.
//   - targetPort: the SOCKS5 requested port.
//   - fb: Firebase client.
//   - batchInterval: batching flush interval for ChunkSender.
//   - batchMaxBytes: max bytes before early flush.
func NewClientSession(
	ctx context.Context,
	targetHost string,
	targetPort uint16,
	fb *firebase.Client,
	batchInterval time.Duration,
	batchMaxBytes int,
) (*ClientSession, error) {
	sessionID := uuid.New().String()
	slog.Info("creating session", "session_id", sessionID, "target", fmt.Sprintf("%s:%d", targetHost, targetPort))

	// Write session metadata with state=Pending.
	meta := protocol.SessionMetadata{
		SessionID:  sessionID,
		TargetHost: targetHost,
		TargetPort: targetPort,
		CreatedAt:  protocol.NowMillis(),
		State:      protocol.SessionStatePending,
	}
	if err := fb.Put(ctx, protocol.PathMetadata(sessionID), &meta); err != nil {
		return nil, fmt.Errorf("writing session metadata: %w", err)
	}

	// Wait for the server to flip state to Active.
	slog.Info("waiting for server to activate session", "session_id", sessionID)
	activateCtx, activateCancel := context.WithTimeout(ctx, activationTimeout)
	defer activateCancel()

	if err := waitForActive(activateCtx, fb, sessionID); err != nil {
		return nil, fmt.Errorf("timed out waiting for server to activate session: %w", err)
	}
	slog.Info("session is active", "session_id", sessionID)

	s2cPath := protocol.PathS2C(sessionID)

	// Build the sender – starts a background flusher goroutine internally.
	flusherCtx, flusherCancel := context.WithCancel(ctx)
	sender := session.NewChunkSender(
		flusherCtx,
		sessionID,
		protocol.PathC2S(sessionID),
		fb,
		batchInterval,
		batchMaxBytes,
	)

	return &ClientSession{
		sessionID:     sessionID,
		fb:            fb,
		sender:        sender,
		s2cPath:       s2cPath,
		cancelFlusher: flusherCancel,
	}, nil
}

// Run runs the bidirectional relay between the SOCKS5 conn and Firebase.
//
// It exits when:
//   - the SOCKS5 conn closes (local app disconnected), or
//   - the server marks the session as Closed.
func (cs *ClientSession) Run(ctx context.Context, conn net.Conn) error {
	defer cs.cancelFlusher()

	sessionID := cs.sessionID

	// ── goroutine A: local conn → Firebase (c2s) ──────────────────────────────
	cCtx, cCancel := context.WithCancel(ctx)
	defer cCancel()

	c2sDone := make(chan struct{})
	go func() {
		defer close(c2sDone)
		buf := make([]byte, readBufSize)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if feedErr := cs.sender.Feed(cCtx, buf[:n]); feedErr != nil {
					slog.Warn("c2s: feed error", "session_id", sessionID, "err", feedErr)
					break
				}
			}
			if err != nil {
				if err != io.EOF {
					slog.Warn("c2s: read error", "session_id", sessionID, "err", err)
				} else {
					slog.Debug("c2s: local conn closed", "session_id", sessionID)
				}
				break
			}
		}
		// Mark session as closing.
		_ = setSessionState(context.Background(), cs.fb, sessionID, protocol.SessionStateClosing)
	}()

	// ── goroutine B: Firebase (s2c) → local conn ──────────────────────────────
	s2cDone := make(chan struct{})
	go func() {
		defer close(s2cDone)
		receiver, byteRx := session.NewChunkReceiver()
		var deliveredUpTo *uint64
		acksS2cPath := protocol.PathAcks(sessionID) + "/s2c_ack"

		for {
			// Check if the server has closed the session.
			var meta protocol.SessionMetadata
			found, err := cs.fb.Get(cCtx, protocol.PathMetadata(sessionID), &meta)
			if err == nil && found {
				if meta.State == protocol.SessionStateClosing || meta.State == protocol.SessionStateClosed {
					slog.Debug("s2c: server closed session", "session_id", sessionID)
					return
				}
			}

			// Fetch new chunks from the s2c queue.
			chunks, err := fetchNewChunks(cCtx, cs.fb, cs.s2cPath, deliveredUpTo)
			if err != nil {
				slog.Warn("s2c: fetch error", "session_id", sessionID, "err", err)
				select {
				case <-cCtx.Done():
					return
				case <-time.After(s2cPollInterval):
					continue
				}
			}

			for _, chunk := range chunks {
				newAck, err := receiver.Ingest(cCtx, chunk)
				if err != nil {
					slog.Warn("s2c: ingest error", "session_id", sessionID, "err", err)
					return
				}
				if newAck != nil {
					deliveredUpTo = newAck

					// Drain reassembled bytes and forward to local conn.
				drainLoop:
					for {
						select {
						case bytes := <-byteRx:
							if _, writeErr := conn.Write(bytes); writeErr != nil {
								slog.Debug("s2c: local conn write failed", "session_id", sessionID)
								return
							}
						default:
							break drainLoop
						}
					}

					// Write ack pointer and delete acknowledged chunks.
					if ackErr := session.UpdateAckAndCleanup(
						cCtx,
						cs.fb,
						sessionID,
						acksS2cPath,
						cs.s2cPath,
						*newAck,
					); ackErr != nil {
						slog.Warn("s2c: ack update failed", "session_id", sessionID, "err", ackErr)
					}
				}
			}

			select {
			case <-cCtx.Done():
				return
			case <-time.After(s2cPollInterval):
			}
		}
	}()

	// Wait for either direction to finish.
	select {
	case <-c2sDone:
		slog.Debug("c2s task finished", "session_id", sessionID)
	case <-s2cDone:
		slog.Debug("s2c task finished", "session_id", sessionID)
	}

	// Cancel all ongoing goroutines.
	cCancel()

	// Mark session closed.
	_ = setSessionState(context.Background(), cs.fb, sessionID, protocol.SessionStateClosed)

	// Best-effort: delete all remaining session data.
	sessionRoot := "sessions/" + sessionID
	if err := cs.fb.Delete(context.Background(), sessionRoot); err != nil {
		slog.Warn("failed to delete session root", "session_id", sessionID, "err", err)
	}

	slog.Info("session closed", "session_id", sessionID)
	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

// waitForActive polls Firebase until the session metadata state changes to
// Active (or Closed, which is an error).
func waitForActive(ctx context.Context, fb *firebase.Client, sessionID string) error {
	path := protocol.PathMetadata(sessionID)
	for {
		var meta protocol.SessionMetadata
		found, err := fb.Get(ctx, path, &meta)
		if err != nil {
			return err
		}
		if found {
			switch meta.State {
			case protocol.SessionStateActive:
				return nil
			case protocol.SessionStateClosed:
				return fmt.Errorf("server rejected session %s", sessionID)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// setSessionState updates only the state field of a session's metadata.
func setSessionState(ctx context.Context, fb *firebase.Client, sessionID string, state protocol.SessionState) error {
	path := protocol.PathMetadata(sessionID)
	var meta protocol.SessionMetadata
	found, err := fb.Get(ctx, path, &meta)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	meta.State = state
	return fb.Put(ctx, path, &meta)
}

// fetchNewChunks fetches all chunks from queuePath with seq > after, sorted by seq.
//
// Firebase REST API converts objects whose keys are consecutive integers
// starting at 0 into a JSON array. We handle both cases here.
func fetchNewChunks(ctx context.Context, fb *firebase.Client, queuePath string, after *uint64) ([]protocol.Chunk, error) {
	// Use a raw map/slice since Firebase may return either an object or array.
	var raw interface{}
	found, err := fb.Get(ctx, queuePath, &raw)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	var chunks []protocol.Chunk

	switch v := raw.(type) {
	case map[string]interface{}:
		for _, val := range v {
			chunk, err := interfaceToChunk(val)
			if err != nil {
				return nil, fmt.Errorf("deserializing chunk from map: %w", err)
			}
			chunks = append(chunks, chunk)
		}
	case []interface{}:
		// Firebase collapsed integer-keyed object into an array.
		for _, val := range v {
			if val == nil {
				continue
			}
			chunk, err := interfaceToChunk(val)
			if err != nil {
				return nil, fmt.Errorf("deserializing chunk from array: %w", err)
			}
			chunks = append(chunks, chunk)
		}
	default:
		slog.Warn("fetchNewChunks: unexpected JSON shape", "type", fmt.Sprintf("%T", raw))
		return nil, nil
	}

	if after != nil {
		filtered := chunks[:0]
		for _, c := range chunks {
			if c.Seq > *after {
				filtered = append(filtered, c)
			}
		}
		chunks = filtered
	}

	// Sort by sequence number.
	sortChunks(chunks)
	return chunks, nil
}

// interfaceToChunk round-trips an interface{} through JSON to get a Chunk.
func interfaceToChunk(v interface{}) (protocol.Chunk, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return protocol.Chunk{}, err
	}
	var chunk protocol.Chunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		return protocol.Chunk{}, err
	}
	return chunk, nil
}

// sortChunks sorts a slice of chunks by sequence number (insertion sort for small slices).
func sortChunks(chunks []protocol.Chunk) {
	n := len(chunks)
	for i := 1; i < n; i++ {
		key := chunks[i]
		j := i - 1
		for j >= 0 && chunks[j].Seq > key.Seq {
			chunks[j+1] = chunks[j]
			j--
		}
		chunks[j+1] = key
	}
}
