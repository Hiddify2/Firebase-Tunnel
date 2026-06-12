// Package protocol defines every wire-level type that flows through Firebase
// Realtime Database between the client and the server.
//
// Database layout:
//
//	sessions/
//	  {session_id}/
//	    metadata/        – SessionMetadata (written by client on creation)
//	    c2s/             – client-to-server queue  (keyed by seq number)
//	      {seq}/         – Chunk
//	    s2c/             – server-to-client queue  (keyed by seq number)
//	      {seq}/         – Chunk
//	    acks/
//	      c2s_ack        – uint64 – highest consecutive seq the server has processed
//	      s2c_ack        – uint64 – highest consecutive seq the client has processed
//
// Sequence numbers start at 0 and are monotonically increasing per direction
// per session. The receiver reassembles chunks in-order before passing them
// upstream. Gaps are tolerated by buffering out-of-order chunks; a chunk is
// delivered only after all predecessors have been delivered.
//
// After a chunk has been acknowledged it is deleted from the database to prevent
// unbounded growth.
package protocol

import (
	"fmt"
	"time"
)

// SessionState represents the lifecycle state of a tunnel session.
type SessionState string

const (
	// SessionStatePending means the client has written metadata; server has not yet accepted.
	SessionStatePending SessionState = "pending"

	// SessionStateActive means the server has opened the outbound TCP connection successfully.
	SessionStateActive SessionState = "active"

	// SessionStateClosing means either side requested orderly shutdown.
	SessionStateClosing SessionState = "closing"

	// SessionStateClosed means the session is fully terminated; safe to garbage-collect.
	SessionStateClosed SessionState = "closed"
)

// SessionMetadata is written by the client when a new SOCKS5 connection is
// accepted. The server reads this to know where to connect.
type SessionMetadata struct {
	// SessionID is the unique session identifier (UUIDv4).
	SessionID string `json:"session_id"`

	// TargetHost is the target host requested by the SOCKS5 client (hostname or IP string).
	TargetHost string `json:"target_host"`

	// TargetPort is the target port requested by the SOCKS5 client.
	TargetPort uint16 `json:"target_port"`

	// CreatedAt is the Unix timestamp (milliseconds) when the session was created.
	CreatedAt uint64 `json:"created_at"`

	// State is the current lifecycle state.
	State SessionState `json:"state"`
}

// Chunk is a single batched, compressed, base64-encoded data chunk stored under
// sessions/{id}/c2s/{seq} or sessions/{id}/s2c/{seq}.
//
// The chunk is the atomic unit of transfer. Each chunk carries:
//   - a monotonically increasing seq number for ordering / deduplication,
//   - the actual payload (base64-encoded, optionally zstd-compressed),
//   - a flag indicating whether the payload is compressed,
//   - a wall-clock timestamp for diagnostic purposes.
type Chunk struct {
	// Seq is a monotonically increasing sequence number (per direction, per session).
	Seq uint64 `json:"seq"`

	// Timestamp is the wall-clock time when the chunk was created (Unix milliseconds).
	Timestamp uint64 `json:"timestamp"`

	// Compressed is true if the raw payload bytes were zstd-compressed before base64-encoding.
	Compressed bool `json:"compressed"`

	// Data is the base64-encoded payload. If Compressed is true the decoded bytes are
	// a zstd stream that decompresses to the original TCP data.
	Data string `json:"data"`
}

// AckRecord is the acknowledgement record stored at sessions/{id}/acks/.
//
// Each side maintains two fields:
//   - C2SAck: the highest consecutive sequence number the server has processed
//     from the c2s queue.
//   - S2CAck: the highest consecutive sequence number the client has processed
//     from the s2c queue.
//
// After updating an ack the writer deletes all chunks with seq ≤ ack from the
// corresponding queue.
type AckRecord struct {
	// C2SAck is the highest consecutive c2s seq processed by the server.
	// nil means no chunk has been acknowledged yet.
	C2SAck *uint64 `json:"c2s_ack,omitempty"`

	// S2CAck is the highest consecutive s2c seq processed by the client.
	// nil means no chunk has been acknowledged yet.
	S2CAck *uint64 `json:"s2c_ack,omitempty"`
}

// ──────────────────────────────────────────────────────────────────────────────
// Firebase path helpers
// ──────────────────────────────────────────────────────────────────────────────

// PathMetadata returns the Firebase path for a session's metadata node.
func PathMetadata(sessionID string) string {
	return "sessions/" + sessionID + "/metadata"
}

// PathC2S returns the Firebase path for the client-to-server queue root.
func PathC2S(sessionID string) string {
	return "sessions/" + sessionID + "/c2s"
}

// PathC2SChunk returns the Firebase path for a specific c2s chunk.
func PathC2SChunk(sessionID string, seq uint64) string {
	return fmt.Sprintf("sessions/%s/c2s/%d", sessionID, seq)
}

// PathS2C returns the Firebase path for the server-to-client queue root.
func PathS2C(sessionID string) string {
	return "sessions/" + sessionID + "/s2c"
}

// PathS2CChunk returns the Firebase path for a specific s2c chunk.
func PathS2CChunk(sessionID string, seq uint64) string {
	return fmt.Sprintf("sessions/%s/s2c/%d", sessionID, seq)
}

// PathAcks returns the Firebase path for the ack record of a session.
func PathAcks(sessionID string) string {
	return "sessions/" + sessionID + "/acks"
}

// PathSessionsRoot returns the root path for all sessions.
func PathSessionsRoot() string {
	return "sessions"
}

// NowMillis returns the current Unix time in milliseconds.
func NowMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}
