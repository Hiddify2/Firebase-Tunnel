// Package session provides the building blocks for managing the lifecycle of a
// tunnel session on both the client side and the server side.
//
// # Session lifecycle
//
//	Client                           Firebase                      Server
//	  │                                 │                             │
//	  │──write metadata (Pending)──────▶│                             │
//	  │                                 │◀──polls sessions list───────│
//	  │                                 │──SSE push (new session)────▶│
//	  │                                 │                             │──connect TCP
//	  │◀──write metadata (Active)───────│◀────────────────────────────│
//	  │                                 │                             │
//	  │──write c2s chunks──────────────▶│──SSE push──────────────────▶│──forward TCP
//	  │◀──read s2c chunks───────────────│◀──write s2c chunks──────────│
//	  │──write c2s_ack─────────────────▶│                             │
//	  │                                 │◀──write s2c_ack─────────────│
//	  │──delete old c2s chunks─────────▶│◀──delete old s2c chunks─────│
//	  │                                 │                             │
//	  │──write metadata (Closed)───────▶│                             │
//	  │                                 │──SSE push──────────────────▶│──close TCP
//
// # ChunkSender
//
// ChunkSender is used by the writing side of a session to:
//  1. Buffer outgoing TCP bytes.
//  2. Flush them as a compressed, base64-encoded Chunk after a configurable
//     timer or byte threshold.
//  3. Write the chunk to Firebase.
//
// # ChunkReceiver
//
// ChunkReceiver is used by the reading side to:
//  1. Receive chunks from Firebase.
//  2. Reassemble them in-order (buffering out-of-order arrivals).
//  3. Send acknowledged byte streams upstream.
//  4. Update the ack pointer and delete old chunks from Firebase.
package session

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fb-tunnel/fb-tunnel-go/compress"
	"github.com/fb-tunnel/fb-tunnel-go/firebase"
	"github.com/fb-tunnel/fb-tunnel-go/protocol"
)

// compressMinBytes is the minimum payload size below which we skip compression
// (saves CPU for tiny packets where the compressed form might even be larger).
const compressMinBytes = 64

// ──────────────────────────────────────────────────────────────────────────────
// ChunkSender
// ──────────────────────────────────────────────────────────────────────────────

// ChunkSender accumulates outgoing TCP bytes, batches them, and writes them to Firebase.
//
// Call Feed whenever you have bytes to send, and call Flush to force a send
// immediately (e.g. on connection close). Internally a background goroutine also
// flushes on a configurable timer.
//
// ChunkSender is safe for concurrent use. Close it via the context cancellation
// passed to NewChunkSender.
type ChunkSender struct {
	rawCh chan []byte
}

// NewChunkSender creates a new ChunkSender and starts the background flusher goroutine.
//
// Parameters:
//   - ctx: cancellation stops the background flusher after draining pending data.
//   - sessionID: the session this sender belongs to.
//   - queuePath: Firebase path for the outgoing queue (e.g. "sessions/X/c2s").
//   - fb: Firebase client used for writing.
//   - batchInterval: how long to wait before flushing an incomplete batch.
//   - batchMaxBytes: flush immediately when the buffer reaches this size.
func NewChunkSender(
	ctx context.Context,
	sessionID string,
	queuePath string,
	fb *firebase.Client,
	batchInterval time.Duration,
	batchMaxBytes int,
) *ChunkSender {
	rawCh := make(chan []byte, 1024)
	s := &ChunkSender{rawCh: rawCh}
	go flusherTask(ctx, sessionID, queuePath, fb, rawCh, batchInterval, batchMaxBytes)
	return s
}

// Feed enqueues data to be included in the next batch.
//
// Returns an error only if the background flusher has exited (channel closed).
func (s *ChunkSender) Feed(ctx context.Context, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case s.rawCh <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flusherTask is the background goroutine that accumulates bytes and flushes them as chunks.
func flusherTask(
	ctx context.Context,
	sessionID string,
	queuePath string,
	fb *firebase.Client,
	rawCh <-chan []byte,
	batchInterval time.Duration,
	batchMaxBytes int,
) {
	buffer := make([]byte, 0, batchMaxBytes)
	var seq uint64
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Timer tick – flush whatever we have.
			if len(buffer) > 0 {
				if err := flushBuffer(ctx, queuePath, fb, &buffer, &seq); err != nil {
					slog.Warn("flusher_task: flush error", "err", err)
				}
			}

		case data, ok := <-rawCh:
			if !ok || data == nil {
				// Channel closed – flush remaining data and exit.
				if len(buffer) > 0 {
					if err := flushBuffer(ctx, queuePath, fb, &buffer, &seq); err != nil {
						slog.Warn("flusher_task: final flush error", "err", err)
					}
				}
				slog.Debug("flusher_task exiting", "session_id", sessionID)
				return
			}
			buffer = append(buffer, data...)
			// Flush early if we hit the size threshold.
			if len(buffer) >= batchMaxBytes {
				if err := flushBuffer(ctx, queuePath, fb, &buffer, &seq); err != nil {
					slog.Warn("flusher_task: over-size flush error", "err", err)
				}
			}

		case <-ctx.Done():
			// Context cancelled – flush remaining data and exit.
			if len(buffer) > 0 {
				// Use a short background context for the final flush.
				flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := flushBuffer(flushCtx, queuePath, fb, &buffer, &seq); err != nil {
					slog.Warn("flusher_task: ctx-cancel flush error", "err", err)
				}
				cancel()
			}
			slog.Debug("flusher_task exiting on context cancel", "session_id", sessionID)
			return
		}
	}
}

// flushBuffer compresses, encodes, and writes the current buffer as a single chunk.
func flushBuffer(
	ctx context.Context,
	queuePath string,
	fb *firebase.Client,
	buffer *[]byte,
	seq *uint64,
) error {
	if len(*buffer) == 0 {
		return nil
	}

	raw := make([]byte, len(*buffer))
	copy(raw, *buffer)
	*buffer = (*buffer)[:0]

	// Compress if above threshold.
	var payloadBytes []byte
	var compressed bool
	if len(raw) >= compressMinBytes {
		c, err := compress.Compress(raw)
		if err != nil {
			slog.Warn("compression failed, sending uncompressed", "err", err)
			payloadBytes = raw
			compressed = false
		} else {
			payloadBytes = c
			compressed = true
		}
	} else {
		payloadBytes = raw
		compressed = false
	}

	encoded := base64.StdEncoding.EncodeToString(payloadBytes)

	chunk := protocol.Chunk{
		Seq:        *seq,
		Timestamp:  protocol.NowMillis(),
		Compressed: compressed,
		Data:       encoded,
	}

	chunkPath := fmt.Sprintf("%s/%d", queuePath, *seq)
	if err := fb.Put(ctx, chunkPath, &chunk); err != nil {
		return fmt.Errorf("writing chunk seq=%d to %s: %w", *seq, chunkPath, err)
	}

	slog.Debug("flushed chunk", "seq", *seq, "raw_bytes", len(raw), "compressed_bytes", len(payloadBytes), "compressed", compressed)
	*seq++
	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// ChunkReceiver
// ──────────────────────────────────────────────────────────────────────────────

// ChunkReceiver reassembles in-order TCP byte streams from chunks received via Firebase.
//
// Call Ingest with each Chunk as you receive it. Chunks are buffered internally if
// they arrive out-of-order; once a contiguous prefix is complete the bytes are sent
// on the channel returned by ByteStream.
//
// The receiver also tracks the highest consecutive seq acknowledged so callers can
// update Firebase ack nodes and delete old chunks.
type ChunkReceiver struct {
	mu      sync.Mutex
	pending map[uint64][]byte
	nextSeq uint64
	outCh   chan []byte
	ackPtr  *uint64
}

// NewChunkReceiver creates a new receiver.
//
// Read from the returned channel to receive reassembled TCP byte stream segments.
// The channel is buffered with capacity 1024.
func NewChunkReceiver() (*ChunkReceiver, <-chan []byte) {
	outCh := make(chan []byte, 1024)
	r := &ChunkReceiver{
		pending: make(map[uint64][]byte),
		nextSeq: 0,
		outCh:   outCh,
		ackPtr:  nil,
	}
	return r, outCh
}

// Ingest ingests a chunk received from Firebase.
//
// Returns the new ack pointer if it advanced (i.e., new data was delivered).
// Callers should write this value to the Firebase ack node and delete chunks
// with seq ≤ ackPtr.
//
// Duplicate chunks (seq < nextSeq) are silently ignored.
func (r *ChunkReceiver) Ingest(ctx context.Context, chunk protocol.Chunk) (*uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	seq := chunk.Seq

	// Ignore duplicates.
	if seq < r.nextSeq {
		slog.Debug("ChunkReceiver: duplicate chunk, ignoring", "seq", seq)
		return nil, nil
	}

	// Decode and decompress the payload.
	payload, err := DecodeChunkPayload(&chunk)
	if err != nil {
		return nil, err
	}

	// Buffer out-of-order chunks.
	r.pending[seq] = payload

	// Drain all consecutive chunks starting from nextSeq.
	oldAck := r.ackPtr
	for {
		data, ok := r.pending[r.nextSeq]
		if !ok {
			break
		}
		currentSeq := r.nextSeq
		delete(r.pending, currentSeq)

		select {
		case r.outCh <- data:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		ack := currentSeq
		r.ackPtr = &ack
		r.nextSeq++
	}

	// Return the new ack pointer only if it advanced.
	if !ackEqual(r.ackPtr, oldAck) {
		return r.ackPtr, nil
	}
	return nil, nil
}

// AckPtr returns the current ack pointer (the highest consecutively delivered seq).
func (r *ChunkReceiver) AckPtr() *uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ackPtr
}

// ackEqual compares two optional uint64 pointers.
func ackEqual(a, b *uint64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// DecodeChunkPayload decodes the base64 payload of a chunk, decompressing if necessary.
func DecodeChunkPayload(chunk *protocol.Chunk) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(chunk.Data)
	if err != nil {
		return nil, fmt.Errorf("base64-decoding chunk seq=%d: %w", chunk.Seq, err)
	}

	if chunk.Compressed {
		out, err := compress.Decompress(decoded)
		if err != nil {
			return nil, fmt.Errorf("decompressing chunk seq=%d: %w", chunk.Seq, err)
		}
		return out, nil
	}
	return decoded, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Ack + cleanup helpers
// ──────────────────────────────────────────────────────────────────────────────

// UpdateAckAndCleanup updates the Firebase ack pointer for a given direction
// and deletes all chunks with seq ≤ ack from the corresponding queue.
//
// Parameters:
//   - fb: Firebase client.
//   - sessionID: session to update.
//   - ackFieldPath: path to the ack field (e.g. "sessions/X/acks/s2c_ack").
//   - queuePath: path to the queue root (e.g. "sessions/X/s2c").
//   - ack: the new ack value to persist.
func UpdateAckAndCleanup(
	ctx context.Context,
	fb *firebase.Client,
	sessionID string,
	ackFieldPath string,
	queuePath string,
	ack uint64,
) error {
	// Write ack pointer.
	if err := fb.Put(ctx, ackFieldPath, ack); err != nil {
		return fmt.Errorf("updating ack for session %s: %w", sessionID, err)
	}

	// Delete all acknowledged chunks (seq 0 through ack inclusive) in parallel.
	var wg sync.WaitGroup
	for seq := uint64(0); seq <= ack; seq++ {
		wg.Add(1)
		s := seq
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("%s/%d", queuePath, s)
			if err := fb.Delete(ctx, path); err != nil {
				slog.Warn("failed to delete chunk", "path", path, "err", err)
			}
		}()
	}
	wg.Wait()

	slog.Debug("updated ack and cleaned up", "ack", ack, "queue", queuePath)
	return nil
}
