package session_test

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/fb-tunnel/fb-tunnel-go/compress"
	"github.com/fb-tunnel/fb-tunnel-go/protocol"
	"github.com/fb-tunnel/fb-tunnel-go/session"
)

// makeUncompressedChunk builds a minimal uncompressed chunk for testing.
func makeUncompressedChunk(seq uint64, data []byte) protocol.Chunk {
	return protocol.Chunk{
		Seq:        seq,
		Timestamp:  0,
		Compressed: false,
		Data:       base64.StdEncoding.EncodeToString(data),
	}
}

// makeCompressedChunk builds a chunk whose payload is zstd-compressed.
func makeCompressedChunk(t *testing.T, seq uint64, data []byte) protocol.Chunk {
	t.Helper()
	compressed, err := compress.Compress(data)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	return protocol.Chunk{
		Seq:        seq,
		Timestamp:  0,
		Compressed: true,
		Data:       base64.StdEncoding.EncodeToString(compressed),
	}
}

// ── ChunkReceiver tests ───────────────────────────────────────────────────────

func TestInOrderDelivery(t *testing.T) {
	ctx := context.Background()
	receiver, byteRx := session.NewChunkReceiver()

	if _, err := receiver.Ingest(ctx, makeUncompressedChunk(0, []byte("hello "))); err != nil {
		t.Fatalf("ingest 0: %v", err)
	}
	if _, err := receiver.Ingest(ctx, makeUncompressedChunk(1, []byte("world"))); err != nil {
		t.Fatalf("ingest 1: %v", err)
	}

	var out []byte
drain:
	for {
		select {
		case b := <-byteRx:
			out = append(out, b...)
		default:
			break drain
		}
	}
	if string(out) != "hello world" {
		t.Errorf("got %q, want %q", out, "hello world")
	}
}

func TestOutOfOrderDelivery(t *testing.T) {
	ctx := context.Background()
	receiver, byteRx := session.NewChunkReceiver()

	// Send chunk 1 first, then 0. Receiver should buffer 1 and only
	// deliver once 0 arrives.
	if _, err := receiver.Ingest(ctx, makeUncompressedChunk(1, []byte("world"))); err != nil {
		t.Fatalf("ingest 1: %v", err)
	}

	// Nothing should be available yet.
	select {
	case b := <-byteRx:
		t.Fatalf("expected no output yet, got %q", b)
	default:
	}

	if _, err := receiver.Ingest(ctx, makeUncompressedChunk(0, []byte("hello "))); err != nil {
		t.Fatalf("ingest 0: %v", err)
	}

	var out []byte
drain:
	for {
		select {
		case b := <-byteRx:
			out = append(out, b...)
		default:
			break drain
		}
	}
	if string(out) != "hello world" {
		t.Errorf("got %q, want %q", out, "hello world")
	}
}

func TestDuplicateChunksAreIgnored(t *testing.T) {
	ctx := context.Background()
	receiver, byteRx := session.NewChunkReceiver()

	if _, err := receiver.Ingest(ctx, makeUncompressedChunk(0, []byte("hello"))); err != nil {
		t.Fatalf("ingest 0: %v", err)
	}
	// Duplicate – should be silently ignored.
	if _, err := receiver.Ingest(ctx, makeUncompressedChunk(0, []byte("DUPLICATE"))); err != nil {
		t.Fatalf("ingest duplicate: %v", err)
	}
	if _, err := receiver.Ingest(ctx, makeUncompressedChunk(1, []byte(" world"))); err != nil {
		t.Fatalf("ingest 1: %v", err)
	}

	var out []byte
drain:
	for {
		select {
		case b := <-byteRx:
			out = append(out, b...)
		default:
			break drain
		}
	}
	if string(out) != "hello world" {
		t.Errorf("got %q, want %q", out, "hello world")
	}
}

func TestAckPointerAdvancesOnDelivery(t *testing.T) {
	ctx := context.Background()
	receiver, _ := session.NewChunkReceiver()

	if ptr := receiver.AckPtr(); ptr != nil {
		t.Errorf("initial ack ptr should be nil, got %d", *ptr)
	}

	ack, err := receiver.Ingest(ctx, makeUncompressedChunk(0, []byte("data")))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if ack == nil {
		t.Fatal("expected ack pointer after in-order delivery, got nil")
	}
	if *ack != 0 {
		t.Errorf("ack pointer: got %d, want 0", *ack)
	}
}

func TestAckPointerNilWhenOutOfOrder(t *testing.T) {
	ctx := context.Background()
	receiver, _ := session.NewChunkReceiver()

	ack, err := receiver.Ingest(ctx, makeUncompressedChunk(1, []byte("data")))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	// Chunk 0 hasn't arrived yet, so nothing is delivered and ack is nil.
	if ack != nil {
		t.Errorf("ack pointer should be nil when chunk 0 not yet seen, got %d", *ack)
	}
}

func TestMultipleOutOfOrderChunks(t *testing.T) {
	ctx := context.Background()
	receiver, byteRx := session.NewChunkReceiver()

	// Insert seq 4, 2, 0, 3, 1 – should assemble to bytes 0,1,2,3,4.
	for _, seq := range []uint64{4, 2, 0, 3, 1} {
		if _, err := receiver.Ingest(ctx, makeUncompressedChunk(seq, []byte{byte(seq)})); err != nil {
			t.Fatalf("ingest %d: %v", seq, err)
		}
	}

	var out []byte
drain:
	for {
		select {
		case b := <-byteRx:
			out = append(out, b...)
		default:
			break drain
		}
	}
	if len(out) != 5 {
		t.Fatalf("expected 5 bytes, got %d", len(out))
	}
	for i, b := range out {
		if b != byte(i) {
			t.Errorf("out[%d] = %d, want %d", i, b, i)
		}
	}
}

func TestAckAdvancesAfterGapFilled(t *testing.T) {
	ctx := context.Background()
	receiver, _ := session.NewChunkReceiver()

	// seq=1 arrives first – no ack yet.
	ack1, _ := receiver.Ingest(ctx, makeUncompressedChunk(1, []byte("b")))
	if ack1 != nil {
		t.Errorf("ack should be nil before seq=0 arrives, got %d", *ack1)
	}

	// seq=0 arrives – both 0 and 1 should now drain; ack should be 1.
	ack0, err := receiver.Ingest(ctx, makeUncompressedChunk(0, []byte("a")))
	if err != nil {
		t.Fatalf("ingest 0: %v", err)
	}
	if ack0 == nil {
		t.Fatal("expected ack after seq=0 fills gap")
	}
	if *ack0 != 1 {
		t.Errorf("expected ack=1 after draining 0 and 1, got %d", *ack0)
	}
}

// ── DecodeChunkPayload tests ──────────────────────────────────────────────────

func TestDecodeChunkPayloadUncompressed(t *testing.T) {
	data := []byte("hello world")
	chunk := makeUncompressedChunk(0, data)
	decoded, err := session.DecodeChunkPayload(&chunk)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(decoded) != "hello world" {
		t.Errorf("got %q, want %q", decoded, "hello world")
	}
}

func TestDecodeChunkPayloadCompressed(t *testing.T) {
	original := []byte("This is a test payload for compression round-trip testing in fb-tunnel-go.")
	chunk := makeCompressedChunk(t, 0, original)
	decoded, err := session.DecodeChunkPayload(&chunk)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(decoded) != string(original) {
		t.Errorf("got %q, want %q", decoded, original)
	}
}

func TestDecodeChunkPayloadBadBase64(t *testing.T) {
	chunk := protocol.Chunk{
		Seq:        0,
		Compressed: false,
		Data:       "!!! NOT BASE64 !!!",
	}
	_, err := session.DecodeChunkPayload(&chunk)
	if err == nil {
		t.Fatal("expected error for invalid base64, got nil")
	}
}

func TestDecodeChunkPayloadEmptyData(t *testing.T) {
	chunk := makeUncompressedChunk(0, []byte{})
	decoded, err := session.DecodeChunkPayload(&chunk)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("expected empty payload, got %q", decoded)
	}
}
