package compress_test

import (
	"bytes"
	"testing"

	"github.com/fb-tunnel/fb-tunnel-go/compress"
)

func TestRoundTripSmallPayload(t *testing.T) {
	original := []byte("Hello, Firebase tunnel!")
	compressed, err := compress.Compress(original)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	decompressed, err := compress.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(original, decompressed) {
		t.Errorf("round-trip failed: got %q, want %q", decompressed, original)
	}
}

func TestRoundTripEmptyPayload(t *testing.T) {
	original := []byte{}
	compressed, err := compress.Compress(original)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	decompressed, err := compress.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(original, decompressed) {
		t.Errorf("round-trip failed: got %v, want empty", decompressed)
	}
}

func TestRoundTripBinaryPayload(t *testing.T) {
	// 4096 bytes of repeating 0–255 pattern.
	src := make([]byte, 4096)
	for i := range src {
		src[i] = byte(i % 256)
	}
	compressed, err := compress.Compress(src)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	decompressed, err := compress.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(src, decompressed) {
		t.Error("round-trip failed for binary payload")
	}
}

func TestRoundTripLargePayload(t *testing.T) {
	// 1 MiB of pseudo-random-ish data.
	src := make([]byte, 1<<20)
	for i := range src {
		src[i] = byte((i * 31) % 256)
	}
	compressed, err := compress.Compress(src)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	decompressed, err := compress.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(src, decompressed) {
		t.Error("round-trip failed for large payload")
	}
}

func TestDecompressInvalidData(t *testing.T) {
	_, err := compress.Decompress([]byte("this is not zstd data"))
	if err == nil {
		t.Fatal("expected error decompressing invalid data, got nil")
	}
}

func TestCompressReducesSize(t *testing.T) {
	// Highly compressible data: 64 KiB of zeros.
	src := make([]byte, 64*1024)
	compressed, err := compress.Compress(src)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if len(compressed) >= len(src) {
		t.Errorf("expected compression to reduce size: original=%d, compressed=%d", len(src), len(compressed))
	}
}

// BenchmarkCompress benchmarks compression of a 32 KiB buffer.
func BenchmarkCompress(b *testing.B) {
	src := make([]byte, 32*1024)
	for i := range src {
		src[i] = byte(i % 256)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := compress.Compress(src); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecompress benchmarks decompression of a 32 KiB buffer.
func BenchmarkDecompress(b *testing.B) {
	src := make([]byte, 32*1024)
	for i := range src {
		src[i] = byte(i % 256)
	}
	compressed, _ := compress.Compress(src)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := compress.Decompress(compressed); err != nil {
			b.Fatal(err)
		}
	}
}
