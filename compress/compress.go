// Package compress provides thin wrappers around zstd compression and
// decompression for the fb-tunnel library.
//
// Both functions are synchronous; they are cheap enough for typical packet sizes
// (<64 KiB). Callers processing larger payloads may run them in goroutines.
//
// Compression level 3 (zstd default) is used. This gives a good balance between
// CPU cost and ratio for network tunnel payloads.
package compress

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// CompressionLevel is the default zstd compression level used throughout the tunnel.
const CompressionLevel = 3

// encoder and decoder are package-level singletons for reuse.
var (
	encoder *zstd.Encoder
	decoder *zstd.Decoder
)

func init() {
	var err error
	encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(CompressionLevel)))
	if err != nil {
		panic(fmt.Sprintf("zstd: failed to create encoder: %v", err))
	}
	decoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Sprintf("zstd: failed to create decoder: %v", err))
	}
}

// Compress compresses data with zstd and returns the compressed bytes.
//
// Returns an error if the underlying zstd encoder fails.
func Compress(data []byte) ([]byte, error) {
	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed, nil
}

// Decompress decompresses a zstd stream and returns the original bytes.
//
// Returns an error if data is not a valid zstd stream or if decompression fails.
func Decompress(data []byte) ([]byte, error) {
	out, err := decoder.DecodeAll(data, make([]byte, 0, len(data)*3))
	if err != nil {
		return nil, fmt.Errorf("zstd decompression failed: %w", err)
	}
	return out, nil
}
