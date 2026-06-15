# fb-tunnel-go

> **Research / Educational Proof-of-Concept**
>
> A Go library and CLI that creates a SOCKS5 proxy tunnel using **Firebase
> Realtime Database** as its transport layer. This project is a faithful Go port
> of the original Rust implementation (`fb-tunnel`). It is intended for studying
> unconventional transport mechanisms and is **not** designed for production use
> or any purpose that violates Firebase Terms of Service.

[![CI](https://github.com/YOUR_ORG/fb-tunnel-go/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_ORG/fb-tunnel-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/fb-tunnel/fb-tunnel-go.svg)](https://pkg.go.dev/github.com/fb-tunnel/fb-tunnel-go)

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Database Layout](#database-layout)
4. [Protocol Design](#protocol-design)
5. [Package Structure](#package-structure)
6. [Using as a Library](#using-as-a-library)
7. [Prerequisites](#prerequisites)
8. [Build Instructions](#build-instructions)
9. [Configuration](#configuration)
10. [Running](#running)
11. [Testing](#testing)
12. [GitHub Workflows](#github-workflows)
13. [Known Limitations](#known-limitations)

---

## Overview

`fb-tunnel-go` creates a bidirectional TCP tunnel by encoding data as JSON nodes
in a Firebase Realtime Database. A local **client** process exposes a standard
SOCKS5 proxy (default `127.0.0.1:1080`); any application that speaks SOCKS5 can
route its traffic through the tunnel. A **server** process (running anywhere
with internet access) reads the queued data from Firebase, opens the real TCP
connection, and forwards traffic in both directions.

```
Application ──SOCKS5──▶ [client] ──Firebase RTDB──▶ [server] ──TCP──▶ Destination
```

---

## Architecture

### Module layout

| Package | Role |
|---------|------|
| `protocol` | Wire types (Chunk, SessionMetadata, AckRecord), path helpers |
| `compress` | zstd compression / decompression (via klauspost/compress) |
| `firebase` | Firebase REST client with SSE streaming and exponential-backoff retries |
| `session` | ChunkSender (batching + write) and ChunkReceiver (in-order reassembly + ack) |
| `socks5` | SOCKS5 handshake (RFC 1928, CONNECT only) |
| `tunnel` | ClientSession and HandleSession – the full bidirectional relay |
| `config` | TOML configuration for client and server |
| `client` | CLI binary (SOCKS5 listener + Firebase forwarder) |
| `server` | CLI binary (session poller + TCP connector + Firebase forwarder) |

---

## Database Layout

```
sessions/
  {session_id}/               ← UUIDv4
    metadata/                 ← SessionMetadata JSON
      session_id: "..."
      target_host: "example.com"
      target_port: 443
      created_at: 1700000000000
      state: "pending" | "active" | "closing" | "closed"

    c2s/                      ← Client → Server queue
      0/                      ← Chunk (seq=0)
        seq: 0
        timestamp: 1700000000050
        compressed: true
        data: "BASE64_ZSTD_PAYLOAD"
      1/ ...

    s2c/                      ← Server → Client queue
      0/ ...

    acks/
      c2s_ack: 3              ← Server has processed c2s chunks 0-3
      s2c_ack: 7              ← Client has processed s2c chunks 0-7
```

---

## Protocol Design

### Session lifecycle

```
Client                     Firebase                    Server
  │                           │                           │
  │── PUT metadata (Pending) ─▶│                           │
  │                           │◀──── polls sessions/ ─────│
  │                           │────── SSE / GET push ─────▶│
  │                           │                           │── TCP connect
  │◀── PUT metadata (Active) ─│◀──────────────────────────│
  │                           │                           │
  │══ data flows c2s / s2c ══▶│◀══════════════════════════│
  │                           │                           │
  │── PUT metadata (Closing) ─▶│                           │
  │                           │────── push ───────────────▶│── TCP close
  │◀── PUT metadata (Closed) ─│◀──────────────────────────│
  │── DELETE sessions/{id} ───▶│                           │
```

### Chunk format

```json
{
  "seq":        123,
  "timestamp":  1700000000050,
  "compressed": true,
  "data":       "BASE64_ENCODED_ZSTD_STREAM"
}
```

### Batching

The client accumulates outgoing TCP bytes for up to `batch_interval_ms` (default
50 ms) **or** until the buffer reaches `batch_max_bytes` (default 32 KiB),
whichever comes first. The accumulated bytes are then:

1. **Compressed** with zstd (level 3) if ≥ 64 bytes.
2. **Base64-encoded** to a JSON-safe string.
3. **Written** as a single chunk to Firebase with a monotonically increasing
   sequence number.

### Acknowledgements and cleanup

After a receiver delivers a contiguous prefix of chunks it writes the highest
consecutive seq number to the `acks/` node and **deletes** all chunks with
seq ≤ ack from the queue.

---

## Package Structure

```
fb-tunnel-go/
├── fbtunnel.go              Root doc file (library entry point)
├── go.mod
├── go.sum
├── .golangci.yml            Linter configuration
├── .github/
│   └── workflows/
│       ├── ci.yml           CI: lint, test (matrix), bench, build
│       ├── release.yml      Release: cross-compile + GitHub Release
│       └── codeql.yml       Weekly CodeQL security scan
├── compress/
│   ├── compress.go          zstd wrappers
│   └── compress_test.go
├── config/
│   ├── config.go            ClientConfig + ServerConfig TOML loading
│   └── config_test.go
├── firebase/
│   ├── firebase.go          REST client, retry logic, SSE streaming
│   └── firebase_test.go
├── protocol/
│   ├── protocol.go          Wire types + Firebase path helpers
│   └── protocol_test.go
├── session/
│   ├── session.go           ChunkSender, ChunkReceiver, ack helpers
│   └── session_test.go
├── socks5/
│   ├── socks5.go            SOCKS5 RFC 1928 handshake
│   └── socks5_test.go
├── tunnel/
│   ├── client.go            ClientSession – client-side relay
│   ├── server.go            HandleSession – server-side relay
│   └── tunnel_test.go
├── client/
│   └── main.go              CLI client binary
└── server/
    └── main.go              CLI server binary
```

---

## Using as a Library

Add the module to your project:

```bash
go get github.com/fb-tunnel/fb-tunnel-go
```

### Client side

```go
import (
    "context"
    "net"
    "time"

    "github.com/fb-tunnel/fb-tunnel-go/config"
    "github.com/fb-tunnel/fb-tunnel-go/firebase"
    "github.com/fb-tunnel/fb-tunnel-go/socks5"
    "github.com/fb-tunnel/fb-tunnel-go/tunnel"
)

cfg := &config.ClientConfig{
    FirebaseURL:     "https://my-project-default-rtdb.firebaseio.com",
    FirebaseSecret:  "your-secret",
    Listen:          "127.0.0.1:1080",
    BatchIntervalMS: 50,
    BatchMaxBytes:   32768,
    RetryLimit:      5,
}
fb := firebase.NewClient(cfg.FirebaseURL, cfg.FirebaseSecret, cfg.RetryLimit)
ln, _ := net.Listen("tcp", cfg.Listen)

for {
    conn, _ := ln.Accept()
    go func(c net.Conn) {
        defer c.Close()
        host, port, err := socks5.Handshake(c)
        if err != nil { return }
        cs, err := tunnel.NewClientSession(context.Background(), host, port, fb,
            time.Duration(cfg.BatchIntervalMS)*time.Millisecond, cfg.BatchMaxBytes)
        if err != nil { return }
        cs.Run(context.Background(), c)
    }(conn)
}
```

### Server side

```go
import (
    "context"
    "encoding/json"
    "time"

    "github.com/fb-tunnel/fb-tunnel-go/config"
    "github.com/fb-tunnel/fb-tunnel-go/firebase"
    "github.com/fb-tunnel/fb-tunnel-go/protocol"
    "github.com/fb-tunnel/fb-tunnel-go/tunnel"
)

cfg := &config.ServerConfig{
    FirebaseURL:     "https://my-project-default-rtdb.firebaseio.com",
    FirebaseSecret:  "your-secret",
    PollIntervalMS:  200,
    SessionTimeoutS: 300,
    RetryLimit:      5,
}
fb := firebase.NewClient(cfg.FirebaseURL, cfg.FirebaseSecret, cfg.RetryLimit)

for {
    sessions := discoverPendingSessions(context.Background(), fb)
    for _, meta := range sessions {
        m := meta
        go tunnel.HandleSession(context.Background(), m, fb, cfg)
    }
    time.Sleep(time.Duration(cfg.PollIntervalMS) * time.Millisecond)
}
```

---

## Prerequisites

- Go 1.22 or later ([go.dev](https://go.dev/dl/))
- A Firebase project with Realtime Database enabled (free Spark plan is fine
  for testing)
- No CGO required – pure Go (zstd via `klauspost/compress`)

---

## Build Instructions

```bash
# Clone
git clone https://github.com/YOUR_ORG/fb-tunnel-go
cd fb-tunnel-go

# Download dependencies
go mod download

# Build all packages
go build ./...

# Build CLI binaries
go build -o fb-tunnel-client ./client
go build -o fb-tunnel-server ./server

# Build optimised release binaries
go build -ldflags="-s -w" -o fb-tunnel-client ./client
go build -ldflags="-s -w" -o fb-tunnel-server ./server

# Cross-compile (example: Linux ARM64 on any host)
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o fb-tunnel-client-linux-arm64 ./client
```

---

## Configuration

### `client.toml`

```toml
firebase_url    = "https://YOUR-PROJECT-default-rtdb.firebaseio.com"
firebase_secret = "YOUR_DATABASE_SECRET"
listen          = "127.0.0.1:1080"   # SOCKS5 listener address
batch_interval_ms  = 50              # flush interval in ms
batch_max_bytes    = 32768           # flush when buffer reaches this
retry_limit        = 5
```

### `server.toml`

```toml
firebase_url    = "https://YOUR-PROJECT-default-rtdb.firebaseio.com"
firebase_secret = "YOUR_DATABASE_SECRET"
poll_interval_ms   = 200   # how often to check for new sessions
session_timeout_s  = 300   # inactivity timeout before cleanup
retry_limit        = 5
```

---

## Running

```bash
# Terminal 1 – server (anywhere with internet access)
./fb-tunnel-server server.toml

# Terminal 2 – client (local machine)
./fb-tunnel-client client.toml

# Now configure your application to use SOCKS5 proxy at 127.0.0.1:1080
curl --socks5-hostname 127.0.0.1:1080 https://example.com
```

---

## Testing

```bash
# Run all tests
go test ./...

# Run with race detector (recommended)
go test -race ./...

# Run with verbose output
go test -v ./...

# Run specific package tests
go test -v ./compress/...
go test -v ./session/...
go test -v ./socks5/...
go test -v ./firebase/...
go test -v ./protocol/...

# Run benchmarks
go test -bench=. -benchmem ./compress/...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

---

## GitHub Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `ci.yml` | push / PR to main | Lint (golangci-lint), test matrix (3 OS × 2 Go versions, race detector), benchmarks, cross-compile binaries |
| `release.yml` | push tag `v*.*.*` | Cross-compile 5 platform binaries + SHA256SUMS, publish GitHub Release |
| `codeql.yml` | push / PR / weekly | GitHub CodeQL security scan |

---

## Known Limitations

- **Not for production**: Firebase Realtime Database has strict rate limits and
  this approach has high latency (≥ 200 ms round-trip).
- **Authentication**: uses legacy Database Secrets. Consider Firebase JWT tokens
  for any real-world adaptation.
- **No TLS between client and Firebase**: Firebase REST API uses HTTPS but data
  contents are visible to anyone with the Database Secret.

## Acknowledgements

The idea behind this project was inspired by [LlamaStudioDev](https://github.com/LlamaStudioDev). Credit goes to them for the original concept and inspiration.
- **CONNECT only**: UDP associate and BIND SOCKS5 commands are not supported.
