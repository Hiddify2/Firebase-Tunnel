// Command fb-tunnel-client is the client binary for the fb-tunnel proof-of-concept.
//
// It performs two roles simultaneously:
//
//  1. SOCKS5 proxy: listens on a local TCP port, accepts SOCKS5 CONNECT
//     requests, and creates a new Firebase tunnel session for each connection.
//
//  2. Firebase forwarder: for each active session, reads data from the
//     local SOCKS5 socket, batches / compresses / base64-encodes it, and writes
//     it to the c2s queue in Firebase. Simultaneously it polls the s2c queue and
//     feeds received data back to the SOCKS5 client socket.
//
// Usage:
//
//	fb-tunnel-client [path/to/client.toml]
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/fb-tunnel/fb-tunnel-go/config"
	"github.com/fb-tunnel/fb-tunnel-go/firebase"
	"github.com/fb-tunnel/fb-tunnel-go/socks5"
	"github.com/fb-tunnel/fb-tunnel-go/tunnel"
)

func main() {
	// Initialise structured logging.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Load configuration.
	configPath := "client.toml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	slog.Info("loading config", "path", configPath)
	cfg, err := config.LoadClientConfig(configPath)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	// Build the shared Firebase client.
	fb := firebase.NewClient(cfg.FirebaseURL, cfg.FirebaseSecret, cfg.RetryLimit)

	// Bind the local SOCKS5 listener.
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		slog.Error("failed to bind SOCKS5 listener", "addr", cfg.Listen, "err", err)
		os.Exit(1)
	}
	slog.Info("SOCKS5 proxy listening", "addr", cfg.Listen)

	ctx := context.Background()

	for {
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("accept error", "err", err)
			continue
		}
		slog.Info("new SOCKS5 connection", "peer", conn.RemoteAddr())

		go func(c net.Conn) {
			if err := handleConnection(ctx, c, fb, cfg); err != nil {
				slog.Error("connection error", "peer", c.RemoteAddr(), "err", err)
			}
		}(conn)
	}
}

// handleConnection handles one SOCKS5 connection end-to-end.
func handleConnection(ctx context.Context, conn net.Conn, fb *firebase.Client, cfg *config.ClientConfig) error {
	defer conn.Close()

	// --- SOCKS5 handshake ---
	targetHost, targetPort, err := socks5.Handshake(conn)
	if err != nil {
		return err
	}
	slog.Info("SOCKS5 CONNECT", "host", targetHost, "port", targetPort)

	// --- Create tunnel session ---
	cs, err := tunnel.NewClientSession(
		ctx,
		targetHost,
		targetPort,
		fb,
		time.Duration(cfg.BatchIntervalMS)*time.Millisecond,
		cfg.BatchMaxBytes,
	)
	if err != nil {
		return err
	}

	// --- Run bidirectional relay ---
	return cs.Run(ctx, conn)
}
