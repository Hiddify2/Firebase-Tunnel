// Package fbtunnel is the root of the fb-tunnel-go library.
//
// # Overview
//
// fb-tunnel-go is a Go library that creates a bidirectional TCP tunnel by
// encoding data as JSON nodes in a Firebase Realtime Database. A local client
// exposes a standard SOCKS5 proxy (default 127.0.0.1:1080); any application
// that speaks SOCKS5 can route its traffic through the tunnel. A server process
// (running anywhere with internet access) reads the queued data from Firebase,
// opens the real TCP connection, and forwards traffic in both directions.
//
//	Application ──SOCKS5──▶ [client] ──Firebase RTDB──▶ [server] ──TCP──▶ Destination
//
// # Packages
//
//   - protocol: Wire message definitions (Chunk, SessionMetadata, AckRecord) and
//     Firebase path helpers.
//   - compress: zstd compression / decompression utilities.
//   - firebase: Firebase Realtime Database REST transport with SSE streaming
//     listener and exponential-backoff retry logic.
//   - session: ChunkSender (batching + compression + Firebase write) and
//     ChunkReceiver (in-order reassembly + ack management).
//   - socks5: SOCKS5 handshake (RFC 1928, CONNECT command only).
//   - tunnel: ClientSession and HandleSession implement the full bidirectional
//     relay logic for client and server respectively.
//   - config: TOML configuration types for client and server.
//
// # Usage as a library
//
// Client side – embed a SOCKS5 proxy that tunnels over Firebase:
//
//	cfg := &config.ClientConfig{
//	    FirebaseURL:     "https://my-project-default-rtdb.firebaseio.com",
//	    FirebaseSecret:  "secret",
//	    Listen:          "127.0.0.1:1080",
//	    BatchIntervalMS: 50,
//	    BatchMaxBytes:   32768,
//	    RetryLimit:      5,
//	}
//	fb := firebase.NewClient(cfg.FirebaseURL, cfg.FirebaseSecret, cfg.RetryLimit)
//	ln, _ := net.Listen("tcp", cfg.Listen)
//	for {
//	    conn, _ := ln.Accept()
//	    go func(c net.Conn) {
//	        host, port, err := socks5.Handshake(c)
//	        if err != nil { return }
//	        cs, err := tunnel.NewClientSession(ctx, host, port, fb,
//	            time.Duration(cfg.BatchIntervalMS)*time.Millisecond, cfg.BatchMaxBytes)
//	        if err != nil { return }
//	        cs.Run(ctx, c)
//	    }(conn)
//	}
//
// Server side – handle sessions discovered in Firebase:
//
//	cfg := &config.ServerConfig{
//	    FirebaseURL:     "https://my-project-default-rtdb.firebaseio.com",
//	    FirebaseSecret:  "secret",
//	    PollIntervalMS:  200,
//	    SessionTimeoutS: 300,
//	    RetryLimit:      5,
//	}
//	fb := firebase.NewClient(cfg.FirebaseURL, cfg.FirebaseSecret, cfg.RetryLimit)
//	for {
//	    sessions := discoverPending(ctx, fb)
//	    for _, meta := range sessions {
//	        go tunnel.HandleSession(ctx, meta, fb, cfg)
//	    }
//	    time.Sleep(time.Duration(cfg.PollIntervalMS) * time.Millisecond)
//	}
//
// # Research / Educational Notice
//
// This project is intended for studying unconventional transport mechanisms and
// is NOT designed for production use or any purpose that violates Firebase Terms
// of Service.
package fbtunnel
