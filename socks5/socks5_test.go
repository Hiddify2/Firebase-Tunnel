package socks5_test

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/fb-tunnel/fb-tunnel-go/socks5"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// dialPair creates a pair of connected net.Conn values (server, client).
func dialPair(t *testing.T) (server net.Conn, client net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	done := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(done)
			return
		}
		done <- c
	}()

	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	server = <-done
	if server == nil {
		t.Fatal("server accept failed")
	}
	return server, client
}

// sendNoAuthGreeting writes a SOCKS5 greeting advertising only no-auth.
func sendNoAuthGreeting(t *testing.T, c net.Conn) {
	t.Helper()
	_, err := c.Write([]byte{0x05, 0x01, 0x00})
	if err != nil {
		t.Fatalf("write greeting: %v", err)
	}
}

// sendConnectIPv4 writes a SOCKS5 CONNECT request for an IPv4 address.
func sendConnectIPv4(t *testing.T, c net.Conn, ip [4]byte, port uint16) {
	t.Helper()
	req := []byte{0x05, 0x01, 0x00, 0x01, ip[0], ip[1], ip[2], ip[3], 0, 0}
	binary.BigEndian.PutUint16(req[8:], port)
	_, err := c.Write(req)
	if err != nil {
		t.Fatalf("write connect request: %v", err)
	}
}

// sendConnectDomain writes a SOCKS5 CONNECT request for a domain name.
func sendConnectDomain(t *testing.T, c net.Conn, domain string, port uint16) {
	t.Helper()
	req := make([]byte, 0, 7+len(domain))
	req = append(req, 0x05, 0x01, 0x00, 0x03, byte(len(domain)))
	req = append(req, []byte(domain)...)
	req = append(req, 0, 0)
	binary.BigEndian.PutUint16(req[len(req)-2:], port)
	_, err := c.Write(req)
	if err != nil {
		t.Fatalf("write connect request (domain): %v", err)
	}
}

// sendConnectIPv6 writes a SOCKS5 CONNECT request for an IPv6 address.
func sendConnectIPv6(t *testing.T, c net.Conn, ip [16]byte, port uint16) {
	t.Helper()
	req := make([]byte, 0, 22)
	req = append(req, 0x05, 0x01, 0x00, 0x04)
	req = append(req, ip[:]...)
	req = append(req, 0, 0)
	binary.BigEndian.PutUint16(req[len(req)-2:], port)
	_, err := c.Write(req)
	if err != nil {
		t.Fatalf("write connect request (ipv6): %v", err)
	}
}

// readN reads exactly n bytes from c.
func readN(t *testing.T, c net.Conn, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	total := 0
	for total < n {
		nr, err := c.Read(buf[total:])
		if err != nil {
			t.Fatalf("read: %v (got %d/%d bytes)", err, total, n)
		}
		total += nr
	}
	return buf
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestHandshakeIPv4(t *testing.T) {
	server, client := dialPair(t)
	defer server.Close()
	defer client.Close()

	resultCh := make(chan struct {
		host string
		port uint16
		err  error
	}, 1)
	go func() {
		host, port, err := socks5.Handshake(server)
		resultCh <- struct {
			host string
			port uint16
			err  error
		}{host, port, err}
	}()

	// Step 1: send greeting.
	sendNoAuthGreeting(t, client)
	// Read method selection.
	resp := readN(t, client, 2)
	if resp[0] != 0x05 || resp[1] != 0x00 {
		t.Fatalf("unexpected method selection: %v", resp)
	}

	// Step 2: send CONNECT for 1.2.3.4:80.
	sendConnectIPv4(t, client, [4]byte{1, 2, 3, 4}, 80)
	// Read success reply (10 bytes).
	reply := readN(t, client, 10)
	if reply[0] != 0x05 || reply[1] != 0x00 {
		t.Fatalf("unexpected reply: %v", reply)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("handshake error: %v", result.err)
	}
	if result.host != "1.2.3.4" {
		t.Errorf("host: got %q, want %q", result.host, "1.2.3.4")
	}
	if result.port != 80 {
		t.Errorf("port: got %d, want 80", result.port)
	}
}

func TestHandshakeDomain(t *testing.T) {
	server, client := dialPair(t)
	defer server.Close()
	defer client.Close()

	resultCh := make(chan struct {
		host string
		port uint16
		err  error
	}, 1)
	go func() {
		host, port, err := socks5.Handshake(server)
		resultCh <- struct {
			host string
			port uint16
			err  error
		}{host, port, err}
	}()

	sendNoAuthGreeting(t, client)
	readN(t, client, 2) // method selection

	sendConnectDomain(t, client, "example.com", 443)
	reply := readN(t, client, 10)
	if reply[0] != 0x05 || reply[1] != 0x00 {
		t.Fatalf("unexpected reply: %v", reply)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("handshake error: %v", result.err)
	}
	if result.host != "example.com" {
		t.Errorf("host: got %q, want %q", result.host, "example.com")
	}
	if result.port != 443 {
		t.Errorf("port: got %d, want 443", result.port)
	}
}

func TestHandshakeIPv6(t *testing.T) {
	server, client := dialPair(t)
	defer server.Close()
	defer client.Close()

	resultCh := make(chan struct {
		host string
		port uint16
		err  error
	}, 1)
	go func() {
		host, port, err := socks5.Handshake(server)
		resultCh <- struct {
			host string
			port uint16
			err  error
		}{host, port, err}
	}()

	sendNoAuthGreeting(t, client)
	readN(t, client, 2)

	var ip6 [16]byte
	ip6[15] = 1 // ::1
	sendConnectIPv6(t, client, ip6, 8080)
	reply := readN(t, client, 10)
	if reply[0] != 0x05 || reply[1] != 0x00 {
		t.Fatalf("unexpected reply: %v", reply)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("handshake error: %v", result.err)
	}
	if result.port != 8080 {
		t.Errorf("port: got %d, want 8080", result.port)
	}
}

func TestHandshakeUnsupportedMethod(t *testing.T) {
	server, client := dialPair(t)
	defer server.Close()
	defer client.Close()

	errCh := make(chan error, 1)
	go func() {
		_, _, err := socks5.Handshake(server)
		errCh <- err
	}()

	// Send greeting with only username/password auth (0x02), no no-auth.
	_, _ = client.Write([]byte{0x05, 0x01, 0x02})
	// Server should respond with 0xFF (no acceptable methods).
	resp := readN(t, client, 2)
	if resp[1] != 0xFF {
		t.Errorf("expected 0xFF method, got 0x%02X", resp[1])
	}

	err := <-errCh
	if err == nil {
		t.Fatal("expected error when client doesn't support no-auth")
	}
}

func TestHandshakeUnsupportedCommand(t *testing.T) {
	server, client := dialPair(t)
	defer server.Close()
	defer client.Close()

	errCh := make(chan error, 1)
	go func() {
		_, _, err := socks5.Handshake(server)
		errCh <- err
	}()

	sendNoAuthGreeting(t, client)
	readN(t, client, 2) // method selection

	// Send BIND (0x02) command instead of CONNECT.
	_, _ = client.Write([]byte{0x05, 0x02, 0x00, 0x01, 1, 2, 3, 4, 0, 80})
	reply := readN(t, client, 10)
	// Should get rep=0x07 (command not supported).
	if reply[1] != 0x07 {
		t.Errorf("expected rep=0x07, got 0x%02X", reply[1])
	}

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for unsupported command")
	}
}

func TestHandshakeUnsupportedAddrType(t *testing.T) {
	server, client := dialPair(t)
	defer server.Close()
	defer client.Close()

	errCh := make(chan error, 1)
	go func() {
		_, _, err := socks5.Handshake(server)
		errCh <- err
	}()

	sendNoAuthGreeting(t, client)
	readN(t, client, 2)

	// Send CONNECT with unknown atyp=0x05.
	_, _ = client.Write([]byte{0x05, 0x01, 0x00, 0x05})
	reply := readN(t, client, 10)
	if reply[1] != 0x08 {
		t.Errorf("expected rep=0x08 (atyp not supported), got 0x%02X", reply[1])
	}

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for unsupported address type")
	}
}

func TestHandshakeWrongVersion(t *testing.T) {
	server, client := dialPair(t)
	defer server.Close()
	defer client.Close()

	errCh := make(chan error, 1)
	go func() {
		_, _, err := socks5.Handshake(server)
		errCh <- err
	}()

	// Send SOCKS4 greeting.
	_, _ = client.Write([]byte{0x04, 0x01, 0x00})

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for wrong SOCKS version")
	}
}
