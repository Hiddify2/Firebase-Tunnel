// Package socks5 implements the SOCKS5 protocol handshake (RFC 1928) for the
// CONNECT command.
//
// Only no-authentication (method 0x00) and the CONNECT command are supported.
// UDP associate and BIND are not implemented because they do not map naturally
// to a Firebase-based transport.
//
// Protocol summary:
//
//	Client → Server:  version=5, nmethods=1, methods=[0x00]
//	Server → Client:  version=5, method=0x00  (no auth)
//
//	Client → Server:  version=5, cmd=CONNECT(1), rsv=0, atyp, dst_addr, dst_port
//	Server → Client:  version=5, rep=0 (success), rsv=0, atyp, bnd_addr, bnd_port
package socks5

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
)

// SOCKS5 protocol constants (RFC 1928).
const (
	socksVersion      = byte(0x05)
	methodNoAuth      = byte(0x00)
	methodNoAccepted  = byte(0xFF)
	cmdConnect        = byte(0x01)
	atypIPv4          = byte(0x01)
	atypDomain        = byte(0x03)
	atypIPv6          = byte(0x04)
	repSuccess        = byte(0x00)
	repGeneralFailure = byte(0x01)
	repCmdNotSupp     = byte(0x07)
	repAtypNotSupp    = byte(0x08)
)

// Handshake performs the full SOCKS5 handshake on conn.
//
// On success returns (targetHost, targetPort, nil).
// On failure sends the appropriate SOCKS5 error reply before returning the error.
// The caller should not close conn on success; it is ready for proxied data.
func Handshake(conn net.Conn) (targetHost string, targetPort uint16, err error) {
	// ── Step 1: version negotiation ──────────────────────────────────────────

	hdr := make([]byte, 2)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return "", 0, fmt.Errorf("reading version header: %w", err)
	}

	if hdr[0] != socksVersion {
		return "", 0, fmt.Errorf("unsupported SOCKS version: %d", hdr[0])
	}

	nmethods := int(hdr[1])
	methods := make([]byte, nmethods)
	if _, err := io.ReadFull(conn, methods); err != nil {
		return "", 0, fmt.Errorf("reading methods: %w", err)
	}

	// We only support no-auth.
	hasNoAuth := false
	for _, m := range methods {
		if m == methodNoAuth {
			hasNoAuth = true
			break
		}
	}
	if !hasNoAuth {
		_, _ = conn.Write([]byte{socksVersion, methodNoAccepted})
		return "", 0, fmt.Errorf("client does not support no-auth method")
	}

	// Tell the client we chose no-auth.
	if _, err := conn.Write([]byte{socksVersion, methodNoAuth}); err != nil {
		return "", 0, fmt.Errorf("writing method selection: %w", err)
	}
	slog.Debug("SOCKS5: negotiated no-auth")

	// ── Step 2: CONNECT request ───────────────────────────────────────────────

	req := make([]byte, 4)
	if _, err := io.ReadFull(conn, req); err != nil {
		return "", 0, fmt.Errorf("reading request header: %w", err)
	}

	if req[0] != socksVersion {
		_ = sendError(conn, repGeneralFailure)
		return "", 0, fmt.Errorf("unexpected version byte in request: %d", req[0])
	}

	if req[1] != cmdConnect {
		_ = sendError(conn, repCmdNotSupp)
		return "", 0, fmt.Errorf("unsupported SOCKS5 command: %d", req[1])
	}

	atyp := req[3]

	// Read destination address based on address type.
	var dstHost string
	switch atyp {
	case atypIPv4:
		ip := make([]byte, 4)
		if _, err := io.ReadFull(conn, ip); err != nil {
			return "", 0, fmt.Errorf("reading IPv4 address: %w", err)
		}
		dstHost = net.IP(ip).String()

	case atypDomain:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return "", 0, fmt.Errorf("reading domain length: %w", err)
		}
		domain := make([]byte, int(lenBuf[0]))
		if _, err := io.ReadFull(conn, domain); err != nil {
			return "", 0, fmt.Errorf("reading domain: %w", err)
		}
		dstHost = string(domain)

	case atypIPv6:
		ip := make([]byte, 16)
		if _, err := io.ReadFull(conn, ip); err != nil {
			return "", 0, fmt.Errorf("reading IPv6 address: %w", err)
		}
		dstHost = net.IP(ip).String()

	default:
		_ = sendError(conn, repAtypNotSupp)
		return "", 0, fmt.Errorf("unsupported address type: %d", atyp)
	}

	// Read destination port (big-endian u16).
	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return "", 0, fmt.Errorf("reading port: %w", err)
	}
	dstPort := binary.BigEndian.Uint16(portBuf)
	slog.Debug("SOCKS5: CONNECT", "host", dstHost, "port", dstPort)

	// ── Step 3: Send success reply ────────────────────────────────────────────

	// BND.ADDR = 0.0.0.0, BND.PORT = 0 (we are a relay, not a real listener).
	reply := []byte{
		socksVersion,
		repSuccess,
		0x00,       // RSV
		atypIPv4,   // BND.ATYP
		0, 0, 0, 0, // BND.ADDR (0.0.0.0)
		0, 0, // BND.PORT (0)
	}
	if _, err := conn.Write(reply); err != nil {
		return "", 0, fmt.Errorf("writing success reply: %w", err)
	}
	slog.Debug("SOCKS5: handshake complete")

	return dstHost, dstPort, nil
}

// sendError sends a SOCKS5 error reply with IPv4 BND address 0.0.0.0:0.
func sendError(conn net.Conn, rep byte) error {
	reply := []byte{socksVersion, rep, 0x00, atypIPv4, 0, 0, 0, 0, 0, 0}
	_, err := conn.Write(reply)
	return err
}
