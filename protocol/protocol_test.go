package protocol_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/fb-tunnel/fb-tunnel-go/protocol"
)

// ── Path helpers ──────────────────────────────────────────────────────────────

func TestPathHelpers(t *testing.T) {
	id := "abc123"

	cases := []struct {
		got  string
		want string
	}{
		{protocol.PathMetadata(id), "sessions/abc123/metadata"},
		{protocol.PathC2S(id), "sessions/abc123/c2s"},
		{protocol.PathC2SChunk(id, 7), "sessions/abc123/c2s/7"},
		{protocol.PathS2C(id), "sessions/abc123/s2c"},
		{protocol.PathS2CChunk(id, 42), "sessions/abc123/s2c/42"},
		{protocol.PathAcks(id), "sessions/abc123/acks"},
		{protocol.PathSessionsRoot(), "sessions"},
	}

	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("got %q, want %q", tc.got, tc.want)
		}
	}
}

// ── Chunk serialization ───────────────────────────────────────────────────────

func TestChunkRoundTripsJSON(t *testing.T) {
	chunk := protocol.Chunk{
		Seq:        5,
		Timestamp:  1_000_000,
		Compressed: true,
		Data:       "SGVsbG8=",
	}
	data, err := json.Marshal(&chunk)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back protocol.Chunk
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Seq != 5 {
		t.Errorf("seq: got %d, want 5", back.Seq)
	}
	if !back.Compressed {
		t.Error("compressed: got false, want true")
	}
	if back.Data != "SGVsbG8=" {
		t.Errorf("data: got %q, want SGVsbG8=", back.Data)
	}
}

func TestChunkJSONFieldNames(t *testing.T) {
	chunk := protocol.Chunk{
		Seq:        3,
		Timestamp:  9999,
		Compressed: false,
		Data:       "AAAA",
	}
	data, _ := json.Marshal(&chunk)
	s := string(data)
	for _, field := range []string{`"seq"`, `"timestamp"`, `"compressed"`, `"data"`} {
		if !strings.Contains(s, field) {
			t.Errorf("expected JSON field %s in %s", field, s)
		}
	}
}

// ── SessionMetadata ───────────────────────────────────────────────────────────

func TestSessionMetadataRoundTripsJSON(t *testing.T) {
	meta := protocol.SessionMetadata{
		SessionID:  "test-session-id",
		TargetHost: "example.com",
		TargetPort: 443,
		CreatedAt:  12345678,
		State:      protocol.SessionStatePending,
	}
	data, err := json.Marshal(&meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back protocol.SessionMetadata
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.SessionID != "test-session-id" {
		t.Errorf("session_id: got %q", back.SessionID)
	}
	if back.TargetHost != "example.com" {
		t.Errorf("target_host: got %q", back.TargetHost)
	}
	if back.TargetPort != 443 {
		t.Errorf("target_port: got %d", back.TargetPort)
	}
	if back.State != protocol.SessionStatePending {
		t.Errorf("state: got %q", back.State)
	}
}

func TestSessionStateJSONValues(t *testing.T) {
	states := map[protocol.SessionState]string{
		protocol.SessionStatePending: `"pending"`,
		protocol.SessionStateActive:  `"active"`,
		protocol.SessionStateClosing: `"closing"`,
		protocol.SessionStateClosed:  `"closed"`,
	}
	for state, want := range states {
		data, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("marshal %v: %v", state, err)
		}
		if string(data) != want {
			t.Errorf("state %v: got %s, want %s", state, data, want)
		}
	}
}

// ── AckRecord ─────────────────────────────────────────────────────────────────

func TestAckRecordRoundTripsJSON(t *testing.T) {
	c2s := uint64(5)
	s2c := uint64(10)
	ack := protocol.AckRecord{
		C2SAck: &c2s,
		S2CAck: &s2c,
	}
	data, err := json.Marshal(&ack)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back protocol.AckRecord
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.C2SAck == nil || *back.C2SAck != 5 {
		t.Errorf("c2s_ack: unexpected %v", back.C2SAck)
	}
	if back.S2CAck == nil || *back.S2CAck != 10 {
		t.Errorf("s2c_ack: unexpected %v", back.S2CAck)
	}
}

func TestAckRecordNilFieldsOmitted(t *testing.T) {
	ack := protocol.AckRecord{}
	data, err := json.Marshal(&ack)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(data)
	if strings.Contains(s, "c2s_ack") || strings.Contains(s, "s2c_ack") {
		t.Errorf("nil ack fields should be omitted: got %s", s)
	}
}

// ── NowMillis ─────────────────────────────────────────────────────────────────

func TestNowMillisIsReasonable(t *testing.T) {
	before := uint64(time.Now().Add(-time.Second).UnixMilli())
	ts := protocol.NowMillis()
	after := uint64(time.Now().Add(time.Second).UnixMilli())

	if ts < before || ts > after {
		t.Errorf("NowMillis() = %d is outside expected range [%d, %d]", ts, before, after)
	}
}
