package gateway

import (
	"testing"
	"time"
)

type stubClientConn struct {
	writes [][]byte
	closed bool
}

func (s *stubClientConn) ReadMessage() ([]byte, error) { return nil, nil }
func (s *stubClientConn) WriteMessage(data []byte) error {
	s.writes = append(s.writes, data)
	return nil
}
func (s *stubClientConn) Close() error {
	s.closed = true
	return nil
}
func (s *stubClientConn) SetReadDeadline(_ time.Time) error  { return nil }
func (s *stubClientConn) SetWriteDeadline(_ time.Time) error { return nil }

func TestKickOnlinePrefersControlWriteBeforeClose(t *testing.T) {
	conn := &stubClientConn{}
	client := NewClient(conn, "u1", 5, SDKTypeGo, "token", "c1", &WsServer{})

	if err := client.KickOnline(); err != nil {
		t.Fatalf("kick online: %v", err)
	}

	if len(conn.writes) != 1 {
		t.Fatalf("write count mismatch: got %d want 1", len(conn.writes))
	}
	if !conn.closed {
		t.Fatal("expected connection to be closed")
	}
}
