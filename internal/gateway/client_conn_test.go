package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNewWebSocketClientConnUsesConfiguredWriteChannelSize(t *testing.T) {
	upgrader := websocket.Upgrader{}
	serverConnCh := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		serverConnCh <- conn
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer clientConn.Close()

	serverConn := <-serverConnCh
	defer serverConn.Close()

	conn := NewWebSocketClientConn(clientConn, 1024, time.Second, time.Second, time.Second, 3)
	defer conn.Close()

	if got := cap(conn.writeChan); got != 3 {
		t.Fatalf("write channel size mismatch: got %d want 3", got)
	}
}

func TestWriteMessageReturnsErrWriteChannelFullWhenBufferIsFull(t *testing.T) {
	conn := &WebsocketClientConn{
		writeChan: make(chan []byte, 1),
		closeChan: make(chan struct{}),
	}
	conn.writeChan <- []byte("full")

	if err := conn.WriteMessage([]byte("next")); err != ErrWriteChannelFull {
		t.Fatalf("write error mismatch: got %v want %v", err, ErrWriteChannelFull)
	}
}
