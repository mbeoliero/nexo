package gateway

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mbeoliero/kit/log"
)

// ClientConn represents a WebSocket connection wrapper
type ClientConn interface {
	ReadMessage() ([]byte, error)
	WriteMessage(data []byte) error
	Close() error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// WebsocketClientConn implements ClientConn using gorilla/websocket
type WebsocketClientConn struct {
	conn       *websocket.Conn
	writeChan  chan []byte
	writeMu    sync.Mutex
	closeOnce  sync.Once
	closed     bool
	closeChan  chan struct{}
	pingPeriod time.Duration
	pongWait   time.Duration
	writeWait  time.Duration
	maxMsgSize int64
}

// NewWebSocketClientConn creates a new websocket client connection
func NewWebSocketClientConn(
	conn *websocket.Conn,
	maxMsgSize int64,
	writeWait time.Duration,
	pongWait time.Duration,
	pingPeriod time.Duration,
	writeChannelSize int,
) *WebsocketClientConn {
	if writeChannelSize <= 0 {
		writeChannelSize = 256
	}
	c := &WebsocketClientConn{
		conn:       conn,
		writeChan:  make(chan []byte, writeChannelSize),
		closeChan:  make(chan struct{}),
		pingPeriod: pingPeriod,
		pongWait:   pongWait,
		writeWait:  writeWait,
		maxMsgSize: maxMsgSize,
	}

	if conn == nil {
		return c
	}

	// Set read limit
	conn.SetReadLimit(maxMsgSize)

	// Set pong handler to extend read deadline
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Start write loop
	go c.writeLoop()

	return c
}

// writeLoop handles all writes to the connection (single writer pattern)
func (c *WebsocketClientConn) writeLoop() {
	ticker := time.NewTicker(c.pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.writeChan:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if !ok {
				// Channel closed, send close message
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
				log.Warn("write message error: %v", err)
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Debug("ping error: %v", err)
				return
			}

		case <-c.closeChan:
			return
		}
	}
}

// ReadMessage reads a message from the connection
func (c *WebsocketClientConn) ReadMessage() ([]byte, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(c.pongWait))
	_, message, err := c.conn.ReadMessage()
	return message, err
}

// WriteMessage queues a message to be written
func (c *WebsocketClientConn) WriteMessage(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed {
		return ErrConnClosed
	}

	select {
	case c.writeChan <- data:
		return nil
	default:
		// Channel full, connection is slow consumer
		return ErrWriteChannelFull
	}
}

// Close closes the connection
func (c *WebsocketClientConn) Close() error {
	c.closeOnce.Do(func() {
		c.writeMu.Lock()
		c.closed = true
		if c.writeChan != nil {
			close(c.writeChan)
		}
		c.writeMu.Unlock()

		if c.closeChan != nil {
			close(c.closeChan)
		}
	})
	return nil
}

// SetReadDeadline sets the read deadline
func (c *WebsocketClientConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline
func (c *WebsocketClientConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
