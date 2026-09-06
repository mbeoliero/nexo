package gateway

import (
	"errors"
	"time"

	"github.com/hertz-contrib/websocket"
)

// ClientConn isolates the socket library so Client can be tested with a fake.
type ClientConn interface {
	ReadMessage() ([]byte, error)
	WriteMessage(data []byte) error
	WritePing() error
	Close() error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

type wsConn struct {
	conn *websocket.Conn
}

func newWsConn(c *websocket.Conn, maxFrameBytes int, pongWait time.Duration) *wsConn {
	c.SetReadLimit(int64(maxFrameBytes))
	c.SetPongHandler(func(string) error { return c.SetReadDeadline(time.Now().Add(pongWait)) })
	return &wsConn{conn: c}
}

func (w *wsConn) ReadMessage() ([]byte, error) {
	typ, data, err := w.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if typ != websocket.TextMessage {
		protocolErr := &websocket.CloseError{Code: websocket.CloseUnsupportedData, Text: "text frames required"}
		closeErr := w.conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(protocolErr.Code, protocolErr.Text), time.Now().Add(time.Second))
		return nil, errors.Join(protocolErr, closeErr)
	}
	return data, nil
}

func (w *wsConn) WriteMessage(data []byte) error {
	return w.conn.WriteMessage(websocket.TextMessage, data)
}
func (w *wsConn) WritePing() error { return w.conn.WriteMessage(websocket.PingMessage, nil) }

func (w *wsConn) Close() error { return w.conn.Close() }

func (w *wsConn) CloseControl(deadline time.Time) error {
	return w.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, ""), deadline)
}

func (c *Client) closeControl() {
	w, ok := c.conn.(interface{ CloseControl(time.Time) error })
	if !ok {
		return
	}
	deadline := time.Now().Add(time.Second)
	c.gw.workMu.Lock()
	ctx := c.gw.shutdownCtx
	c.gw.workMu.Unlock()
	if ctx != nil {
		if ctx.Err() != nil {
			return
		}
		if d, ok := ctx.Deadline(); ok {
			deadline = minTime(deadline, d)
		}
	}
	_ = w.CloseControl(deadline)
}

func (w *wsConn) SetReadDeadline(t time.Time) error  { return w.conn.SetReadDeadline(t) }
func (w *wsConn) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }
