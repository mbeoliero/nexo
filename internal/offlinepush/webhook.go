package offlinepush

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Webhook POSTs {event_id, user_ids, notification, preview} once, no retries. Signature:
// X-Nexo-Signature = hex(HMAC-SHA256(secret, ts + "\n" + hex(sha256(body)))), X-Nexo-Timestamp = ts.
const maxDrain = 64 << 10

type Webhook struct {
	url    string
	secret string
	client *http.Client
	now    func() time.Time
}

func NewWebhook(url, secret string, timeout time.Duration) *Webhook {
	return &Webhook{
		url: url, secret: secret, now: time.Now,
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

type webhookBody struct {
	EventId      string       `json:"event_id"`
	UserIds      []string     `json:"user_ids"`
	Notification Notification `json:"notification"`
	Preview      string       `json:"preview"` // default text; receivers may ignore it and read notification.content
}

func (w *Webhook) Push(ctx context.Context, userIds []string, n Notification) error {
	body, err := json.Marshal(webhookBody{EventId: n.EventId(), UserIds: userIds, Notification: n, Preview: n.Preview()})
	if err != nil {
		return fmt.Errorf("offlinepush/webhook: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("offlinepush/webhook: %w", err)
	}
	ts := strconv.FormatInt(w.now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Nexo-Timestamp", ts)
	req.Header.Set("X-Nexo-Signature", Sign(w.secret, ts, body))
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("offlinepush/webhook: %w", err)
	}
	defer resp.Body.Close()
	// Drain so the transport can reuse the connection.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("offlinepush/webhook: status %d", resp.StatusCode)
	}
	return nil
}

func Sign(secret, ts string, body []byte) string {
	sum := sha256.Sum256(body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "\n" + hex.EncodeToString(sum[:])))
	return hex.EncodeToString(mac.Sum(nil))
}
