// Package sdk is the HTTP client for a nexo node (design §15.2): Bearer routes for clients and
// HMAC-signed /internal routes for the platform backend. It depends on nothing else in this module.
package sdk

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	apiPrefix        = "/api/v1"
	maxResponseBytes = 8 << 20
)

type Client struct {
	baseUrl    string
	http       *http.Client
	token      string
	tokenMu    sync.RWMutex
	platformId int
	internal   *internalAuth
}

type internalAuth struct {
	service string
	secret  string
}

type Option func(*Client)

func WithHttpClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithToken sets the Bearer token (platform JWT or native token); Login sets it too.
func WithToken(token string) Option { return func(c *Client) { c.SetToken(token) } }

// WithPlatformId sends X-Platform-Id on every request (open-im numbering; server default otherwise).
func WithPlatformId(id int) Option { return func(c *Client) { c.platformId = id } }

// WithInternalAuth enables the Internal* methods: HMAC per docs/integration.md.
func WithInternalAuth(service, secret string) Option {
	return func(c *Client) { c.internal = &internalAuth{service: service, secret: secret} }
}

// New takes the node or LB origin plus the route prefix the server was mounted under, e.g.
// "https://im.example.com" or "https://app.example.com/im".
func New(baseUrl string, opts ...Option) *Client {
	c := &Client{baseUrl: strings.TrimRight(baseUrl, "/"), http: &http.Client{Timeout: 10 * time.Second}}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) SetToken(token string) {
	c.tokenMu.Lock()
	c.token = token
	c.tokenMu.Unlock()
}

func (c *Client) Token() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.token
}

type RequestOption func(*request)

type request struct {
	userId     string
	platformId int
}

// AsUser impersonates a platform user on the as-user internal routes (X-User-Id).
func AsUser(userId string) RequestOption { return func(r *request) { r.userId = userId } }

// PlatformId overrides the client-level X-Platform-Id for one request.
func PlatformId(id int) RequestOption { return func(r *request) { r.platformId = id } }

type envelope struct {
	Code    *int           `json:"code"`
	Message string         `json:"message"`
	Data    jsontext.Value `json:"data"`
}

func (c *Client) get(ctx context.Context, path string, q url.Values, out any) error {
	return c.do(ctx, http.MethodGet, path, q, nil, out, false, nil)
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	return c.do(ctx, http.MethodPost, path, nil, body, out, false, nil)
}

func (c *Client) put(ctx context.Context, path string, body, out any) error {
	return c.do(ctx, http.MethodPut, path, nil, body, out, false, nil)
}

func (c *Client) internalGet(ctx context.Context, path string, q url.Values, out any, opts []RequestOption) error {
	return c.do(ctx, http.MethodGet, path, q, nil, out, true, opts)
}

func (c *Client) internalPost(ctx context.Context, path string, body, out any, opts []RequestOption) error {
	return c.do(ctx, http.MethodPost, path, nil, body, out, true, opts)
}

func (c *Client) do(ctx context.Context, method, path string, q url.Values, body, out any, internal bool, opts []RequestOption) error {
	var ro request
	for _, o := range opts {
		o(&ro)
	}
	if internal && c.internal == nil {
		return errors.New("sdk: internal route needs WithInternalAuth")
	}
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return fmt.Errorf("sdk: encode: %w", err)
		}
	}
	rawPath := apiPrefix + path
	rawQuery := q.Encode()
	full := c.baseUrl + rawPath
	if rawQuery != "" {
		full += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, full, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("sdk: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if platform := cmp.Or(ro.platformId, c.platformId); platform > 0 {
		req.Header.Set("X-Platform-Id", strconv.Itoa(platform))
	}
	if internal {
		if ro.userId != "" {
			req.Header.Set("X-User-Id", ro.userId)
		}
		c.sign(req, method, req.URL.EscapedPath(), req.URL.RawQuery, payload)
	} else if token := c.Token(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("sdk: %s %s: %w", method, rawPath, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("sdk: read: %w", err)
	}
	if len(raw) > maxResponseBytes {
		return &Error{HttpStatus: resp.StatusCode, Message: "response exceeds 8 MiB"}
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return &Error{HttpStatus: resp.StatusCode, Message: "invalid response envelope"}
	}
	if env.Code != nil && *env.Code != 0 {
		return &Error{Code: *env.Code, Message: env.Message, HttpStatus: resp.StatusCode}
	}
	// Preserve a proxy's status and message even when its JSON has no envelope code.
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return &Error{HttpStatus: resp.StatusCode, Message: cmp.Or(env.Message, fmt.Sprintf("unexpected status %d", resp.StatusCode))}
	}
	if env.Code == nil {
		return &Error{HttpStatus: resp.StatusCode, Message: "invalid envelope: missing or null code"}
	}
	if out == nil {
		return nil
	}
	if len(env.Data) == 0 || env.Data.Kind() == 'n' {
		return &Error{HttpStatus: resp.StatusCode, Message: "invalid envelope: missing or null data"}
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		return fmt.Errorf("sdk: decode: %w", err)
	}
	return nil
}

// sign: hex(HMAC-SHA256(secret, service\nts\nnonce\nMETHOD\nrawPath\nrawQuery\nuserId\nplatformId\nhex(sha256(body)))).
func (c *Client) sign(req *http.Request, method, rawPath, rawQuery string, body []byte) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := newNonce()
	sum := sha256.Sum256(body)
	payload := strings.Join([]string{c.internal.service, ts, nonce, method, rawPath, rawQuery,
		req.Header.Get("X-User-Id"), req.Header.Get("X-Platform-Id"), hex.EncodeToString(sum[:])}, "\n")
	mac := hmac.New(sha256.New, []byte(c.internal.secret))
	mac.Write([]byte(payload))
	req.Header.Set("X-Service-Name", c.internal.service)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", hex.EncodeToString(mac.Sum(nil)))
}

func newNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("sdk: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
