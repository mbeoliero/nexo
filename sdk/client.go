package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"


	"github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Client is the SDK client for Nexo IM API
type Client struct {
	baseURL    string
	httpClient *client.Client
	token      string
}

// ClientOption is a function to configure the client
type ClientOption func(*Client)

// WithHertzClient sets a custom Hertz client
func WithHertzClient(httpClient *client.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithToken sets the authentication token
func WithToken(token string) ClientOption {
	return func(c *Client) {
		c.token = token
	}
}

// NewClient creates a new SDK client
func NewClient(baseURL string, opts ...ClientOption) (*Client, error) {
	httpClient, err := client.NewClient(
		client.WithDialTimeout(10*time.Second),
		client.WithClientReadTimeout(30*time.Second),
		client.WithWriteTimeout(30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create http client: %w", err)
	}

	c := &Client{
		baseURL:    baseURL,
		httpClient: httpClient,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// MustNewClient creates a new SDK client and panics on error
func MustNewClient(baseURL string, opts ...ClientOption) *Client {
	c, err := NewClient(baseURL, opts...)
	if err != nil {
		panic(err)
	}
	return c
}

// SetToken sets the authentication token
func (c *Client) SetToken(token string) {
	c.token = token
}

// GetToken returns the current token
func (c *Client) GetToken() string {
	return c.token
}

// request makes an HTTP request and decodes the response
func (c *Client) request(ctx context.Context, method, path string, body any, result any) error {
	_, err := c.requestWithMeta(ctx, method, path, body, result)
	return err
}

func (c *Client) requestWithMeta(ctx context.Context, method, path string, body any, result any) (map[string]any, error) {
	reqURL := c.baseURL + path

	req := &protocol.Request{}
	resp := &protocol.Response{}

	req.SetMethod(method)
	req.SetRequestURI(reqURL)
	req.Header.Set("Content-Type", "application/json")

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		req.SetBody(jsonBody)
	}

	err := c.httpClient.Do(ctx, req, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	return decodeAPIResponse(resp.Body(), result)
}

// get makes a GET request with query parameters
func (c *Client) get(ctx context.Context, path string, params map[string]string, result any) error {
	_, err := c.getWithMeta(ctx, path, params, result)
	return err
}

func (c *Client) getWithMeta(ctx context.Context, path string, params map[string]string, result any) (map[string]any, error) {
	reqURL := c.baseURL + path
	if len(params) > 0 {
		query := url.Values{}
		for k, v := range params {
			query.Set(k, v)
		}
		reqURL += "?" + query.Encode()
	}

	req := &protocol.Request{}
	resp := &protocol.Response{}

	req.SetMethod(consts.MethodGet)
	req.SetRequestURI(reqURL)

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	err := c.httpClient.Do(ctx, req, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	return decodeAPIResponse(resp.Body(), result)
}

func decodeAPIResponse(body []byte, result any) (map[string]any, error) {
	var apiResp Response
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if apiResp.Code != 0 {
		return nil, &Error{Code: apiResp.Code, Msg: apiResp.ErrorText()}
	}

	if result != nil && apiResp.Data != nil {
		dataBytes, err := json.Marshal(apiResp.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal response data: %w", err)
		}
		if err := json.Unmarshal(dataBytes, result); err != nil {
			return nil, fmt.Errorf("failed to decode response data: %w", err)
		}
	}

	return apiResp.Meta, nil
}

// post makes a POST request
func (c *Client) post(ctx context.Context, path string, body interface{}, result interface{}) error {
	return c.request(ctx, consts.MethodPost, path, body, result)
}

func (c *Client) postWithMeta(ctx context.Context, path string, body interface{}, result interface{}) (map[string]any, error) {
	return c.requestWithMeta(ctx, consts.MethodPost, path, body, result)
}

// put makes a PUT request
func (c *Client) put(ctx context.Context, path string, body interface{}, result interface{}) error {
	return c.request(ctx, consts.MethodPut, path, body, result)
}
