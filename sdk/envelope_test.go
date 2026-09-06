package sdk_test

import (
	"cmp"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mbeoliero/nexo/sdk"
)

type envelopeTransport struct {
	status int
	body   string
}

func (tr envelopeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: tr.status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(tr.body)),
		Request:    req,
	}, nil
}

func TestResponseEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name         string
		body         string
		status       int
		invalid      bool
		invalidData  bool
		businessCode int
		wantMessage  string
	}{
		{name: "null", body: `null`, invalid: true},
		{name: "empty-object", body: `{}`, invalid: true},
		{name: "message-only", body: `{"message":"upstream failed"}`, invalid: true},
		{name: "null-code", body: `{"code":null,"data":{}}`, invalid: true},
		{name: "string-code", body: `{"code":"0","data":{}}`, invalid: true},
		{name: "boolean-code", body: `{"code":false,"data":{}}`, invalid: true},
		{name: "fraction-code", body: `{"code":0.5,"data":{}}`, invalid: true},
		{name: "overflow-code", body: `{"code":9223372036854775808}`, invalid: true},
		{name: "duplicate-code", body: `{"code":0,"code":0,"data":{}}`, invalid: true},
		{name: "wrong-case-code", body: `{"Code":0,"data":{}}`, invalid: true},
		{name: "array", body: `[]`, invalid: true},
		{name: "string", body: `"ok"`, invalid: true},
		{name: "empty-body", invalid: true},
		{name: "broken-json", body: `{`, invalid: true},
		{name: "broken-login-body", body: `{"data":{"token":"private-response-token"},`, invalid: true},
		{name: "trailing-document", body: `{"code":0,"data":{}} {}`, invalid: true},
		{name: "missing-data", body: `{"code":0}`, invalidData: true},
		{name: "null-data", body: `{"code":0,"data":null}`, invalidData: true},
		{name: "whitespace-null-data", body: "{\"code\":0,\"data\": \n null \t}", invalidData: true},
		{name: "array-data", body: `{"code":0,"data":[]}`, invalidData: true},
		{name: "string-data", body: `{"code":0,"data":""}`, invalidData: true},
		{name: "boolean-data", body: `{"code":0,"data":true}`, invalidData: true},
		{name: "empty-data-object", body: `{"code":0,"data":{}}`},
		{name: "valid", body: `{"code":0,"message":"","data":{"user_id":"u___1","token":"issued","users":[]}}`},
		{name: "business-error", body: `{"code":10001,"message":"invalid input"}`, businessCode: 10001},
		{name: "system-error", body: `{"code":20002,"message":"store failed","data":null}`, businessCode: 20002},
		{name: "future-code", body: `{"code":30001,"message":"future error"}`, businessCode: 30001},
		// A proxy's JSON error body carries no envelope code; status and message must still surface.
		{name: "proxy-error", status: 502, body: `{"message":"unavailable"}`, invalid: true, wantMessage: "unavailable"},
		{name: "non-2xx-success-code", status: 503, body: `{"code":0,"data":{}}`, invalid: true},
		{name: "non-2xx-business-code", status: 401, body: `{"code":10102,"message":"expired"}`, businessCode: 10102},
		{name: "informational-status", status: 199, body: `{"code":0,"data":{}}`, invalid: true},
		{name: "created-status", status: 201, body: `{"code":0,"data":{}}`},
		{name: "no-content-without-envelope", status: 204, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := cmp.Or(tc.status, http.StatusOK)
			for _, call := range []struct {
				name      string
				needsData bool
				run       func(context.Context, *sdk.Client) error
			}{
				{name: "me", needsData: true, run: func(ctx context.Context, c *sdk.Client) error {
					_, err := c.Me(ctx)
					return err
				}},
				{name: "login", needsData: true, run: func(ctx context.Context, c *sdk.Client) error {
					_, err := c.Login(ctx, sdk.LoginRequest{})
					return err
				}},
				{name: "logout", run: func(ctx context.Context, c *sdk.Client) error { return c.Logout(ctx) }},
				{name: "internal-users", needsData: true, run: func(ctx context.Context, c *sdk.Client) error {
					_, err := c.InternalUsers(ctx, []string{"u___1"})
					return err
				}},
				{name: "internal-health", run: func(ctx context.Context, c *sdk.Client) error { return c.InternalHealth(ctx) }},
			} {
				t.Run(call.name, func(t *testing.T) {
					c := sdk.New(
						"http://nexo.invalid",
						sdk.WithToken("original"),
						sdk.WithInternalAuth("test", "test-secret"),
						sdk.WithHttpClient(&http.Client{Transport: envelopeTransport{status: status, body: tc.body}}),
					)
					err := call.run(t.Context(), c)
					wantError := tc.invalid || tc.businessCode != 0 || tc.invalidData && call.needsData
					if (err != nil) != wantError {
						t.Fatalf("error=%v, want error=%v", err, wantError)
					}
					if err != nil && c.Token() != "original" {
						t.Error("failed response changed client token")
					}
					if err != nil && strings.Contains(err.Error(), "private-response-token") {
						t.Error("protocol error echoed a response token")
					}
					if tc.businessCode != 0 {
						e, ok := errors.AsType[*sdk.Error](err)
						if !ok || e.Code != tc.businessCode || e.HttpStatus != status {
							t.Fatalf("lost envelope code/status: %v", err)
						}
					}
					if tc.wantMessage != "" {
						e, ok := errors.AsType[*sdk.Error](err)
						if !ok || e.HttpStatus != status || e.Message != tc.wantMessage {
							t.Fatalf("want http %d %q, got %v", status, tc.wantMessage, err)
						}
					}
				})
			}
		})
	}
}

func TestResponseBodyLimit(t *testing.T) {
	const limit = 8 << 20
	prefix := `{"code":0,"data":{"user_id":"u___1"}}`
	atLimit := prefix + strings.Repeat(" ", limit-len(prefix))
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "at-limit", body: atLimit},
		{name: "oversized-valid-json", body: atLimit + " "},
		{name: "truncated-valid-prefix", body: atLimit + "{}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := sdk.New("http://nexo.invalid", sdk.WithHttpClient(&http.Client{
				Transport: envelopeTransport{status: http.StatusOK, body: tc.body},
			}))
			p, err := c.Me(t.Context())
			if len(tc.body) > limit {
				if err == nil {
					t.Fatal("oversized response accepted after truncation")
				}
			} else if err != nil || p.UserId != "u___1" {
				t.Fatalf("valid response at limit: %+v, %v", p, err)
			}
		})
	}
}
