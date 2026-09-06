package sdk_test

import (
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/mbeoliero/nexo/sdk"
)

type tokenTransport func(*http.Request) (*http.Response, error)

func (f tokenTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func tokenResponse(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}
}

func TestTokenMutation(t *testing.T) {
	t.Parallel()
	t.Run("initial_and_explicit_values", func(t *testing.T) {
		if got := sdk.New("http://sdk.test").Token(); got != "" {
			t.Fatalf("default Token() = %q, want empty", got)
		}
		c := sdk.New("http://sdk.test", sdk.WithToken("initial"))
		if got := c.Token(); got != "initial" {
			t.Fatalf("WithToken: got %q", got)
		}
		for _, token := range []string{"replacement", ""} {
			c.SetToken(token)
			if got := c.Token(); got != token {
				t.Fatalf("SetToken(%q): got %q", token, got)
			}
		}
	})

	for _, tc := range []struct {
		name      string
		login     bool
		body      string
		transport error
		want      string
		wantErr   bool
	}{
		{name: "login_success", login: true, body: `{"code":0,"data":{"token":"logged-in"}}`, want: "logged-in"},
		{name: "logout_success", body: `{"code":0,"data":null}`},
		{name: "logout_success_without_data", body: `{"code":0}`},
		{name: "login_rejected", login: true, body: `{"code":10101,"message":"denied"}`, want: "initial", wantErr: true},
		{name: "logout_rejected", body: `{"code":10101,"message":"denied"}`, want: "initial", wantErr: true},
		{name: "login_transport_failure", login: true, transport: errors.New("offline"), want: "initial", wantErr: true},
		{name: "logout_transport_failure", transport: errors.New("offline"), want: "initial", wantErr: true},
		{name: "login_decode_failure", login: true, body: `{"code":0,"data":{"token":42}}`, want: "initial", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := tokenTransport(func(r *http.Request) (*http.Response, error) {
				if got := r.Header.Get("Authorization"); got != "Bearer initial" {
					t.Errorf("Authorization = %q, want initial token", got)
				}
				if tc.transport != nil {
					return nil, tc.transport
				}
				return tokenResponse(tc.body), nil
			})
			c := sdk.New("http://sdk.test", sdk.WithToken("initial"), sdk.WithHttpClient(&http.Client{Transport: transport}))
			var err error
			if tc.login {
				var session sdk.Session
				session, err = c.Login(t.Context(), sdk.LoginRequest{Username: "alice", Password: "unused"})
				if !tc.wantErr && session.Token != tc.want {
					t.Errorf("Login token = %q, want %q", session.Token, tc.want)
				}
			} else {
				err = c.Logout(t.Context())
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := c.Token(); got != tc.want {
				t.Fatalf("Token() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTokenRequestSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		token  string
		header string
	}{
		{name: "authenticated", token: "initial", header: "Bearer initial"},
		{name: "anonymous"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var c *sdk.Client
				wantHeader := tc.header
				transport := tokenTransport(func(r *http.Request) (*http.Response, error) {
					if got := r.Header.Get("Authorization"); got != wantHeader {
						t.Errorf("Authorization = %q, want %q", got, wantHeader)
					}
					// Reentry must not deadlock: no token lock may span the transport call.
					c.SetToken("replacement")
					if got := c.Token(); got != "replacement" {
						t.Errorf("Token() during request = %q", got)
					}
					if got := r.Header.Get("Authorization"); got != wantHeader {
						t.Errorf("in-flight Authorization changed to %q", got)
					}
					return tokenResponse(`{"code":0,"data":{"user_id":"u___1"}}`), nil
				})
				c = sdk.New("http://sdk.test", sdk.WithToken(tc.token), sdk.WithHttpClient(&http.Client{Transport: transport}))
				me, err := c.Me(t.Context())
				noErr(t, err)
				if me.UserId != "u___1" {
					t.Fatalf("Me user_id = %q", me.UserId)
				}
				wantHeader = "Bearer replacement"
				_, err = c.Me(t.Context())
				noErr(t, err)
			})
		})
	}
}

func TestTokenConcurrentAccess(t *testing.T) {
	t.Parallel()
	t.Run("set_get_login_logout_me", func(t *testing.T) {
		tokens := []string{"", "short", "a-much-longer-explicit-token", "logged-in-token"}
		headers := []string{"", "Bearer short", "Bearer a-much-longer-explicit-token", "Bearer logged-in-token"}
		transport := tokenTransport(func(r *http.Request) (*http.Response, error) {
			if got := r.Header.Get("Authorization"); !slices.Contains(headers, got) {
				t.Errorf("Authorization is not an entire allowed token: %q", got)
			}
			switch r.URL.Path {
			case "/api/v1/auth/login":
				return tokenResponse(`{"code":0,"data":{"token":"logged-in-token"}}`), nil
			case "/api/v1/auth/logout":
				return tokenResponse(`{"code":0,"data":null}`), nil
			case "/api/v1/user/me":
				return tokenResponse(`{"code":0,"data":{"user_id":"u___1"}}`), nil
			default:
				return nil, errors.New("unexpected route")
			}
		})
		c := sdk.New("http://sdk.test", sdk.WithToken(tokens[1]), sdk.WithHttpClient(&http.Client{Transport: transport}))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for operation := range 5 {
			wg.Go(func() {
				<-start
				for i := range 300 {
					switch operation {
					case 0:
						c.SetToken(tokens[i%len(tokens)])
					case 1:
						if got := c.Token(); !slices.Contains(tokens, got) {
							t.Errorf("Token() is not an entire allowed token: %q", got)
						}
					case 2:
						session, err := c.Login(t.Context(), sdk.LoginRequest{Username: "alice", Password: "unused"})
						if err != nil || session.Token != "logged-in-token" {
							t.Errorf("Login = %+v, %v", session, err)
						}
					case 3:
						if err := c.Logout(t.Context()); err != nil {
							t.Errorf("Logout: %v", err)
						}
					case 4:
						me, err := c.Me(t.Context())
						if err != nil || me.UserId != "u___1" {
							t.Errorf("Me = %+v, %v", me, err)
						}
					}
				}
			})
		}
		close(start)
		wg.Wait()
		c.SetToken("final-local-mutation")
		if got := c.Token(); got != "final-local-mutation" {
			t.Fatalf("final Token() = %q", got)
		}
	})
}
