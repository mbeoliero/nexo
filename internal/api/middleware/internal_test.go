package middleware

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/cache/local"
)

func TestInternalTlsTransport(t *testing.T) {
	certServer := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := certServer.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(certServer.Certificate())
	certServer.Close()
	for _, tc := range []struct {
		name                                       string
		tls, absolute, require, trusted, forwarded bool
		want                                       int
	}{
		{name: "signed absolute without requirement", absolute: true, want: 200},
		{name: "plain absolute https", absolute: true, require: true, want: 403},
		{name: "plain origin", require: true, want: 403},
		{name: "real tls", tls: true, require: true, want: 200},
		{name: "untrusted forwarded", require: true, forwarded: true, want: 403},
		{name: "trusted forwarded", require: true, trusted: true, forwarded: true, want: 200},
		{name: "trusted plain", require: true, trusted: true, want: 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			cache := local.New()
			t.Cleanup(func() { cache.Close() })
			m := InternalAuth{Verifier: auth.NewInternal([]string{"secret"}, []string{"gateway"}, time.Minute, cache), RequireTls: tc.require}
			if tc.trusted {
				_, cidr, _ := net.ParseCIDR("127.0.0.1/32")
				m.TrustedProxies = []*net.IPNet{cidr}
			}
			opts := []hconfig.Option{server.WithListener(ln), server.WithTransport(standard.NewTransporter)}
			if tc.tls {
				opts = append(opts, server.WithTLS(&tls.Config{
					Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
				}))
			}
			h := server.New(opts...)
			h.GET("/internal", m.Verify, func(_ context.Context, c *app.RequestContext) { webx.OK(c, nil) })
			done := make(chan error, 1)
			go func() { done <- h.Run() }()
			t.Cleanup(func() {
				_ = h.Close()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if tc.tls {
				conn = tls.Client(conn, &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS12})
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			target := lo.Ternary(tc.absolute, "https://"+ln.Addr().String()+"/internal", "/internal")
			r := auth.InternalRequest{Service: "gateway", Timestamp: strconv.FormatInt(time.Now().Unix(), 10), Nonce: "0123456789abcdef", Method: "GET", RawPath: target}
			forwarded := lo.Ternary(tc.forwarded, "X-Forwarded-Proto: https\r\n", "")
			_, err = fmt.Fprintf(conn,
				"GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n"+
					"X-Service-Name: %s\r\nX-Timestamp: %s\r\nX-Nonce: %s\r\nX-Signature: %s\r\n%s\r\n",
				target, ln.Addr(), r.Service, r.Timestamp, r.Nonce, auth.Sign("secret", r), forwarded,
			)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}
