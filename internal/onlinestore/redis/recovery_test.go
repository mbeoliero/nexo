package redis

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/onlinestore"
	redisclient "github.com/redis/go-redis/v9"
)

func TestRenewRestoresLostRegistration(t *testing.T) {
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	dir, err := os.MkdirTemp("/tmp", "nexo-redis-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "redis.sock")
	cmd := exec.Command(binary, "--port", "0", "--unixsocket", socket, "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	cli := redisclient.NewClient(&redisclient.Options{Network: "unix", Addr: socket})
	t.Cleanup(func() { _ = cli.Close() })
	for range 100 {
		if cli.Ping(t.Context()).Err() == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cli.Ping(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	s := &Store{cli: cli, ttl: time.Minute, now: time.Now}
	c := onlinestore.ConnRef{UserId: "recovery", PlatformId: 1, ConnId: "c1"}
	for _, mode := range []string{"expired-key", "removed-member"} {
		t.Run(mode, func(t *testing.T) {
			if err := s.Add(t.Context(), "n1", c); err != nil {
				t.Fatal(err)
			}
			if mode == "expired-key" {
				if err := cli.PExpire(t.Context(), key(c.UserId), time.Millisecond).Err(); err != nil {
					t.Fatal(err)
				}
				time.Sleep(5 * time.Millisecond)
			} else {
				if err := cli.ZRem(t.Context(), key(c.UserId), member("n1", c)).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := cli.ZCard(t.Context(), key(c.UserId)).Result(); err != nil || n != 0 {
				t.Fatalf("registration was not lost: size=%d err=%v", n, err)
			}
			if err := s.Renew(t.Context(), "n1", []onlinestore.ConnRef{c}); err != nil {
				t.Fatal(err)
			}
			got, err := s.Online(t.Context(), []string{c.UserId})
			if err != nil || len(got[c.UserId]) != 1 {
				t.Fatalf("registration not restored: %v %v", got, err)
			}
		})
	}
}
