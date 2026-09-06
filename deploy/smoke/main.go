package main

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/gorilla/websocket"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/sdk"
)

// Acceptance smoke for the 3-node compose stack (design §12 phase 6). Run: make compose-up && go run ./deploy/smoke
var lb = "http://127.0.0.1:18080"

func login(name string, platform int) (tok, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := sdk.New(lb, sdk.WithHttpClient(&http.Client{Timeout: 10 * time.Second}))
	_, err := client.Register(ctx, sdk.RegisterRequest{Username: name, Password: "pw123456", Nickname: name})
	if err != nil && sdk.CodeOf(err) != errcode.ErrUserExists.Code {
		panic(fmt.Errorf("register %q: %w", name, err))
	}
	session, err := client.Login(ctx, sdk.LoginRequest{Username: name, Password: "pw123456", PlatformId: platform})
	if err != nil {
		panic(fmt.Errorf("login %q: %w", name, err))
	}
	if session.Token == "" {
		panic("login: empty token")
	}
	me, err := client.Me(ctx)
	if err != nil {
		panic(fmt.Errorf("me %q: %w", name, err))
	}
	if me.UserId == "" {
		panic("me: empty user_id")
	}
	return session.Token, me.UserId
}

// conn reads on its own goroutine: a gorilla read timeout would poison the socket.
type conn struct {
	ws     *websocket.Conn
	frames chan []byte
	closed chan error
}

func dial(port int, tok string, platform int) *conn {
	ws, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://127.0.0.1:%d/ws?platform_id=%d&token=%s", port, platform, tok), nil)
	if err != nil {
		panic(fmt.Sprintf("dial %d: %v", port, err))
	}
	c := &conn{ws: ws, frames: make(chan []byte, 16), closed: make(chan error, 1)}
	go func() {
		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				c.closed <- err
				return
			}
			c.frames <- raw
		}
	}()
	return c
}

func (c *conn) send(s string) { c.ws.WriteMessage(websocket.TextMessage, []byte(s)) }

func (c *conn) waitClose(wait time.Duration) error {
	select {
	case err := <-c.closed:
		return err
	case <-time.After(wait):
		return nil
	}
}

func read(c *conn, who string, wait time.Duration) map[string]any {
	select {
	case raw := <-c.frames:
		var m map[string]any
		json.Unmarshal(raw, &m)
		if len(raw) > 160 {
			raw = append(raw[:160], "..."...)
		}
		fmt.Printf("%s <- %s\n", who, raw)
		return m
	case err := <-c.closed:
		fmt.Printf("%s <- (closed: %v)\n", who, err)
		c.closed <- err
		return nil
	case <-time.After(wait):
		fmt.Printf("%s <- (nothing within %s)\n", who, wait)
		return nil
	}
}

func reqId(m map[string]any) int {
	if m == nil {
		return 0
	}
	v, _ := m["req_id"].(float64)
	return int(v)
}

func main() {
	u := fmt.Sprint(time.Now().UnixMilli())
	ta, aliceId := login("a_"+u, 1)
	tb, bobId := login("b_"+u, 1)
	fmt.Println("ids:", aliceId, bobId)
	ok := true
	check := func(name string, cond bool) {
		fmt.Printf("[%s] %v\n", name, cond)
		ok = ok && cond
	}

	// 1. cross-node push: alice on nexo1, bob on nexo2
	alice := dial(18081, ta, 1)
	bob := dial(18082, tb, 1)
	alice.send(`{"req_id":1003,"op_id":"a1","data":{"client_msg_id":"c1","session_type":1,"recv_id":"` + bobId + `","content_type":1,"content":"{\"text\":\"hi\"}"}}`)
	check("ack on nexo1", reqId(read(alice, "alice@1", 3*time.Second)) == 1003)
	check("push on nexo2", reqId(read(bob, "bob@2", 3*time.Second)) == 2001)

	// 2. online_status across nodes
	client := sdk.New(lb, sdk.WithHttpClient(&http.Client{Timeout: 10 * time.Second}), sdk.WithToken(ta))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	st, err := client.OnlineStatus(ctx, []string{aliceId, bobId})
	cancel()
	if err != nil {
		panic(fmt.Errorf("online_status: %w", err))
	}
	fmt.Println("online_status:", st)
	both := len(st) == 2
	for _, it := range st {
		both = both && it.Online
	}
	check("both online via different nodes", both)

	// 3. same-platform login on nexo3 kicks alice on nexo1; same token reconnect does not
	alice2 := dial(18083, ta, 1)
	check("same token on another node: no kick", read(alice, "alice@1", time.Second) == nil)
	ta2, _ := login("a_"+u, 1)
	alice3 := dial(18083, ta2, 1)
	k1 := read(alice, "alice@1 (old token)", 3*time.Second)
	k2 := read(alice2, "alice@3 (old token)", 3*time.Second)
	check("old connections kicked with 2002 on both nodes", reqId(k1) == 2002 && reqId(k2) == 2002)
	check("new login untouched", read(alice3, "alice@3 new", time.Second) == nil)

	// 4. kill nexo2: bob reconnects elsewhere and resyncs by seq
	alice3.send(`{"req_id":1003,"op_id":"a2","data":{"client_msg_id":"c2","session_type":1,"recv_id":"` + bobId + `","content_type":1,"content":"{}"}}`)
	read(alice3, "alice@3", 3*time.Second)
	read(bob, "bob@2", 3*time.Second)
	fmt.Println("stopping nexo2 ...")
	if err := stopNode("nexo2"); err != nil {
		panic(err)
	}
	err = bob.waitClose(25 * time.Second)
	ce, isClose := err.(*websocket.CloseError)
	check("bob got 1001 going away on shutdown", isClose && ce.Code == websocket.CloseGoingAway)
	alice3.send(`{"req_id":1003,"op_id":"a3","data":{"client_msg_id":"c3","session_type":1,"recv_id":"` + bobId + `","content_type":1,"content":"{}"}}`)
	read(alice3, "alice@3", 3*time.Second)
	bob2 := dial(18080, tb, 1) // through nginx, lands on a live node
	bob2.send(`{"req_id":1001,"data":{}}`)
	ms := read(bob2, "bob@lb max_seqs", 3*time.Second)
	items, _ := ms["data"].(map[string]any)["items"].([]any)
	check("max_seq=3 after reconnect", len(items) == 1 && fmt.Sprint(items[0].(map[string]any)["max_seq"]) == "3")

	fmt.Println("ALL OK:", ok)
	if !ok {
		os.Exit(1)
	}
}

func stopNode(name string) error {
	return runCmd("docker", "compose", "-f", "deploy/docker-compose.yml", "stop", name)
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}
