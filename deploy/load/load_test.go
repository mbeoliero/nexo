package main

import (
	"encoding/json/v2"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/sdk"
)

func testOptions() options {
	return options{nodes: []string{"http://node1.invalid", "http://node2.invalid"}, users: 4, active: 4,
		duration: time.Second, rate: 1, timeout: time.Second, workers: 1, disposable: true}
}

func testRunner(count int) (*runner, []sdk.Message) {
	r := newRunner(testOptions())
	r.started, r.elapsed = time.Now(), time.Second
	r.connected.Store(4)
	for i := range r.users {
		r.users[i].id = fmt.Sprintf("u___%d", i+1)
		r.users[i].records = make([]record, count)
	}
	messages := make([]sdk.Message, 0, 4*count)
	for i := range r.users {
		for j := range count {
			r.users[i].records[j] = record{started: r.started, written: true}
			messages = append(messages, sdk.Message{
				ServerMsgId: fmt.Sprintf("server-%d-%d", i, j), ClientMsgId: r.messageId(i, j),
				ConversationId: r.conversation(i), Seq: int64(j*2 + i%2 + 1), SendTime: 123,
				SenderId: r.users[i].id, RecvId: r.users[i^1].id, SessionType: sdk.SessionTypeSingle,
				ContentType: sdk.ContentTypeText, Content: r.content(i, j),
			})
		}
	}
	return r, messages
}

func testJSON[T any](t *testing.T, value T) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func ackFrame(m sdk.Message) map[string]any {
	return map[string]any{"req_id": 1003, "op_id": m.ClientMsgId, "code": 0, "data": sdk.Ack{
		ServerMsgId: m.ServerMsgId, ConversationId: m.ConversationId, Seq: m.Seq, SendTime: m.SendTime,
	}}
}

func pushFrame(m sdk.Message) map[string]any {
	return map[string]any{"req_id": 2001, "data": m}
}

func TestSendPhaseHeadroom(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		active int
		period time.Duration
	}{
		{name: "one sender", active: 1, period: time.Second},
		{name: "1000 senders", active: 1000, period: time.Second},
		{name: "10000 senders", active: 10000, period: time.Second},
		{name: "high rate", active: 1000, period: time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var previous time.Duration
			for user := range tc.active {
				offset := sendOffset(tc.period, user, tc.active)
				if offset < 0 || tc.period-offset < tc.period/10 {
					t.Fatalf("user %d: offset %s leaves less than 10%% headroom", user, offset)
				}
				if user > 0 && offset <= previous {
					t.Fatalf("user %d: staggering not increasing", user)
				}
				previous = offset
			}
		})
	}
	r := newRunner(testOptions())
	if got := r.report().SendPhaseFraction; got != 0.9 {
		t.Fatalf("reported phase fraction = %v, want 0.9", got)
	}
}

func TestSendWindowExpired(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		o := testOptions()
		o.duration = 3 * time.Second
		r := newRunner(o)
		r.started = time.Now().Add(-o.duration - 2*time.Millisecond)
		scheduled := r.started.Add(1750 * time.Millisecond)
		err := r.send(3, 1, scheduled, r.started.Add(o.duration))
		want := "send window expired: message_index=1 scheduled_offset=1.75s " +
			"late_by=1.252s cutoff_overrun=2ms remaining=2"
		if err == nil || err.Error() != want {
			t.Fatalf("send error = %v, want %q", err, want)
		}
		if r.sent.Load() != 0 || !r.users[3].records[1].started.IsZero() || r.users[3].records[1].written {
			t.Fatal("expired send must not start or write a message")
		}
	})
}

func TestChecker(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"valid push before ACK", "missing code", "error code", "wrong recipient", "wrong recv id", "wrong body",
		"wrong conversation", "wrong server id", "wrong seq", "duplicate push", "unknown client id",
		"missing push", "recovered push", "recovery without full pull", "missing ACK", "unwritten",
		"one missing view", "duplicate server id across conversations",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, messages := testRunner(1)
			if name == "duplicate server id across conversations" {
				messages[2].ServerMsgId = messages[0].ServerMsgId
			}
			for i, m := range messages {
				push, ack, recipient := m, ackFrame(m), i^1
				if i == 0 {
					switch name {
					case "missing code":
						delete(ack, "code")
					case "error code":
						ack["code"] = 20001
					case "wrong recipient":
						recipient = 2
					case "wrong recv id":
						push.RecvId = r.users[2].id
					case "wrong body":
						push.Content = `{"text":"wrong"}`
					case "wrong conversation":
						push.ConversationId = r.conversation(2)
					case "wrong server id":
						push.ServerMsgId = "other"
					case "wrong seq":
						push.Seq++
					case "unknown client id":
						push.ClientMsgId = "another-run:0:0"
					}
				}
				r.receive(recipient, testJSON(t, pushFrame(push)))
				if i == 0 && name == "duplicate push" {
					r.receive(recipient, testJSON(t, pushFrame(push)))
				}
				r.receive(i, testJSON(t, ack))
				for side := range 2 {
					if err := r.verifyStored(i/2*2+side, m); err != nil {
						r.fail("persisted verification: %v", err)
					}
				}
			}
			rec := &r.users[0].records[0]
			switch name {
			case "missing push", "recovered push", "recovery without full pull":
				rec.pushSeen = false
				r.opts.allowMissedPush = name != "missing push"
				if name == "recovery without full pull" {
					rec.pulled = 1
				}
			case "missing ACK":
				r.opts.allowMissedPush = true
				rec.ackSeen = false
			case "unwritten":
				r.opts.allowMissedPush = true
				rec.written = false
			case "one missing view":
				r.opts.allowMissedPush = true
				rec.pulled = 1
			}
			want := name == "valid push before ACK" || name == "recovered push"
			got := r.report()
			if got.Passed != want {
				t.Fatalf("Passed=%v want %v: %+v", got.Passed, want, got)
			}
			if name == "recovered push" && (got.RecoveredByPull != 1 || got.Verified != 4) {
				t.Fatalf("recovery counts: %+v", got)
			}
			if name == "duplicate server id across conversations" && got.DuplicateServerIds != 1 {
				t.Fatalf("duplicate server ids=%d, want 1", got.DuplicateServerIds)
			}
		})
	}
}

func TestVerifyView(t *testing.T) {
	for _, name := range []string{
		"102 messages", "seq hole", "duplicate", "missing", "extra", "max seq", "min seq",
		"max conversation", "max has more", "max cursor", "missing second view",
	} {
		t.Run(name, func(t *testing.T) {
			r, all := testRunner(51)
			for _, m := range all {
				i, _, err := r.locate(m.ClientMsgId)
				if err != nil {
					t.Fatal(err)
				}
				r.receive(i^1, testJSON(t, pushFrame(m)))
				r.receive(i, testJSON(t, ackFrame(m)))
			}
			var calls [4]int
			for viewer := range r.users {
				messages := []sdk.Message{}
				for _, m := range all {
					if m.ConversationId == r.conversation(viewer) {
						messages = append(messages, m)
					}
				}
				slices.SortFunc(messages, func(a, b sdk.Message) int { return int(a.Seq - b.Seq) })
				maxSeqs := sdk.MaxSeqsResult{Items: []sdk.MaxSeqItem{{
					ConversationId: r.conversation(viewer), MaxSeq: 102, MinSeq: 1,
				}}}
				if viewer == 1 {
					switch name {
					case "seq hole":
						messages = slices.Delete(messages, 50, 51)
					case "duplicate":
						messages[50] = messages[49]
					case "missing":
						messages = messages[:101]
					case "extra":
						messages = append(messages, messages[101])
					case "max seq":
						maxSeqs.Items[0].MaxSeq--
					case "min seq":
						maxSeqs.Items[0].MinSeq++
					case "max conversation":
						maxSeqs.Items[0].ConversationId = r.conversation(2)
					case "max has more":
						maxSeqs.HasMore = true
					case "max cursor":
						maxSeqs.NextCursor = "unexpected"
					}
				}
				token := fmt.Sprintf("token-%d", viewer)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if req.Method != http.MethodGet || req.Header.Get("Authorization") != "Bearer "+token {
						t.Error("history request must use viewer's own authenticated SDK client")
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					var data any
					switch req.URL.Path {
					case "/api/v1/message/max_seqs":
						data = maxSeqs
					case "/api/v1/message/pull":
						q := req.URL.Query()
						begin, err := strconv.Atoi(q.Get("begin_seq"))
						if err != nil || begin != calls[viewer]*100+1 || q.Get("end_seq") != "103" ||
							q.Get("limit") != "100" || q.Get("conversation_id") != r.conversation(viewer) {
							t.Errorf("invalid pull query: %s", req.URL.RawQuery)
						}
						start := min(calls[viewer]*100, len(messages))
						end := min(start+100, len(messages))
						calls[viewer]++
						data = sdk.PullResult{Messages: messages[start:end], HasMore: end < len(messages)}
					default:
						t.Errorf("unexpected endpoint %s", req.URL.Path)
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					if err := json.MarshalWrite(w, map[string]any{"code": 0, "data": data}); err != nil {
						t.Error(err)
					}
				}))
				t.Cleanup(server.Close)
				r.users[viewer].client = sdk.New(server.URL, sdk.WithToken(token))
				if viewer == 1 && name == "missing second view" {
					continue
				}
				if err := r.verifyView(t.Context(), viewer, 102); err != nil {
					r.fail("verifyView: %v", err)
				}
			}
			got := r.report()
			if got.Passed != (name == "102 messages") {
				t.Fatalf("unexpected report: %+v", got)
			}
			if got.Passed && (calls != [4]int{2, 2, 2, 2} || got.Verified != 204) {
				t.Fatalf("pagination calls=%v verified=%d", calls, got.Verified)
			}
		})
	}
}

func TestIdleHistory(t *testing.T) {
	for _, unexpected := range []bool{false, true} {
		t.Run(strconv.FormatBool(unexpected), func(t *testing.T) {
			r, _ := testRunner(1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path != "/api/v1/message/max_seqs" {
					t.Errorf("unexpected idle-user request: %s", req.URL.Path)
				}
				result := sdk.MaxSeqsResult{Items: []sdk.MaxSeqItem{}}
				if unexpected {
					result.Items = append(result.Items, sdk.MaxSeqItem{ConversationId: "wrong-recipient", MaxSeq: 1})
				}
				if err := json.MarshalWrite(w, map[string]any{"code": 0, "data": result}); err != nil {
					t.Error(err)
				}
			}))
			t.Cleanup(server.Close)
			r.users[0].client = sdk.New(server.URL)
			if err := r.verifyView(t.Context(), 0, 0); (err != nil) != unexpected {
				t.Fatalf("unexpected=%v error=%v", unexpected, err)
			}
		})
	}
}

func TestLateFramesCannotImproveResult(t *testing.T) {
	t.Parallel()
	r, messages := testRunner(1)
	r.frozen.Store(true)
	r.receive(1, testJSON(t, pushFrame(messages[0])))
	r.receive(0, testJSON(t, ackFrame(messages[0])))
	if got := r.report(); got.Acked != 0 || got.Received != 0 || got.Passed {
		t.Fatalf("late frames counted: %+v", got)
	}
}

func TestNodeFor(t *testing.T) {
	t.Parallel()
	for _, nodes := range []int{2, 3, 10} {
		t.Run(strconv.Itoa(nodes), func(t *testing.T) {
			links := make(map[[2]int]bool)
			for pair := range nodes * (nodes - 1) {
				a, b := nodeFor(pair*2, nodes), nodeFor(pair*2+1, nodes)
				if a < 0 || a >= nodes || b < 0 || b >= nodes || a == b {
					t.Fatalf("pair %d: %d -> %d", pair, a, b)
				}
				links[[2]int{a, b}] = true
			}
			if len(links) != nodes*(nodes-1) || (nodes == 10 && !links[[2]int{0, 4}]) {
				t.Fatalf("directed cross-node coverage: %v", links)
			}
		})
	}
}

func TestOptionsValidate(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*options){
		"unsafe":            func(o *options) { o.disposable = false },
		"one node":          func(o *options) { o.nodes = o.nodes[:1] },
		"odd users":         func(o *options) { o.users = 3 },
		"no users":          func(o *options) { o.users = 0 },
		"no active":         func(o *options) { o.active = 0 },
		"too many active":   func(o *options) { o.active = 5 },
		"zero rate":         func(o *options) { o.rate = 0 },
		"negative rate":     func(o *options) { o.rate = -1 },
		"NaN":               func(o *options) { o.rate = math.NaN() },
		"infinite":          func(o *options) { o.rate = math.Inf(1) },
		"negative infinite": func(o *options) { o.rate = math.Inf(-1) },
		"rate cap":          func(o *options) { o.rate = 1001 },
		"duration":          func(o *options) { o.duration = 0 },
		"ramp":              func(o *options) { o.ramp = -1 },
		"settle":            func(o *options) { o.settle = -1 },
		"timeout":           func(o *options) { o.timeout = 0 },
		"workers":           func(o *options) { o.workers = 0 },
		"no messages":       func(o *options) { o.duration = time.Millisecond },
		"message cap":       func(o *options) { o.duration = 1000001 * time.Second },
	}
	for _, origin := range []string{"http://node1.invalid", "ftp://host", "http://", "http://%",
		"http://user:pass@host", "http://host/path", "http://host?q=x", "http://host#fragment"} {
		cases[origin] = func(o *options) { o.nodes = []string{o.nodes[0], origin} }
	}
	if err := testOptions().validate(); err != nil {
		t.Fatalf("valid options: %v", err)
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := testOptions()
			mutate(&o)
			if o.validate() == nil {
				t.Fatal("accepted invalid options")
			}
		})
	}
}

func TestPercentiles(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, 1, 2, 19, 20, 99, 100, 101} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			values := make([]time.Duration, n)
			for i := range n {
				values[i] = time.Duration(n-i) * time.Millisecond
			}
			got := percentiles(values)
			want := latency{P95Ms: math.Ceil(float64(n) * .95), P99Ms: math.Ceil(float64(n) * .99)}
			if got != want {
				t.Fatalf("got %+v want %+v", got, want)
			}
		})
	}
}
