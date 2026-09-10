package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mbeoliero/nexo/sdk"
)

type identity struct {
	serverMsgId string
	seq         int64
	sendTime    int64
}

type record struct {
	started     time.Time
	ack         identity
	push        identity
	ackLatency  time.Duration
	pushLatency time.Duration
	written     bool
	ackSeen     bool
	pushSeen    bool
	pulled      uint8 // One bit for each authenticated participant's history view.
}

func identityOf(a sdk.Ack) identity {
	return identity{serverMsgId: a.ServerMsgId, seq: a.Seq, sendTime: a.SendTime}
}

func messageIdentity(m sdk.Message) identity {
	return identity{serverMsgId: m.ServerMsgId, seq: m.Seq, sendTime: m.SendTime}
}

func validIdentity(id identity) bool { return id.serverMsgId != "" && id.seq > 0 && id.sendTime > 0 }

func (r *runner) messageId(i, j int) string {
	return r.runId + ":" + strconv.Itoa(i) + ":" + strconv.Itoa(j)
}

func (r *runner) content(i, j int) string {
	// IDs use only base32, decimal digits and colons, so this is already valid JSON.
	return `{"text":"` + r.messageId(i, j) + `"}`
}

func (r *runner) conversation(i int) string {
	a, b := r.users[i].id, r.users[i^1].id
	return "si_" + min(a, b) + ":" + max(a, b)
}

func (r *runner) locate(id string) (int, int, error) {
	tail, ok := strings.CutPrefix(id, r.runId+":")
	if !ok {
		return 0, 0, errors.New("unknown run in client_msg_id")
	}
	left, right, ok := strings.Cut(tail, ":")
	i, errI := strconv.Atoi(left)
	j, errJ := strconv.Atoi(right)
	if !ok || errI != nil || errJ != nil || i < 0 || i >= r.opts.active {
		return 0, 0, errors.New("invalid sender in client_msg_id")
	}
	if j < 0 || j >= len(r.users[i].records) || id != r.messageId(i, j) {
		return 0, 0, errors.New("invalid ordinal in client_msg_id")
	}
	return i, j, nil
}

func (r *runner) checkMessage(m sdk.Message) (int, int, error) {
	i, j, err := r.locate(m.ClientMsgId)
	if err != nil {
		return 0, 0, err
	}
	correctRoute := m.SenderId == r.users[i].id && m.RecvId == r.users[i^1].id && m.ConversationId == r.conversation(i)
	correctBody := m.ContentType == sdk.ContentTypeText && m.Content == r.content(i, j)
	if !correctRoute || !correctBody || m.SessionType != sdk.SessionTypeSingle || m.GroupId != "" || !validIdentity(messageIdentity(m)) {
		return 0, 0, errors.New("message route, body or identity differs from send request")
	}
	return i, j, nil
}

func (r *runner) verifyPair(ctx context.Context, pair int) error {
	total := len(r.users[pair*2].records) + len(r.users[pair*2+1].records)
	for side := range 2 {
		if err := ctx.Err(); err != nil {
			return err
		}
		viewer := pair*2 + side
		if err := r.verifyView(ctx, viewer, total); err != nil {
			r.fail("pull verification user=%d node=%d: %v", viewer, r.users[viewer].node+1, err)
		}
	}
	return nil
}

func (r *runner) verifyView(ctx context.Context, viewer, total int) error {
	client := r.users[viewer].client
	conv := r.conversation(viewer)
	maxSeqs, err := client.MaxSeqs(ctx, "", 100)
	if err != nil {
		return err
	}
	if total == 0 {
		if len(maxSeqs.Items) != 0 || maxSeqs.HasMore || maxSeqs.NextCursor != "" {
			return errors.New("idle user has unexpected persisted conversations")
		}
		return nil
	}
	if len(maxSeqs.Items) != 1 || maxSeqs.HasMore || maxSeqs.NextCursor != "" {
		return fmt.Errorf("expected exactly one new conversation, got %d", len(maxSeqs.Items))
	}
	item := maxSeqs.Items[0]
	if item.ConversationId != conv || item.MaxSeq != int64(total) || item.MinSeq != 1 {
		r.fail("max_seq/visibility mismatch user=%d expected=%d got=%d min=%d", viewer, total, item.MaxSeq, item.MinSeq)
	}
	var got int
	for next := int64(1); ; {
		page, err := client.Pull(ctx, sdk.PullRequest{ConversationId: conv, BeginSeq: next, EndSeq: int64(total) + 1, Limit: 100})
		if err != nil {
			return err
		}
		if len(page.Messages) == 0 && page.HasMore {
			return fmt.Errorf("empty page with has_more at seq=%d", next)
		}
		for _, msg := range page.Messages {
			if msg.Seq != next || next > int64(total) {
				return fmt.Errorf("seq gap/duplicate/extra: expected=%d got=%d", next, msg.Seq)
			}
			if err := r.verifyStored(viewer, msg); err != nil {
				return err
			}
			next++
			got++
		}
		if !page.HasMore {
			break
		}
	}
	if got != total {
		return fmt.Errorf("persisted message count expected=%d got=%d", total, got)
	}
	return nil
}

func (r *runner) verifyStored(viewer int, msg sdk.Message) error {
	i, j, err := r.checkMessage(msg)
	if err != nil {
		return err
	}
	if i/2 != viewer/2 {
		return errors.New("message belongs to another pair")
	}
	r.pairs[i/2].Lock()
	defer r.pairs[i/2].Unlock()
	rec := &r.users[i].records[j]
	bit := uint8(1 << (viewer % 2))
	if rec.started.IsZero() || rec.pulled&bit != 0 {
		return fmt.Errorf("unsent/duplicate persisted message %s", msg.ClientMsgId)
	}
	id := messageIdentity(msg)
	if rec.ackSeen && rec.ack != id {
		return fmt.Errorf("persisted message differs from ACK %s", msg.ClientMsgId)
	}
	if rec.pushSeen && rec.push != id {
		return fmt.Errorf("persisted message differs from push %s", msg.ClientMsgId)
	}
	rec.pulled |= bit
	return nil
}

type latency struct {
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
}

func percentiles(values []time.Duration) latency {
	if len(values) == 0 {
		return latency{}
	}
	slices.Sort(values)
	return latency{
		P95Ms: float64(values[(len(values)*95+99)/100-1]) / float64(time.Millisecond),
		P99Ms: float64(values[(len(values)*99+99)/100-1]) / float64(time.Millisecond),
	}
}

type linkReport struct {
	FromNode int `json:"from_node"`
	ToNode   int `json:"to_node"`
	Planned  int `json:"planned"`
	Sent     int `json:"sent"`
	Acked    int `json:"acked"`
	Received int `json:"received"`
	Verified int `json:"verified_both_views"`
}

type loadReport struct {
	Passed             bool         `json:"passed"`
	RunId              string       `json:"run_id"`
	Nodes              []string     `json:"nodes"`
	Users              int          `json:"users"`
	Connected          int64        `json:"connected"`
	Active             int          `json:"active"`
	RatePerUser        float64      `json:"rate_per_user"`
	SendPhaseFraction  float64      `json:"send_phase_fraction"`
	DurationSeconds    float64      `json:"duration_seconds"`
	ElapsedSeconds     float64      `json:"elapsed_seconds"`
	TargetPerSecond    float64      `json:"target_per_second"`
	ActualPerSecond    float64      `json:"actual_per_second"`
	Planned            int          `json:"planned"`
	Sent               int          `json:"sent"`
	Acked              int          `json:"acked"`
	Received           int          `json:"received"`
	Verified           int          `json:"verified_both_views"`
	MissingAck         int          `json:"missing_ack"`
	MissingPush        int          `json:"missing_push"`
	RecoveredByPull    int          `json:"recovered_by_pull"`
	Unverified         int          `json:"unverified_messages"`
	IdentityMismatches int          `json:"ack_push_identity_mismatches"`
	DuplicateServerIds int          `json:"duplicate_server_ids"`
	Disconnects        int64        `json:"disconnects"`
	Resyncs            int64        `json:"resyncs"`
	AllowMissedPush    bool         `json:"allow_missed_push"`
	AckLatency         latency      `json:"ack_latency"`
	ReceiveLatency     latency      `json:"receive_latency"`
	Links              []linkReport `json:"links"`
	Errors             int64        `json:"errors"`
	ErrorSamples       []string     `json:"error_samples"`
	OmittedErrors      int64        `json:"omitted_errors"`
}

func (r *runner) report() loadReport {
	out := loadReport{
		RunId: r.runId, Nodes: r.opts.nodes, Users: r.opts.users, Connected: r.connected.Load(), Active: r.opts.active,
		RatePerUser: r.opts.rate, DurationSeconds: r.opts.duration.Seconds(), ElapsedSeconds: r.elapsed.Seconds(),
		TargetPerSecond: float64(r.opts.active) * r.opts.rate, AllowMissedPush: r.opts.allowMissedPush,
		SendPhaseFraction: sendPhaseFraction,
		Disconnects:       r.disconnect.Load(), Resyncs: r.resync.Load(), Links: []linkReport{}, ErrorSamples: []string{},
	}
	ackTimes, pushTimes := []time.Duration{}, []time.Duration{}
	serverIds := make(map[string]struct{})
	links := make(map[[2]int]*linkReport)
	for i := range r.users {
		r.pairs[i/2].Lock()
		key := [2]int{r.users[i].node + 1, r.users[i^1].node + 1}
		link := links[key]
		if link == nil {
			link = &linkReport{FromNode: key[0], ToNode: key[1]}
			links[key] = link
		}
		for _, rec := range r.users[i].records {
			out.Planned++
			link.Planned++
			if rec.written {
				out.Sent++
				link.Sent++
			}
			if rec.ackSeen {
				out.Acked++
				link.Acked++
				ackTimes = append(ackTimes, rec.ackLatency)
				if _, exists := serverIds[rec.ack.serverMsgId]; exists {
					out.DuplicateServerIds++
				}
				serverIds[rec.ack.serverMsgId] = struct{}{}
			}
			if rec.pushSeen {
				out.Received++
				link.Received++
				pushTimes = append(pushTimes, rec.pushLatency)
			}
			if rec.pulled == 3 {
				out.Verified++
				link.Verified++
				if !rec.pushSeen {
					out.RecoveredByPull++
				}
			}
			if rec.ackSeen && rec.pushSeen && rec.ack != rec.push {
				out.IdentityMismatches++
			}
		}
		r.pairs[i/2].Unlock()
	}
	for from := 1; from <= len(r.opts.nodes); from++ {
		for to := 1; to <= len(r.opts.nodes); to++ {
			if link := links[[2]int{from, to}]; link != nil && link.Planned > 0 {
				out.Links = append(out.Links, *link)
			}
		}
	}
	if r.elapsed > 0 {
		out.ActualPerSecond = float64(out.Sent) / r.elapsed.Seconds()
	}
	out.MissingAck = out.Planned - out.Acked
	out.MissingPush = out.Planned - out.Received
	out.Unverified = out.Planned - out.Verified
	out.AckLatency, out.ReceiveLatency = percentiles(ackTimes), percentiles(pushTimes)
	out.Errors = r.errors.Load()
	r.errorMu.Lock()
	out.ErrorSamples = append(out.ErrorSamples, r.samples...)
	r.errorMu.Unlock()
	out.OmittedErrors = out.Errors - int64(len(out.ErrorSamples))
	complete := out.Connected == int64(out.Users) && out.Sent == out.Planned && out.MissingAck == 0 && out.Unverified == 0
	correct := out.Errors == 0 && out.IdentityMismatches == 0 && out.DuplicateServerIds == 0 && out.Disconnects == 0
	out.Passed = complete && correct && (out.MissingPush == 0 || out.AllowMissedPush)
	return out
}
