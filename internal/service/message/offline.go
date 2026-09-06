package message

import (
	"context"
	"slices"
	"time"

	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/offlinepush"
)

const offlinePushTimeout = 10 * time.Second

// spawnOfflinePush runs offlinePush on a bounded worker set. When all workers are busy (slow
// webhook, dependency outage) the push is dropped and counted: an app push is best-effort, and an
// unbounded backlog would exhaust memory and connections.
func (s *Service) spawnOfflinePush(ctx context.Context, ev PushEvent) {
	select {
	case s.pushSem <- struct{}{}:
	default:
		n := s.pushDropped.Add(1)
		log.CtxWarn(ctx, "offline push dropped conv=%s seq=%d: %d workers busy (dropped so far %d)", ev.ConversationId, ev.Message.Seq, offlinePushWorkers, n)
		return
	}
	s.pushWg.Go(func() {
		defer func() { <-s.pushSem }()
		s.offlinePush(ctx, ev)
	})
}

// Wait blocks until in-flight offline pushes finish or ctx is done. Call before closing the store.
func (s *Service) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() { s.pushWg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// OfflinePushDropped is the number of pushes skipped because every worker was busy.
func (s *Service) OfflinePushDropped() int64 { return s.pushDropped.Load() }

// offlinePush resolves recipients minus the sender, drops muted users, keeps only those whose
// visible range covers the message (the roster is a candidate filter, not authorization; design §6.1),
// drops online users and hands the rest to the Pusher. Fail-closed: any store/OnlineStore error pushes nobody (A8).
func (s *Service) offlinePush(ctx context.Context, ev PushEvent) {
	target := s.push.Load()
	if target == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, offlinePushTimeout)
	defer cancel()
	targets, err := s.Recipients(ctx, ev)
	if err != nil {
		log.CtxError(ctx, "offline push recipients conv=%s seq=%d: %v", ev.ConversationId, ev.Message.Seq, err)
		return
	}
	targets = slices.DeleteFunc(targets, func(id string) bool { return id == ev.SenderId })
	if len(targets) == 0 {
		return
	}
	muted, err := s.store.MutedOwners(ctx, ev.ConversationId, targets)
	if err != nil {
		log.CtxError(ctx, "offline push mute filter conv=%s: %v", ev.ConversationId, err)
		return
	}
	targets = slices.DeleteFunc(targets, func(id string) bool { return slices.Contains(muted, id) })
	if len(targets) == 0 {
		return
	}
	if targets, err = s.VisibleTo(ctx, ev.ConversationId, targets, ev.Message.Seq); err != nil {
		log.CtxError(ctx, "offline push visibility conv=%s seq=%d: %v", ev.ConversationId, ev.Message.Seq, err)
		return
	}
	if len(targets) == 0 {
		return
	}
	offline := targets
	if target.online != nil {
		online, err := target.online.Online(ctx, targets)
		if err != nil {
			log.CtxError(ctx, "offline push online check conv=%s: %v (skipping)", ev.ConversationId, err)
			return
		}
		offline = slices.DeleteFunc(targets, func(id string) bool { return len(online[id]) > 0 })
	}
	if len(offline) == 0 {
		return
	}
	if err := target.pusher.Push(ctx, offline, notification(ev)); err != nil {
		log.CtxError(ctx, "offline push conv=%s seq=%d users=%d: %v", ev.ConversationId, ev.Message.Seq, len(offline), err)
	}
}

func notification(ev PushEvent) offlinepush.Notification {
	m := ev.Message
	return offlinepush.Notification{
		ConversationId: ev.ConversationId, Seq: m.Seq, SessionType: ev.SessionType, SenderId: ev.SenderId, GroupId: ev.GroupId,
		ContentType: m.ContentType, Content: m.Content, SendTime: m.SendTime,
	}
}
