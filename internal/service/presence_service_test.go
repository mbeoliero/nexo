package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mbeoliero/kit/log"
	"github.com/mbeoliero/nexo/pkg/constant"
)

func capturePresenceLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	log.SetOutput(buf)
	t.Cleanup(func() {
		log.SetOutput(io.Discard)
	})
	return buf
}

type stubLocalPresenceReader struct {
	results map[string][]*PlatformStatusDetail
}

func (s *stubLocalPresenceReader) GetUsersLocalPresence(_ []string) map[string][]*PlatformStatusDetail {
	return s.results
}

type stubRemotePresenceReader struct {
	results map[string][]PresenceConnRef
	err     error
}

func (s *stubRemotePresenceReader) GetUsersPresenceConnRefs(_ context.Context, userIDs []string) (map[string][]PresenceConnRef, error) {
	if s.err != nil {
		return nil, s.err
	}
	result := make(map[string][]PresenceConnRef, len(userIDs))
	for _, userID := range userIDs {
		result[userID] = append(result[userID], s.results[userID]...)
	}
	return result, nil
}

type stubPresenceStats struct {
	partial int
}

func (s *stubPresenceStats) IncPresencePartial() {
	s.partial++
}

func TestGetUsersOnlineStatusMergesLocalAndRemotePresence(t *testing.T) {
	service := NewPresenceService("i1", &stubLocalPresenceReader{
		results: map[string][]*PlatformStatusDetail{
			"u1": {{PlatformId: 5, PlatformName: "Web", ConnId: "local"}},
		},
	}, &stubRemotePresenceReader{
		results: map[string][]PresenceConnRef{
			"u1": {{UserId: "u1", ConnId: "remote", InstanceId: "i2", PlatformId: 1}},
		},
	})

	got, err := service.GetUsersOnlineStatus(context.Background(), []string{"u1"})
	if err != nil {
		t.Fatalf("get users online status: %v", err)
	}

	if len(got.Users) != 1 || got.Users[0].Status != constant.StatusOnline || len(got.Users[0].DetailPlatformStatus) != 2 {
		t.Fatalf("presence merge mismatch: %+v", got.Users)
	}
}

func TestGetUsersOnlineStatusReturnsPartialLocalOnlyOnRemoteFailure(t *testing.T) {
	logBuf := capturePresenceLogs(t)
	stats := &stubPresenceStats{}
	service := NewPresenceService("i1", &stubLocalPresenceReader{
		results: map[string][]*PlatformStatusDetail{
			"u1": {{PlatformId: 5, PlatformName: "Web", ConnId: "local"}},
		},
	}, &stubRemotePresenceReader{err: errors.New("boom")}).SetStats(stats)

	got, err := service.GetUsersOnlineStatus(context.Background(), []string{"u1"})
	if err != nil {
		t.Fatalf("get users online status: %v", err)
	}
	if !got.Partial || got.DataSource != "local_only" {
		t.Fatalf("partial response mismatch: %+v", got)
	}
	if stats.partial != 1 {
		t.Fatalf("presence partial count mismatch: got %d want 1", stats.partial)
	}
	if !strings.Contains(logBuf.String(), "presence remote read degraded") || !strings.Contains(logBuf.String(), "data_source=local_only") {
		t.Fatalf("expected degraded presence logs, got %q", logBuf.String())
	}
}

func TestGetUsersOnlineStatusDoesNotTreatRouteableFalseAsOffline(t *testing.T) {
	service := NewPresenceService("i1", &stubLocalPresenceReader{}, &stubRemotePresenceReader{
		results: map[string][]PresenceConnRef{
			"u1": {{UserId: "u1", ConnId: "remote", InstanceId: "i2", PlatformId: 1}},
		},
	})

	got, err := service.GetUsersOnlineStatus(context.Background(), []string{"u1"})
	if err != nil {
		t.Fatalf("get users online status: %v", err)
	}
	if got.Users[0].Status != constant.StatusOnline {
		t.Fatalf("expected user to remain online: %+v", got.Users[0])
	}
}

func TestGetUsersOnlineStatusKeepsPlatformDetailShape(t *testing.T) {
	service := NewPresenceService("i1", &stubLocalPresenceReader{}, &stubRemotePresenceReader{
		results: map[string][]PresenceConnRef{
			"u1": {{UserId: "u1", ConnId: "remote", InstanceId: "i2", PlatformId: 1}},
		},
	})

	got, err := service.GetUsersOnlineStatus(context.Background(), []string{"u1"})
	if err != nil {
		t.Fatalf("get users online status: %v", err)
	}

	details := got.Users[0].DetailPlatformStatus
	if len(details) != 1 || details[0].PlatformId != 1 || details[0].PlatformName == "" || details[0].ConnId != "remote" {
		t.Fatalf("platform detail mismatch: %+v", details)
	}
}
