package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/pkg/errcode"
)

func TestResolveConversationSeqStateReturnsInternalServerOnSeqConversationError(t *testing.T) {
	_, _, err := resolveConversationSeqState(nil, errcode.ErrInternalServer, nil, nil)
	if err != errcode.ErrInternalServer {
		t.Fatalf("expected internal server error, got %v", err)
	}
}

func TestResolveConversationSeqStateReturnsInternalServerOnSeqUserError(t *testing.T) {
	_, _, err := resolveConversationSeqState(&entity.SeqConversation{MaxSeq: 9}, nil, nil, errcode.ErrInternalServer)
	if err != errcode.ErrInternalServer {
		t.Fatalf("expected internal server error, got %v", err)
	}
}

func TestResolveConversationSeqStateBuildsUnreadState(t *testing.T) {
	maxSeq, readSeq, err := resolveConversationSeqState(
		&entity.SeqConversation{MaxSeq: 9},
		nil,
		&entity.SeqUser{ReadSeq: 4},
		nil,
	)
	if err != nil {
		t.Fatalf("resolve conversation seq state: %v", err)
	}
	if maxSeq != 9 {
		t.Fatalf("max seq mismatch: got %d want %d", maxSeq, 9)
	}
	if readSeq != 4 {
		t.Fatalf("read seq mismatch: got %d want %d", readSeq, 4)
	}
}

func TestGetMaxReadSeqRejectsUnauthorizedConversationBeforeSeqLookup(t *testing.T) {
	svc := &ConversationService{}

	_, _, err := callGetMaxReadSeqNoPanic(svc, context.Background(), "u3", "si_u1:u2")
	if err != errcode.ErrNoPermission {
		t.Fatalf("expected no permission, got %v", err)
	}
}

func TestMarkReadRejectsUnauthorizedConversationBeforeSeqLookup(t *testing.T) {
	svc := &ConversationService{}

	err := callMarkReadNoPanic(svc, context.Background(), "u3", "si_u1:u2", 10)
	if err != errcode.ErrNoPermission {
		t.Fatalf("expected no permission, got %v", err)
	}
}

func callGetMaxReadSeqNoPanic(svc *ConversationService, ctx context.Context, userId, conversationId string) (maxSeq, readSeq int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return svc.GetMaxReadSeq(ctx, userId, conversationId)
}

func callMarkReadNoPanic(svc *ConversationService, ctx context.Context, userId, conversationId string, readSeq int64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return svc.MarkRead(ctx, userId, conversationId, readSeq)
}
