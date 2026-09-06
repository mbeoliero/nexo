package server

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

// Every type a service method takes or returns must be nameable from this package (design §15.1).
func TestAliasesCoverServiceSignatures(t *testing.T) {
	named := map[reflect.Type]bool{}
	for _, v := range []any{
		Config{}, Identity{}, Notification{}, GatewayStats{},
		Profile{}, Session{}, ProfileUpdate{}, OnlineStatus{},
		GroupInfo{}, GroupMember{}, GroupCreateInput{},
		Message{}, Ack{}, SendInput{}, PullInput{}, PullResult{}, MaxSeqItem{}, MaxSeqsResult{}, PushEvent{},
		ConversationItem{}, ConversationList{}, ConversationOpt{},
	} {
		named[reflect.TypeOf(v)] = true
	}
	named[reflect.TypeFor[Authenticator]()] = true
	named[reflect.TypeFor[Pusher]()] = true

	var missing []string
	check := func(owner string, tp reflect.Type) {
		for tp.Kind() == reflect.Ptr || tp.Kind() == reflect.Slice {
			tp = tp.Elem()
		}
		if strings.Contains(tp.PkgPath(), "/internal/") && !named[tp] {
			missing = append(missing, owner+": "+tp.String())
		}
	}
	for _, svc := range []reflect.Type{
		reflect.TypeFor[*UserService](), reflect.TypeFor[*GroupService](),
		reflect.TypeFor[*MessageService](), reflect.TypeFor[*ConversationService](),
	} {
		for i := range svc.NumMethod() {
			m := svc.Method(i)
			if slices.Contains([]string{"SetOnlineStore", "SetOfflinePush", "SetMemberCacheTtl", "SetSendRateLimit", "ResolvePush"}, m.Name) {
				continue // wiring, called by app / gateway only
			}
			for j := 1; j < m.Type.NumIn(); j++ {
				check(svc.Elem().String()+"."+m.Name, m.Type.In(j))
			}
			for j := range m.Type.NumOut() {
				check(svc.Elem().String()+"."+m.Name, m.Type.Out(j))
			}
		}
	}
	if len(missing) > 0 {
		t.Fatalf("service signatures use internal types without a server alias:\n%s", strings.Join(slices.Compact(missing), "\n"))
	}
}
