package user

import (
	"errors"
	"strings"
	"testing"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

// Extra is opaque text capped at the MySQL TEXT ceiling on every write path;
// storetest.RunInsertConstraints proves that ceiling round-trips in every database.
func TestExtraBytes(t *testing.T) {
	extra := " \nnot JSON 中文 😀 \t"
	extra += strings.Repeat("a", store.MaxExtraBytes-len(extra))
	const id = "u___1"

	for _, operation := range []string{"upsert-create", "upsert-update", "partial-update"} {
		t.Run(operation, func(t *testing.T) {
			ctx := t.Context()
			s := New(storetest.NewMem(), nil)
			var before Profile
			var err error
			if operation != "upsert-create" {
				if before, err = s.Upsert(ctx, id, "old", "old-avatar", "old-extra"); err != nil {
					t.Fatal(err)
				}
			}
			write := func(value string) (Profile, error) {
				if operation == "partial-update" {
					return s.Update(ctx, id, Update{Nickname: new("new"), Extra: &value})
				}
				return s.Upsert(ctx, id, "new", "new-avatar", value)
			}

			if _, err := write(extra + "a"); !errors.Is(err, errcode.ErrInvalidParam) {
				t.Fatalf("extra over the limit: %v", err)
			}
			stored, err := s.Get(ctx, id)
			if operation == "upsert-create" {
				if !errors.Is(err, errcode.ErrUserNotFound) {
					t.Fatalf("rejected create persisted: %v", err)
				}
			} else if err != nil || stored != before {
				t.Fatalf("rejected update changed profile: %+v, %v", stored, err)
			}

			got, err := write(extra)
			if err != nil || got.Extra != extra {
				t.Fatalf("extra at the limit (%d bytes): %v", len(extra), err)
			}
			if stored, err := s.Get(ctx, id); err != nil || stored != got {
				t.Fatalf("stored profile: %+v, %v", stored, err)
			}
			if operation == "partial-update" && got.Avatar != before.Avatar {
				t.Fatal("partial update changed avatar")
			}
		})
	}

	t.Run("nil-keeps-empty-clears", func(t *testing.T) {
		ctx := t.Context()
		s := New(storetest.NewMem(), nil)
		before, err := s.Upsert(ctx, id, "name", "avatar", " keep me ")
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.Update(ctx, id, Update{Nickname: new("changed")})
		if err != nil || got.Extra != before.Extra {
			t.Fatalf("nil extra changed value: %v", err)
		}
		got, err = s.Update(ctx, id, Update{Extra: new("")})
		if err != nil || got.Extra != "" || got.Nickname != "changed" || got.Avatar != "avatar" {
			t.Fatalf("empty extra did not clear only extra: %v", err)
		}
		stored, err := s.Get(ctx, id)
		if err != nil || stored != got {
			t.Fatalf("clear did not persist: %v", err)
		}
	})
}
