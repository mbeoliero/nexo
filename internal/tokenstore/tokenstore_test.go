package tokenstore

import (
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/cache/local"
)

func TestSamePlatformOverwrites(t *testing.T) {
	ctx := t.Context()
	c := local.New()
	defer c.Close()
	s := New(c)

	if err := s.Set(ctx, "nx__1", 5, "t1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, "nx__1", 1, "t-ios", time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Check(ctx, "nx__1", 5, "t1"); !ok {
		t.Fatal("t1 should be valid")
	}
	if err := s.Set(ctx, "nx__1", 5, "t2", time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Check(ctx, "nx__1", 5, "t1"); ok {
		t.Fatal("t1 must be invalid after second login on the same platform")
	}
	if ok, _ := s.Check(ctx, "nx__1", 5, "t2"); !ok {
		t.Fatal("t2 should be valid")
	}
	if ok, _ := s.Check(ctx, "nx__1", 1, "t-ios"); !ok {
		t.Fatal("other platform must be untouched")
	}
	if err := s.Delete(ctx, "nx__1", 5, "t1"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Check(ctx, "nx__1", 5, "t2"); err != nil || !ok {
		t.Fatalf("stale logout must preserve t2: valid=%v err=%v", ok, err)
	}
	if err := s.Delete(ctx, "nx__1", 5, "t2"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Check(ctx, "nx__1", 5, "t2"); ok {
		t.Fatal("t2 must be invalid after logout")
	}
}
