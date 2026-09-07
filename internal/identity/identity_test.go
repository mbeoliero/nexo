package identity

import "testing"

func TestActorRoundTrip(t *testing.T) {
	cases := []struct {
		actor Actor
		want  string
	}{
		{Actor{Id: 42, Role: RoleUser}, "u___42"},
		{Actor{Id: 7, Role: RoleAgent}, "ag__7"},
	}
	for _, c := range cases {
		got, err := c.actor.UserId()
		if err != nil || got != c.want {
			t.Fatalf("UserId(%+v) = %q, %v; want %q", c.actor, got, err, c.want)
		}
		back, err := ParseActor(got)
		if err != nil || back != c.actor {
			t.Fatalf("ParseActor(%q) = %+v, %v; want %+v", got, back, err, c.actor)
		}
	}
}

func TestParseActorRejects(t *testing.T) {
	for _, s := range []string{"", "u___", "u___x", "xx__1", "nx__abc", "u___1:2",
		// Non-canonical spellings of an existing actor: accepting them forks the user's history.
		"u___05", "u___+5", "u___-5", "u___0", "u___ 5"} {
		if _, err := ParseActor(s); err == nil {
			t.Errorf("ParseActor(%q) should fail", s)
		}
	}
	if _, err := (Actor{Id: 1, Role: "bot"}).UserId(); err == nil {
		t.Error("unknown role should fail")
	}
}

func TestValid(t *testing.T) {
	const native = "nx__0190a6e1-2b3c-7d4e-8f5a-6b7c8d9e0f1a"
	for s, want := range map[string]bool{
		"u___1": true, "ag__1": true, native: true,
		"u___": false, "u___a": false, "zz__1": false, "nx__a:b": false, "nx__0190": false,
		// Canonical spelling only: MySQL PAD SPACE would route a padded id to the same row.
		native + " ": false, "nx__0190A6E1-2B3C-7D4E-8F5A-6B7C8D9E0F1A": false,
	} {
		if got := Valid(s); got != want {
			t.Errorf("Valid(%q) = %v, want %v", s, got, want)
		}
	}
}
