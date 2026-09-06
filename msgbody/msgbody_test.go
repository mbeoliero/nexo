package msgbody

import (
	"errors"
	"slices"
	"testing"
)

func TestParse(t *testing.T) {
	t.Parallel()
	b, err := Parse(Text, `{"text":"hi"}`)
	if err != nil || b.Text != "hi" || b.Type != Text {
		t.Fatalf("text: %+v %v", b, err)
	}
	b, err = Parse(File, `{"file":"https://x/f.pdf","name":"f.pdf"}`)
	if err != nil || b.Url != "https://x/f.pdf" || b.Name != "f.pdf" {
		t.Fatalf("file: %+v %v", b, err)
	}
	b, err = Parse(Image, `{"image":"https://x/a.png"}`)
	if err != nil || b.Url != "https://x/a.png" {
		t.Fatalf("image: %+v %v", b, err)
	}
	b, err = Parse(Custom, `{"k":1}`)
	if err != nil || string(b.Custom) != `{"k":1}` {
		t.Fatalf("custom: %+v %v", b, err)
	}
	if _, err := Parse(7, `{}`); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("unknown type: %v", err)
	}
	if _, err := Parse(Text, `not json`); err == nil {
		t.Fatal("invalid json must fail")
	}
	if _, err := Parse(Custom, `not json`); err == nil {
		t.Fatal("invalid custom json must fail")
	}
	if b, err := Parse(Text, `{}`); err != nil || b.Text != "" {
		t.Fatalf("missing key is not an error: %+v %v", b, err)
	}
}

func TestPreview(t *testing.T) {
	t.Parallel()
	long := `{"text":"` + string(slices.Repeat([]rune("字"), 60)) + `"}`
	if got := Preview(Text, long); len([]rune(got)) != PreviewMaxRunes+1 {
		t.Fatalf("len=%d", len([]rune(got)))
	}
	cases := []struct {
		typ  int32
		raw  string
		want string
	}{
		{Text, `{"text":"hello"}`, "hello"},
		{Image, `{}`, "[图片]"},
		{Video, `{}`, "[视频]"},
		{Audio, `{}`, "[语音]"},
		{File, `{}`, "[文件]"},
		{Custom, `{"k":1}`, ""},
		{Text, `not json`, ""},
		{9, `{}`, ""},
	}
	for _, c := range cases {
		if got := Preview(c.typ, c.raw); got != c.want {
			t.Fatalf("type=%d raw=%s: got %q want %q", c.typ, c.raw, got, c.want)
		}
	}
}
