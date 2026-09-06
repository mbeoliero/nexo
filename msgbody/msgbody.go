// Package msgbody is the single definition of message content: content_type codes, the wire
// shape of `content`, and the default push preview. It depends on stdlib only so webhook
// receivers and embedding hosts can import it (design §6.6).
//
// Wire shape (flat, one key per type):
//
//	1 text   {"text": "..."}
//	2 image  {"image": "<url>"}
//	3 video  {"video": "<url>"}
//	4 audio  {"audio": "<url>"}
//	5 file   {"file": "<url>", "name": "<optional>"}
//	100      any JSON value; opaque to the server
package msgbody

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
)

const (
	Text   int32 = 1
	Image  int32 = 2
	Video  int32 = 3
	Audio  int32 = 4
	File   int32 = 5
	Custom int32 = 100
)

const PreviewMaxRunes = 50

var ErrUnknownType = errors.New("msgbody: unknown content_type")

func ValidType(t int32) bool {
	switch t {
	case Text, Image, Video, Audio, File, Custom:
		return true
	}
	return false
}

type Body struct {
	Type   int32
	Text   string         // Text
	Url    string         // Image / Video / Audio / File
	Name   string         // File, optional
	Custom jsontext.Value // Custom: the whole content, untouched
}

type wire struct {
	Text  string `json:"text"`
	Image string `json:"image"`
	Video string `json:"video"`
	Audio string `json:"audio"`
	File  string `json:"file"`
	Name  string `json:"name"`
}

// Parse decodes raw by contentType. It fails on an unknown type or invalid JSON; a known
// type with a missing key yields an empty Body of that type (the server stores what it
// was sent and only guarantees it is JSON).
func Parse(contentType int32, raw string) (Body, error) {
	if !ValidType(contentType) {
		return Body{}, fmt.Errorf("%w: %d", ErrUnknownType, contentType)
	}
	b := Body{Type: contentType}
	if contentType == Custom {
		v := jsontext.Value(raw)
		if !v.IsValid() {
			return Body{}, errors.New("msgbody: content is not valid JSON")
		}
		b.Custom = v
		return b, nil
	}
	var w wire
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return Body{}, fmt.Errorf("msgbody: %w", err)
	}
	switch contentType {
	case Text:
		b.Text = w.Text
	case Image:
		b.Url = w.Image
	case Video:
		b.Url = w.Video
	case Audio:
		b.Url = w.Audio
	case File:
		b.Url, b.Name = w.File, w.Name
	}
	return b, nil
}

// Preview is the default notification text: text truncated to PreviewMaxRunes, a
// placeholder for media, and "" for Custom so the caller decides.
func (b Body) Preview() string {
	switch b.Type {
	case Text:
		r := []rune(b.Text)
		if len(r) > PreviewMaxRunes {
			return string(r[:PreviewMaxRunes]) + "…"
		}
		return b.Text
	case Image:
		return "[图片]"
	case Video:
		return "[视频]"
	case Audio:
		return "[语音]"
	case File:
		return "[文件]"
	}
	return ""
}

// Preview parses and previews in one step; invalid content previews as "".
func Preview(contentType int32, raw string) string {
	b, err := Parse(contentType, raw)
	if err != nil {
		return ""
	}
	return b.Preview()
}
