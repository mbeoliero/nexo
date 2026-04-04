package sdk

import (
	"encoding/json"
	"testing"
)

func TestResponseErrorTextPrefersMessageField(t *testing.T) {
	resp := Response{
		Msg:     "legacy message",
		Message: "server shutting down",
	}

	if got := resp.ErrorText(); got != "server shutting down" {
		t.Fatalf("error text mismatch: got %q want %q", got, "server shutting down")
	}
}

func TestResponseErrorTextFallsBackToLegacyMsg(t *testing.T) {
	resp := Response{Msg: "legacy message"}

	if got := resp.ErrorText(); got != "legacy message" {
		t.Fatalf("error text mismatch: got %q want %q", got, "legacy message")
	}
}

func TestResponseUnmarshalAcceptsMessageFieldAndMeta(t *testing.T) {
	var resp Response
	if err := json.Unmarshal([]byte(`{"code":1008,"message":"server shutting down","meta":{"partial":true,"data_source":"local_only"}}`), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.Message != "server shutting down" {
		t.Fatalf("message mismatch: got %q", resp.Message)
	}
	if resp.Meta == nil || resp.Meta["partial"] != true || resp.Meta["data_source"] != "local_only" {
		t.Fatalf("meta mismatch: %+v", resp.Meta)
	}
}
