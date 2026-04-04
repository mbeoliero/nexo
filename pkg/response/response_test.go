package response

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/mbeoliero/nexo/pkg/errcode"
)

func TestErrorUses503ForServerShuttingDown(t *testing.T) {
	ctx := app.NewContext(0)

	Error(context.Background(), ctx, errcode.ErrServerShuttingDown)

	if got := ctx.Response.StatusCode(); got != 503 {
		t.Fatalf("status code mismatch: got %d want 503", got)
	}
}

func TestSuccessWithMetaIncludesMeta(t *testing.T) {
	ctx := app.NewContext(0)

	SuccessWithMeta(context.Background(), ctx, map[string]any{"ok": true}, Meta{
		"partial":     true,
		"data_source": "local_only",
	})

	var resp Response
	if err := json.Unmarshal(ctx.Response.Body(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.Meta == nil || resp.Meta["partial"] != true || resp.Meta["data_source"] != "local_only" {
		t.Fatalf("meta mismatch: %+v", resp.Meta)
	}
}
