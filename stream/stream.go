package stream

import (
	"context"
	"fmt"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
	aguistream "github.com/mattsp1290/eino-agui/stream"

	"github.com/mattsp1290/eino-agent/agui"
)

// Option configures StreamTurn.
type Option = aguistream.Option

// WithLiveToolCallEvents enables live TOOL_CALL_* emission through eino-agui.
var WithLiveToolCallEvents = aguistream.WithLiveToolCallEvents

// StreamTurn delegates Eino model stream tapping to eino-agui/stream.
func StreamTurn(ctx context.Context, bridge *agui.Bridge, model einomodel.ToolCallingChatModel, messages []*einoschema.Message, opts ...Option) (*einoschema.Message, error) {
	if bridge == nil || bridge.Emitter() == nil {
		return nil, fmt.Errorf("agui stream bridge required")
	}
	return aguistream.StreamTurn(ctx, bridge.Emitter(), model, messages, opts...)
}
