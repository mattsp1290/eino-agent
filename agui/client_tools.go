package agui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

const (
	// MetadataClientTool marks tools whose implementation lives on the AG-UI
	// client rather than in the server runtime.
	MetadataClientTool = "agui_client_tool"
	// MetadataClientToolGeneration records the per-session definition version.
	MetadataClientToolGeneration = "agui_client_tool_generation"
	// PermissionClientTool is the default permission class for client tools.
	PermissionClientTool = "agui.client_tool"
)

var ErrClientToolDispatchRequired = errors.New("client tool dispatcher required")

// ClientToolDispatcher sends a model-requested AG-UI client tool call to the
// host/client transport and returns the client-produced result.
type ClientToolDispatcher interface {
	ExecuteClientTool(ctx context.Context, call runtime.ToolCall) (runtime.ToolResult, error)
}

// ClientToolDispatcherFunc adapts a function into a dispatcher.
type ClientToolDispatcherFunc func(context.Context, runtime.ToolCall) (runtime.ToolResult, error)

func (fn ClientToolDispatcherFunc) ExecuteClientTool(ctx context.Context, call runtime.ToolCall) (runtime.ToolResult, error) {
	return fn(ctx, call)
}

// ClientToolSnapshot is the client-defined tool set active for one session.
type ClientToolSnapshot struct {
	SessionID   session.ID
	Generation  uint64
	Tools       []aguitypes.Tool
	Permissions []string
	Metadata    map[string]string
}

// Clone returns a defensive copy of the snapshot containers.
func (s ClientToolSnapshot) Clone() ClientToolSnapshot {
	next := s
	next.Tools = cloneClientTools(s.Tools)
	next.Permissions = cloneStrings(s.Permissions)
	next.Metadata = cloneStringsMap(s.Metadata)
	return next
}

// RuntimeTools converts client definitions into model-facing runtime tools.
func (s ClientToolSnapshot) RuntimeTools(dispatcher ClientToolDispatcher) ([]runtime.Tool, error) {
	if dispatcher == nil {
		return nil, ErrClientToolDispatchRequired
	}
	infos, err := ClientToolInfos(s.Tools)
	if err != nil {
		return nil, err
	}
	result := make([]runtime.Tool, 0, len(infos))
	for _, info := range infos {
		if info == nil {
			continue
		}
		metadata := cloneStringsMap(s.Metadata)
		if metadata == nil {
			metadata = map[string]string{}
		}
		metadata[MetadataClientTool] = "true"
		metadata[MetadataClientToolGeneration] = strconv.FormatUint(s.Generation, 10)
		permissions := cloneStrings(s.Permissions)
		if len(permissions) == 0 {
			permissions = []string{PermissionClientTool}
		}
		result = append(result, runtime.Tool{
			Name:         info.Name,
			Info:         cloneToolInfo(info),
			Executor:     clientToolExecutor{dispatcher: dispatcher},
			RetrySafe:    false,
			Scope:        clientToolScope(s.SessionID, info.Name, permissions),
			Concurrency:  runtime.ToolConcurrencyParallel,
			InputDecoder: clientToolDecoder{},
			Retention:    runtime.RetentionPolicy{MaxInlineBytes: 4096},
			Metadata:     metadata,
		})
	}
	return result, nil
}

type clientToolDecoder struct{}

func (clientToolDecoder) DecodeToolInput(_ context.Context, raw json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%w: invalid json", agenttools.ErrMalformedInput)
	}
	return cloneRaw(raw), nil
}

type clientToolExecutor struct {
	dispatcher ClientToolDispatcher
}

func (e clientToolExecutor) Execute(ctx context.Context, call runtime.ToolCall) (runtime.ToolResult, error) {
	if e.dispatcher == nil {
		return runtime.ToolResult{}, ErrClientToolDispatchRequired
	}
	return e.dispatcher.ExecuteClientTool(ctx, call)
}

func clientToolScope(sessionID session.ID, name string, permissions []string) runtime.ToolScope {
	return runtime.ToolScope{
		ConcurrencyKey: string(sessionID) + ":agui-client:" + name,
		Permissions:    cloneStrings(permissions),
	}
}

func cloneClientTools(src []aguitypes.Tool) []aguitypes.Tool {
	if src == nil {
		return nil
	}
	dst := make([]aguitypes.Tool, len(src))
	for i, tool := range src {
		dst[i] = tool
		dst[i].Parameters = cloneJSONValue(tool.Parameters)
	}
	return dst
}

func cloneJSONValue(src any) any {
	if src == nil {
		return nil
	}
	raw, err := json.Marshal(src)
	if err != nil {
		return src
	}
	var dst any
	if err := json.Unmarshal(raw, &dst); err != nil {
		return src
	}
	return dst
}

func cloneToolInfo(src *einoschema.ToolInfo) *einoschema.ToolInfo {
	if src == nil {
		return nil
	}
	next := *src
	if src.Extra != nil {
		next.Extra = make(map[string]any, len(src.Extra))
		for key, value := range src.Extra {
			next.Extra[key] = value
		}
	}
	return &next
}

func cloneRaw(src json.RawMessage) json.RawMessage {
	if src == nil {
		return nil
	}
	dst := make(json.RawMessage, len(src))
	copy(dst, src)
	return dst
}

func cloneStrings(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func cloneStringsMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
