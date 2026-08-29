package agui

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	aguitools "github.com/mattsp1290/eino-agui/tools"

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
	ExecuteClientTool(ctx context.Context, call runtime.ToolCall) (json.RawMessage, error)
}

// ClientToolDispatcherFunc adapts a function into a dispatcher.
type ClientToolDispatcherFunc func(context.Context, runtime.ToolCall) (json.RawMessage, error)

func (fn ClientToolDispatcherFunc) ExecuteClientTool(ctx context.Context, call runtime.ToolCall) (json.RawMessage, error) {
	return fn(ctx, call)
}

// ClientToolSnapshot is the client-defined tool set active for one session.
type ClientToolSnapshot struct {
	SessionID            session.ID
	Generation           uint64
	DispatcherArtifactID string
	Tools                []aguitypes.Tool
	Permissions          []string
	Metadata             map[string]string
}

// Clone returns a defensive copy of the snapshot containers.
func (s ClientToolSnapshot) Clone() (ClientToolSnapshot, error) {
	next := s
	var err error
	next.Tools, err = cloneClientTools(s.Tools)
	if err != nil {
		return ClientToolSnapshot{}, err
	}
	next.Permissions = cloneStrings(s.Permissions)
	next.Metadata = cloneStringsMap(s.Metadata)
	return next, nil
}

// Definitions converts client definitions into canonical typed tools.
func (s ClientToolSnapshot) Definitions(dispatcher ClientToolDispatcher) ([]agenttools.Definition, error) {
	if dispatcher == nil {
		return nil, ErrClientToolDispatchRequired
	}
	frozen, err := s.Clone()
	if err != nil {
		return nil, err
	}
	infos, err := aguitools.ClientToolInfos(frozen.Tools)
	if err != nil {
		return nil, err
	}
	result := make([]agenttools.Definition, 0, len(infos))
	for _, info := range infos {
		if info == nil {
			continue
		}
		metadata := cloneStringsMap(frozen.Metadata)
		if metadata == nil {
			metadata = map[string]string{}
		}
		metadata[MetadataClientTool] = "true"
		metadata[MetadataClientToolGeneration] = strconv.FormatUint(frozen.Generation, 10)
		permissions := cloneStrings(frozen.Permissions)
		if len(permissions) == 0 {
			permissions = []string{PermissionClientTool}
		}
		result = append(result, agenttools.Definition{
			Name: info.Name, Description: info.Desc, Parameters: info.ParamsOneOf,
			Execute: func(ctx context.Context, execution agenttools.Execution) (json.RawMessage, error) {
				if dispatcher == nil {
					return nil, ErrClientToolDispatchRequired
				}
				return dispatcher.ExecuteClientTool(ctx, execution.Call)
			},
			Scope: func(context.Context, runtime.ToolScopeContext) runtime.ToolScope {
				return runtime.ToolScope{Permissions: cloneStrings(permissions)}
			},
			Permissions: permissions, Retention: runtime.RetentionPolicy{MaxInlineBytes: 4096}, Metadata: metadata,
		})
	}
	return result, nil
}

func cloneClientTools(src []aguitypes.Tool) ([]aguitypes.Tool, error) {
	if src == nil {
		return nil, nil
	}
	dst := make([]aguitypes.Tool, len(src))
	for i, tool := range src {
		dst[i] = tool
		parameters, err := cloneJSONValue(tool.Parameters)
		if err != nil {
			return nil, err
		}
		dst[i].Parameters = parameters
	}
	return dst, nil
}

func cloneJSONValue(src any) (any, error) {
	if src == nil {
		return nil, nil
	}
	raw, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}
	var dst any
	if err := json.Unmarshal(raw, &dst); err != nil {
		return nil, err
	}
	return dst, nil
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
