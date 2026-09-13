package wasmext

import (
	"context"

	einoobs "github.com/mattsp1290/eino-obs"

	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.2.0/types"
)

type engine interface {
	Compile(context.Context, []byte, worldContract) (compiledComponent, error)
	Close() error
}

type compiledComponent interface {
	Interrupt()
	Close() error
}

type toolComponent interface {
	compiledComponent
	ToolMetadata(context.Context) (wittypes.ToolMetadata, error)
	ToolPermissionPattern(context.Context, string) (string, error)
	ExecuteTool(context.Context, toolExecuteRequest) (string, error)
}

type permissionsComponent interface {
	compiledComponent
	DecidePermissions(context.Context, wittypes.PermissionRequest) (wittypes.PermissionDecision, error)
}

type contextComponent interface {
	compiledComponent
	LoadContext(context.Context, wittypes.TurnMetadata) ([]wittypes.Message, error)
}

type eventComponent interface {
	compiledComponent
	EmitEvent(context.Context, wittypes.BoundedEvent) error
}

type hookComponent interface {
	compiledComponent
	BeforeRun(context.Context, wittypes.TurnMetadata) error
	AfterRun(context.Context, wittypes.TurnMetadata) error
}

type middlewareComponent interface {
	compiledComponent
	BeforeToolCall(context.Context, toolMiddlewareBeforeRequest) (wittypes.Replacement, error)
	AfterToolCall(context.Context, toolMiddlewareAfterRequest) (wittypes.Replacement, error)
}

type engineFactory func(Limits) (engine, error)

var newEngine engineFactory = newWasmtimeEngine

type worldContract struct {
	world      string
	exportName string
	functions  []string
	identity   moduleIdentity
	observer   *einoobs.Observer
}

// Contract world/export names are pinned to the eino-agent:extensions
// package version (see wit/eino-agent-extensions.wit). A guest built
// against a different package version presents a different export name and
// fails compilation here as a clean "contract" error (required world
// export missing), before any call -- this is the enforcement point for
// rejecting an incompatible ABI without a crash.
var (
	toolContract = worldContract{
		world: "eino-agent:extensions/tool@0.2.0", exportName: "eino-agent:extensions/tool-api@0.2.0", functions: []string{"metadata", "permission-pattern", "execute"},
	}
	permissionsPolicyContract = worldContract{
		world: "eino-agent:extensions/permissions-policy@0.2.0", exportName: "eino-agent:extensions/permissions-policy-api@0.2.0", functions: []string{"decide"},
	}
	contextSourceContract = worldContract{
		world: "eino-agent:extensions/context-source@0.2.0", exportName: "eino-agent:extensions/context-source-api@0.2.0", functions: []string{"load-context"},
	}
	eventSinkContract = worldContract{
		world: "eino-agent:extensions/event-sink@0.2.0", exportName: "eino-agent:extensions/event-sink-api@0.2.0", functions: []string{"emit"},
	}
	hookContract = worldContract{
		world: "eino-agent:extensions/hook@0.2.0", exportName: "eino-agent:extensions/hook-api@0.2.0", functions: []string{"before-run", "after-run"},
	}
	toolMiddlewareContract = worldContract{
		world: "eino-agent:extensions/tool-middleware@0.2.0", exportName: "eino-agent:extensions/tool-middleware-api@0.2.0", functions: []string{"before-tool-call", "after-tool-call"},
	}
)
