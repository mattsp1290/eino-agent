package wasmext

import (
	"context"

	einoobs "github.com/mattsp1290/eino-obs"
)

type engine interface {
	Compile(context.Context, []byte, worldContract) (compiledComponent, error)
	Close() error
}

type compiledComponent interface {
	Call(context.Context, string, any, any) error
	Interrupt()
	Close() error
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

var (
	toolContract = worldContract{
		world: "eino-agent:extensions/tool@0.1.0", exportName: "eino-agent:extensions/tool-api@0.1.0", functions: []string{"metadata", "execute"},
	}
	permissionsPolicyContract = worldContract{
		world: "eino-agent:extensions/permissions-policy@0.1.0", exportName: "eino-agent:extensions/permissions-policy-api@0.1.0", functions: []string{"decide"},
	}
	contextSourceContract = worldContract{
		world: "eino-agent:extensions/context-source@0.1.0", exportName: "eino-agent:extensions/context-source-api@0.1.0", functions: []string{"load-context"},
	}
	eventSinkContract = worldContract{
		world: "eino-agent:extensions/event-sink@0.1.0", exportName: "eino-agent:extensions/event-sink-api@0.1.0", functions: []string{"emit"},
	}
	hookContract = worldContract{
		world: "eino-agent:extensions/hook@0.1.0", exportName: "eino-agent:extensions/hook-api@0.1.0", functions: []string{"before-run", "before-turn", "after-turn", "after-run"},
	}
	toolMiddlewareContract = worldContract{
		world: "eino-agent:extensions/tool-middleware@0.1.0", exportName: "eino-agent:extensions/tool-middleware-api@0.1.0", functions: []string{"before-tool-call", "after-tool-call"},
	}
)
