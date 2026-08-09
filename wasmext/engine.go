package wasmext

import "context"

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
}

var (
	toolContract = worldContract{
		world: "eino-agent:extensions/tool@0.1.0", exportName: "eino-agent:extensions/tool-api@0.1.0", functions: []string{"metadata", "execute"},
	}
	permissionsPolicyContract = worldContract{
		world: "eino-agent:extensions/permissions-policy@0.1.0", exportName: "eino-agent:extensions/permissions-policy-api@0.1.0", functions: []string{"decide"},
	}
)
