package einotools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-tools/applypatch"
	"github.com/mattsp1290/eino-tools/fileops"
	"github.com/mattsp1290/eino-tools/glob"
	"github.com/mattsp1290/eino-tools/search"
	"github.com/mattsp1290/eino-tools/shell"
	"github.com/mattsp1290/eino-tools/tracker"
	"github.com/mattsp1290/eino-tools/trackerwrite"
	"github.com/mattsp1290/eino-tools/urlfetch"
	"github.com/mattsp1290/eino-tools/userinteract"

	"github.com/mattsp1290/eino-agent/internal/workspace"
	"github.com/mattsp1290/eino-agent/runtime"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

const (
	defaultMaxInlineBytes int64 = 64 * 1024
	metadataSource              = "eino-tools"
)

type invokableTool interface {
	Info(context.Context) (*einoschema.ToolInfo, error)
	InvokableRun(context.Context, string, ...tool.Option) (string, error)
}

type workspaceFactory func(string) (invokableTool, error)
type staticFactory func() (invokableTool, error)

// Options controls optional leaf-tool adapters.
type Options struct {
	Locker        *workspace.Locker
	UserSurface   userinteract.Surface
	UserStdin     io.Reader
	UserStderr    io.Writer
	ShellOptions  *shell.Options
	TrackerWriter tracker.CloseWriter
}

// RegisterDefaults registers the standard eino-tools leaf tool set.
func RegisterDefaults(ctx context.Context, registry *agenttools.Registry, options Options) ([]agenttools.Registration, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: registry required", agenttools.ErrInvalidDefinition)
	}
	if options.Locker == nil {
		options.Locker = &workspace.Locker{}
	}
	registrations := []agenttools.Registration{}
	for _, spec := range workspaceSpecs(options) {
		registration, err := registerWorkspace(ctx, registry, spec)
		if err != nil {
			return nil, err
		}
		registrations = append(registrations, registration)
	}
	for _, spec := range staticSpecs(options) {
		registration, err := registerStatic(ctx, registry, spec)
		if err != nil {
			return nil, err
		}
		registrations = append(registrations, registration)
	}
	return registrations, nil
}

type workspaceSpec struct {
	name       string
	concurrent bool
	factory    workspaceFactory
	locker     *workspace.Locker
}

type staticSpec struct {
	name    string
	factory staticFactory
}

func workspaceSpecs(options Options) []workspaceSpec {
	shellOptions := options.ShellOptions
	return []workspaceSpec{
		{name: fileops.NameRead, factory: func(root string) (invokableTool, error) { return fileops.NewReadTool(root) }, locker: options.Locker},
		{name: fileops.NameWrite, factory: func(root string) (invokableTool, error) { return fileops.NewWriteTool(root) }, locker: options.Locker},
		{name: fileops.NameEdit, factory: func(root string) (invokableTool, error) { return fileops.NewEditTool(root) }, locker: options.Locker},
		{name: fileops.NameList, factory: func(root string) (invokableTool, error) { return fileops.NewListTool(root) }, locker: options.Locker},
		{name: glob.Name, factory: func(root string) (invokableTool, error) { return glob.New(root) }, locker: options.Locker},
		{name: search.Name, factory: func(root string) (invokableTool, error) { return search.New(root) }, locker: options.Locker},
		{name: applypatch.Name, factory: func(root string) (invokableTool, error) { return applypatch.New(root) }, locker: options.Locker},
		{name: shell.Name, factory: func(root string) (invokableTool, error) {
			if shellOptions == nil {
				return shell.New(root)
			}
			return shell.New(root, *shellOptions)
		}, locker: options.Locker},
	}
}

func staticSpecs(options Options) []staticSpec {
	surface := options.UserSurface
	if surface == "" {
		surface = userinteract.SurfaceMCP
	}
	stdin := options.UserStdin
	if stdin == nil {
		stdin = os.Stdin
	}
	stderr := options.UserStderr
	if stderr == nil {
		stderr = os.Stderr
	}
	specs := []staticSpec{
		{name: urlfetch.Name, factory: func() (invokableTool, error) { return urlfetch.New() }},
		{name: userinteract.Name, factory: func() (invokableTool, error) {
			return userinteract.New(surface, userinteract.Options{Stdin: stdin, Stderr: stderr})
		}},
	}
	if options.TrackerWriter != nil {
		specs = append(specs, staticSpec{name: trackerwrite.Name, factory: func() (invokableTool, error) {
			return trackerwrite.New(options.TrackerWriter)
		}})
	}
	return specs
}

func registerWorkspace(ctx context.Context, registry *agenttools.Registry, spec workspaceSpec) (agenttools.Registration, error) {
	infoTool, err := spec.factory("/")
	if err != nil {
		return agenttools.Registration{}, err
	}
	info, err := infoTool.Info(ctx)
	if err != nil {
		return agenttools.Registration{}, err
	}
	definition := baseDefinition(spec.name, info)
	definition.Concurrency = runtime.ToolConcurrencySequential
	if spec.concurrent {
		definition.Concurrency = runtime.ToolConcurrencyParallel
	}
	definition.Scope = workspaceScope(spec.name, definition.Concurrency)
	definition.Execute = executeWorkspace(spec)
	return registry.Register(definition)
}

func registerStatic(ctx context.Context, registry *agenttools.Registry, spec staticSpec) (agenttools.Registration, error) {
	infoTool, err := spec.factory()
	if err != nil {
		return agenttools.Registration{}, err
	}
	info, err := infoTool.Info(ctx)
	if err != nil {
		return agenttools.Registration{}, err
	}
	definition := baseDefinition(spec.name, info)
	definition.Execute = func(ctx context.Context, execution agenttools.Execution) (any, error) {
		tool, err := spec.factory()
		if err != nil {
			return nil, err
		}
		return invoke(ctx, tool, execution.Input.(json.RawMessage))
	}
	return registry.Register(definition)
}

func baseDefinition(name string, info *einoschema.ToolInfo) agenttools.Definition {
	return agenttools.Definition{
		Name:        name,
		Description: info.Desc,
		Parameters:  info.ParamsOneOf,
		Decode:      decodeRaw,
		Normalize:   normalizeRaw,
		Encode:      encodeRaw,
		Retention: runtime.RetentionPolicy{
			MaxInlineBytes: defaultMaxInlineBytes,
			StoreExternal:  true,
		},
		Metadata: map[string]string{"source": metadataSource},
	}
}

func executeWorkspace(spec workspaceSpec) agenttools.Executor {
	return func(ctx context.Context, execution agenttools.Execution) (any, error) {
		root, err := canonicalRoot(execution.Snapshot)
		if err != nil {
			return nil, err
		}
		run := func() error {
			tool, err := spec.factory(root)
			if err != nil {
				return err
			}
			result, err := invoke(ctx, tool, execution.Input.(json.RawMessage))
			if err != nil {
				return err
			}
			execution.Input = result
			return nil
		}
		if spec.concurrent {
			if err := run(); err != nil {
				return nil, err
			}
			return execution.Input, nil
		}
		if err := spec.locker.Do(ctx, root, run); err != nil {
			return nil, err
		}
		return execution.Input, nil
	}
}

func invoke(ctx context.Context, tool invokableTool, input json.RawMessage) (json.RawMessage, error) {
	output, err := tool.InvokableRun(ctx, string(input))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(output), nil
}

func decodeRaw(_ context.Context, raw json.RawMessage) (any, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%w: invalid json", agenttools.ErrMalformedInput)
	}
	return cloneRaw(raw), nil
}

func normalizeRaw(_ context.Context, input any) (json.RawMessage, error) {
	raw, ok := input.(json.RawMessage)
	if !ok {
		return nil, fmt.Errorf("%w: expected raw json, got %T", agenttools.ErrMalformedInput, input)
	}
	return cloneRaw(raw), nil
}

func encodeRaw(_ context.Context, value any) (json.RawMessage, error) {
	raw, ok := value.(json.RawMessage)
	if !ok {
		return nil, fmt.Errorf("%w: expected raw json result, got %T", agenttools.ErrMalformedInput, value)
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("eino-tools returned invalid json")
	}
	return cloneRaw(raw), nil
}

func workspaceScope(name string, concurrency runtime.ToolConcurrency) agenttools.ScopeResolver {
	return func(snapshot runtime.TurnSnapshot, _ agenttools.Definition) runtime.ToolScope {
		root, err := canonicalRoot(snapshot)
		if err != nil {
			root = snapshot.Config.Metadata["workspace_root"]
		}
		key := "workspace:" + root
		if concurrency == runtime.ToolConcurrencyParallel {
			key += ":" + name
		}
		return runtime.ToolScope{
			WorkspaceID:    snapshot.Config.Metadata["workspace_id"],
			Root:           root,
			ConcurrencyKey: key,
		}
	}
}

func canonicalRoot(snapshot runtime.TurnSnapshot) (string, error) {
	return workspace.CanonicalRoot(snapshot.Config.Metadata["workspace_root"])
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	cloned := make(json.RawMessage, len(raw))
	copy(cloned, raw)
	return cloned
}
