package einotools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mattsp1290/eino-tools/catalog"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

const (
	defaultMaxInlineBytes     int64 = 64 * 1024
	metadataSource                  = "eino-tools"
	maxPermissionPatternBytes       = 4096
)

// Options controls host-owned policy and leaf dependencies for one standard
// catalog mount. Permissions are keyed by catalog registration ID.
type Options struct {
	Catalog     catalog.Options
	Scope       extension.Scope
	Retention   *runtime.RetentionPolicy
	Permissions map[string][]string
	Metadata    map[string]string
}

type catalogLoader func(catalog.Options) ([]catalog.Definition, error)

// MountStandard publishes the complete eino-tools standard catalog through
// the canonical composition registry. The caller owns component artifact and
// configuration identity and must keep them stable for durable resume.
func MountStandard(ctx context.Context, registry *composition.Registry, component extension.Component, options Options) (*composition.Mount, error) {
	return mountStandard(ctx, registry, component, options, catalog.Standard)
}

func mountStandard(ctx context.Context, registry *composition.Registry, component extension.Component, options Options, load catalogLoader) (*composition.Mount, error) {
	if registry == nil || load == nil {
		return nil, fmt.Errorf("%w: composition registry required", agenttools.ErrInvalidDefinition)
	}
	definitions, err := load(options.Catalog)
	if err != nil {
		return nil, err
	}
	registrations, err := translateCatalog(ctx, component, options, definitions)
	if err != nil {
		return nil, err
	}
	return registry.Mount(ctx, component, composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
		for _, registration := range registrations {
			if err := registrar.Tool(registration); err != nil {
				return err
			}
		}
		return nil
	}))
}

func translateCatalog(ctx context.Context, component extension.Component, options Options, definitions []catalog.Definition) ([]composition.ToolRegistration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	known := make(map[string]bool, len(definitions))
	for _, definition := range definitions {
		if known[definition.ID] {
			return nil, fmt.Errorf("%w: duplicate catalog ID %q", agenttools.ErrInvalidDefinition, definition.ID)
		}
		known[definition.ID] = true
	}
	for id := range options.Permissions {
		if !known[id] {
			return nil, fmt.Errorf("%w: permissions reference unknown catalog ID %q", agenttools.ErrInvalidDefinition, id)
		}
	}
	metadata, err := standardMetadata(options.Metadata)
	if err != nil {
		return nil, err
	}
	retention := runtime.RetentionPolicy{MaxInlineBytes: defaultMaxInlineBytes, StoreExternal: true}
	if options.Retention != nil {
		retention = *options.Retention
	}
	registrations := make([]composition.ToolRegistration, 0, len(definitions))
	for index, source := range definitions {
		definition, err := translateDefinition(source, retention, options.Permissions[source.ID], metadata)
		if err != nil {
			return nil, fmt.Errorf("translate catalog definition %q: %w", source.ID, err)
		}
		registrations = append(registrations, composition.ToolRegistration{
			ID:    source.ID,
			Order: runtime.OrderApplication + index, Scope: options.Scope,
			SourceSchemaHash: source.SchemaHash, SourceExecutorHash: source.ExecutorHash,
			Definition: definition,
		})
	}
	return registrations, nil
}

func translateDefinition(source catalog.Definition, retention runtime.RetentionPolicy, permissions []string, metadata map[string]string) (agenttools.Definition, error) {
	if source.Info == nil || source.New == nil {
		return agenttools.Definition{}, fmt.Errorf("%w: catalog definition is incomplete", agenttools.ErrInvalidDefinition)
	}
	if _, err := inputPolicyFor(source.ID); err != nil {
		return agenttools.Definition{}, err
	}
	info, err := source.Info()
	if err != nil {
		return agenttools.Definition{}, err
	}
	if info == nil || info.Name != source.Name || info.ParamsOneOf == nil {
		return agenttools.Definition{}, fmt.Errorf("%w: catalog metadata mismatch", agenttools.ErrInvalidDefinition)
	}
	return agenttools.Definition{
		Name: source.Name, Description: info.Desc, Parameters: info.ParamsOneOf,
		Decode: decodeRaw,
		Normalize: func(_ context.Context, input any) (json.RawMessage, error) {
			raw, ok := input.(json.RawMessage)
			if !ok {
				return nil, fmt.Errorf("%w: expected raw JSON, got %T", agenttools.ErrMalformedInput, input)
			}
			return normalizeCatalogInput(source.ID, raw)
		},
		Pattern: func(_ context.Context, input any) (string, error) {
			raw, ok := input.(json.RawMessage)
			if !ok {
				return "", fmt.Errorf("%w: expected raw JSON, got %T", agenttools.ErrMalformedInput, input)
			}
			return permissionPattern(source.ID, raw)
		},
		Encode: func(_ context.Context, value any) (json.RawMessage, error) {
			raw, ok := value.(json.RawMessage)
			if !ok || !json.Valid(raw) {
				return nil, fmt.Errorf("eino-tools returned invalid JSON")
			}
			return cloneRaw(raw), nil
		},
		Execute: executeDefinition(source), RetrySafe: source.RetrySafe,
		Retention: retention, Permissions: append([]string(nil), permissions...), Metadata: cloneStringMap(metadata),
	}, nil
}
