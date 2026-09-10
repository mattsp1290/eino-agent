package runtime

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mattsp1290/eino-agent/internal/jsonobject"
)

// ErrMalformedToolArguments reports tool-call arguments that could not be
// decoded as a canonical JSON object during argument-alias remapping.
var ErrMalformedToolArguments = errors.New("malformed tool arguments")

// toolAliasIndex is the per-turn name resolution index built from a
// TurnSnapshot's already-materialized tools (snapshot.Tools), which each
// carry their own Aliases/ArgumentAliases (see tools.Definition and
// runtime.Tool). It never consults the RunPlan or a live registry: the
// snapshot is the frozen, resolved tool set for this turn.
type toolAliasIndex struct {
	tools     map[string]Tool
	canonical map[string]string // requested name (canonical or alias) -> canonical name
}

// buildTurnToolAliasIndex indexes tools by canonical name and by every alias
// each tool declares, rejecting an alias that collides with any canonical
// name or with another tool's alias. This mirrors (defensively, at turn
// scope) the collision rejection RunPlan compile already performs across an
// entire plan (see extension_plan.go's buildTurnToolAliasIndex), since
// snapshot.Tools can combine tools from more than one source.
func buildTurnToolAliasIndex(tools []Tool) (toolAliasIndex, error) {
	index := toolAliasIndex{tools: make(map[string]Tool, len(tools)), canonical: make(map[string]string, len(tools))}
	for _, tool := range tools {
		if _, dup := index.tools[tool.Name]; dup {
			return toolAliasIndex{}, fmt.Errorf("duplicate effective tool %q", tool.Name)
		}
		index.tools[tool.Name] = tool
		index.canonical[tool.Name] = tool.Name
	}
	for _, tool := range tools {
		for _, alias := range tool.Aliases {
			if _, isCanonical := index.tools[alias]; isCanonical {
				return toolAliasIndex{}, fmt.Errorf("tool alias %q collides with a tool name", alias)
			}
			if existing, dup := index.canonical[alias]; dup && existing != tool.Name {
				return toolAliasIndex{}, fmt.Errorf("tool alias %q is registered by both %q and %q", alias, existing, tool.Name)
			}
			index.canonical[alias] = tool.Name
		}
	}
	return index, nil
}

// resolveToolCall resolves one model tool call: requestedName may be a
// canonical tool name or a registered alias. It returns the materialized
// Tool, the canonical name, canonical (argument-alias-remapped) arguments,
// and requestedName unchanged for the caller to persist as
// session.ToolCall.RequestedName. An unknown name (canonical or alias)
// fails closed with the same "tool %q unavailable" shape prepareToolCalls
// used before aliasing existed, before any claim is made — an advertised
// alias never grants access to a tool absent from snapshot.Tools.
func resolveToolCall(snapshot TurnSnapshot, requestedName string, args json.RawMessage) (Tool, string, json.RawMessage, string, error) {
	index, err := buildTurnToolAliasIndex(snapshot.Tools)
	if err != nil {
		return Tool{}, "", nil, "", err
	}
	canonicalName, ok := index.canonical[requestedName]
	if !ok {
		return Tool{}, "", nil, "", fmt.Errorf("tool %q unavailable", requestedName)
	}
	tool, ok := index.tools[canonicalName]
	if !ok || tool.Executor == nil {
		return Tool{}, "", nil, "", fmt.Errorf("tool %q unavailable", canonicalName)
	}
	remapped, err := remapToolArguments(args, tool.ArgumentAliases)
	if err != nil {
		return Tool{}, "", nil, "", err
	}
	return tool, canonicalName, remapped, requestedName, nil
}

// remapToolArguments applies argument-alias remapping to a canonical JSON
// object with the exact semantics of upstream Eino's compose.remapArgs
// (compose/tool_node.go): for each canonical key with declared aliases, if
// an alias key is present in args and the canonical key is NOT already
// present, the alias key's value is moved to the canonical key and the
// alias key is removed. If the canonical key is already present, the alias
// key (if also present) is left untouched — an unrecognized field, subject
// to normal schema validation, exactly as upstream leaves it "kept as-is."
func remapToolArguments(args json.RawMessage, argumentAliases map[string][]string) (json.RawMessage, error) {
	if len(argumentAliases) == 0 {
		return args, nil
	}
	object, err := jsonobject.Decode(args)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedToolArguments, err)
	}
	changed := false
	for canonical, aliases := range argumentAliases {
		if _, hasCanonical := object[canonical]; hasCanonical {
			continue
		}
		for _, alias := range aliases {
			if value, ok := object[alias]; ok {
				object[canonical] = value
				delete(object, alias)
				changed = true
				break
			}
		}
	}
	if !changed {
		return args, nil
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("encode remapped tool arguments: %w", err)
	}
	return encoded, nil
}
