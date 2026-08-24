package agui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

// MountClientTools publishes one immutable, session-scoped AG-UI client tool
// generation through the canonical composition registry.
func MountClientTools(ctx context.Context, registry *composition.Registry, snapshot agentagui.ClientToolSnapshot, dispatcher agentagui.ClientToolDispatcher) (*composition.Mount, error) {
	if registry == nil || snapshot.SessionID == "" || snapshot.Generation == 0 || snapshot.DispatcherArtifactID == "" {
		return nil, agenttools.ErrInvalidDefinition
	}
	definitions, err := snapshot.Definitions(dispatcher)
	if err != nil {
		return nil, err
	}
	if len(definitions) == 0 {
		return nil, agenttools.ErrInvalidDefinition
	}
	seen := make(map[string]bool, len(definitions))
	for _, definition := range definitions {
		if definition.Name == "" || seen[definition.Name] {
			return nil, fmt.Errorf("%w: duplicate or empty client tool name %q", agenttools.ErrInvalidDefinition, definition.Name)
		}
		seen[definition.Name] = true
	}
	component, err := clientComponent(snapshot)
	if err != nil {
		return nil, err
	}
	return registry.Mount(ctx, component, composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
		for _, definition := range definitions {
			if err := registrar.Tool(composition.ToolRegistration{
				ID: definition.Name, InstanceID: component.InstanceID,
				Scope: extension.SessionScope(string(snapshot.SessionID)), Definition: definition,
			}); err != nil {
				return err
			}
		}
		return nil
	}))
}

func clientComponent(snapshot agentagui.ClientToolSnapshot) (extension.Component, error) {
	frozen, err := snapshot.Clone()
	if err != nil {
		return extension.Component{}, err
	}
	payload := struct {
		SessionID            session.ID        `json:"session_id"`
		Generation           uint64            `json:"generation"`
		DispatcherArtifactID string            `json:"dispatcher_artifact_id"`
		Tools                []aguitypes.Tool  `json:"tools"`
		Permissions          []string          `json:"permissions,omitempty"`
		Metadata             map[string]string `json:"metadata,omitempty"`
	}{frozen.SessionID, frozen.Generation, frozen.DispatcherArtifactID, frozen.Tools, frozen.Permissions, frozen.Metadata}
	raw, err := json.Marshal(payload)
	if err != nil {
		return extension.Component{}, err
	}
	artifactSum := sha256.Sum256(raw)
	configRaw, err := json.Marshal(struct {
		SessionID  session.ID `json:"session_id"`
		Generation uint64     `json:"generation"`
	}{frozen.SessionID, frozen.Generation})
	if err != nil {
		return extension.Component{}, err
	}
	configSum := sha256.Sum256(configRaw)
	sessionSum := sha256.Sum256([]byte(frozen.SessionID))
	return extension.Component{
		InstanceID: "agui-client-" + hex.EncodeToString(sessionSum[:8]) + "-" + strconv.FormatUint(frozen.Generation, 10),
		Artifact: extension.Artifact{
			Name: "agui-client-tools", Version: "1", Hash: hex.EncodeToString(artifactSum[:]),
			ConfigHash: hex.EncodeToString(configSum[:]), SourceKind: extension.SourceNative,
		},
	}, nil
}

// ClientNames returns the current client tool name set for call classification.
func ClientNames(tools []aguitypes.Tool, blockedNames ...map[string]bool) map[string]bool {
	result := make(map[string]bool, len(tools))
	blocked := map[string]bool{}
	if len(blockedNames) > 0 {
		blocked = blockedNames[0]
	}
	for _, tool := range tools {
		if tool.Name != "" && !blocked[tool.Name] {
			result[tool.Name] = true
		}
	}
	return result
}
