package composition

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mattsp1290/eino-agent/extension"
)

const (
	toolSchemaIdentityVersion   = "eino-agent-tool-schema-v2"
	toolExecutorIdentityVersion = "eino-agent-tool-executor-v2"
)

// ToolSourceIdentity is the immutable upstream schema and executor identity
// for an adapter-supplied tool. Its zero value means no upstream identity.
type ToolSourceIdentity struct {
	schemaHash   string
	executorHash string
}

// NewToolSourceIdentity validates and binds the two halves of an upstream
// tool identity.
func NewToolSourceIdentity(schemaHash, executorHash string) (ToolSourceIdentity, error) {
	if !validSHA256Hex(schemaHash) || !validSHA256Hex(executorHash) {
		return ToolSourceIdentity{}, fmt.Errorf("%w: invalid tool source identity", extension.ErrInvalidRegistration)
	}
	return ToolSourceIdentity{schemaHash: schemaHash, executorHash: executorHash}, nil
}

func (identity ToolSourceIdentity) validate() error {
	if identity == (ToolSourceIdentity{}) {
		return nil
	}
	if !validSHA256Hex(identity.schemaHash) || !validSHA256Hex(identity.executorHash) {
		return fmt.Errorf("%w: invalid tool source identity", extension.ErrInvalidRegistration)
	}
	return nil
}

func composedToolSchemaHash(registration ToolRegistration) (string, error) {
	definitionHash, err := toolSchemaHash(registration.Definition)
	if err != nil {
		return "", err
	}
	return hashToolIdentity(struct {
		Version        string `json:"version"`
		SourceHash     string `json:"source_hash"`
		DefinitionHash string `json:"definition_hash"`
	}{
		Version: toolSchemaIdentityVersion, SourceHash: registration.SourceIdentity.schemaHash,
		DefinitionHash: definitionHash,
	})
}

func composedToolExecutorHash(sourceHash, artifactHash string) (string, error) {
	return hashToolIdentity(struct {
		Version      string `json:"version"`
		SourceHash   string `json:"source_hash"`
		ArtifactHash string `json:"artifact_hash"`
	}{Version: toolExecutorIdentityVersion, SourceHash: sourceHash, ArtifactHash: artifactHash})
}

func hashToolIdentity(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func validSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
