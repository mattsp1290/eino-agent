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

func composedToolSchemaHash(registration ToolRegistration) (string, error) {
	definitionHash, err := toolSchemaHash(registration.Definition)
	if err != nil {
		return "", err
	}
	return hashToolIdentity(struct {
		Version        string `json:"version"`
		SourceHash     string `json:"source_hash"`
		DefinitionHash string `json:"definition_hash"`
		Order          int    `json:"order"`
	}{
		Version: toolSchemaIdentityVersion, SourceHash: registration.SourceSchemaHash,
		DefinitionHash: definitionHash, Order: registration.Order,
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

func validateToolSourceIdentity(schemaHash, executorHash string) error {
	if schemaHash == "" && executorHash == "" {
		return nil
	}
	if schemaHash == "" || executorHash == "" {
		return fmt.Errorf("%w: tool source schema and executor hashes must be supplied together", extension.ErrInvalidRegistration)
	}
	if !validSHA256Hex(schemaHash) || !validSHA256Hex(executorHash) {
		return fmt.Errorf("%w: invalid tool source identity", extension.ErrInvalidRegistration)
	}
	return nil
}

func validSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
