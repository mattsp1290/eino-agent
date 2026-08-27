package session

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// ExtensionPlanSchemaVersion is the only supported durable plan descriptor schema.
const ExtensionPlanSchemaVersion = 1

// ModelRequestID identifies one durable provider-attempt audit record.
type ModelRequestID string

type HandlerKind string

const (
	HandlerNotification HandlerKind = "notification"
	HandlerInterceptor  HandlerKind = "interceptor"
)

type ArtifactIdentity struct {
	Name       string
	Version    string
	Hash       string
	ConfigHash string
	SourceKind string
}

type ExtensionScope struct {
	Kind string
	Key  string
}

type RegistrationIdentity struct {
	ID       string
	Contract string
	Version  string
	Order    int
	Scope    ExtensionScope
	Kind     HandlerKind
}

type HandlerPlanIdentity struct {
	InstanceID    string
	Artifact      ArtifactIdentity
	Registrations []RegistrationIdentity
}

type ToolPlanIdentity struct {
	InstanceID               string
	Artifact                 ArtifactIdentity
	Name, RegistrationID     string
	Scope                    ExtensionScope
	SchemaHash, ExecutorHash string
}

type PromptPlanIdentity struct {
	InstanceID           string
	Artifact             ArtifactIdentity
	Name, RegistrationID string
	Scope                ExtensionScope
	Order                int
}

type GuardPlanIdentity struct {
	InstanceID     string
	Artifact       ArtifactIdentity
	RegistrationID string
	Scope          ExtensionScope
	Order          int
}

type RestrictionPlanIdentity struct {
	InstanceID                string
	Artifact                  ArtifactIdentity
	RegistrationID, RulesHash string
	Scope                     ExtensionScope
}

// ExtensionPlanDescriptor is the complete current durable identity of one
// executable run plan. Each capability kind is statically separated so invalid
// cross-kind payload combinations cannot be represented.
type ExtensionPlanDescriptor struct {
	SchemaVersion int
	Fingerprint   string
	Handlers      []HandlerPlanIdentity
	Tools         []ToolPlanIdentity
	Prompts       []PromptPlanIdentity
	Guards        []GuardPlanIdentity
	Restrictions  []RestrictionPlanIdentity
}

func (d ExtensionPlanDescriptor) Clone() ExtensionPlanDescriptor {
	next := d
	next.Handlers = make([]HandlerPlanIdentity, len(d.Handlers))
	for index, identity := range d.Handlers {
		identity.Registrations = append([]RegistrationIdentity(nil), identity.Registrations...)
		next.Handlers[index] = identity
	}
	next.Tools = append([]ToolPlanIdentity(nil), d.Tools...)
	next.Prompts = append([]PromptPlanIdentity(nil), d.Prompts...)
	next.Guards = append([]GuardPlanIdentity(nil), d.Guards...)
	next.Restrictions = append([]RestrictionPlanIdentity(nil), d.Restrictions...)
	return next
}

// ValidateExtensionPlan validates the complete current descriptor identity.
func ValidateExtensionPlan(descriptor ExtensionPlanDescriptor) error {
	if descriptor.SchemaVersion != ExtensionPlanSchemaVersion {
		return fmt.Errorf("unsupported extension plan schema %d", descriptor.SchemaVersion)
	}
	seen := make(map[string]bool)
	artifacts := make(map[string]ArtifactIdentity)
	handlerInstances := make(map[string]bool)
	var sessionKey string
	checkComponent := func(instanceID string, artifact ArtifactIdentity) error {
		if instanceID == "" || artifact.Name == "" || artifact.Version == "" || artifact.Hash == "" || artifact.ConfigHash == "" {
			return errors.New("invalid extension component identity")
		}
		if artifact.SourceKind != "native" && artifact.SourceKind != "wasm" {
			return errors.New("invalid extension artifact source kind")
		}
		if existing, ok := artifacts[instanceID]; ok && existing != artifact {
			return errors.New("extension instance has conflicting artifact identities")
		}
		artifacts[instanceID] = artifact
		return nil
	}
	checkScope := func(scope ExtensionScope) error {
		switch scope.Kind {
		case "global":
			if scope.Key != "" {
				return errors.New("global extension scope has key")
			}
		case "session":
			if scope.Key == "" {
				return errors.New("session extension scope missing key")
			}
			if sessionKey != "" && sessionKey != scope.Key {
				return errors.New("extension plan contains multiple session keys")
			}
			sessionKey = scope.Key
		default:
			return errors.New("invalid extension scope")
		}
		return nil
	}

	for _, identity := range descriptor.Handlers {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if handlerInstances[identity.InstanceID] {
			return errors.New("duplicate handler plan identity")
		}
		handlerInstances[identity.InstanceID] = true
		if len(identity.Registrations) == 0 {
			return errors.New("handler identity missing registrations")
		}
		for _, registration := range identity.Registrations {
			if registration.ID == "" || registration.Contract == "" || registration.Version == "" || registration.Kind != HandlerNotification && registration.Kind != HandlerInterceptor {
				return errors.New("invalid handler registration identity")
			}
			if err := checkScope(registration.Scope); err != nil {
				return err
			}
			key := identity.InstanceID + "\x00handlers\x00" + registration.ID + "\x00" + registration.Contract + "\x00" + registration.Version + "\x00" + string(registration.Kind) + "\x00" + registration.Scope.Kind + "\x00" + registration.Scope.Key
			if seen[key] {
				return errors.New("duplicate handler registration identity")
			}
			seen[key] = true
		}
	}
	for _, identity := range descriptor.Tools {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if identity.Name == "" || identity.RegistrationID == "" || identity.SchemaHash == "" || identity.ExecutorHash == "" {
			return errors.New("invalid tool plan identity")
		}
		if err := checkScope(identity.Scope); err != nil {
			return err
		}
		if err := addPlanIdentity(seen, identity.InstanceID+"\x00tool\x00"+identity.RegistrationID+"\x00"+identity.Name+"\x00"+identity.Scope.Kind+"\x00"+identity.Scope.Key); err != nil {
			return err
		}
	}
	for _, identity := range descriptor.Prompts {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if identity.Name == "" || identity.RegistrationID == "" {
			return errors.New("invalid prompt plan identity")
		}
		if err := checkScope(identity.Scope); err != nil {
			return err
		}
		if err := addPlanIdentity(seen, identity.InstanceID+"\x00prompt\x00"+identity.RegistrationID+"\x00"+identity.Name+"\x00"+identity.Scope.Kind+"\x00"+identity.Scope.Key); err != nil {
			return err
		}
	}
	for _, identity := range descriptor.Guards {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if identity.RegistrationID == "" {
			return errors.New("invalid guard plan identity")
		}
		if err := checkScope(identity.Scope); err != nil {
			return err
		}
		if err := addPlanIdentity(seen, identity.InstanceID+"\x00guard\x00"+identity.RegistrationID+"\x00"+identity.Scope.Kind+"\x00"+identity.Scope.Key); err != nil {
			return err
		}
	}
	for _, identity := range descriptor.Restrictions {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if identity.RegistrationID == "" || identity.RulesHash == "" {
			return errors.New("invalid restriction plan identity")
		}
		if err := checkScope(identity.Scope); err != nil {
			return err
		}
		if err := addPlanIdentity(seen, identity.InstanceID+"\x00restriction\x00"+identity.RegistrationID+"\x00"+identity.Scope.Kind+"\x00"+identity.Scope.Key); err != nil {
			return err
		}
	}
	return nil
}

func addPlanIdentity(seen map[string]bool, key string) error {
	if seen[key] {
		return errors.New("duplicate extension plan identity")
	}
	seen[key] = true
	return nil
}

// FingerprintExtensionPlan validates and hashes a canonical restart-stable descriptor.
func FingerprintExtensionPlan(descriptor ExtensionPlanDescriptor) (string, error) {
	if err := ValidateExtensionPlan(descriptor); err != nil {
		return "", err
	}
	next := descriptor.Clone()
	next.Fingerprint = ""
	for index := range next.Handlers {
		sort.Slice(next.Handlers[index].Registrations, func(i, j int) bool {
			return compareRegistrationIdentity(next.Handlers[index].Registrations[i], next.Handlers[index].Registrations[j]) < 0
		})
	}
	sort.Slice(next.Handlers, func(i, j int) bool { return compareHandlerPlanIdentity(next.Handlers[i], next.Handlers[j]) < 0 })
	sort.Slice(next.Tools, func(i, j int) bool { return compareToolPlanIdentity(next.Tools[i], next.Tools[j]) < 0 })
	sort.Slice(next.Prompts, func(i, j int) bool { return comparePromptPlanIdentity(next.Prompts[i], next.Prompts[j]) < 0 })
	sort.Slice(next.Guards, func(i, j int) bool { return compareGuardPlanIdentity(next.Guards[i], next.Guards[j]) < 0 })
	sort.Slice(next.Restrictions, func(i, j int) bool {
		return compareRestrictionPlanIdentity(next.Restrictions[i], next.Restrictions[j]) < 0
	})
	normalizeEmptyPlanSlices(&next)
	raw, err := json.Marshal(next)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func normalizeEmptyPlanSlices(descriptor *ExtensionPlanDescriptor) {
	if len(descriptor.Handlers) == 0 {
		descriptor.Handlers = nil
	}
	if len(descriptor.Tools) == 0 {
		descriptor.Tools = nil
	}
	if len(descriptor.Prompts) == 0 {
		descriptor.Prompts = nil
	}
	if len(descriptor.Guards) == 0 {
		descriptor.Guards = nil
	}
	if len(descriptor.Restrictions) == 0 {
		descriptor.Restrictions = nil
	}
}

func compareHandlerPlanIdentity(left, right HandlerPlanIdentity) int {
	if result := compareComponentIdentity(left.InstanceID, left.Artifact, right.InstanceID, right.Artifact); result != 0 {
		return result
	}
	for index := 0; index < min(len(left.Registrations), len(right.Registrations)); index++ {
		if result := compareRegistrationIdentity(left.Registrations[index], right.Registrations[index]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(left.Registrations), len(right.Registrations))
}

func compareToolPlanIdentity(left, right ToolPlanIdentity) int {
	if result := compareComponentIdentity(left.InstanceID, left.Artifact, right.InstanceID, right.Artifact); result != 0 {
		return result
	}
	if result := cmp.Compare(left.RegistrationID, right.RegistrationID); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Name, right.Name); result != 0 {
		return result
	}
	if result := compareScope(left.Scope, right.Scope); result != 0 {
		return result
	}
	if result := cmp.Compare(left.SchemaHash, right.SchemaHash); result != 0 {
		return result
	}
	return cmp.Compare(left.ExecutorHash, right.ExecutorHash)
}

func comparePromptPlanIdentity(left, right PromptPlanIdentity) int {
	if result := compareComponentIdentity(left.InstanceID, left.Artifact, right.InstanceID, right.Artifact); result != 0 {
		return result
	}
	if result := cmp.Compare(left.RegistrationID, right.RegistrationID); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Name, right.Name); result != 0 {
		return result
	}
	if result := compareScope(left.Scope, right.Scope); result != 0 {
		return result
	}
	return cmp.Compare(left.Order, right.Order)
}

func compareGuardPlanIdentity(left, right GuardPlanIdentity) int {
	if result := compareComponentIdentity(left.InstanceID, left.Artifact, right.InstanceID, right.Artifact); result != 0 {
		return result
	}
	if result := cmp.Compare(left.RegistrationID, right.RegistrationID); result != 0 {
		return result
	}
	if result := compareScope(left.Scope, right.Scope); result != 0 {
		return result
	}
	return cmp.Compare(left.Order, right.Order)
}

func compareRestrictionPlanIdentity(left, right RestrictionPlanIdentity) int {
	if result := compareComponentIdentity(left.InstanceID, left.Artifact, right.InstanceID, right.Artifact); result != 0 {
		return result
	}
	if result := cmp.Compare(left.RegistrationID, right.RegistrationID); result != 0 {
		return result
	}
	if result := compareScope(left.Scope, right.Scope); result != 0 {
		return result
	}
	return cmp.Compare(left.RulesHash, right.RulesHash)
}

func compareComponentIdentity(leftID string, left ArtifactIdentity, rightID string, right ArtifactIdentity) int {
	if result := cmp.Compare(leftID, rightID); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Name, right.Name); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Version, right.Version); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Hash, right.Hash); result != 0 {
		return result
	}
	if result := cmp.Compare(left.ConfigHash, right.ConfigHash); result != 0 {
		return result
	}
	return cmp.Compare(left.SourceKind, right.SourceKind)
}

func compareRegistrationIdentity(left, right RegistrationIdentity) int {
	if result := cmp.Compare(left.Order, right.Order); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Contract, right.Contract); result != 0 {
		return result
	}
	if result := cmp.Compare(left.ID, right.ID); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Version, right.Version); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Kind, right.Kind); result != 0 {
		return result
	}
	return compareScope(left.Scope, right.Scope)
}

func compareScope(left, right ExtensionScope) int {
	if result := cmp.Compare(left.Kind, right.Kind); result != 0 {
		return result
	}
	return cmp.Compare(left.Key, right.Key)
}
