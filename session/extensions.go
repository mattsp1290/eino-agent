package session

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/mattsp1290/eino-agent/extension"
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

type RegistrationIdentity struct {
	ID       string
	Contract string
	Version  string
	Order    int
	Scope    extension.Scope
	Kind     HandlerKind
}

type HandlerPlanIdentity struct {
	InstanceID    string
	Artifact      extension.Artifact
	Registrations []RegistrationIdentity
}

type ToolPlanIdentity struct {
	InstanceID               string
	Artifact                 extension.Artifact
	Name, RegistrationID     string
	Scope                    extension.Scope
	SchemaHash, ExecutorHash string
}

type PromptPlanIdentity struct {
	InstanceID           string
	Artifact             extension.Artifact
	Name, RegistrationID string
	Scope                extension.Scope
	Order                int
}

type GuardPlanIdentity struct {
	InstanceID     string
	Artifact       extension.Artifact
	RegistrationID string
	Scope          extension.Scope
	Order          int
}

type RestrictionPlanIdentity struct {
	InstanceID                string
	Artifact                  extension.Artifact
	RegistrationID, RulesHash string
	Scope                     extension.Scope
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
	seenHandlers := make(map[handlerIdentityKey]bool)
	seenTools := make(map[toolIdentityKey]bool)
	seenPrompts := make(map[promptIdentityKey]bool)
	seenGuards := make(map[guardIdentityKey]bool)
	seenRestrictions := make(map[restrictionIdentityKey]bool)
	artifacts := make(map[string]extension.Artifact)
	handlerInstances := make(map[string]bool)
	var sessionKey string
	checkComponent := func(instanceID string, artifact extension.Artifact) error {
		if err := extension.ValidateComponent(extension.Component{InstanceID: instanceID, Artifact: artifact}); err != nil {
			return fmt.Errorf("invalid extension component identity: %w", err)
		}
		if existing, ok := artifacts[instanceID]; ok && existing != artifact {
			return errors.New("extension instance has conflicting artifact identities")
		}
		artifacts[instanceID] = artifact
		return nil
	}
	checkScope := func(scope extension.Scope) error {
		if err := extension.ValidateScope(scope); err != nil {
			return err
		}
		switch scope.Kind {
		case extension.ScopeGlobal:
		case extension.ScopeSession:
			if sessionKey != "" && sessionKey != scope.Key {
				return errors.New("extension plan contains multiple session keys")
			}
			sessionKey = scope.Key
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
			if extension.ValidateIdentifier(registration.ID) != nil || extension.ValidateContract(extension.Contract{ID: registration.Contract, Version: registration.Version}) != nil || registration.Kind != HandlerNotification && registration.Kind != HandlerInterceptor {
				return errors.New("invalid handler registration identity")
			}
			if err := checkScope(registration.Scope); err != nil {
				return err
			}
			key := handlerIdentityKey{InstanceID: identity.InstanceID, RegistrationID: registration.ID, Contract: registration.Contract, Version: registration.Version, Kind: registration.Kind, Scope: registration.Scope}
			if seenHandlers[key] {
				return errors.New("duplicate handler registration identity")
			}
			seenHandlers[key] = true
		}
	}
	for _, identity := range descriptor.Tools {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if extension.ValidateIdentifier(identity.Name) != nil || extension.ValidateIdentifier(identity.RegistrationID) != nil || identity.SchemaHash == "" || identity.ExecutorHash == "" {
			return errors.New("invalid tool plan identity")
		}
		if err := checkScope(identity.Scope); err != nil {
			return err
		}
		key := toolIdentityKey{InstanceID: identity.InstanceID, RegistrationID: identity.RegistrationID, Name: identity.Name, Scope: identity.Scope}
		if seenTools[key] {
			return errors.New("duplicate extension plan identity")
		}
		seenTools[key] = true
	}
	for _, identity := range descriptor.Prompts {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if extension.ValidateIdentifier(identity.Name) != nil || extension.ValidateIdentifier(identity.RegistrationID) != nil {
			return errors.New("invalid prompt plan identity")
		}
		if err := checkScope(identity.Scope); err != nil {
			return err
		}
		key := promptIdentityKey{InstanceID: identity.InstanceID, RegistrationID: identity.RegistrationID, Name: identity.Name, Scope: identity.Scope}
		if seenPrompts[key] {
			return errors.New("duplicate extension plan identity")
		}
		seenPrompts[key] = true
	}
	for _, identity := range descriptor.Guards {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if extension.ValidateIdentifier(identity.RegistrationID) != nil {
			return errors.New("invalid guard plan identity")
		}
		if err := checkScope(identity.Scope); err != nil {
			return err
		}
		key := guardIdentityKey{InstanceID: identity.InstanceID, RegistrationID: identity.RegistrationID, Scope: identity.Scope}
		if seenGuards[key] {
			return errors.New("duplicate extension plan identity")
		}
		seenGuards[key] = true
	}
	for _, identity := range descriptor.Restrictions {
		if err := checkComponent(identity.InstanceID, identity.Artifact); err != nil {
			return err
		}
		if extension.ValidateIdentifier(identity.RegistrationID) != nil || strings.TrimSpace(identity.RulesHash) == "" {
			return errors.New("invalid restriction plan identity")
		}
		if err := checkScope(identity.Scope); err != nil {
			return err
		}
		key := restrictionIdentityKey{InstanceID: identity.InstanceID, RegistrationID: identity.RegistrationID, Scope: identity.Scope}
		if seenRestrictions[key] {
			return errors.New("duplicate extension plan identity")
		}
		seenRestrictions[key] = true
	}
	return nil
}

type handlerIdentityKey struct {
	InstanceID, RegistrationID, Contract, Version string
	Kind                                          HandlerKind
	Scope                                         extension.Scope
}

type toolIdentityKey struct {
	InstanceID, RegistrationID, Name string
	Scope                            extension.Scope
}

type promptIdentityKey struct {
	InstanceID, RegistrationID, Name string
	Scope                            extension.Scope
}

type guardIdentityKey struct {
	InstanceID, RegistrationID string
	Scope                      extension.Scope
}

type restrictionIdentityKey struct {
	InstanceID, RegistrationID string
	Scope                      extension.Scope
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

func compareComponentIdentity(leftID string, left extension.Artifact, rightID string, right extension.Artifact) int {
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

func compareScope(left, right extension.Scope) int {
	if result := cmp.Compare(left.Kind, right.Kind); result != 0 {
		return result
	}
	return cmp.Compare(left.Key, right.Key)
}
