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

type RegistrationIdentity struct {
	ID       string
	Contract string
	Version  string
	Order    int
	Scope    extension.Scope
	Kind     extension.HandlerKind
}

type ToolPlanIdentity struct {
	Name, RegistrationID     string
	Scope                    extension.Scope
	SchemaHash, ExecutorHash string
	Order                    int
}

type PromptPlanIdentity struct {
	Name, RegistrationID string
	Scope                extension.Scope
	Order                int
}

type GuardPlanIdentity struct {
	RegistrationID string
	Scope          extension.Scope
	Order          int
}

type RestrictionPlanIdentity struct {
	RegistrationID, RulesHash string
	Scope                     extension.Scope
}

// ComponentPlan is the complete durable behavior identity owned by one
// extension component.
type ComponentPlan struct {
	InstanceID   string
	Artifact     extension.Artifact
	Handlers     []RegistrationIdentity
	Tools        []ToolPlanIdentity
	Prompts      []PromptPlanIdentity
	Guards       []GuardPlanIdentity
	Restrictions []RestrictionPlanIdentity
}

// ExtensionPlanDescriptor is the complete current durable identity of one
// executable run plan. Component ownership is represented once around typed
// capability collections.
type ExtensionPlanDescriptor struct {
	SchemaVersion int
	Fingerprint   string
	Components    []ComponentPlan
}

// SealedExtensionPlan is a validated, canonical extension-plan identity. The
// zero value is invalid; callers obtain values through SealExtensionPlan or
// VerifyExtensionPlanForSession.
type SealedExtensionPlan struct {
	descriptor ExtensionPlanDescriptor
}

// Descriptor returns a defensive copy of the canonical durable descriptor.
func (p SealedExtensionPlan) Descriptor() ExtensionPlanDescriptor {
	return p.descriptor.Clone()
}

// Fingerprint returns the canonical plan identity, or an empty string for the
// invalid zero value.
func (p SealedExtensionPlan) Fingerprint() string {
	return p.descriptor.Fingerprint
}

// Matches reports whether two sealed plans have the same complete identity.
func (p SealedExtensionPlan) Matches(other SealedExtensionPlan) bool {
	return p.Fingerprint() != "" && p.Fingerprint() == other.Fingerprint()
}

func (d ExtensionPlanDescriptor) Clone() ExtensionPlanDescriptor {
	next := d
	next.Components = make([]ComponentPlan, len(d.Components))
	for index, component := range d.Components {
		component.Handlers = append([]RegistrationIdentity(nil), component.Handlers...)
		component.Tools = append([]ToolPlanIdentity(nil), component.Tools...)
		component.Prompts = append([]PromptPlanIdentity(nil), component.Prompts...)
		component.Guards = append([]GuardPlanIdentity(nil), component.Guards...)
		component.Restrictions = append([]RestrictionPlanIdentity(nil), component.Restrictions...)
		next.Components[index] = component
	}
	return next
}

func validateExtensionPlan(descriptor ExtensionPlanDescriptor) error {
	if descriptor.SchemaVersion != ExtensionPlanSchemaVersion {
		return fmt.Errorf("unsupported extension plan schema %d", descriptor.SchemaVersion)
	}
	seenComponents := make(map[string]bool)
	var sessionKey string
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

	for _, component := range descriptor.Components {
		if err := extension.ValidateComponent(extension.Component{InstanceID: component.InstanceID, Artifact: component.Artifact}); err != nil {
			return fmt.Errorf("invalid extension component identity: %w", err)
		}
		if seenComponents[component.InstanceID] {
			return errors.New("duplicate extension component identity")
		}
		seenComponents[component.InstanceID] = true
		if len(component.Handlers)+len(component.Tools)+len(component.Prompts)+len(component.Guards)+len(component.Restrictions) == 0 {
			return errors.New("extension component plan has no behavior")
		}
		seenHandlers := make(map[handlerIdentityKey]bool)
		seenTools := make(map[toolIdentityKey]bool)
		seenPrompts := make(map[promptIdentityKey]bool)
		seenGuards := make(map[guardIdentityKey]bool)
		seenRestrictions := make(map[restrictionIdentityKey]bool)
		for _, registration := range component.Handlers {
			if extension.ValidateIdentifier(registration.ID) != nil || extension.ValidateContract(extension.Contract{ID: registration.Contract, Version: registration.Version}) != nil || !validHandlerKind(registration.Kind) {
				return errors.New("invalid handler registration identity")
			}
			if err := checkScope(registration.Scope); err != nil {
				return err
			}
			key := handlerIdentityKey{RegistrationID: registration.ID, Contract: registration.Contract, Version: registration.Version, Kind: registration.Kind, Scope: registration.Scope}
			if seenHandlers[key] {
				return errors.New("duplicate handler registration identity")
			}
			seenHandlers[key] = true
		}
		for _, identity := range component.Tools {
			if extension.ValidateIdentifier(identity.Name) != nil || extension.ValidateIdentifier(identity.RegistrationID) != nil || identity.SchemaHash == "" || identity.ExecutorHash == "" {
				return errors.New("invalid tool plan identity")
			}
			if err := checkScope(identity.Scope); err != nil {
				return err
			}
			key := toolIdentityKey{RegistrationID: identity.RegistrationID, Name: identity.Name, Scope: identity.Scope}
			if seenTools[key] {
				return errors.New("duplicate extension plan identity")
			}
			seenTools[key] = true
		}
		for _, identity := range component.Prompts {
			if extension.ValidateIdentifier(identity.Name) != nil || extension.ValidateIdentifier(identity.RegistrationID) != nil {
				return errors.New("invalid prompt plan identity")
			}
			if err := checkScope(identity.Scope); err != nil {
				return err
			}
			key := promptIdentityKey{RegistrationID: identity.RegistrationID, Name: identity.Name, Scope: identity.Scope}
			if seenPrompts[key] {
				return errors.New("duplicate extension plan identity")
			}
			seenPrompts[key] = true
		}
		for _, identity := range component.Guards {
			if extension.ValidateIdentifier(identity.RegistrationID) != nil {
				return errors.New("invalid guard plan identity")
			}
			if err := checkScope(identity.Scope); err != nil {
				return err
			}
			key := guardIdentityKey{RegistrationID: identity.RegistrationID, Scope: identity.Scope}
			if seenGuards[key] {
				return errors.New("duplicate extension plan identity")
			}
			seenGuards[key] = true
		}
		for _, identity := range component.Restrictions {
			if extension.ValidateIdentifier(identity.RegistrationID) != nil || strings.TrimSpace(identity.RulesHash) == "" {
				return errors.New("invalid restriction plan identity")
			}
			if err := checkScope(identity.Scope); err != nil {
				return err
			}
			key := restrictionIdentityKey{RegistrationID: identity.RegistrationID, Scope: identity.Scope}
			if seenRestrictions[key] {
				return errors.New("duplicate extension plan identity")
			}
			seenRestrictions[key] = true
		}
	}
	return nil
}

func validHandlerKind(kind extension.HandlerKind) bool {
	switch kind {
	case extension.HandlerNotification, extension.HandlerHook, extension.HandlerTransform, extension.HandlerGate, extension.HandlerAround:
		return true
	default:
		return false
	}
}

type handlerIdentityKey struct {
	RegistrationID, Contract, Version string
	Kind                              extension.HandlerKind
	Scope                             extension.Scope
}

type toolIdentityKey struct {
	RegistrationID, Name string
	Scope                extension.Scope
}

type promptIdentityKey struct {
	RegistrationID, Name string
	Scope                extension.Scope
}

type guardIdentityKey struct {
	RegistrationID string
	Scope          extension.Scope
}

type restrictionIdentityKey struct {
	RegistrationID string
	Scope          extension.Scope
}

// SealExtensionPlan validates and seals one newly reconstructed, fingerprintless
// descriptor. The returned value owns a canonical defensive copy.
func SealExtensionPlan(descriptor ExtensionPlanDescriptor) (SealedExtensionPlan, error) {
	if descriptor.Fingerprint != "" {
		return SealedExtensionPlan{}, errors.New("new extension plan already has a fingerprint")
	}
	next, err := canonicalExtensionPlan(descriptor)
	if err != nil {
		return SealedExtensionPlan{}, err
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return SealedExtensionPlan{}, err
	}
	digest := sha256.Sum256(raw)
	next.Fingerprint = hex.EncodeToString(digest[:])
	return SealedExtensionPlan{descriptor: next}, nil
}

// VerifyExtensionPlanForSession validates a persisted descriptor, verifies its
// fingerprint, and binds every session-scoped identity to sessionID.
func VerifyExtensionPlanForSession(sessionID ID, descriptor ExtensionPlanDescriptor) (SealedExtensionPlan, error) {
	if sessionID == "" || descriptor.Fingerprint == "" {
		return SealedExtensionPlan{}, errors.New("persisted extension plan requires session and fingerprint")
	}
	want := descriptor.Fingerprint
	descriptor.Fingerprint = ""
	sealed, err := SealExtensionPlan(descriptor)
	if err != nil {
		return SealedExtensionPlan{}, err
	}
	if sealed.Fingerprint() != want {
		return SealedExtensionPlan{}, errors.New("extension plan fingerprint mismatch")
	}
	for _, component := range sealed.descriptor.Components {
		if err := validateComponentSessionScopes(sessionID, component); err != nil {
			return SealedExtensionPlan{}, err
		}
	}
	return sealed, nil
}

func canonicalExtensionPlan(descriptor ExtensionPlanDescriptor) (ExtensionPlanDescriptor, error) {
	descriptor.Fingerprint = ""
	if err := validateExtensionPlan(descriptor); err != nil {
		return ExtensionPlanDescriptor{}, err
	}
	next := descriptor.Clone()
	for index := range next.Components {
		component := &next.Components[index]
		sort.Slice(component.Handlers, func(i, j int) bool {
			return compareRegistrationIdentity(component.Handlers[i], component.Handlers[j]) < 0
		})
		sort.Slice(component.Tools, func(i, j int) bool { return compareToolPlanIdentity(component.Tools[i], component.Tools[j]) < 0 })
		sort.Slice(component.Prompts, func(i, j int) bool { return comparePromptPlanIdentity(component.Prompts[i], component.Prompts[j]) < 0 })
		sort.Slice(component.Guards, func(i, j int) bool { return compareGuardPlanIdentity(component.Guards[i], component.Guards[j]) < 0 })
		sort.Slice(component.Restrictions, func(i, j int) bool {
			return compareRestrictionPlanIdentity(component.Restrictions[i], component.Restrictions[j]) < 0
		})
		normalizeEmptyComponentSlices(component)
	}
	sort.Slice(next.Components, func(i, j int) bool { return compareComponentPlan(next.Components[i], next.Components[j]) < 0 })
	normalizeEmptyPlanSlices(&next)
	return next, nil
}

func validateComponentSessionScopes(sessionID ID, component ComponentPlan) error {
	validate := func(scope extension.Scope) error {
		if scope.Kind == extension.ScopeSession && scope.Key != string(sessionID) {
			return errors.New("extension plan session scope mismatch")
		}
		return nil
	}
	for _, identity := range component.Handlers {
		if err := validate(identity.Scope); err != nil {
			return err
		}
	}
	for _, identity := range component.Tools {
		if err := validate(identity.Scope); err != nil {
			return err
		}
	}
	for _, identity := range component.Prompts {
		if err := validate(identity.Scope); err != nil {
			return err
		}
	}
	for _, identity := range component.Guards {
		if err := validate(identity.Scope); err != nil {
			return err
		}
	}
	for _, identity := range component.Restrictions {
		if err := validate(identity.Scope); err != nil {
			return err
		}
	}
	return nil
}

func normalizeEmptyPlanSlices(descriptor *ExtensionPlanDescriptor) {
	if len(descriptor.Components) == 0 {
		descriptor.Components = nil
	}
}

func normalizeEmptyComponentSlices(component *ComponentPlan) {
	if len(component.Handlers) == 0 {
		component.Handlers = nil
	}
	if len(component.Tools) == 0 {
		component.Tools = nil
	}
	if len(component.Prompts) == 0 {
		component.Prompts = nil
	}
	if len(component.Guards) == 0 {
		component.Guards = nil
	}
	if len(component.Restrictions) == 0 {
		component.Restrictions = nil
	}
}

func compareComponentPlan(left, right ComponentPlan) int {
	return compareComponentIdentity(left.InstanceID, left.Artifact, right.InstanceID, right.Artifact)
}

func compareToolPlanIdentity(left, right ToolPlanIdentity) int {
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
	if result := cmp.Compare(left.ExecutorHash, right.ExecutorHash); result != 0 {
		return result
	}
	return cmp.Compare(left.Order, right.Order)
}

func comparePromptPlanIdentity(left, right PromptPlanIdentity) int {
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
	if result := cmp.Compare(left.RegistrationID, right.RegistrationID); result != 0 {
		return result
	}
	if result := compareScope(left.Scope, right.Scope); result != 0 {
		return result
	}
	return cmp.Compare(left.Order, right.Order)
}

func compareRestrictionPlanIdentity(left, right RestrictionPlanIdentity) int {
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
