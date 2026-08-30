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
	Fingerprint string
	Components  []ComponentPlan
}

// SealedExtensionPlan is a validated, canonical extension-plan identity. The
// zero value is invalid; callers obtain values through
// SealExtensionPlanForSession or VerifyExtensionPlanForSession.
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

func validateExtensionPlan(sessionID ID, descriptor ExtensionPlanDescriptor) error {
	seenComponents := make(map[string]bool)
	for _, component := range descriptor.Components {
		if err := validateComponentPlan(sessionID, component, seenComponents); err != nil {
			return err
		}
	}
	return nil
}

func validateComponentPlan(sessionID ID, component ComponentPlan, seen map[string]bool) error {
	if err := extension.ValidateComponent(extension.Component{InstanceID: component.InstanceID, Artifact: component.Artifact}); err != nil {
		return fmt.Errorf("invalid extension component identity: %w", err)
	}
	if seen[component.InstanceID] {
		return errors.New("duplicate extension component identity")
	}
	seen[component.InstanceID] = true
	if len(component.Handlers)+len(component.Tools)+len(component.Prompts)+len(component.Guards)+len(component.Restrictions) == 0 {
		return errors.New("extension component plan has no behavior")
	}
	if err := validateHandlerIdentities(sessionID, component.Handlers); err != nil {
		return err
	}
	if err := validateToolIdentities(sessionID, component.Tools); err != nil {
		return err
	}
	if err := validatePromptIdentities(sessionID, component.Prompts); err != nil {
		return err
	}
	if err := validateGuardIdentities(sessionID, component.Guards); err != nil {
		return err
	}
	return validateRestrictionIdentities(sessionID, component.Restrictions)
}

func validatePlanScope(sessionID ID, scope extension.Scope) error {
	if err := extension.ValidateScope(scope); err != nil {
		return err
	}
	if scope.Kind == extension.ScopeSession && (sessionID == "" || scope.Key != string(sessionID)) {
		return errors.New("extension plan session scope mismatch")
	}
	return nil
}

func validateHandlerIdentities(sessionID ID, values []RegistrationIdentity) error {
	seen := make(map[handlerIdentityKey]bool, len(values))
	for _, value := range values {
		if extension.ValidateIdentifier(value.ID) != nil || extension.ValidateContract(extension.Contract{ID: value.Contract, Version: value.Version}) != nil || !validHandlerKind(value.Kind) {
			return errors.New("invalid handler registration identity")
		}
		key := handlerIdentityKey{RegistrationID: value.ID, Contract: value.Contract, Version: value.Version, Kind: value.Kind, Scope: value.Scope}
		if err := validateUniqueScope(sessionID, value.Scope, seen, key); err != nil {
			return err
		}
	}
	return nil
}

func validateToolIdentities(sessionID ID, values []ToolPlanIdentity) error {
	seen := make(map[toolIdentityKey]bool, len(values))
	for _, value := range values {
		if extension.ValidateIdentifier(value.Name) != nil || extension.ValidateIdentifier(value.RegistrationID) != nil || value.SchemaHash == "" || value.ExecutorHash == "" {
			return errors.New("invalid tool plan identity")
		}
		key := toolIdentityKey{RegistrationID: value.RegistrationID, Name: value.Name, Scope: value.Scope}
		if err := validateUniqueScope(sessionID, value.Scope, seen, key); err != nil {
			return err
		}
	}
	return nil
}

func validatePromptIdentities(sessionID ID, values []PromptPlanIdentity) error {
	seen := make(map[promptIdentityKey]bool, len(values))
	for _, value := range values {
		if extension.ValidateIdentifier(value.Name) != nil || extension.ValidateIdentifier(value.RegistrationID) != nil {
			return errors.New("invalid prompt plan identity")
		}
		key := promptIdentityKey{RegistrationID: value.RegistrationID, Name: value.Name, Scope: value.Scope}
		if err := validateUniqueScope(sessionID, value.Scope, seen, key); err != nil {
			return err
		}
	}
	return nil
}

func validateGuardIdentities(sessionID ID, values []GuardPlanIdentity) error {
	seen := make(map[guardIdentityKey]bool, len(values))
	for _, value := range values {
		if extension.ValidateIdentifier(value.RegistrationID) != nil {
			return errors.New("invalid guard plan identity")
		}
		key := guardIdentityKey{RegistrationID: value.RegistrationID, Scope: value.Scope}
		if err := validateUniqueScope(sessionID, value.Scope, seen, key); err != nil {
			return err
		}
	}
	return nil
}

func validateRestrictionIdentities(sessionID ID, values []RestrictionPlanIdentity) error {
	seen := make(map[restrictionIdentityKey]bool, len(values))
	for _, value := range values {
		if extension.ValidateIdentifier(value.RegistrationID) != nil || strings.TrimSpace(value.RulesHash) == "" {
			return errors.New("invalid restriction plan identity")
		}
		key := restrictionIdentityKey{RegistrationID: value.RegistrationID, Scope: value.Scope}
		if err := validateUniqueScope(sessionID, value.Scope, seen, key); err != nil {
			return err
		}
	}
	return nil
}

func validateUniqueScope[K comparable](sessionID ID, scope extension.Scope, seen map[K]bool, key K) error {
	if err := validatePlanScope(sessionID, scope); err != nil {
		return err
	}
	if seen[key] {
		return errors.New("duplicate extension plan identity")
	}
	seen[key] = true
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

// SealExtensionPlanForSession validates and seals one newly reconstructed,
// fingerprintless descriptor for sessionID. The returned value owns a canonical
// defensive copy. An empty sessionID permits global scopes only.
func SealExtensionPlanForSession(sessionID ID, descriptor ExtensionPlanDescriptor) (SealedExtensionPlan, error) {
	if descriptor.Fingerprint != "" {
		return SealedExtensionPlan{}, errors.New("new extension plan already has a fingerprint")
	}
	return sealExtensionPlan(sessionID, descriptor)
}

func sealExtensionPlan(sessionID ID, descriptor ExtensionPlanDescriptor) (SealedExtensionPlan, error) {
	next, err := canonicalExtensionPlan(sessionID, descriptor)
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
	sealed, err := sealExtensionPlan(sessionID, descriptor)
	if err != nil {
		return SealedExtensionPlan{}, err
	}
	if sealed.Fingerprint() != want {
		return SealedExtensionPlan{}, errors.New("extension plan fingerprint mismatch")
	}
	return sealed, nil
}

func canonicalExtensionPlan(sessionID ID, descriptor ExtensionPlanDescriptor) (ExtensionPlanDescriptor, error) {
	descriptor.Fingerprint = ""
	if err := validateExtensionPlan(sessionID, descriptor); err != nil {
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
