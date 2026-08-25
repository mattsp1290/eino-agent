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

type ExtensionKind string

const (
	ExtensionHandlers    ExtensionKind = "handlers"
	ExtensionTool        ExtensionKind = "tool"
	ExtensionPrompt      ExtensionKind = "prompt"
	ExtensionGuard       ExtensionKind = "guard"
	ExtensionRestriction ExtensionKind = "restriction"
)

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

type HandlerPlanIdentity struct{ Registrations []RegistrationIdentity }
type ToolPlanIdentity struct {
	Name, RegistrationID, SchemaHash, ExecutorHash string
	Scope                                          ExtensionScope
}
type PromptPlanIdentity struct {
	Name, RegistrationID string
	Scope                ExtensionScope
	Order                int
}
type GuardPlanIdentity struct {
	RegistrationID string
	Scope          ExtensionScope
	Order          int
}
type RestrictionPlanIdentity struct {
	RegistrationID, RulesHash string
	Scope                     ExtensionScope
}

// ExtensionPlanEntry is a tagged durable identity. Exactly one kind payload is present.
type ExtensionPlanEntry struct {
	InstanceID  string
	Artifact    ArtifactIdentity
	Handlers    *HandlerPlanIdentity     `json:",omitempty"`
	Tool        *ToolPlanIdentity        `json:",omitempty"`
	Prompt      *PromptPlanIdentity      `json:",omitempty"`
	Guard       *GuardPlanIdentity       `json:",omitempty"`
	Restriction *RestrictionPlanIdentity `json:",omitempty"`
}

func (e ExtensionPlanEntry) Clone() ExtensionPlanEntry {
	next := e
	if e.Handlers != nil {
		next.Handlers = &HandlerPlanIdentity{Registrations: append([]RegistrationIdentity(nil), e.Handlers.Registrations...)}
	}
	if e.Tool != nil {
		value := *e.Tool
		next.Tool = &value
	}
	if e.Prompt != nil {
		value := *e.Prompt
		next.Prompt = &value
	}
	if e.Guard != nil {
		value := *e.Guard
		next.Guard = &value
	}
	if e.Restriction != nil {
		value := *e.Restriction
		next.Restriction = &value
	}
	return next
}

func (e ExtensionPlanEntry) Kind() (ExtensionKind, error) {
	var kind ExtensionKind
	count := 0
	set := func(candidate ExtensionKind, present bool) {
		if present {
			kind = candidate
			count++
		}
	}
	set(ExtensionHandlers, e.Handlers != nil)
	set(ExtensionTool, e.Tool != nil)
	set(ExtensionPrompt, e.Prompt != nil)
	set(ExtensionGuard, e.Guard != nil)
	set(ExtensionRestriction, e.Restriction != nil)
	if count != 1 {
		return "", errors.New("extension plan entry must contain exactly one kind payload")
	}
	return kind, nil
}

type ExtensionPlanDescriptor struct {
	SchemaVersion int
	Fingerprint   string
	Entries       []ExtensionPlanEntry
}

func (d ExtensionPlanDescriptor) Clone() ExtensionPlanDescriptor {
	next := d
	next.Entries = make([]ExtensionPlanEntry, len(d.Entries))
	for index, entry := range d.Entries {
		next.Entries[index] = entry.Clone()
	}
	return next
}

// ValidateExtensionPlan validates the complete current descriptor identity.
func ValidateExtensionPlan(descriptor ExtensionPlanDescriptor) error {
	if descriptor.SchemaVersion != ExtensionPlanSchemaVersion {
		return fmt.Errorf("unsupported extension plan schema %d", descriptor.SchemaVersion)
	}
	seen := make(map[string]bool)
	var sessionKey string
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
	for _, entry := range descriptor.Entries {
		if entry.InstanceID == "" || entry.Artifact.Name == "" || entry.Artifact.Version == "" || entry.Artifact.Hash == "" || entry.Artifact.ConfigHash == "" {
			return errors.New("invalid extension component identity")
		}
		if entry.Artifact.SourceKind != "native" && entry.Artifact.SourceKind != "wasm" {
			return errors.New("invalid extension artifact source kind")
		}
		kind, err := entry.Kind()
		if err != nil {
			return err
		}
		base := entry.InstanceID + "\x00" + string(kind) + "\x00"
		switch kind {
		case ExtensionHandlers:
			if len(entry.Handlers.Registrations) == 0 {
				return errors.New("handler identity missing registrations")
			}
			for _, registration := range entry.Handlers.Registrations {
				if registration.ID == "" || registration.Contract == "" || registration.Version == "" || registration.Kind != HandlerNotification && registration.Kind != HandlerInterceptor {
					return errors.New("invalid handler registration identity")
				}
				if err := checkScope(registration.Scope); err != nil {
					return err
				}
				key := base + registration.ID + "\x00" + registration.Contract + "\x00" + registration.Version + "\x00" + string(registration.Kind) + "\x00" + registration.Scope.Kind + "\x00" + registration.Scope.Key
				if seen[key] {
					return errors.New("duplicate handler registration identity")
				}
				seen[key] = true
			}
		case ExtensionTool:
			value := entry.Tool
			if value.Name == "" || value.RegistrationID == "" || value.SchemaHash == "" || value.ExecutorHash == "" {
				return errors.New("invalid tool plan identity")
			}
			if err := checkScope(value.Scope); err != nil {
				return err
			}
			if err := addPlanIdentity(seen, base+value.RegistrationID+"\x00"+value.Name+"\x00"+value.Scope.Kind+"\x00"+value.Scope.Key); err != nil {
				return err
			}
		case ExtensionPrompt:
			value := entry.Prompt
			if value.Name == "" || value.RegistrationID == "" {
				return errors.New("invalid prompt plan identity")
			}
			if err := checkScope(value.Scope); err != nil {
				return err
			}
			if err := addPlanIdentity(seen, base+value.RegistrationID+"\x00"+value.Name+"\x00"+value.Scope.Kind+"\x00"+value.Scope.Key); err != nil {
				return err
			}
		case ExtensionGuard:
			value := entry.Guard
			if value.RegistrationID == "" {
				return errors.New("invalid guard plan identity")
			}
			if err := checkScope(value.Scope); err != nil {
				return err
			}
			if err := addPlanIdentity(seen, base+value.RegistrationID+"\x00"+value.Scope.Kind+"\x00"+value.Scope.Key); err != nil {
				return err
			}
		case ExtensionRestriction:
			value := entry.Restriction
			if value.RegistrationID == "" || value.RulesHash == "" {
				return errors.New("invalid restriction plan identity")
			}
			if err := checkScope(value.Scope); err != nil {
				return err
			}
			if err := addPlanIdentity(seen, base+value.RegistrationID+"\x00"+value.Scope.Kind+"\x00"+value.Scope.Key); err != nil {
				return err
			}
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
	for index := range next.Entries {
		if handlers := next.Entries[index].Handlers; handlers != nil {
			sort.Slice(handlers.Registrations, func(i, j int) bool {
				return compareRegistrationIdentity(handlers.Registrations[i], handlers.Registrations[j]) < 0
			})
		}
	}
	sort.Slice(next.Entries, func(i, j int) bool { return compareExtensionPlanEntry(next.Entries[i], next.Entries[j]) < 0 })
	raw, err := json.Marshal(next)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func compareExtensionPlanEntry(left, right ExtensionPlanEntry) int {
	leftKind, _ := left.Kind()
	rightKind, _ := right.Kind()
	for _, result := range []int{cmp.Compare(left.InstanceID, right.InstanceID), cmp.Compare(leftKind, rightKind), cmp.Compare(left.Artifact.Name, right.Artifact.Name), cmp.Compare(left.Artifact.Version, right.Artifact.Version), cmp.Compare(left.Artifact.Hash, right.Artifact.Hash), cmp.Compare(left.Artifact.ConfigHash, right.Artifact.ConfigHash), cmp.Compare(left.Artifact.SourceKind, right.Artifact.SourceKind)} {
		if result != 0 {
			return result
		}
	}
	leftRaw, _ := json.Marshal(left)
	rightRaw, _ := json.Marshal(right)
	return cmp.Compare(string(leftRaw), string(rightRaw))
}

func compareRegistrationIdentity(left, right RegistrationIdentity) int {
	for _, result := range []int{cmp.Compare(left.Order, right.Order), cmp.Compare(left.Contract, right.Contract), cmp.Compare(left.ID, right.ID), cmp.Compare(left.Version, right.Version), cmp.Compare(left.Kind, right.Kind), cmp.Compare(left.Scope.Kind, right.Scope.Kind), cmp.Compare(left.Scope.Key, right.Scope.Key)} {
		if result != 0 {
			return result
		}
	}
	return 0
}
