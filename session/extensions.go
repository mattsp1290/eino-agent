package session

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// ExtensionPlanSchemaVersion is the current durable plan descriptor schema.
// Version 1 records prompt and guard order explicitly.
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
}

type ExtensionPlanEntry struct {
	InstanceID    string
	Kind          ExtensionKind
	Artifact      ArtifactIdentity
	Required      bool
	Scope         ExtensionScope
	Registrations []RegistrationIdentity
	CapabilityID  string
	Order         int `json:",omitempty"`
	SchemaHash    string
	ExecutorHash  string
}

func (e ExtensionPlanEntry) Clone() ExtensionPlanEntry {
	next := e
	next.Registrations = append([]RegistrationIdentity(nil), e.Registrations...)
	return next
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

// FingerprintExtensionPlan returns a canonical restart-stable descriptor hash.
// The Fingerprint field itself is excluded from the digest.
func FingerprintExtensionPlan(descriptor ExtensionPlanDescriptor) (string, error) {
	next := descriptor.Clone()
	next.Fingerprint = ""
	for index := range next.Entries {
		sort.Slice(next.Entries[index].Registrations, func(i, j int) bool {
			return compareRegistrationIdentity(next.Entries[index].Registrations[i], next.Entries[index].Registrations[j]) < 0
		})
	}
	sort.Slice(next.Entries, func(i, j int) bool {
		return compareExtensionPlanEntry(next.Entries[i], next.Entries[j]) < 0
	})
	raw, err := json.Marshal(next)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func compareExtensionPlanEntry(left, right ExtensionPlanEntry) int {
	for _, result := range []int{
		cmp.Compare(left.InstanceID, right.InstanceID),
		cmp.Compare(left.Kind, right.Kind),
		cmp.Compare(left.CapabilityID, right.CapabilityID),
		compareExtensionScope(left.Scope, right.Scope),
		cmp.Compare(left.Order, right.Order),
		cmp.Compare(left.Artifact.Name, right.Artifact.Name),
		cmp.Compare(left.Artifact.Version, right.Artifact.Version),
		cmp.Compare(left.Artifact.Hash, right.Artifact.Hash),
		cmp.Compare(left.Artifact.ConfigHash, right.Artifact.ConfigHash),
		cmp.Compare(left.Artifact.SourceKind, right.Artifact.SourceKind),
		compareBool(left.Required, right.Required),
		cmp.Compare(left.SchemaHash, right.SchemaHash),
		cmp.Compare(left.ExecutorHash, right.ExecutorHash),
		compareRegistrationIdentities(left.Registrations, right.Registrations),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRegistrationIdentities(left, right []RegistrationIdentity) int {
	for index := 0; index < min(len(left), len(right)); index++ {
		if result := compareRegistrationIdentity(left[index], right[index]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(left), len(right))
}

func compareRegistrationIdentity(left, right RegistrationIdentity) int {
	for _, result := range []int{
		cmp.Compare(left.Order, right.Order),
		cmp.Compare(left.Contract, right.Contract),
		cmp.Compare(left.ID, right.ID),
		cmp.Compare(left.Version, right.Version),
		compareExtensionScope(left.Scope, right.Scope),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareExtensionScope(left, right ExtensionScope) int {
	if result := cmp.Compare(left.Kind, right.Kind); result != 0 {
		return result
	}
	return cmp.Compare(left.Key, right.Key)
}

func compareBool(left, right bool) int {
	if left == right {
		return 0
	}
	if !left {
		return -1
	}
	return 1
}
