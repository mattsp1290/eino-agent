package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// ModelRequestID identifies one durable provider-attempt audit record.
type ModelRequestID string

type PlanMode string

const (
	PlanStrict        PlanMode = "strict"
	PlanPartialLegacy PlanMode = "partial-legacy"
	PlanLegacy        PlanMode = "legacy"
)

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
	SchemaHash    string
	ExecutorHash  string
}

type ExtensionPlanDescriptor struct {
	SchemaVersion int
	Mode          PlanMode
	Fingerprint   string
	Entries       []ExtensionPlanEntry
}

func (d ExtensionPlanDescriptor) Clone() ExtensionPlanDescriptor {
	next := d
	next.Entries = make([]ExtensionPlanEntry, len(d.Entries))
	for index, entry := range d.Entries {
		next.Entries[index] = entry
		next.Entries[index].Registrations = append([]RegistrationIdentity(nil), entry.Registrations...)
	}
	return next
}

// FingerprintExtensionPlan returns a canonical restart-stable descriptor hash.
// The Fingerprint field itself is excluded from the digest.
func FingerprintExtensionPlan(descriptor ExtensionPlanDescriptor) (string, error) {
	next := descriptor.Clone()
	next.Fingerprint = ""
	sort.Slice(next.Entries, func(i, j int) bool {
		left, right := next.Entries[i], next.Entries[j]
		if left.InstanceID != right.InstanceID {
			return left.InstanceID < right.InstanceID
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		return left.CapabilityID < right.CapabilityID
	})
	for index := range next.Entries {
		sort.Slice(next.Entries[index].Registrations, func(i, j int) bool {
			left, right := next.Entries[index].Registrations[i], next.Entries[index].Registrations[j]
			if left.Order != right.Order {
				return left.Order < right.Order
			}
			if left.Contract != right.Contract {
				return left.Contract < right.Contract
			}
			return left.ID < right.ID
		})
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
