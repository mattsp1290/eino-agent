package agui

import (
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

func TestRulesSafetyGates(t *testing.T) {
	t.Parallel()

	rules := byFamily(Rules())
	for _, family := range []EventFamily{EventReasoning, EventStateSnapshot} {
		rule := rules[family]
		if rule.SnapshotSafe {
			t.Fatalf("%s SnapshotSafe = true, want gated content to default false", family)
		}
		if len(rule.Gates) == 0 {
			t.Fatalf("%s has no gates", family)
		}
	}
	encrypted := rules[EventEncryptedReasoning]
	if encrypted.Persist != DispositionOmit || encrypted.Replay != DispositionOmit || encrypted.LiveTail != DispositionOmit {
		t.Fatalf("encrypted reasoning rule = %#v, want omit/omit/omit", encrypted)
	}
	providerState := rules[EventProviderState]
	if providerState.Persist != DispositionOmit || providerState.Replay != DispositionOmit || providerState.LiveTail != DispositionOmit || providerState.SnapshotSafe {
		t.Fatalf("provider state rule = %#v, want omit/omit/omit", providerState)
	}
}

func TestRulesFamiliesAreUnique(t *testing.T) {
	t.Parallel()

	seen := map[EventFamily]struct{}{}
	for _, rule := range Rules() {
		if rule.Family == "" {
			t.Fatal("rule has empty family")
		}
		if _, ok := seen[rule.Family]; ok {
			t.Fatalf("duplicate rule for family %s", rule.Family)
		}
		seen[rule.Family] = struct{}{}
	}
	want := []EventFamily{
		EventRunLifecycle,
		EventText,
		EventReasoning,
		EventEncryptedReasoning,
		EventProviderState,
		EventToolCall,
		EventToolResult,
		EventStateSnapshot,
		EventStateDelta,
		EventMessagesSnapshot,
		EventActivity,
		EventStep,
		EventCustom,
		EventError,
	}
	for _, family := range want {
		if _, ok := seen[family]; !ok {
			t.Fatalf("missing rule for family %s", family)
		}
	}
}

// TestRulesPersistImpliesSessionPart pins the invariant that a rule claiming
// DispositionPersist (durable session facts) must name the durable
// SessionPart that backs it. Without this, a family can drift into declaring
// durability it does not provide -- exactly what happened to
// EventStateSnapshot and EventStep, whose SessionPart was always "" (nothing
// backs them yet) while Persist claimed DispositionPersist.
//
// This intentionally does not require Replay == DispositionReplay to imply a
// single SessionPart: EventMessagesSnapshot legitimately replays by
// projecting the full durable message/part history rather than one part
// kind, so it has no single SessionPart despite being genuinely replayable.
func TestRulesPersistImpliesSessionPart(t *testing.T) {
	t.Parallel()

	for _, rule := range Rules() {
		if rule.Persist == DispositionPersist && rule.SessionPart == "" {
			t.Fatalf("%s: Persist = DispositionPersist but SessionPart is empty (no durable backing)", rule.Family)
		}
	}

	// EventStateSnapshot and EventStep are the two families with no durable
	// backing today; pin that they are honest about it (DispositionOmit, not
	// an aspirational DispositionPersist/DispositionReplay).
	rules := byFamily(Rules())
	for _, family := range []EventFamily{EventStateSnapshot, EventStep} {
		rule := rules[family]
		if rule.SessionPart != "" {
			t.Fatalf("%s: SessionPart = %q, want empty -- update this test if a PartKind now backs it", family, rule.SessionPart)
		}
		if rule.Persist != DispositionOmit || rule.Replay != DispositionOmit {
			t.Fatalf("%s: Persist=%s Replay=%s, want Omit/Omit while SessionPart is empty", family, rule.Persist, rule.Replay)
		}
	}
}

func TestRulesRedactionIsExplicit(t *testing.T) {
	t.Parallel()

	for _, rule := range Rules() {
		switch rule.Redaction {
		case session.RedactionNone, session.RedactionMetadata, session.RedactionContent:
		default:
			t.Fatalf("%s has invalid redaction %q", rule.Family, rule.Redaction)
		}
	}
}

func byFamily(rules []Rule) map[EventFamily]Rule {
	result := make(map[EventFamily]Rule, len(rules))
	for _, rule := range rules {
		result[rule.Family] = rule
	}
	return result
}
