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
