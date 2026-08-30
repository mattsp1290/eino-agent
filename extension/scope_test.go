package extension

import "testing"

func TestScopeApplies(t *testing.T) {
	tests := []struct {
		name         string
		registration Scope
		target       Scope
		want         bool
	}{
		{name: "global to global", registration: GlobalScope(), target: GlobalScope(), want: true},
		{name: "global to session", registration: GlobalScope(), target: SessionScope("session-a"), want: true},
		{name: "matching session", registration: SessionScope("session-a"), target: SessionScope("session-a"), want: true},
		{name: "mismatched session", registration: SessionScope("session-a"), target: SessionScope("session-b")},
		{name: "session to global", registration: SessionScope("session-a"), target: GlobalScope()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ScopeApplies(test.registration, test.target); got != test.want {
				t.Fatalf("ScopeApplies(%+v, %+v) = %v, want %v", test.registration, test.target, got, test.want)
			}
		})
	}
}
