package session

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateSessionTitle(t *testing.T) {
	tests := []struct {
		name  string
		title string
		valid bool
	}{
		{name: "empty", title: "", valid: true},
		{name: "whitespace", title: " \n\t", valid: true},
		{name: "byte boundary", title: strings.Repeat("a", DiscoveryMaxTitleBytes), valid: true},
		{name: "multibyte boundary", title: strings.Repeat("é", DiscoveryMaxTitleBytes/2), valid: true},
		{name: "too large", title: strings.Repeat("a", DiscoveryMaxTitleBytes+1)},
		{name: "multibyte too large", title: strings.Repeat("é", DiscoveryMaxTitleBytes/2+1)},
		{name: "invalid utf8", title: string([]byte{0xff})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSessionTitle(test.title)
			if test.valid && err != nil {
				t.Fatalf("ValidateSessionTitle error = %v", err)
			}
			if !test.valid && !errors.Is(err, ErrSessionTitleInvalid) {
				t.Fatalf("ValidateSessionTitle error = %v, want ErrSessionTitleInvalid", err)
			}
		})
	}
}

func TestSessionTitleRequestValidation(t *testing.T) {
	valid := SessionTitleRequest{SessionID: "session", WorkspaceID: "", Title: "title"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, request := range []SessionTitleRequest{
		{WorkspaceID: "workspace", Title: "title"},
		{SessionID: ID(strings.Repeat("s", DiscoveryMaxIdentityBytes+1)), Title: "title"},
		{SessionID: "session", WorkspaceID: strings.Repeat("w", DiscoveryMaxIdentityBytes+1), Title: "title"},
		{SessionID: ID(string([]byte{0xff})), Title: "title"},
		{SessionID: "session", WorkspaceID: string([]byte{0xff}), Title: "title"},
	} {
		if err := request.Validate(); !errors.Is(err, ErrSessionTitleInvalid) {
			t.Fatalf("request %#v error = %v", request, err)
		}
	}
}
