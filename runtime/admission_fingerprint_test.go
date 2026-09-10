package runtime

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/session"
)

func TestAdmissionFingerprintCanonicalizesNilContainers(t *testing.T) {
	base := Request{AdmissionKey: "key-1", Message: UserMessage{Content: "héllo  "}, Config: orchestratorConfig()}
	base.Config.Metadata = nil
	base.Config.Agent.Options = nil
	base.Config.Tools.Enabled = nil
	base.Config.Tools.Disabled = nil
	base.Config.Tools.Permissions = nil
	base.Metadata = nil
	left, err := fingerprintAdmission(frozenRequest(base))
	if err != nil {
		t.Fatal(err)
	}
	base.Config.Metadata = map[string]string{}
	base.Config.Agent.Options = map[string]string{}
	base.Config.Tools.Enabled = []string{}
	base.Config.Tools.Disabled = []string{}
	base.Config.Tools.Permissions = []config.PermissionRule{}
	base.Metadata = map[string]string{}
	right, err := fingerprintAdmission(frozenRequest(base))
	if err != nil || left != right {
		t.Fatalf("fingerprints %x %x err=%v", left, right, err)
	}
}

func TestAdmissionFingerprintIncludesExecutionInputAndExcludesTelemetry(t *testing.T) {
	request := Request{AdmissionKey: "key-2", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	baseline, err := fingerprintAdmission(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Config.Observability.Service = "telemetry-only"
	if got, err := fingerprintAdmission(request); err != nil || got != baseline {
		t.Fatalf("telemetry fingerprint=%x err=%v", got, err)
	}
	request.Config.Metadata["new"] = "value"
	if got, err := fingerprintAdmission(request); err != nil || got == baseline {
		t.Fatalf("metadata fingerprint=%x err=%v", got, err)
	}
}

func TestKeyedAdmissionBoundsAndWorkspaceValidation(t *testing.T) {
	request := Request{AdmissionKey: "bad/key", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	if _, err := fingerprintAdmission(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("key error=%v", err)
	}
	request.AdmissionKey = "valid"
	request.Message.Content = strings.Repeat("x", maxAdmissionMessageBytes+1)
	if _, err := fingerprintAdmission(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("message error=%v", err)
	}
	request.Message.Content = "hello"
	request.Config.Metadata["workspace_root"] = "relative"
	if err := validateKeyedWorkspace(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("workspace error=%v", err)
	}
}

func TestAdmissionInputBudgetRejectsOversizedMetadataBeforeClone(t *testing.T) {
	request := Request{AdmissionKey: "bounded", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	request.Metadata = map[string]string{"hostile": strings.Repeat("x", maxAdmissionPayloadBytes)}
	if err := validateAdmissionInputBudget(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("budget error=%v", err)
	}
}

func TestAdmissionInputJSONSizeMatchesCanonicalPayload(t *testing.T) {
	base := Request{AdmissionKey: "sized", Message: UserMessage{Content: ""}, Config: orchestratorConfig()}
	cases := []Request{
		base,
		func() Request {
			request := base
			request.Message.Content = "quoted \" <html> \u2028"
			request.Metadata = map[string]string{"line\n": "tab\t"}
			return request
		}(),
		func() Request {
			request := base
			request.Config.Tools.Enabled = []string{"read", "write"}
			request.Config.Tools.Disabled = []string{"shell"}
			request.Config.Tools.Permissions = []config.PermissionRule{{Permission: "filesystem", Pattern: "/tmp/**", Action: "allow"}}
			return request
		}(),
	}
	for index, request := range cases {
		got, err := admissionInputJSONSize(request)
		encoded, marshalErr := json.Marshal(admissionFingerprintPayloadFor(request))
		if err != nil || marshalErr != nil || got != len(encoded) {
			t.Fatalf("case %d size=%d encoded=%d err=%v marshal=%v", index, got, len(encoded), err, marshalErr)
		}
	}
}
