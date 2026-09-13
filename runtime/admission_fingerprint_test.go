package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/session"
)

func TestAdmissionFingerprintCanonicalizesNilContainers(t *testing.T) {
	base := Request{AdmissionKey: "key-1", Message: TextUserMessage("héllo  "), Config: orchestratorConfig()}
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
	request := Request{AdmissionKey: "key-2", Message: TextUserMessage("hello"), Config: keyedAdmissionConfig(t)}
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
	request := Request{AdmissionKey: "bad/key", Message: TextUserMessage("hello"), Config: keyedAdmissionConfig(t)}
	if _, err := fingerprintAdmission(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("key error=%v", err)
	}
	request.AdmissionKey = "valid"
	request.Message = TextUserMessage(strings.Repeat("x", maxAdmissionMessageBytes+1))
	if _, err := fingerprintAdmission(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("message error=%v", err)
	}
	request.Message = TextUserMessage("hello")
	request.Config.Metadata["workspace_root"] = "relative"
	if err := validateKeyedWorkspace(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("workspace error=%v", err)
	}
}

func TestAdmissionInputBudgetRejectsOversizedMetadataBeforeClone(t *testing.T) {
	request := Request{AdmissionKey: "bounded", Message: TextUserMessage("hello"), Config: keyedAdmissionConfig(t)}
	request.Metadata = map[string]string{"hostile": strings.Repeat("x", maxAdmissionPayloadBytes)}
	if err := validateAdmissionInputBudget(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("budget error=%v", err)
	}
}

func TestAdmissionInputJSONSizeMatchesCanonicalPayload(t *testing.T) {
	base := Request{AdmissionKey: "sized", Message: TextUserMessage(""), Config: orchestratorConfig()}
	cases := []Request{
		base,
		func() Request {
			request := base
			request.Message = TextUserMessage("quoted \" <html> \u2028")
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

func TestAdmissionFingerprintUsesOrderedTypedPayloadAndIgnoresRuntimeIDs(t *testing.T) {
	request := Request{
		AdmissionKey: "typed",
		Message: UserMessage{Blocks: []session.ContentBlock{
			{Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "hello", Annotations: nil}},
			{Kind: session.BlockKindUserInputImage, Media: &session.MediaBlock{Base64Data: "aGVsbG8=", MIMEType: "image/png", Detail: "high"}},
			{Kind: session.BlockKindMCPToolApprovalResponse, MCPApprovalResponse: &session.MCPApprovalResponseBlock{ApprovalRequestID: "approval-1", Approve: true, Reason: "ok"}},
		}},
		Config: orchestratorConfig(),
	}
	baseline, err := fingerprintAdmission(request)
	if err != nil {
		t.Fatal(err)
	}
	withIDs := request
	withIDs.Message.Blocks = append([]session.ContentBlock(nil), request.Message.Blocks...)
	for index := range withIDs.Message.Blocks {
		withIDs.Message.Blocks[index].ID = fmt.Sprintf("generated-%d", index)
	}
	if got, err := fingerprintAdmission(withIDs); err != nil || got != baseline {
		t.Fatalf("generated ids changed fingerprint: got=%x want=%x err=%v", got, baseline, err)
	}

	mutations := []func(*Request){
		func(r *Request) {
			r.Message.Blocks[0].Text = &session.TextBlock{Text: "changed", Annotations: []session.TextAnnotation{}}
		},
		func(r *Request) {
			r.Message.Blocks[1].Media = &session.MediaBlock{Base64Data: "aGVsbG8=", MIMEType: "image/jpeg", Detail: "high"}
		},
		func(r *Request) {
			r.Message.Blocks[2].MCPApprovalResponse = &session.MCPApprovalResponseBlock{ApprovalRequestID: "approval-1", Approve: false, Reason: "no"}
		},
		func(r *Request) { r.Message.Blocks[0], r.Message.Blocks[1] = r.Message.Blocks[1], r.Message.Blocks[0] },
	}
	for index, mutate := range mutations {
		changed := request
		changed.Message.Blocks = append([]session.ContentBlock(nil), request.Message.Blocks...)
		mutate(&changed)
		got, err := fingerprintAdmission(changed)
		if err != nil || got == baseline {
			t.Fatalf("mutation %d fingerprint=%x err=%v", index, got, err)
		}
	}
}

func TestAdmissionFingerprintCanonicalizesTextAnnotationContainers(t *testing.T) {
	request := Request{AdmissionKey: "annotations", Message: TextUserMessage("hello"), Config: orchestratorConfig()}
	request.Message.Blocks[0].Text.Annotations = nil
	left, err := fingerprintAdmission(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Message.Blocks[0].Text.Annotations = []session.TextAnnotation{}
	right, err := fingerprintAdmission(request)
	if err != nil || left != right {
		t.Fatalf("nil/empty annotations fingerprints %x %x err=%v", left, right, err)
	}
}

func TestAdmissionInputBudgetRejectsInvalidAndPathologicalTypedInput(t *testing.T) {
	request := Request{AdmissionKey: "typed-bounds", Message: TextUserMessage(string([]byte{0xff})), Config: orchestratorConfig()}
	if err := validateAdmissionInputBudget(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("invalid UTF-8 error=%v", err)
	}
	request.Message.Blocks = make([]session.ContentBlock, session.MaxContentLimits().MaxBlocks+1)
	if err := validateAdmissionInputBudget(request); !errors.Is(err, session.ErrAdmissionInvalid) {
		t.Fatalf("block-count error=%v", err)
	}
}
