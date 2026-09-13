package model

import (
	"errors"
	"strings"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"
)

// TestValidateAllowedToolListRejectsInvalidSelectors exercises every rejection
// branch of validateAllowedToolList (model/agentic_options.go) through the
// public ValidateControls entry point the agentic streamer calls before
// dispatch. This closes the coverage gap the fix-pass review found: prior to
// this test, replacing validateAllowedToolList's body with `return nil` left
// `go test ./model/... ./runtime/...` green.
func TestValidateAllowedToolListRejectsInvalidSelectors(t *testing.T) {
	knownTool := &einoschema.ToolInfo{Name: "search"}

	cases := []struct {
		name       string
		choiceType einoschema.ToolChoice
		allowed    []*einoschema.AllowedTool
		wantErrSub string
	}{
		{
			name:       "unknown function name rejected",
			choiceType: einoschema.ToolChoiceAllowed,
			allowed:    []*einoschema.AllowedTool{{FunctionName: "does_not_exist"}},
			wantErrSub: "unknown function",
		},
		{
			name:       "entry specifying none of the three selectors rejected",
			choiceType: einoschema.ToolChoiceAllowed,
			allowed:    []*einoschema.AllowedTool{{}},
			wantErrSub: "exactly one of",
		},
		{
			name:       "entry specifying two of the three selectors rejected",
			choiceType: einoschema.ToolChoiceAllowed,
			allowed: []*einoschema.AllowedTool{{
				FunctionName: "search",
				MCPTool:      &einoschema.AllowedMCPTool{ServerLabel: "srv", Name: "mcp_search"},
			}},
			wantErrSub: "exactly one of",
		},
		{
			name:       "MCP selector missing server label rejected",
			choiceType: einoschema.ToolChoiceAllowed,
			allowed: []*einoschema.AllowedTool{{
				MCPTool: &einoschema.AllowedMCPTool{Name: "mcp_search"},
			}},
			wantErrSub: "server label and a name",
		},
		{
			name:       "MCP selector missing name rejected",
			choiceType: einoschema.ToolChoiceAllowed,
			allowed: []*einoschema.AllowedTool{{
				MCPTool: &einoschema.AllowedMCPTool{ServerLabel: "srv"},
			}},
			wantErrSub: "server label and a name",
		},
		{
			name:       "server tool selector missing name rejected",
			choiceType: einoschema.ToolChoiceAllowed,
			allowed: []*einoschema.AllowedTool{{
				ServerTool: &einoschema.AllowedServerTool{},
			}},
			wantErrSub: "server allowed tool requires a name",
		},
		{
			name:       "forced selector applies the same unknown-function rejection",
			choiceType: einoschema.ToolChoiceForced,
			allowed:    []*einoschema.AllowedTool{{FunctionName: "does_not_exist"}},
			wantErrSub: "unknown function",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			toolChoice := &einoschema.AgenticToolChoice{Type: tc.choiceType}
			switch tc.choiceType {
			case einoschema.ToolChoiceAllowed:
				toolChoice.Allowed = &einoschema.AgenticAllowedToolChoice{Tools: tc.allowed}
			case einoschema.ToolChoiceForced:
				toolChoice.Forced = &einoschema.AgenticForcedToolChoice{Tools: tc.allowed}
			}
			controls := RequestControls{
				Tools:      []*einoschema.ToolInfo{knownTool},
				ToolChoice: toolChoice,
			}
			err := ValidateControls(controls)
			var modelErr Error
			if !errors.As(err, &modelErr) || modelErr.Code != "tool_partition_invalid" {
				t.Fatalf("error = %v, want tool_partition_invalid", err)
			}
			if !strings.Contains(modelErr.Message, tc.wantErrSub) {
				t.Fatalf("error message = %q, want substring %q", modelErr.Message, tc.wantErrSub)
			}
		})
	}
}

// TestValidateAllowedToolListAcceptsEachSelectorKind proves the positive
// side of the same rule: exactly one of FunctionName/MCPTool/ServerTool,
// correctly populated, is accepted for both Allowed and Forced tool choice.
func TestValidateAllowedToolListAcceptsEachSelectorKind(t *testing.T) {
	knownTool := &einoschema.ToolInfo{Name: "search"}

	cases := []struct {
		name    string
		allowed *einoschema.AllowedTool
	}{
		{name: "function selector", allowed: &einoschema.AllowedTool{FunctionName: "search"}},
		{name: "mcp selector", allowed: &einoschema.AllowedTool{MCPTool: &einoschema.AllowedMCPTool{ServerLabel: "srv", Name: "mcp_search"}}},
		{name: "server selector", allowed: &einoschema.AllowedTool{ServerTool: &einoschema.AllowedServerTool{Name: "web_search"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name+"/allowed", func(t *testing.T) {
			controls := RequestControls{
				Tools: []*einoschema.ToolInfo{knownTool},
				ToolChoice: &einoschema.AgenticToolChoice{
					Type:    einoschema.ToolChoiceAllowed,
					Allowed: &einoschema.AgenticAllowedToolChoice{Tools: []*einoschema.AllowedTool{tc.allowed}},
				},
			}
			if err := ValidateControls(controls); err != nil {
				t.Fatalf("ValidateControls() = %v, want nil", err)
			}
		})
		t.Run(tc.name+"/forced", func(t *testing.T) {
			controls := RequestControls{
				Tools: []*einoschema.ToolInfo{knownTool},
				ToolChoice: &einoschema.AgenticToolChoice{
					Type:   einoschema.ToolChoiceForced,
					Forced: &einoschema.AgenticForcedToolChoice{Tools: []*einoschema.AllowedTool{tc.allowed}},
				},
			}
			if err := ValidateControls(controls); err != nil {
				t.Fatalf("ValidateControls() = %v, want nil", err)
			}
		})
	}
}
