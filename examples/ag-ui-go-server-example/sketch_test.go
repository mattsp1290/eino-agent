package agui_go_server_example

import (
	"encoding/json"
	"testing"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/session"
)

func TestTerminalTextUserMessageAcceptsStringLikeContent(t *testing.T) {
	t.Parallel()

	text := "  terminal-user\n"
	tests := map[string]any{
		"string":          text,
		"string pointer":  &text,
		"bytes":           []byte(text),
		"raw JSON string": json.RawMessage(`"  terminal-user\n"`),
	}
	for name, content := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			message, err := terminalTextUserMessage([]aguitypes.Message{{Role: aguitypes.RoleUser, Content: content}})
			if err != nil {
				t.Fatalf("terminalTextUserMessage error = %v", err)
			}
			if message.Content != text {
				t.Fatalf("Content = %q, want %q", message.Content, text)
			}
		})
	}
}

func TestStartRequestUsesOnlyTerminalPlainTextUserMessage(t *testing.T) {
	t.Parallel()

	request, err := StartRequest("session-1", RunInput{
		ThreadID: "thread-1",
		RunID:    "changing-wire-run",
		Messages: []aguitypes.Message{
			{Role: aguitypes.RoleUser, Content: "prior-user"},
			{Role: aguitypes.RoleAssistant, Content: "prior-assistant"},
			{Role: aguitypes.RoleUser, Content: "  terminal-user\n"},
		},
	}, config.Snapshot{})
	if err != nil {
		t.Fatalf("StartRequest error = %v", err)
	}
	if request.SessionID != session.ID("session-1") {
		t.Fatalf("SessionID = %q", request.SessionID)
	}
	if request.Message.Content != "  terminal-user\n" {
		t.Fatalf("Message.Content = %q", request.Message.Content)
	}
	if len(request.Metadata) != 1 || request.Metadata["agui_thread_id"] != "thread-1" {
		t.Fatalf("Metadata = %#v, want only stable thread identity", request.Metadata)
	}
}

func TestStartRequestRejectsUnsupportedTerminalMessage(t *testing.T) {
	t.Parallel()

	tests := map[string][]aguitypes.Message{
		"missing": nil,
		"trailing assistant": {
			{Role: aguitypes.RoleUser, Content: "valid earlier user"},
			{Role: aguitypes.RoleAssistant, Content: "not a submission"},
		},
		"trailing tool": {
			{Role: aguitypes.RoleTool, Content: "tool result"},
		},
		"blank": {
			{Role: aguitypes.RoleUser, Content: " \t\n"},
		},
		"mixed text and image": {
			{Role: aguitypes.RoleUser, Content: []aguitypes.InputContent{
				{Type: aguitypes.InputContentTypeText, Text: "hello"},
				{Type: aguitypes.InputContentTypeImage, Source: &aguitypes.InputContentSource{Type: aguitypes.InputContentSourceTypeURL, Value: "https://example.com/image.png"}},
			}},
		},
		"structured": {
			{Role: aguitypes.RoleUser, Content: map[string]any{"text": "hello"}},
		},
		"unknown content kind": {
			{Role: aguitypes.RoleUser, Content: []aguitypes.InputContent{{Type: "unknown", Text: "hello"}}},
		},
		"invalid UTF-8": {
			{Role: aguitypes.RoleUser, Content: []byte{0xff}},
		},
	}
	for name, messages := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := StartRequest("session-1", RunInput{Messages: messages}, config.Snapshot{}); err == nil {
				t.Fatal("StartRequest error = nil")
			}
		})
	}
}
