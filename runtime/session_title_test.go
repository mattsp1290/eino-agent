package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

func TestExecutorSessionTitleCapabilityIsOptInAndRunFenced(t *testing.T) {
	store := newAdmissionStore()
	now := time.Now().UTC()
	store.sessions["test-session"] = session.Session{ID: "test-session", WorkspaceID: "workspace", Title: "old", CreatedAt: now, UpdatedAt: now}
	store.runs["test-run"] = session.Run{ID: "test-run", SessionID: "test-session", ClaimToken: "test-claim", Status: session.RunRunning, Config: map[string]string{"workspace_id": "workspace"}}
	host := mustConfiguredOrchestrator(WithStore(store))
	plan := mustTestRunPlan(testDispatchPlanSpec(nil))
	defer plan.Release()
	execution := newRunExecution(host, plan, store.runs["test-run"])

	var retained SessionTitleWriter
	tool := Tool{Name: "rename", AllowSessionTitle: true, Executor: runtimeToolExecutorFunc(func(ctx context.Context, call ToolCall) (ToolResult, error) {
		if call.SessionTitle == nil {
			return ToolResult{}, errors.New("missing title capability")
		}
		retained = call.SessionTitle
		result, err := call.SessionTitle.SetTitle(ctx, "agent title")
		if err != nil || !result.Changed {
			return ToolResult{}, errors.New("title mutation failed")
		}
		return ToolResult{Output: "ok"}, nil
	})}
	outcome := host.executeToolOutcome(context.Background(), execution, tool, ToolCall{ID: "call", Name: "rename", Input: json.RawMessage(`{}`)})
	if outcome.RawError != nil || retained == nil || store.sessions["test-session"].Title != "agent title" {
		t.Fatalf("outcome = %#v, session = %#v", outcome, store.sessions["test-session"])
	}

	terminal := store.runs["test-run"]
	terminal.Status = session.RunCompleted
	store.runs[terminal.ID] = terminal
	if _, err := retained.SetTitle(context.Background(), "stale title"); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("retained writer error = %v, want ErrConflict", err)
	}
	if store.sessions["test-session"].Title != "agent title" {
		t.Fatal("stale writer changed title")
	}

	defaultTool := Tool{Name: "default", Executor: runtimeToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
		if call.SessionTitle != nil {
			return ToolResult{}, errors.New("default tool received title capability")
		}
		return ToolResult{Output: "ok"}, nil
	})}
	if outcome := host.executeToolOutcome(context.Background(), execution, defaultTool, ToolCall{ID: "default-call", Name: "default", Input: json.RawMessage(`{}`)}); outcome.RawError != nil {
		t.Fatal(outcome.RawError)
	}
}

func TestSessionTitleCapabilityIsStrippedFromExtensionsAndProtected(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	mount, err := registry.Mount(context.Background(), testExtensionComponent("title-isolation"), extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.OnAround(registrar, ToolExecutePoint, extension.Registration{ID: "execute", Scope: extension.GlobalScope()}, func(ctx context.Context, input ToolExecution, proceed extension.Proceed) error {
			if input.Call.SessionTitle != nil {
				return errors.New("middleware received title capability")
			}
			return proceed(ctx)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	plan := mustTestRunPlan(testDispatchPlanSpec(dispatch))
	defer plan.Release()
	host := mustConfiguredOrchestrator()
	executorSaw := false
	tool := Tool{Name: "rename", AllowSessionTitle: true, Executor: runtimeToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
		executorSaw = call.SessionTitle != nil
		return ToolResult{Output: "ok"}, nil
	})}
	if outcome := host.executeToolOutcome(context.Background(), newTestRunExecution(host, plan), tool, ToolCall{ID: "call", Name: "rename", Input: json.RawMessage(`{}`)}); outcome.RawError != nil || !executorSaw {
		t.Fatalf("outcome = %#v, executorSaw = %v", outcome, executorSaw)
	}

	original := PreparedToolCall{Tool: extensionTool(tool), Call: ToolCall{ID: "call", Name: "rename", Input: json.RawMessage(`{}`)}}
	candidate := original
	candidate.Call.SessionTitle = boundSessionTitleWriter{}
	if err := validatePreparedToolCallInput(original, candidate); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("capability injection error = %v", err)
	}
	candidate = original
	candidate.Tool.AllowSessionTitle = false
	if err := validatePreparedToolCallInput(original, candidate); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("opt-in mutation error = %v", err)
	}
}
