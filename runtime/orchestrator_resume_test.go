package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestResumePreservesDeniedDispositionAfterFreshResultTransform(t *testing.T) {
	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "dead-owner", session.ToolCallPending)
	var notices []ToolSettledNotice
	var executed atomic.Bool
	toolRegistry := staticToolRegistry{tools: []Tool{{
		Name:      "echo",
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executed.Store(true)
			return ToolResult{}, nil
		}),
	}}}
	plan := transformedPermissionRunPlan(t, toolRegistry, &notices)
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store),
		WithPermissions(permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
			return permissions.Decision{Action: permissions.ActionDeny, Message: "blocked"}, nil
		})),
		WithClock(func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) }), WithOwnerID("owner-1"),
	)
	done := make(chan Result, 1)
	orchestrator.executeResume(ctx, newRunExecution(orchestrator, plan, run), run, done)
	result := <-done
	if result.Error != nil || executed.Load() {
		t.Fatalf("resume result = %+v", result)
	}
	call, err := store.GetToolCall(ctx, "call-resume")
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != session.ToolCallFailed || call.Error != "" || !strings.Contains(string(call.Output), `"status":"expected_failure"`) || !strings.Contains(string(call.Output), "transformed denial") {
		t.Fatalf("resumed denied call = %+v", call)
	}
	if len(notices) != 1 || notices[0].Status != session.ToolCallFailed || notices[0].Result.Metadata["permission_status"] != "denied" {
		t.Fatalf("settled notices = %#v", notices)
	}
}

func transformedPermissionRunPlan(t *testing.T, tools staticToolRegistry, notices *[]ToolSettledNotice) *RunPlan {
	t.Helper()
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "permission-transform", Artifact: extension.Artifact{Name: "permission-transform", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.OnTransform(registrar, ToolResultTransformPoint, extension.Registration{ID: "transform", Scope: extension.GlobalScope()}, func(_ context.Context, input ToolResultTransform) (ToolResultTransform, error) {
			input.Result = ToolResult{Output: "transformed denial"}
			return input, nil
		}); err != nil {
			return err
		}
		return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "settled", Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolSettledNotice) error {
			*notices = append(*notices, notice)
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	plan := newTestToolPlanWithDispatch(tools, dispatch)
	t.Cleanup(func() { _ = mount.Close(context.Background()) })
	t.Cleanup(func() { plan.release() })
	return plan
}

func TestResumeToolLifecycleNotificationsFollowDurableClaim(t *testing.T) {
	for _, test := range []struct {
		name string
		call session.ToolCallStatus
		want []string
	}{
		{name: "pending", call: session.ToolCallPending, want: []string{"sink:running", "published:running", "started", "sink:completed", "published:completed", "settled"}},
		{name: "running", call: session.ToolCallRunning, want: []string{"sink:interrupted", "published:interrupted", "settled"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := newTestExtensionRegistry(nil)
			component := extension.Component{InstanceID: "resume-lifecycle", Artifact: extension.Artifact{Name: "resume-lifecycle", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
			var notices []string
			var publishedIDs []session.EventID
			mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
				if err := extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", Scope: extension.GlobalScope()}, func(_ context.Context, event Event) error {
					if event.Kind == EventToolCallUpdated && event.ToolCallID == "call-resume" {
						notices = append(notices, "published:"+toolEventStatus(event))
						publishedIDs = append(publishedIDs, event.EventID)
					}
					return nil
				}); err != nil {
					return err
				}
				if err := extension.On(registrar, ToolStartedPoint, extension.Registration{ID: "started", Scope: extension.GlobalScope()}, func(context.Context, ToolStartedNotice) error {
					notices = append(notices, "started")
					return nil
				}); err != nil {
					return err
				}
				return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "settled", Scope: extension.GlobalScope()}, func(context.Context, ToolSettledNotice) error {
					notices = append(notices, "settled")
					return nil
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
			defer dispatch.Release()

			store, run := resumeStoreWithTool(t, "old-owner", test.call)
			toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
				return ToolResult{Output: "ok"}, nil
			})}}}
			var sinkIDs []session.EventID
			sink := EventSinkFunc(func(_ context.Context, event Event) error {
				if event.Kind == EventToolCallUpdated && event.ToolCallID == "call-resume" {
					notices = append(notices, "sink:"+toolEventStatus(event))
					sinkIDs = append(sinkIDs, event.EventID)
					return errors.New("transport unavailable")
				}
				return nil
			})
			orch := mustConfiguredOrchestrator(
				WithStore(store),
				WithEventSink(sink),
				WithClock(func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) }),
				WithOwnerID("new-owner"),
			)
			done := make(chan Result, 1)
			orch.executeResume(context.Background(), newRunExecution(orch, newTestToolPlanWithDispatch(toolRegistry, dispatch), run), run, done)
			result := <-done
			if result.Error != nil {
				t.Fatalf("resumeRun result = %+v", result)
			}
			if !reflect.DeepEqual(notices, test.want) {
				t.Fatalf("notices = %v, want %v", notices, test.want)
			}
			if !reflect.DeepEqual(sinkIDs, publishedIDs) {
				t.Fatalf("sink IDs = %v, published IDs = %v", sinkIDs, publishedIDs)
			}
			batch, err := store.ListEvents(context.Background(), run.SessionID, session.EventCursor{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			var durableIDs []session.EventID
			durableIDSet := make(map[session.EventID]bool)
			for _, event := range batch.Events {
				if event.ToolCallID == "call-resume" && event.Kind == string(EventToolCallUpdated) {
					durableIDs = append(durableIDs, event.ID)
					durableIDSet[event.ID] = true
				}
			}
			for _, id := range sinkIDs {
				if !durableIDSet[id] {
					t.Fatalf("durable IDs = %v, published ID %s missing", durableIDs, id)
				}
			}
		})
	}
}

func TestStreamingOrchestratorResumeClaimsPendingToolOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-resume", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{
		ID:            "run-resume",
		SessionID:     "session-resume",
		OwnerID:       "owner-1",
		ClaimToken:    "old-claim",
		Agent:         "agent",
		ProviderID:    "fake",
		ModelID:       "test",
		Status:        session.RunPending,
		Config:        map[string]string{"workspace_id": "workspace-1", "workspace_root": "/workspace"},
		ExtensionPlan: testEchoPlanDescriptor(),
		CreatedAt:     now,
	}, time.Millisecond)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	time.Sleep(2 * time.Millisecond)
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-resume", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	if _, err := execution.CreateToolCall(ctx, testCreateToolRequest(session.ToolCall{
		ID:              "call-resume",
		SessionID:       run.SessionID,
		RunID:           run.ID,
		MessageID:       "assistant-resume",
		ResultMessageID: "result-message-resume",
		ResultPartID:    "result-part-resume",
		Name:            "echo",
		Pattern:         "echo",
		Input:           []byte(`{"text":"hi"}`),
		Status:          session.ToolCallPending,
	}, "event-create-resume", now)); err != nil {
		t.Fatalf("create tool call: %v", err)
	}
	var executions atomic.Int64
	var patternCalls atomic.Int64
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Pattern: PermissionPatternResolverFunc(func(context.Context, json.RawMessage) (string, error) {
		patternCalls.Add(1)
		return "", errors.New("resume must reuse persisted pattern")
	}), Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		executions.Add(1)
		time.Sleep(10 * time.Millisecond)
		return ToolResult{Output: "ok"}, nil
	})}}}
	orch := mustConfiguredOrchestrator(
		WithStore(store), WithClock(func() time.Time { return now }), WithOwnerID("owner-1"),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
	)

	start := make(chan struct{})
	results := make(chan Result, 2)
	for range 2 {
		go func() {
			<-start
			handle, err := orch.Resume(context.Background(), run.ID)
			if err != nil {
				results <- Result{RunID: run.ID, Status: session.RunFailed, Error: err}
				return
			}
			results <- <-handle.Done()
		}()
	}
	close(start)
	first := <-results
	second := <-results
	if first.Error != nil && second.Error != nil {
		t.Fatalf("resume results = %+v / %+v", first, second)
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions = %d, want 1", executions.Load())
	}
	if patternCalls.Load() != 0 {
		t.Fatalf("permission pattern resolver calls = %d, want 0 on resume", patternCalls.Load())
	}
	call, err := store.GetToolCall(ctx, "call-resume")
	if err != nil {
		t.Fatalf("get tool call: %v", err)
	}
	if call.Status != session.ToolCallCompleted {
		t.Fatalf("tool call status = %s, want completed", call.Status)
	}
	finished, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if finished.Status != session.RunInterrupted {
		t.Fatalf("run status = %s, want interrupted", finished.Status)
	}
	if _, err := store.ActiveRun(ctx, run.SessionID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("active run err = %v, want ErrNotFound", err)
	}
	batch, err := store.ListMessages(ctx, run.SessionID, session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	var toolResults int
	for _, part := range batch.Parts {
		if part.Kind == session.PartToolResult {
			toolResults++
		}
	}
	if toolResults != 1 {
		t.Fatalf("tool result parts = %d, want 1", toolResults)
	}
}

func TestStreamingOrchestratorResumeTakesStaleRunOwnership(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "dead-owner", session.ToolCallPending)
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	var executions atomic.Int64
	var resumedContext ToolContext
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
		executions.Add(1)
		resumedContext = call.Context.Clone()
		return ToolResult{Output: "ok"}, nil
	})}}}
	orch := mustConfiguredOrchestrator(
		WithStore(store), WithClock(func() time.Time { return now }), WithOwnerID("owner-1"),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
	)
	handle, err := orch.Resume(ctx, run.ID)
	if err != nil {
		t.Fatalf("Resume error: %v", err)
	}
	result := <-handle.Done()
	if result.Error != nil {
		t.Fatalf("resume result = %+v", result)
	}
	finished, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if finished.OwnerID != "owner-1" || finished.Status != session.RunInterrupted {
		t.Fatalf("finished run = %+v, want owner-1 interrupted", finished)
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions = %d, want 1", executions.Load())
	}
	if !reflect.DeepEqual(resumedContext.Turn.ToolNames, []string{"echo"}) || resumedContext.Turn.RunID != run.ID || resumedContext.Turn.SessionID != run.SessionID || resumedContext.WorkspaceID != "workspace-1" || resumedContext.WorkspaceRoot != "/workspace" {
		t.Fatalf("resumed tool context = %#v", resumedContext)
	}
}

func TestRunHeartbeatPreventsResumeAcrossInjectedClockSkew(t *testing.T) {
	t.Parallel()
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	streamer := scriptedStreamer(func(ctx context.Context, _ model.Request) ([]*einoschema.Message, error) {
		close(entered)
		select {
		case <-release:
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	plan := mustTestRunPlan(RunPlanSpec{})
	provider := staticRunPlanProvider{plan: plan}
	owner := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithRunPlanProvider(provider),
		WithOwnerID("owner-a"), WithLease(100*time.Millisecond),
		WithClock(func() time.Time { return time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC) }),
	)
	handle, err := owner.Start(context.Background(), Request{SessionID: "heartbeat-session", Input: []*einoschema.Message{einoschema.UserMessage("wait")}, Config: orchestratorConfig()})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	initial, err := store.GetRun(context.Background(), handle.RunID())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, getErr := store.GetRun(context.Background(), handle.RunID())
		if getErr != nil {
			t.Fatal(getErr)
		}
		now := time.Now()
		if now.After(initial.LeaseUntil) && current.LeaseUntil.After(now) {
			break
		}
		if now.After(deadline) {
			t.Fatalf("heartbeat did not renew initial lease %s; current lease %s", initial.LeaseUntil, current.LeaseUntil)
		}
		time.Sleep(5 * time.Millisecond)
	}
	resumer := mustConfiguredOrchestrator(
		WithStore(store), WithRunPlanProvider(provider), WithOwnerID("owner-b"), WithLease(100*time.Millisecond),
		WithClock(func() time.Time { return time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC) }),
	)
	if _, err := resumer.Resume(context.Background(), handle.RunID()); !errors.Is(err, session.ErrSessionBusy) {
		t.Fatalf("Resume error = %v, want ErrSessionBusy", err)
	}
	close(release)
	result := <-handle.Done()
	if result.Error != nil || result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
}

func TestStreamingOrchestratorResumeDoesNotReexecuteRunningTool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "owner-1", session.ToolCallRunning)
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	var executions atomic.Int64
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		executions.Add(1)
		return ToolResult{Output: "ok"}, nil
	})}}}
	orch := mustConfiguredOrchestrator(
		WithStore(store), WithClock(func() time.Time { return now }), WithOwnerID("owner-1"),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
	)
	handle, err := orch.Resume(ctx, run.ID)
	if err != nil {
		t.Fatalf("Resume error: %v", err)
	}
	result := <-handle.Done()
	if result.Error != nil {
		t.Fatalf("resume result = %+v", result)
	}
	if executions.Load() != 0 {
		t.Fatalf("tool executions = %d, want 0", executions.Load())
	}
	call, err := store.GetToolCall(ctx, "call-resume")
	if err != nil {
		t.Fatalf("get tool call: %v", err)
	}
	if call.Status != session.ToolCallInterrupted {
		t.Fatalf("tool status = %s, want interrupted", call.Status)
	}
}

func resumeStoreWithTool(t *testing.T, owner string, status session.ToolCallStatus) (*sqlitestore.Store, session.Run) {
	t.Helper()
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-resume", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{
		ID:            "run-resume",
		SessionID:     "session-resume",
		OwnerID:       owner,
		ClaimToken:    "old-claim",
		Agent:         "agent",
		ProviderID:    "fake",
		ModelID:       "test",
		Status:        session.RunPending,
		Config:        map[string]string{"workspace_id": "workspace-1", "workspace_root": "/workspace"},
		ExtensionPlan: testEchoPlanDescriptor(),
		CreatedAt:     now,
	}, time.Millisecond)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	time.Sleep(2 * time.Millisecond)
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-resume", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	call := session.ToolCall{
		ID:              "call-resume",
		SessionID:       run.SessionID,
		RunID:           run.ID,
		MessageID:       "assistant-resume",
		ResultMessageID: "result-resume",
		ResultPartID:    "part-resume",
		Name:            "echo",
		Pattern:         "echo",
		Input:           []byte(`{"text":"hi"}`),
		Status:          session.ToolCallPending,
	}
	created, err := execution.CreateToolCall(ctx, testCreateToolRequest(call, "event-create-resume", now))
	if err != nil {
		t.Fatalf("create tool call: %v", err)
	}
	createdCall := created.Call
	if status == session.ToolCallRunning {
		createdCall.ClaimedBy = owner
		createdCall.ClaimToken = "claim-resume"
		createdCall.StartedAt = now
		if _, err := execution.ClaimToolCall(ctx, testClaimToolRequest(createdCall, "event-claim-resume", time.Millisecond, now)); err != nil {
			t.Fatalf("claim tool call: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	return store, run
}
