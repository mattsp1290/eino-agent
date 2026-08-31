package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestEmptyPlanAcquiresAndResumesThroughRequiredProvider(t *testing.T) {
	orchestrator := mustConfiguredOrchestrator()
	plan, err := orchestrator.acquireRunPlan(context.Background(), RunPlanRequest{SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	if descriptor.Fingerprint == "" {
		t.Fatalf("empty descriptor = %#v", descriptor)
	}
	resumed, err := orchestrator.acquireResumePlan(context.Background(), "session", descriptor)
	if err != nil {
		t.Fatalf("resume empty strict plan = %v", err)
	}
	resumedDescriptor := resumed.Descriptor()
	if resumedDescriptor.Fingerprint != descriptor.Fingerprint || len(resumedDescriptor.Components) != 0 {
		t.Fatalf("resumed descriptor = %#v, want %#v", resumedDescriptor, descriptor)
	}
}

func TestAcquireResumePlanRejectsInvalidPersistedFingerprintBeforeProvider(t *testing.T) {
	valid := session.ExtensionPlanDescriptor{Components: []session.ComponentPlan{handlerPlanEntryForTest("callbacks")}}
	sealed, _ := session.SealExtensionPlanForSession("", valid)
	valid = sealed.Descriptor()
	for name, descriptor := range map[string]session.ExtensionPlanDescriptor{
		"missing": func() session.ExtensionPlanDescriptor { next := valid.Clone(); next.Fingerprint = ""; return next }(),
		"stale": func() session.ExtensionPlanDescriptor {
			next := valid.Clone()
			next.Components[0].InstanceID = "corrupt"
			return next
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			resumeCalls := 0
			orchestrator := mustConfiguredOrchestrator(WithRunPlanProvider(staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{}), resumeCalls: &resumeCalls}))
			if _, err := orchestrator.acquireResumePlan(context.Background(), "session", descriptor); !errors.Is(err, ErrExtensionPlanMismatch) {
				t.Fatalf("acquireResumePlan = %v, want ErrExtensionPlanMismatch", err)
			}
			if resumeCalls != 0 {
				t.Fatalf("AcquireResumePlan calls = %d, want 0", resumeCalls)
			}
		})
	}
}

func TestAcquireResumePlanPropagatesDurableSessionIdentity(t *testing.T) {
	plan := mustTestRunPlan(RunPlanSpec{})
	provider := &capturingResumePlanProvider{plan: plan}
	orchestrator := mustConfiguredOrchestrator(WithRunPlanProvider(provider))
	descriptor := plan.Descriptor()
	resumed, err := orchestrator.acquireResumePlan(context.Background(), "durable-session", descriptor)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Release()
	if provider.request.SessionID != "durable-session" || provider.request.Plan.Fingerprint() != descriptor.Fingerprint {
		t.Fatalf("resume request = %#v", provider.request)
	}
}

func handlerPlanEntryForTest(instance string) session.ComponentPlan {
	return session.ComponentPlan{
		InstanceID: instance,
		Artifact: extension.Artifact{
			Name: instance, Version: "1", Hash: instance + "-hash",
			ConfigHash: instance + "-config", SourceKind: extension.SourceNative,
		},
		Handlers: []session.RegistrationIdentity{{
			ID: "handler", Contract: "test/handler", Version: "1",
			Scope: extension.GlobalScope(), Kind: extension.HandlerNotification,
		}},
	}
}

func TestStartReleasesAcquiredPlanWhenResolverPanics(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	mount, err := registry.Mount(context.Background(), extension.Component{InstanceID: "release-on-panic", Artifact: extension.Artifact{Name: "release-on-panic", Version: "1", Hash: "hash", ConfigHash: "config", SourceKind: extension.SourceNative}}, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, ModelRequestedPoint, extension.Registration{ID: "release", Scope: extension.GlobalScope()}, func(context.Context, ModelRequestedNotice) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewRunPlan(testDispatchPlanSpec(dispatch))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator := newTestOrchestrator(newAdmissionStore(), scriptedStreamer(nil),
		WithRunPlanProvider(staticRunPlanProvider{plan: plan}),
		WithModelResolver(model.ResolverFunc(func(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
			panic("resolver failed")
		})),
	)
	defer func() {
		if recovered := recover(); recovered != "resolver failed" {
			t.Fatalf("recovered = %#v", recovered)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := mount.Close(ctx); err != nil {
			t.Fatalf("mount remained leased after panic: %v", err)
		}
	}()
	_, _ = orchestrator.Start(context.Background(), Request{SessionID: "session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
}

func TestRunPlanDescriptorReturnsDefensiveClone(t *testing.T) {
	plan, err := NewRunPlan(RunPlanSpec{})
	if err != nil {
		t.Fatal(err)
	}
	first := plan.Descriptor()
	first.Fingerprint = "mutated"
	if plan.Descriptor().Fingerprint == "mutated" {
		t.Fatal("descriptor mutation changed sealed plan")
	}
}

func TestResumeRunCallbacksOnlyDoesNotRequireTools(t *testing.T) {
	store := newAdmissionStore()
	now := time.Now().UTC()
	if _, err := store.CreateSession(context.Background(), session.Session{ID: "session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := store.AdmitRun(context.Background(), session.Run{ID: "run", SessionID: "session", OwnerID: "old-owner", ClaimToken: "old-claim", Status: session.RunPending, CreatedAt: now}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	orchestrator := mustConfiguredOrchestrator(WithStore(store), WithOwnerID("new-owner"), WithClock(func() time.Time { return now }))
	execution := newRunExecution(orchestrator, mustTestRunPlan(RunPlanSpec{}), run)
	done := make(chan Result, 1)
	orchestrator.executeResume(context.Background(), execution, run, done)
	result := <-done
	if errors.Is(result.Error, ErrInvalidOrchestrator) {
		t.Fatalf("resumeRun required settlement store for callback-only plan: %v", result.Error)
	}
}

func TestTerminalResumeReturnsCompletedHandleWithoutExecution(t *testing.T) {
	base := newAdmissionStore()
	run := session.Run{ID: "terminal-run", SessionID: "terminal-session", Status: session.RunFailed, Error: "already failed"}
	base.runs[run.ID] = run
	store := &terminalResumeStore{admissionStore: base}
	resumeCalls := 0
	plan := mustTestRunPlan(RunPlanSpec{})
	defer plan.Release()
	orchestrator := mustConfiguredOrchestrator(WithStore(store), WithRunPlanProvider(staticRunPlanProvider{plan: plan, resumeCalls: &resumeCalls}))

	handle, err := orchestrator.Resume(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.RunID != run.ID || result.Status != run.Status || result.Error == nil || result.Error.Error() != run.Error {
		t.Fatalf("terminal result = %+v", result)
	}
	if resumeCalls != 0 {
		t.Fatalf("resume plan calls = %d, want 0", resumeCalls)
	}
	if err := handle.Interrupt(context.Background(), "ignored"); err != nil {
		t.Fatalf("Interrupt = %v", err)
	}
	if _, open := <-handle.Done(); open {
		t.Fatal("terminal handle Done remained open")
	}
}

type terminalResumeStore struct {
	*admissionStore
}

func (s *terminalResumeStore) ClaimRun(context.Context, session.RunClaim) (session.Run, error) {
	panic("terminal resume claimed run")
}

func (s *terminalResumeStore) Execution(session.RunFence) session.ExecutionStore {
	panic("terminal resume requested execution store")
}

func TestExecuteResumeSettledDurationStartsAtResumeExecution(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "resume-duration", Artifact: extension.Artifact{Name: "resume-duration", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var duration time.Duration
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, RunSettledPoint, extension.Registration{ID: "settled", Scope: extension.GlobalScope()}, func(_ context.Context, notice RunSettledNotice) error {
			duration = notice.Duration
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
	store := newAdmissionStore()
	run := session.Run{ID: "run", SessionID: "session", ClaimToken: "claim", Status: session.RunPending, CreatedAt: time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)}
	store.runs[run.ID] = run
	now := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	orchestrator := mustConfiguredOrchestrator(WithStore(store), WithClock(func() time.Time { return now }))
	done := make(chan Result, 1)
	orchestrator.executeResume(context.Background(), newRunExecution(orchestrator, newTestDispatchPlan(dispatch), run), run, done)
	result := <-done
	if err := dispatch.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result.Error != nil || duration != 0 {
		t.Fatalf("resume result=%+v duration=%s", result, duration)
	}
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunSettledNoticeRequiresDurableFreshTerminalState(t *testing.T) {
	finishErr := errors.New("terminal finish failed")
	providerErr := errors.New("provider failed")
	for _, test := range []struct {
		name       string
		streamer   scriptedStreamer
		finishErr  error
		wantStatus session.RunStatus
		wantNotice int
		wantError  bool
		wantCauses []error
	}{
		{name: "completed", streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		}), wantStatus: session.RunCompleted, wantNotice: 1},
		{name: "failed and persisted", streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return nil, providerErr
		}), wantStatus: session.RunFailed, wantNotice: 1, wantError: true, wantCauses: []error{providerErr}},
		{name: "work and terminal persistence failed", streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return nil, providerErr
		}), finishErr: finishErr, wantStatus: session.RunFailed, wantError: true, wantCauses: []error{providerErr, finishErr}},
		{name: "terminal persistence failed", streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		}), finishErr: finishErr, wantStatus: session.RunFailed, wantNotice: 0, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newAdmissionStore()
			store := &runLifecycleStore{admissionStore: base, terminalFinishErr: test.finishErr}
			events := &capturingSink{}
			var notices []RunSettledNotice
			plan, closePlan := settledNoticePlan(t, &notices)
			defer closePlan()
			orchestrator := mustConfiguredOrchestrator(
				WithStore(store), WithModelResolver(resolvedModel{streamer: test.streamer}),
				WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
				WithOwnerID("owner-1"), WithQueueSize(2), WithRunPlanProvider(staticRunPlanProvider{plan: plan}), WithEventSink(events),
			)

			result := startAndWait(t, orchestrator)
			if err := plan.FlushNotifications(context.Background()); err != nil {
				t.Fatal(err)
			}
			if result.Status != test.wantStatus || (result.Error != nil) != test.wantError {
				t.Fatalf("result = %+v", result)
			}
			for _, cause := range test.wantCauses {
				if !errors.Is(result.Error, cause) {
					t.Fatalf("result error = %v, want cause %v", result.Error, cause)
				}
			}
			if len(notices) != test.wantNotice {
				t.Fatalf("settled notices = %#v, want %d", notices, test.wantNotice)
			}
			if len(notices) == 1 && notices[0].Result.Status != test.wantStatus {
				t.Fatalf("settled notice result = %+v", notices[0].Result)
			}
			if test.wantNotice > 0 {
				events.waitForKind(t, EventRunFinished, test.wantNotice)
			}
			if got := countEvents(events.snapshot(), EventRunFinished); got != test.wantNotice {
				t.Fatalf("run_finished events = %d, want %d", got, test.wantNotice)
			}
		})
	}
}

func TestRunSettledNoticeRequiresDurableResumeTerminalState(t *testing.T) {
	finishErr := errors.New("terminal finish failed")
	listErr := errors.New("unfinished calls unavailable")
	for _, test := range []struct {
		name       string
		finishErr  error
		listErr    error
		wantNotice int
		wantError  error
	}{
		{name: "interrupted and persisted", wantNotice: 1},
		{name: "terminal persistence failed", finishErr: finishErr, wantError: finishErr},
		{name: "pre-finalization failure", listErr: listErr, wantNotice: 1, wantError: listErr},
		{name: "work and terminal persistence failed", finishErr: finishErr, listErr: listErr, wantError: listErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newAdmissionStore()
			run := session.Run{ID: "resume-run", SessionID: "resume-session", OwnerID: "old-owner", ClaimToken: "resume-claim", Status: session.RunPending, CreatedAt: time.Now().UTC()}
			base.runs[run.ID] = run
			store := &runLifecycleStore{admissionStore: base, terminalFinishErr: test.finishErr, listErr: test.listErr}
			events := &capturingSink{}
			var notices []RunSettledNotice
			plan, closePlan := settledNoticePlan(t, &notices)
			defer closePlan()
			orchestrator := mustConfiguredOrchestrator(WithStore(store), WithOwnerID("new-owner"), WithClock(time.Now), WithEventSink(events))
			done := make(chan Result, 1)
			orchestrator.executeResume(context.Background(), newRunExecution(orchestrator, plan, run), run, done)
			result := <-done
			if err := plan.FlushNotifications(context.Background()); err != nil {
				t.Fatal(err)
			}

			if !errors.Is(result.Error, test.wantError) {
				t.Fatalf("resume result = %+v, want error %v", result, test.wantError)
			}
			if test.finishErr != nil && test.listErr != nil && !errors.Is(result.Error, test.finishErr) {
				t.Fatalf("resume result = %+v, want settlement error %v", result, test.finishErr)
			}
			if calls := store.settlementCalls.Load(); calls != 1 {
				t.Fatalf("settlement calls = %d, want 1", calls)
			}
			if len(notices) != test.wantNotice {
				t.Fatalf("settled notices = %#v, want %d", notices, test.wantNotice)
			}
			if test.wantNotice > 0 {
				events.waitForKind(t, EventRunFinished, test.wantNotice)
			}
			if got := countEvents(events.snapshot(), EventRunFinished); got != test.wantNotice {
				t.Fatalf("run_finished events = %d, want %d", got, test.wantNotice)
			}
			wantStatus := session.RunInterrupted
			if test.listErr != nil {
				wantStatus = session.RunFailed
			}
			if len(notices) == 1 && notices[0].Result.Status != wantStatus {
				t.Fatalf("settled notice result = %+v, want %s", notices[0].Result, wantStatus)
			}
		})
	}
}

func TestExecuteResumeRecoversOrchestrationPanicAndSettlesOnce(t *testing.T) {
	base := newAdmissionStore()
	run := session.Run{ID: "panic-run", SessionID: "panic-session", ClaimToken: "panic-claim", Status: session.RunPending, CreatedAt: time.Now().UTC()}
	base.runs[run.ID] = run
	store := &runLifecycleStore{admissionStore: base, panicList: true}
	orchestrator := mustConfiguredOrchestrator(WithStore(store), WithClock(time.Now))
	done := make(chan Result, 1)
	orchestrator.executeResume(context.Background(), newRunExecution(orchestrator, mustTestRunPlan(RunPlanSpec{}), run), run, done)
	result := <-done
	if result.Status != session.RunFailed || result.Error == nil || !strings.Contains(result.Error.Error(), "resume run panic") {
		t.Fatalf("resume panic result = %+v", result)
	}
	if calls := store.settlementCalls.Load(); calls != 1 {
		t.Fatalf("settlement calls = %d, want 1", calls)
	}
	if _, open := <-done; open {
		t.Fatal("resume panic result channel remained open")
	}
}

func countEvents(events []session.EventRecord, kind string) int {
	var count int
	for _, event := range events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

type runLifecycleStore struct {
	*admissionStore
	terminalFinishErr error
	listErr           error
	panicList         bool
	settlementCalls   atomic.Int32
}

func (s *runLifecycleStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &runLifecycleExecution{ExecutionStore: s.admissionStore.Execution(fence), terminalFinishErr: s.terminalFinishErr, settlementCalls: &s.settlementCalls}
}

type runLifecycleExecution struct {
	session.ExecutionStore
	terminalFinishErr error
	settlementCalls   *atomic.Int32
}

func (s *runLifecycleExecution) SettleRun(ctx context.Context, request session.SettleRunRequest) (session.RunSettlementResult, error) {
	s.settlementCalls.Add(1)
	if request.Settlement.Status != "" && s.terminalFinishErr != nil {
		return session.RunSettlementResult{}, s.terminalFinishErr
	}
	return s.ExecutionStore.SettleRun(ctx, request)
}

func (s *runLifecycleStore) FinishRun(ctx context.Context, run session.Run) error {
	if run.Terminal() && s.terminalFinishErr != nil {
		return s.terminalFinishErr
	}
	return s.admissionStore.FinishRun(ctx, run)
}

func (s *runLifecycleStore) ListUnfinishedToolCalls(ctx context.Context, runID session.RunID) ([]session.ToolCall, error) {
	if s.panicList {
		panic("unfinished calls panic")
	}
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.admissionStore.ListUnfinishedToolCalls(ctx, runID)
}

func settledNoticePlan(t *testing.T, notices *[]RunSettledNotice) (*RunPlan, func()) {
	t.Helper()
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "settlement-gate", Artifact: extension.Artifact{Name: "settlement-gate", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, RunSettledPoint, extension.Registration{ID: "settled", Scope: extension.GlobalScope()}, func(_ context.Context, notice RunSettledNotice) error {
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
	return newTestDispatchPlan(dispatch), func() {
		if err := mount.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

type staticRunPlanProvider struct {
	plan        *RunPlan
	resumeCalls *int
}

type capturingResumePlanProvider struct {
	plan    *RunPlan
	request ResumePlanRequest
}

func (p *capturingResumePlanProvider) AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error) {
	return p.plan, nil
}

func (p *capturingResumePlanProvider) AcquireResumePlan(_ context.Context, request ResumePlanRequest) (*RunPlan, error) {
	p.request = request
	return p.plan, nil
}

func (p staticRunPlanProvider) AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error) {
	return p.plan, nil
}

func (p staticRunPlanProvider) AcquireResumePlan(context.Context, ResumePlanRequest) (*RunPlan, error) {
	if p.resumeCalls != nil {
		(*p.resumeCalls)++
	}
	return p.plan, nil
}
