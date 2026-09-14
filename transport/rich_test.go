package transport

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestDecodeUserMessageMapsEveryRepresentableInputKind(t *testing.T) {
	t.Parallel()

	body := `{"content":[
		{"type":"text","text":"hello"},
		{"type":"image","source":{"type":"url","value":"https://example.test/pic.png","mimeType":"image/png"}},
		{"type":"document","source":{"type":"data","value":"YmFzZTY0","mimeType":"application/pdf"},"filename":"report.pdf"}
	]}`
	r := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(body))
	message, err := DecodeUserMessage(r)
	if err != nil {
		t.Fatalf("DecodeUserMessage error = %v", err)
	}
	if len(message.Blocks) != 3 {
		t.Fatalf("blocks = %d, want 3: %#v", len(message.Blocks), message.Blocks)
	}
	if message.Blocks[0].Kind != session.BlockKindUserInputText || message.Blocks[0].Text.Text != "hello" {
		t.Fatalf("text block = %#v", message.Blocks[0])
	}
	if message.Blocks[1].Kind != session.BlockKindUserInputImage || message.Blocks[1].Media.URL != "https://example.test/pic.png" {
		t.Fatalf("image block = %#v", message.Blocks[1])
	}
	if message.Blocks[2].Kind != session.BlockKindUserInputFile || message.Blocks[2].Media.Base64Data != "YmFzZTY0" || message.Blocks[2].Media.Name != "report.pdf" {
		t.Fatalf("document block = %#v", message.Blocks[2])
	}
	for _, block := range message.Blocks {
		if block.ID != "" {
			t.Fatalf("caller-visible block ID must stay empty (Start mints it): %#v", block)
		}
	}
}

func TestDecodeUserMessageRejectsEmptyAndOversizedInput(t *testing.T) {
	t.Parallel()

	if _, err := DecodeUserMessage(httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(`{"content":[]}`))); err == nil {
		t.Fatal("empty content accepted")
	}
	if _, err := DecodeUserMessage(httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(`{"content":[{"type":"image","source":{"type":"bogus","value":"x"}}]}`))); err == nil {
		t.Fatal("unknown media source type accepted")
	}
	if _, err := DecodeUserMessage(httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(`{"content":[{"type":"image"}]}`))); err == nil {
		t.Fatal("media content with no source accepted")
	}
	for _, body := range []string{
		`{"content":[{"type":"text","text":"hi"}],"unknown":true}`,
		`{"content":[{"type":"text","text":"hi","unknown":true}]}`,
		`{"content":[{"type":"text","text":"hi"}]}{}`,
	} {
		if _, err := DecodeUserMessage(httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(body))); err == nil {
			t.Fatalf("invalid strict JSON accepted: %s", body)
		}
	}
	oversized := bytes.Repeat([]byte("a"), maxRichRequestBytes+1)
	if _, err := DecodeUserMessage(httptest.NewRequest(http.MethodPost, "/messages", bytes.NewReader(oversized))); err == nil {
		t.Fatal("oversized body accepted")
	}
}

type fakeEnqueuer struct {
	called  bool
	request runtime.EnqueueRequest
}

func (f *fakeEnqueuer) Enqueue(_ context.Context, _ session.ID, request runtime.EnqueueRequest) (session.InboxItem, error) {
	f.called = true
	f.request = request
	return session.InboxItem{ID: "inbox-1"}, nil
}

func TestEnqueueHandlerRunsAuthBeforeReadingBodyAndRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()

	authCalled := false
	enqueuer := &fakeEnqueuer{}
	handler := EnqueueHandler(func(ctx context.Context, _ *http.Request) (context.Context, error) {
		authCalled = true
		return ctx, nil
	}, func(context.Context, *http.Request) (session.ID, session.RunID, error) {
		return "session-1", "run-1", nil
	}, enqueuer)

	// Missing idempotency key is rejected before Enqueue is ever called.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/enqueue", strings.NewReader(`{"content":[{"type":"text","text":"hi"}]}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (missing idempotency key)", rec.Code)
	}
	if !authCalled {
		t.Fatal("auth must run before body validation")
	}
	if enqueuer.called {
		t.Fatal("Enqueue called despite missing idempotency key")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/enqueue", strings.NewReader(`{"content":[{"type":"text","text":"hi"}]}`))
	req.Header.Set("Idempotency-Key", "key-1")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !enqueuer.called || enqueuer.request.RunID != "run-1" || enqueuer.request.IdempotencyKey != "key-1" {
		t.Fatalf("enqueue request = %#v", enqueuer.request)
	}
}

func TestEnqueueHandlerAuthFailureNeverReachesEnqueue(t *testing.T) {
	t.Parallel()

	enqueuer := &fakeEnqueuer{}
	handler := EnqueueHandler(func(ctx context.Context, _ *http.Request) (context.Context, error) {
		return ctx, ErrUnauthorized
	}, func(context.Context, *http.Request) (session.ID, session.RunID, error) {
		t.Fatal("lookupRunID must not run when auth fails")
		return "", "", nil
	}, enqueuer)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/enqueue", strings.NewReader(`{"content":[{"type":"text","text":"hi"}]}`))
	req.Header.Set("Idempotency-Key", "key-1")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if enqueuer.called {
		t.Fatal("Enqueue called despite auth failure")
	}
}

type fakeResumer struct {
	called  bool
	request runtime.ResumeRequest
}

func (f *fakeResumer) ResumeRun(_ context.Context, runID session.RunID, request runtime.ResumeRequest) (runtime.Handle, error) {
	f.called = true
	f.request = request
	return resumeHandle{runID: runID}, nil
}

func TestResumeTargetedHandlerValidatesBoundsAndForwardsTargets(t *testing.T) {
	t.Parallel()

	resumer := &fakeResumer{}
	handler := ResumeTargetedHandler(nil, func(context.Context, *http.Request) (session.RunID, error) {
		return "run-resume-targeted", nil
	}, resumer)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/resume", strings.NewReader(`{"targets":{"interrupt-1":{"approve":true}}}`))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !resumer.called {
		t.Fatal("ResumeRun not called")
	}
	decision, ok := resumer.request.Targets["interrupt-1"].(map[string]any)
	if !ok || decision["approve"] != true {
		t.Fatalf("targets = %#v", resumer.request.Targets)
	}
	if rec.Header().Get("Eino-Agent-Run-ID") != "run-resume-targeted" {
		t.Fatalf("run header = %q", rec.Header().Get("Eino-Agent-Run-ID"))
	}
}

func TestResumeTargetedHandlerRejectsEmptyAndOversizedTargets(t *testing.T) {
	t.Parallel()

	resumer := &fakeResumer{}
	handler := ResumeTargetedHandler(nil, func(context.Context, *http.Request) (session.RunID, error) {
		return "run-resume-targeted", nil
	}, resumer)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/resume", strings.NewReader(`{"targets":{}}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (no targets)", rec.Code)
	}
	if resumer.called {
		t.Fatal("ResumeRun called despite no targets")
	}

	rec = httptest.NewRecorder()
	oversizedID := strings.Repeat("a", maxIDBytes+1)
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/resume", strings.NewReader(`{"targets":{"`+oversizedID+`":true}}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (oversized target id)", rec.Code)
	}
	if resumer.called {
		t.Fatal("ResumeRun called despite an oversized target id")
	}
}

func TestResumeTargetedHandlerNeverInfersPermissionFromTargetPossession(t *testing.T) {
	t.Parallel()

	resumer := &fakeResumer{}
	authRan := false
	handler := ResumeTargetedHandler(func(ctx context.Context, _ *http.Request) (context.Context, error) {
		authRan = true
		return ctx, ErrUnauthorized
	}, func(context.Context, *http.Request) (session.RunID, error) {
		t.Fatal("lookupRunID must not run when auth fails")
		return "", nil
	}, resumer)
	rec := httptest.NewRecorder()
	// A well-formed, in-bounds target id alone must not grant access: auth
	// still runs first and still fails.
	req := httptest.NewRequest(http.MethodPost, "/resume", strings.NewReader(`{"targets":{"interrupt-1":{"approve":true}}}`))
	handler.ServeHTTP(rec, req)
	if !authRan {
		t.Fatal("auth must run")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if resumer.called {
		t.Fatal("ResumeRun called despite auth failure -- permission inferred from target possession")
	}
}
