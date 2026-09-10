package runtime

import (
	"context"
	"sync"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func assertConcurrentKeyedAdmission(t *testing.T, ctx context.Context, stores []session.Store, sessionID session.ID) {
	t.Helper()
	const contenders = 8
	if len(stores) < 2 {
		t.Fatal("concurrency test requires independent stores")
	}
	ids := &sequenceIDs{}
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	newOrchestrator := func(target session.Store) *StreamingOrchestrator {
		return mustConfiguredOrchestrator(
			WithStore(target), WithModelResolver(resolvedModel{streamer: streamer}),
			WithIDGenerator(ids), WithRunPlanProvider(emptyTestRunPlanProvider()),
		)
	}
	request := Request{SessionID: sessionID, AdmissionKey: "same-event", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	results := make(chan AdmissionResult, contenders)
	errs := make(chan error, contenders)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index := range contenders {
		wg.Add(1)
		go func(orch *StreamingOrchestrator) {
			defer wg.Done()
			<-start
			result, err := orch.Start(ctx, request)
			results <- result
			errs <- err
		}(newOrchestrator(stores[index%len(stores)]))
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	newCount := 0
	var receipt session.AdmissionReceipt
	for result := range results {
		if result.Disposition == AdmissionNew {
			newCount++
			<-result.Handle.Done()
		}
		if receipt == (session.AdmissionReceipt{}) {
			receipt = result.Receipt
		} else if result.Receipt != receipt {
			t.Fatalf("receipts differ: %+v and %+v", receipt, result.Receipt)
		}
	}
	if newCount != 1 {
		t.Fatalf("new admissions=%d, want 1", newCount)
	}
}
