package composition

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
)

func testHandlerFactory() runtime.HandlerFactory {
	return func(context.Context, runtime.HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		return &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}, nil
	}
}

func TestRegistrarHandlerRejectsInvalidRegistrations(t *testing.T) {
	cases := map[string]HandlerRegistration{
		"missing_kind":    {ID: "h1", Scope: extension.GlobalScope(), Descriptor: HandlerDescriptor{Version: "1"}, Factory: testHandlerFactory()},
		"missing_version": {ID: "h1", Scope: extension.GlobalScope(), Descriptor: HandlerDescriptor{Kind: "agentsmd"}, Factory: testHandlerFactory()},
		"missing_factory": {ID: "h1", Scope: extension.GlobalScope(), Descriptor: HandlerDescriptor{Kind: "agentsmd", Version: "1"}},
		"invalid_id":      {ID: "bad\x00id", Scope: extension.GlobalScope(), Descriptor: HandlerDescriptor{Kind: "agentsmd", Version: "1"}, Factory: testHandlerFactory()},
		"malformed_config": {
			ID: "h1", Scope: extension.GlobalScope(), Descriptor: HandlerDescriptor{Kind: "agentsmd", Version: "1", Config: json.RawMessage(`{not json`)}, Factory: testHandlerFactory(),
		},
	}
	for name, registration := range cases {
		t.Run(name, func(t *testing.T) {
			registrar := &Registrar{}
			if err := registrar.Handler(registration); !errors.Is(err, extension.ErrInvalidRegistration) {
				t.Fatalf("Handler error = %v, want ErrInvalidRegistration", err)
			}
		})
	}
}

func TestRegistrarHandlerRejectsDuplicateIDInSameScope(t *testing.T) {
	registrar := &Registrar{}
	registration := HandlerRegistration{ID: "h1", Scope: extension.GlobalScope(), Descriptor: HandlerDescriptor{Kind: "agentsmd", Version: "1"}, Factory: testHandlerFactory()}
	if err := registrar.Handler(registration); err != nil {
		t.Fatal(err)
	}
	if err := registrar.Handler(registration); !errors.Is(err, extension.ErrDuplicateRegistration) {
		t.Fatalf("second Handler error = %v, want ErrDuplicateRegistration", err)
	}
}

func TestHandlerConfigHashIsCanonicalAndDeterministic(t *testing.T) {
	a, err := handlerConfigHash(json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := handlerConfigHash(json.RawMessage(`{"b":2,"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("hash differs by key order: %s vs %s", a, b)
	}
	c, err := handlerConfigHash(json.RawMessage(`{"a":1,"b":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if a == c {
		t.Fatal("hash did not change for different config content")
	}
	empty, err := handlerConfigHash(nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty == "" {
		t.Fatal("empty config produced no hash")
	}
}

// TestMountHandlerAppearsInAcquiredRunPlanAgentHandlers proves the full
// composition-to-runtime path: a mounted Handler registration is visible on
// the acquired RunPlan's AgentHandlers, ordered, with its identity (not the
// factory closure) carried through.
func TestMountHandlerAppearsInAcquiredRunPlanAgentHandlers(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), component("handler-component"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Handler(HandlerRegistration{
			ID: "agentsmd-1", Order: 5, Scope: extension.GlobalScope(),
			Descriptor: HandlerDescriptor{Kind: "agentsmd", Version: "1", Config: json.RawMessage(`{"agents_md_files":["AGENTS.md"]}`)},
			Factory:    testHandlerFactory(),
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()

	handlers := plan.AgentHandlers()
	if len(handlers) != 1 {
		t.Fatalf("AgentHandlers = %+v, want 1", handlers)
	}
	if handlers[0].ID != "agentsmd-1" || handlers[0].Kind != "agentsmd" || handlers[0].Version != "1" || handlers[0].Order != 5 {
		t.Fatalf("handler identity = %+v", handlers[0])
	}
	if handlers[0].Factory == nil {
		t.Fatal("handler factory is nil")
	}
	descriptor := plan.Descriptor()
	if len(descriptor.Components) != 1 || len(descriptor.Components[0].AgentHandlers) != 1 {
		t.Fatalf("sealed descriptor components = %+v", descriptor.Components)
	}
	if descriptor.Components[0].AgentHandlers[0].ConfigHash == "" {
		t.Fatal("sealed AgentHandlers[0].ConfigHash is empty")
	}
}
