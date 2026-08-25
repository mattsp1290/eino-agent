package model

import (
	"context"
	"errors"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"
)

func TestAdapterResolverResolvesAdapterAndClonesRuntime(t *testing.T) {
	t.Parallel()

	adapter := &testAdapter{
		provider: Provider{ID: "fake", Name: "Fake"},
		models:   []Descriptor{{ID: "m1", ProviderID: "fake"}},
	}
	runtime := Runtime{
		Env:     map[string]string{"key": "value"},
		Auth:    map[string]string{"token": "secret"},
		Options: map[string]string{"temperature": "0"},
	}
	resolved, err := (AdapterResolver{Adapters: []Adapter{adapter}}).Resolve(context.Background(), Selection{ProviderID: "fake", ModelID: "m1"}, runtime)
	if err != nil {
		t.Fatalf("Resolve error = %v", err)
	}
	runtime.Env["key"] = "changed"
	runtime.Auth["token"] = "changed"
	runtime.Options["temperature"] = "1"
	if adapter.runtime.Env["key"] != "value" {
		t.Fatalf("adapter runtime env = %q, want value", adapter.runtime.Env["key"])
	}
	if adapter.runtime.Auth["token"] != "secret" || adapter.runtime.Options["temperature"] != "0" {
		t.Fatalf("adapter runtime leaked caller mutation: %#v", adapter.runtime)
	}
	if resolved.Provider.ID != "fake" || resolved.Model.ID != "m1" || resolved.Streamer == nil {
		t.Fatalf("resolved = %#v", resolved)
	}
	if _, ok := resolved.Provider.Options["token"]; ok {
		t.Fatalf("resolved provider options exposed auth token: %#v", resolved.Provider.Options)
	}
	if resolved.Streamer != adapter {
		t.Fatalf("resolved streamer = %#v, want adapter", resolved.Streamer)
	}
}

func TestAdapterResolverReportsUnavailableOptionalAdapter(t *testing.T) {
	t.Parallel()

	adapter := &testAdapter{
		provider: Provider{ID: "fake"},
		availableErr: Error{
			Code:    "missing_dependency",
			Message: "optional provider unavailable",
			Cause:   ErrProviderUnavailable,
		},
	}

	_, err := (AdapterResolver{Adapters: []Adapter{adapter}}).Resolve(context.Background(), Selection{ProviderID: "fake", ModelID: "m1"}, Runtime{})
	if !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("Resolve error = %v, want ErrProviderUnavailable", err)
	}
}

func TestAdapterResolverReportsMissingProvider(t *testing.T) {
	t.Parallel()

	_, err := (AdapterResolver{}).Resolve(context.Background(), Selection{ProviderID: "missing", ModelID: "m1"}, Runtime{})
	if !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("Resolve error = %v, want ErrProviderUnavailable", err)
	}
}

func TestAdapterResolverClonesCatalogDescriptor(t *testing.T) {
	t.Parallel()

	source := Descriptor{
		ID:         "m1",
		ProviderID: "fake",
		Capabilities: map[string]bool{
			"tools": true,
		},
		Options: map[string]string{
			"family": "test",
		},
	}
	catalog := testCatalog{descriptor: source}
	adapter := &testAdapter{provider: Provider{ID: "fake"}}

	resolved, err := (AdapterResolver{Adapters: []Adapter{adapter}, Catalog: catalog}).Resolve(context.Background(), Selection{ProviderID: "fake", ModelID: "m1"}, Runtime{})
	if err != nil {
		t.Fatalf("Resolve error = %v", err)
	}
	resolved.Model.Capabilities["tools"] = false
	resolved.Model.Options["family"] = "mutated"
	if !source.Capabilities["tools"] || source.Options["family"] != "test" {
		t.Fatalf("source descriptor mutated: %#v", source)
	}
}

func TestRequestCloneCopiesMutableEinoObjects(t *testing.T) {
	t.Parallel()

	index := 3
	url := "https://example.test/image.png"
	params := einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
		"text": {Type: einoschema.String, Required: true},
	})
	request := Request{
		Identity: Identity{TraceAttributes: map[string]string{"trace": "value"}},
		Messages: []*einoschema.Message{{
			Content: "hello",
			ToolCalls: []einoschema.ToolCall{{
				Index: &index,
			}},
			UserInputMultiContent: []einoschema.MessageInputPart{{
				Type:  einoschema.ChatMessagePartTypeImageURL,
				Image: &einoschema.MessageInputImage{MessagePartCommon: einoschema.MessagePartCommon{URL: &url}},
			}},
		}},
		Tools: []*einoschema.ToolInfo{{
			Name:        "tool",
			ParamsOneOf: params,
		}},
		Options: map[string]string{"temperature": "0"},
	}

	cloned, err := request.Clone()
	if err != nil {
		t.Fatal(err)
	}
	cloned.Identity.TraceAttributes["trace"] = "changed"
	cloned.Messages[0].Content = "changed"
	*cloned.Messages[0].ToolCalls[0].Index = 4
	*cloned.Messages[0].UserInputMultiContent[0].Image.URL = "changed"
	cloned.Tools[0].Name = "changed"
	cloned.Options["temperature"] = "1"

	if request.Identity.TraceAttributes["trace"] != "value" ||
		request.Messages[0].Content != "hello" ||
		index != 3 ||
		url != "https://example.test/image.png" ||
		request.Tools[0].Name != "tool" ||
		request.Options["temperature"] != "0" {
		t.Fatalf("request mutated after clone: %#v", request)
	}
	if cloned.Tools[0].ParamsOneOf == nil || cloned.Tools[0].ParamsOneOf == params {
		t.Fatal("tool parameter schema was not cloned")
	}
}

func TestRequestCloneRejectsUnsupportedMetadata(t *testing.T) {
	t.Parallel()
	tests := map[string]Request{
		"message extra": {Messages: []*einoschema.Message{{Extra: map[string]any{"value": 1}}}},
		"tool extra":    {Tools: []*einoschema.ToolInfo{{Name: "tool", Extra: map[string]any{"value": 1}}}},
		"streaming metadata": {Messages: []*einoschema.Message{{AssistantGenMultiContent: []einoschema.MessageOutputPart{{
			Type: einoschema.ChatMessagePartTypeText, Text: "chunk", StreamingMeta: &einoschema.MessageStreamingMeta{Index: 1},
		}}}}},
	}
	for name, request := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := request.Clone(); err == nil {
				t.Fatal("Clone succeeded for unsupported metadata")
			}
		})
	}
}

type testAdapter struct {
	provider     Provider
	models       []Descriptor
	runtime      Runtime
	availableErr error
}

func (a *testAdapter) Provider() Provider {
	return a.provider
}

func (a *testAdapter) Models(context.Context) ([]Descriptor, error) {
	return a.models, nil
}

func (a *testAdapter) Build(_ context.Context, _ Selection, runtime Runtime) (Streamer, error) {
	a.runtime = runtime
	return a, nil
}

func (a *testAdapter) Available(context.Context, Runtime) error {
	return a.availableErr
}

func (a *testAdapter) StreamProvider(context.Context, Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	return einoschema.StreamReaderFromArray([]*einoschema.Message{}), nil
}

type testCatalog struct {
	descriptor Descriptor
}

func (c testCatalog) ListProviders(context.Context) ([]Provider, error) {
	return []Provider{{ID: c.descriptor.ProviderID}}, nil
}

func (c testCatalog) ListModels(context.Context, ProviderID) ([]Descriptor, error) {
	return []Descriptor{c.descriptor}, nil
}

func (c testCatalog) GetModel(context.Context, ProviderID, ID) (Descriptor, error) {
	return c.descriptor, nil
}

func (c testCatalog) DefaultModel(context.Context) (Selection, error) {
	return Selection{ProviderID: c.descriptor.ProviderID, ModelID: c.descriptor.ID}, nil
}
