package model

import (
	"context"
	"errors"
	"testing"

	einomodel "github.com/cloudwego/eino/components/model"
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
	if resolved.Provider.ID != "fake" || resolved.Model.ID != "m1" || resolved.Client == nil {
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
	nestedMessageExtra := map[string]any{"labels": []any{"original"}}
	nestedToolExtra := map[string]any{"labels": []any{"original"}}
	params := einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
		"text": {Type: einoschema.String, Required: true},
	})
	request := Request{
		Identity: Identity{TraceAttributes: map[string]string{"trace": "value"}},
		Messages: []*einoschema.Message{{
			Content: "hello",
			Extra:   map[string]any{"nested": nestedMessageExtra},
			ToolCalls: []einoschema.ToolCall{{
				Index: &index,
				Extra: map[string]any{"nested": nestedToolExtra},
			}},
			UserInputMultiContent: []einoschema.MessageInputPart{{
				Type:  einoschema.ChatMessagePartTypeImageURL,
				Extra: map[string]any{"labels": []any{"part-original"}},
				Image: &einoschema.MessageInputImage{MessagePartCommon: einoschema.MessagePartCommon{URL: &url, Extra: map[string]any{"labels": []any{"image-original"}}}},
			}},
		}},
		Tools: []*einoschema.ToolInfo{{
			Name:        "tool",
			ParamsOneOf: params,
			Extra:       map[string]any{"nested": map[string]any{"labels": []any{"original"}}},
		}},
		Options: map[string]string{"temperature": "0"},
	}

	cloned := request.Clone()
	cloned.Identity.TraceAttributes["trace"] = "changed"
	cloned.Messages[0].Content = "changed"
	cloned.Messages[0].Extra["nested"].(map[string]any)["labels"].([]any)[0] = "changed"
	*cloned.Messages[0].ToolCalls[0].Index = 4
	cloned.Messages[0].ToolCalls[0].Extra["nested"].(map[string]any)["labels"].([]any)[0] = "changed"
	*cloned.Messages[0].UserInputMultiContent[0].Image.URL = "changed"
	cloned.Messages[0].UserInputMultiContent[0].Extra["labels"].([]any)[0] = "changed"
	//nolint:staticcheck // Deep cloning must cover Eino's still-serialized legacy media metadata.
	cloned.Messages[0].UserInputMultiContent[0].Image.Extra["labels"].([]any)[0] = "changed"
	cloned.Tools[0].Name = "changed"
	cloned.Tools[0].Extra["nested"].(map[string]any)["labels"].([]any)[0] = "changed"
	cloned.Options["temperature"] = "1"

	if request.Identity.TraceAttributes["trace"] != "value" ||
		request.Messages[0].Content != "hello" ||
		nestedMessageExtra["labels"].([]any)[0] != "original" ||
		index != 3 ||
		nestedToolExtra["labels"].([]any)[0] != "original" ||
		url != "https://example.test/image.png" ||
		request.Messages[0].UserInputMultiContent[0].Extra["labels"].([]any)[0] != "part-original" ||
		//nolint:staticcheck // Deep cloning must cover Eino's still-serialized legacy media metadata.
		request.Messages[0].UserInputMultiContent[0].Image.Extra["labels"].([]any)[0] != "image-original" ||
		request.Tools[0].Name != "tool" ||
		request.Tools[0].Extra["nested"].(map[string]any)["labels"].([]any)[0] != "original" ||
		request.Options["temperature"] != "0" {
		t.Fatalf("request mutated after clone: %#v", request)
	}
	if cloned.Tools[0].ParamsOneOf == nil || cloned.Tools[0].ParamsOneOf == params {
		t.Fatal("tool parameter schema was not cloned")
	}
}

func TestRequestCloneDistinguishesAliasedSliceHeadersByShape(t *testing.T) {
	t.Parallel()

	type fullFirst struct {
		Full   []string
		Prefix []string
	}
	type prefixFirst struct {
		Prefix []string
		Full   []string
	}
	base := []string{"first", "second", "third"}
	request := Request{Messages: []*einoschema.Message{{Extra: map[string]any{
		"full_first":   fullFirst{Full: base[:3], Prefix: base[:1]},
		"prefix_first": prefixFirst{Prefix: base[:1], Full: base[:3]},
	}}}}

	cloned := request.Clone()
	first := cloned.Messages[0].Extra["full_first"].(fullFirst)
	second := cloned.Messages[0].Extra["prefix_first"].(prefixFirst)
	if len(first.Full) != 3 || len(first.Prefix) != 1 || len(second.Full) != 3 || len(second.Prefix) != 1 {
		t.Fatalf("cloned slice lengths = first(%d,%d) second(%d,%d)", len(first.Full), len(first.Prefix), len(second.Full), len(second.Prefix))
	}
	if first.Full[2] != "third" || first.Prefix[0] != "first" || second.Full[2] != "third" || second.Prefix[0] != "first" {
		t.Fatalf("cloned slice values = first(%v,%v) second(%v,%v)", first.Full, first.Prefix, second.Full, second.Prefix)
	}
	first.Full[0] = "changed"
	second.Prefix[0] = "also changed"
	if base[0] != "first" {
		t.Fatalf("source backing slice mutated: %v", base)
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

func (a *testAdapter) Build(_ context.Context, _ Selection, runtime Runtime) (einomodel.ToolCallingChatModel, error) {
	a.runtime = runtime
	return testModel{}, nil
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

type testModel struct{}

func (testModel) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	return einoschema.AssistantMessage("", nil), nil
}

func (testModel) Stream(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	return einoschema.StreamReaderFromArray([]*einoschema.Message{}), nil
}

func (testModel) WithTools([]*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	return testModel{}, nil
}
