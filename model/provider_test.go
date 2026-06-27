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
	if adapter.runtime.Env["key"] != "value" {
		t.Fatalf("adapter runtime env = %q, want value", adapter.runtime.Env["key"])
	}
	if resolved.Provider.ID != "fake" || resolved.Model.ID != "m1" || resolved.Client == nil {
		t.Fatalf("resolved = %#v", resolved)
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
