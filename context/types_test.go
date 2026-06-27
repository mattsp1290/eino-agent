package agentcontext

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestAssemblerPropagatesIdentityAndBoundsItems(t *testing.T) {
	t.Parallel()

	wantIdentity := Identity{
		SessionID:  "session-1",
		RunID:      "run-1",
		AgentID:    "agent",
		ProviderID: model.ProviderID("provider"),
		ModelID:    model.ID("model"),
		Trace: TraceContext{
			TraceID: "trace",
			Attributes: map[string]string{
				"tenant": "test",
			},
		},
	}
	assembler := Assembler{
		Bounds: Bounds{MaxItems: 2, MaxBytesPerItem: 128, MaxTotalBytes: 256},
		Loaders: []Loader{
			LoaderFunc(func(_ context.Context, request Request) ([]Item, error) {
				if request.Identity.SessionID != wantIdentity.SessionID {
					t.Fatalf("session id = %q, want %q", request.Identity.SessionID, wantIdentity.SessionID)
				}
				request.Identity.Trace.Attributes["tenant"] = "mutated"
				return []Item{{
					SourceName: "project",
					Kind:       KindProjectInstructions,
					Content:    "read tests",
					URI:        "project://instructions",
				}}, nil
			}),
		},
	}

	bundle, err := assembler.LoadContext(context.Background(), Request{Identity: wantIdentity})
	if err != nil {
		t.Fatalf("LoadContext error = %v", err)
	}
	if bundle.TotalBytes <= len("read tests") {
		t.Fatalf("total bytes = %d, want more than content bytes", bundle.TotalBytes)
	}
	if got := wantIdentity.Trace.Attributes["tenant"]; got != "test" {
		t.Fatalf("loader mutated request identity attribute to %q", got)
	}
}

func TestAssemblerHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	assembler := Assembler{
		Loaders: []Loader{
			LoaderFunc(func(context.Context, Request) ([]Item, error) {
				called = true
				return nil, nil
			}),
		},
	}

	_, err := assembler.LoadContext(ctx, Request{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadContext error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("loader called after cancellation")
	}
}

func TestAssemblerHonorsCancellationAfterLoaderReturns(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	assembler := Assembler{
		Loaders: []Loader{
			LoaderFunc(func(context.Context, Request) ([]Item, error) {
				cancel()
				return []Item{{Content: "stale"}}, nil
			}),
		},
	}

	_, err := assembler.LoadContext(ctx, Request{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadContext error = %v, want context.Canceled", err)
	}
}

func TestAssemblerRejectsOversizedContext(t *testing.T) {
	t.Parallel()

	assembler := Assembler{
		Bounds: Bounds{MaxItems: 1, MaxBytesPerItem: 4, MaxTotalBytes: 8},
		Loaders: []Loader{
			LoaderFunc(func(context.Context, Request) ([]Item, error) {
				return []Item{{Content: "too large"}}, nil
			}),
		},
	}

	_, err := assembler.LoadContext(context.Background(), Request{})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("LoadContext error = %v, want ErrTooLarge", err)
	}
}

func TestAssemblerCountsReferenceMetadataInBounds(t *testing.T) {
	t.Parallel()

	assembler := Assembler{
		Bounds: Bounds{MaxItems: 1, MaxBytesPerItem: 32, MaxTotalBytes: 32},
		Loaders: []Loader{
			LoaderFunc(func(context.Context, Request) ([]Item, error) {
				return []Item{{
					SourceName: "attachment",
					URI:        "attachment://small",
					MIMEType:   "text/plain",
					Metadata: map[string]string{
						"description": "this metadata is intentionally too large",
					},
				}}, nil
			}),
		},
	}

	_, err := assembler.LoadContext(context.Background(), Request{})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("LoadContext error = %v, want ErrTooLarge", err)
	}
}

func TestAssemblerRejectsLocalOnlyReferences(t *testing.T) {
	t.Parallel()

	for _, uri := range []string{"/Users/matt/project/AGENTS.md", `C:\Users\matt\file.txt`, "file:///tmp/prompt.md"} {
		t.Run(uri, func(t *testing.T) {
			t.Parallel()
			assembler := Assembler{
				Loaders: []Loader{
					LoaderFunc(func(context.Context, Request) ([]Item, error) {
						return []Item{{SourceName: "bad", URI: uri}}, nil
					}),
				},
			}

			_, err := assembler.LoadContext(context.Background(), Request{})
			if !errors.Is(err, ErrLocalPath) {
				t.Fatalf("LoadContext error = %v, want ErrLocalPath", err)
			}
		})
	}
}

func TestAssemblerRejectsLocalOnlySourceLocationAndOptions(t *testing.T) {
	t.Parallel()

	tests := []Request{
		{Source: Source{Location: "/Users/matt/project/AGENTS.md"}},
		{Source: Source{Location: `\\server\share\prompt.md`}},
		{Source: Source{Options: map[string]string{"path": `C:\Users\matt\file.txt`}}},
		{Source: Source{Options: map[string]string{"attachment_uri": "file:///tmp/attachment.txt"}}},
	}
	for _, request := range tests {
		t.Run(request.Source.Location+request.Source.Options["path"]+request.Source.Options["attachment_uri"], func(t *testing.T) {
			t.Parallel()
			assembler := Assembler{
				Loaders: []Loader{
					LoaderFunc(func(context.Context, Request) ([]Item, error) {
						return []Item{{Content: "unused"}}, nil
					}),
				},
			}

			_, err := assembler.LoadContext(context.Background(), request)
			if !errors.Is(err, ErrLocalPath) {
				t.Fatalf("LoadContext error = %v, want ErrLocalPath", err)
			}
		})
	}
}

func TestAssemblerNormalizesNegativeBoundsToDefaults(t *testing.T) {
	t.Parallel()

	assembler := Assembler{
		Bounds: Bounds{MaxItems: -1, MaxBytesPerItem: -1, MaxTotalBytes: -1},
		Loaders: []Loader{
			LoaderFunc(func(context.Context, Request) ([]Item, error) {
				items := make([]Item, DefaultBounds().MaxItems+1)
				for i := range items {
					items[i] = Item{Content: "x"}
				}
				return items, nil
			}),
		},
	}

	_, err := assembler.LoadContext(context.Background(), Request{})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("LoadContext error = %v, want ErrTooLarge", err)
	}
}

func TestIdentityCloneCopiesTraceAttributes(t *testing.T) {
	t.Parallel()

	identity := Identity{
		SessionID: session.ID("session"),
		Trace: TraceContext{
			Attributes: map[string]string{"key": "value"},
		},
	}

	cloned := identity.Clone()
	cloned.Trace.Attributes["key"] = "changed"
	if identity.Trace.Attributes["key"] != "value" {
		t.Fatalf("identity trace attributes mutated to %q", identity.Trace.Attributes["key"])
	}
}
