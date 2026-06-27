package plugins

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
)

func TestRegistryRejectsInvalidPlugins(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if err := registry.Register(nil); !errors.Is(err, ErrInvalidPlugin) {
		t.Fatalf("Register nil error = %v, want ErrInvalidPlugin", err)
	}
	if err := registry.Register(testPlugin{name: ""}); !errors.Is(err, ErrInvalidPlugin) {
		t.Fatalf("Register empty name error = %v, want ErrInvalidPlugin", err)
	}
	if err := registry.Register(testPlugin{name: "dup"}); err != nil {
		t.Fatalf("Register dup: %v", err)
	}
	if err := registry.Register(testPlugin{name: "dup"}); !errors.Is(err, ErrInvalidPlugin) {
		t.Fatalf("Register duplicate error = %v, want ErrInvalidPlugin", err)
	}
}

func TestRegistryApplyAllRequiresSnapshot(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if _, err := registry.ApplyAll(context.Background(), nil); !errors.Is(err, ErrInvalidPlugin) {
		t.Fatalf("ApplyAll nil snapshot error = %v, want ErrInvalidPlugin", err)
	}
}

type testPlugin struct {
	name string
}

func (p testPlugin) Name() string {
	return p.name
}

func (p testPlugin) Apply(context.Context, *config.Snapshot) error {
	return nil
}
