package wasmext

import (
	"context"
	"errors"

	"github.com/mattsp1290/eino-agent/extension"
)

type testExtensionRegistry struct {
	*extension.Registry[struct{}]
}

func newTestExtensionRegistry(reporter extension.Reporter) *testExtensionRegistry {
	return &testExtensionRegistry{Registry: extension.NewRegistry[struct{}](reporter)}
}

func (r *testExtensionRegistry) Mount(ctx context.Context, component extension.Component, installer extension.Installer) (*extension.Mount[struct{}], error) {
	prepared, err := r.PrepareMount(ctx, component, installer)
	if err != nil {
		return nil, err
	}
	mount, err := r.CommitMount(prepared, struct{}{}, nil, nil)
	if err != nil {
		return nil, errors.Join(err, prepared.Rollback(ctx))
	}
	return mount, nil
}

func (r *testExtensionRegistry) Snapshot(target extension.Scope) (*extension.Plan, error) {
	snapshot, err := r.Registry.Snapshot(target)
	if err != nil {
		return nil, err
	}
	return snapshot.Dispatch(), nil
}
