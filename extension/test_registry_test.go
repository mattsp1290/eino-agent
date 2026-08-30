package extension

import (
	"context"
	"errors"
)

type testRegistry struct {
	*Registry[struct{}]
}

func newTestRegistry(reporter Reporter, points ...Point) *testRegistry {
	catalog := append([]Point{testNotice, testAround}, points...)
	registry, err := NewRegistry[struct{}](reporter, catalog...)
	if err != nil {
		panic(err)
	}
	return &testRegistry{Registry: registry}
}

func (r *testRegistry) Mount(ctx context.Context, component Component, installer Installer) (*Mount[struct{}], error) {
	prepared, err := r.PrepareMount(ctx, component, installer)
	if err != nil {
		return nil, err
	}
	mount, err := r.Registry.CommitMount(prepared, struct{}{}, nil, nil)
	if err != nil {
		return nil, errorsJoin(err, prepared.Rollback(ctx))
	}
	return mount, nil
}

func (r *testRegistry) CommitMount(prepared *PreparedMount[struct{}]) (*Mount[struct{}], error) {
	return r.Registry.CommitMount(prepared, struct{}{}, nil, nil)
}

func (r *testRegistry) Snapshot(target Scope) (*Plan, error) {
	snapshot, err := r.Registry.Snapshot(target)
	if err != nil {
		return nil, err
	}
	return snapshot.Dispatch(), nil
}

func (r *testRegistry) SnapshotInstances(target Scope, ids []string) (*Plan, error) {
	snapshot, err := r.Registry.SnapshotInstances(target, ids)
	if err != nil {
		return nil, err
	}
	return snapshot.Dispatch(), nil
}

func activeMountCount(r *testRegistry) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mounts)
}

func errorsJoin(primary, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	return errors.Join(primary, cleanup)
}
