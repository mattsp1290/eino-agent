package extension

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
)

func BenchmarkSnapshot(b *testing.B) {
	registry := newTestRegistry(nil)
	_, _ = registry.Mount(context.Background(), testComponent("bench"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		for index := 0; index < 10; index++ {
			if err := On(registrar, testNotice, spec("bench", string(rune('a'+index)), index, GlobalScope()), func(context.Context, testPayload) error { return nil }); err != nil {
				return err
			}
		}
		return nil
	}))
	b.ResetTimer()
	for range b.N {
		plan, _ := registry.Snapshot(GlobalScope())
		plan.Release()
	}
}

func BenchmarkNotifyZero(b *testing.B) {
	plan := &Plan{}
	for range b.N {
		Notify(plan, context.Background(), testNotice, testPayload{})
	}
}

func BenchmarkNotifyTen(b *testing.B) {
	registry := newTestRegistry(nil)
	_, _ = registry.Mount(context.Background(), testComponent("bench"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		for index := 0; index < 10; index++ {
			if err := On(registrar, testNotice, spec("bench", string(rune('a'+index)), index, GlobalScope()), func(context.Context, testPayload) error { return nil }); err != nil {
				return err
			}
		}
		return nil
	}))
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	b.ResetTimer()
	for range b.N {
		Notify(plan, context.Background(), testNotice, testPayload{})
	}
}

func BenchmarkInvokeTen(b *testing.B) {
	registry := newTestRegistry(nil)
	for index := 0; index < 10; index++ {
		id := string(rune('a' + index))
		_, _ = registry.Mount(context.Background(), testComponent(id), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return OnAround(registrar, testAround, spec(id, id, index, GlobalScope()), func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
				return next(ctx, input)
			})
		}))
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	b.ResetTimer()
	for range b.N {
		_, _ = InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) { return "ok", nil })
	}
}

func BenchmarkConcurrentMountSnapshotClose(b *testing.B) {
	registry := newTestRegistry(nil)
	b.RunParallel(func(parallel *testing.PB) {
		var sequence atomic.Uint64
		for parallel.Next() {
			id := "bench-" + strconv.FormatUint(sequence.Add(1), 10)
			mount, err := registry.Mount(context.Background(), testComponent(id), InstallerFunc(func(_ context.Context, registrar Registrar) error {
				return On(registrar, testNotice, spec(id, "notice", 0, GlobalScope()), func(context.Context, testPayload) error { return nil })
			}))
			if err != nil {
				continue
			}
			plan, _ := registry.Snapshot(GlobalScope())
			plan.Release()
			_ = mount.Close(context.Background())
		}
	})
}

func FuzzSessionScope(f *testing.F) {
	f.Add("session-1", "session-1")
	f.Add("session-1", "session-2")
	f.Fuzz(func(t *testing.T, registered, target string) {
		if registered == "" || target == "" {
			t.Skip()
		}
		applies := scopeApplies(SessionScope(registered), SessionScope(target))
		if applies != (registered == target) {
			t.Fatalf("scope result = %t", applies)
		}
	})
}
