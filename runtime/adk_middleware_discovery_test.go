package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	einoschema "github.com/cloudwego/eino/schema"
)

// TestDiscoverHandlerToolsFailsClosedForKnownToolBearingKind proves S2's
// fix: a construction error for a Kind this package knows is always
// tool-bearing (filesystem/plantask/skill/toolsearch) fails plan compile
// closed instead of silently sealing zero tools.
func TestDiscoverHandlerToolsFailsClosedForKnownToolBearingKind(t *testing.T) {
	boom := errors.New("boom: misconfigured filesystem recipe")
	factory := HandlerFactory(func(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		return nil, boom
	})
	_, err := discoverHandlerTools("fs-1", HandlerKindFilesystem, factory)
	if err == nil {
		t.Fatal("known tool-bearing kind's construction error was silently absorbed")
	}
	if !errors.Is(err, errHandlerDiscoveryFailed) {
		t.Fatalf("err = %v, want errHandlerDiscoveryFailed", err)
	}
}

// TestDiscoverHandlerToolsDegradesGracefullyForOtherKinds proves the
// documented, bounded limitation still holds for a Kind this package does
// NOT know is always tool-bearing (e.g. summarization, which legitimately
// requires a real Model/Store and injects no tools anyway): a construction
// error degrades to "declares zero tools", not a plan-compile failure.
func TestDiscoverHandlerToolsDegradesGracefullyForOtherKinds(t *testing.T) {
	factory := HandlerFactory(func(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		return nil, errors.New("summarization requires a real store")
	})
	specs, err := discoverHandlerTools("summarization-1", HandlerKindSummarization, factory)
	if err != nil {
		t.Fatalf("err = %v, want nil (graceful degradation)", err)
	}
	if len(specs) != 0 {
		t.Fatalf("specs = %+v, want empty", specs)
	}
}

// TestDiscoverHandlerToolsBoundsTheProbeContext proves I6's "bounded
// deadline" fix without actually sleeping past it: the context handed to
// the factory carries a deadline no later than discoveryProbeBudget from
// now, so a factory that blocks (e.g. on network I/O) cannot hang plan
// compilation forever.
func TestDiscoverHandlerToolsBoundsTheProbeContext(t *testing.T) {
	var sawDeadline time.Time
	var hasDeadline bool
	factory := HandlerFactory(func(ctx context.Context, _ HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		sawDeadline, hasDeadline = ctx.Deadline()
		return nil, errors.New("stop here, we only need the context")
	})
	if _, err := discoverHandlerTools("bounded-1", "test-non-strict-kind", factory); err != nil {
		t.Fatalf("err = %v, want nil (non-strict kind degrades gracefully)", err)
	}
	if !hasDeadline {
		t.Fatal("factory's context carried no deadline")
	}
	if remaining := time.Until(sawDeadline); remaining <= 0 || remaining > discoveryProbeBudget {
		t.Fatalf("deadline %v from now, want within (0, %v]", remaining, discoveryProbeBudget)
	}
}

// TestHandlerProbeBuildContextNeverTouchesTheFilesystem proves I6's
// "no filesystem side effects" fix: the writable scratch backends
// handlerProbeBuildContext supplies are the in-memory probeWritableBackend,
// never writableWorkspaceBackend (which would require a real, on-disk
// directory).
func TestHandlerProbeBuildContextNeverTouchesTheFilesystem(t *testing.T) {
	build := handlerProbeBuildContext()
	if _, ok := build.PlanTaskBackend.(probeWritableBackend); !ok {
		t.Fatalf("PlanTaskBackend = %T, want probeWritableBackend", build.PlanTaskBackend)
	}
	if _, ok := build.ReductionBackend.(probeWritableBackend); !ok {
		t.Fatalf("ReductionBackend = %T, want probeWritableBackend", build.ReductionBackend)
	}
	// A write through the in-memory backend must succeed without ever
	// touching a real directory -- proving it is genuinely functional, not
	// just a nil stub.
	if err := build.PlanTaskBackend.Write(context.Background(), &adkfilesystem.WriteRequest{FilePath: "probe.json", Content: "{}"}); err != nil {
		t.Fatalf("in-memory PlanTaskBackend.Write error = %v", err)
	}
}
