package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/plantask"
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

// TestDiscoverHandlerToolsEnforcesDeadlineAgainstAContextIgnoringFactory is
// round-two W6 review I6's actual enforcement proof (not merely that the
// probe's context carries a deadline, but that discoverHandlerTools itself
// returns promptly even when the factory never looks at ctx.Done() at
// all): a factory that sleeps well past discoveryProbeBudget and ignores
// ctx entirely must still make discoverHandlerTools return -- with an
// error, for a knownToolBearingHandlerKinds Kind -- within the budget plus
// a small scheduling margin, not after the factory eventually wakes up.
// discoveryProbeBudget is temporarily shrunk (it is a var precisely for
// this) so the test itself does not need to sleep for the production
// 5-second budget.
func TestDiscoverHandlerToolsEnforcesDeadlineAgainstAContextIgnoringFactory(t *testing.T) {
	original := discoveryProbeBudget
	discoveryProbeBudget = 50 * time.Millisecond
	defer func() { discoveryProbeBudget = original }()

	factoryReturned := make(chan struct{})
	factory := HandlerFactory(func(ctx context.Context, _ HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		// Deliberately ignores ctx: sleeps ten times the shrunk budget,
		// simulating a factory that blocks on e.g. network I/O without
		// ever checking its own context.
		time.Sleep(10 * discoveryProbeBudget)
		close(factoryReturned)
		return nil, errors.New("factory finally returned, far too late")
	})

	start := time.Now()
	_, err := discoverHandlerTools("slow-fs", HandlerKindFilesystem, factory)
	elapsed := time.Since(start)

	if !errors.Is(err, errHandlerDiscoveryFailed) {
		t.Fatalf("err = %v, want errHandlerDiscoveryFailed", err)
	}
	// Generous margin (10x the shrunk budget, still far less than the
	// factory's own 10x sleep) to absorb scheduler jitter in CI without
	// weakening the actual assertion: discoverHandlerTools must return
	// long before the ignored context's factory ever does.
	if margin := 10 * discoveryProbeBudget; elapsed > margin {
		t.Fatalf("discoverHandlerTools took %v, want at most %v (the ignoring factory must not be able to block it)", elapsed, margin)
	}
	select {
	case <-factoryReturned:
		t.Fatal("the ignoring factory already returned by the time the assertion ran -- test budget too generous, tighten it")
	default:
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

// TestIsWriteLikeToolNameClassifiesPlantaskCorrectly proves the round-two
// W6 review's HA-S3/RW-S3 fix: plantask's TaskCreate/TaskUpdate -- which
// genuinely change durable task state but contain none of the substring
// heuristic's markers ("write", "edit", "execute", "shell", "delete") --
// are now classified write-like via the explicit override table, while
// its read-only TaskList/TaskGet (and the substring heuristic's ordinary
// cases) are unaffected.
func TestIsWriteLikeToolNameClassifiesPlantaskCorrectly(t *testing.T) {
	cases := []struct {
		name      string
		writeLike bool
	}{
		{plantask.TaskCreateToolName, true},
		{plantask.TaskUpdateToolName, true},
		{plantask.TaskListToolName, false},
		{plantask.TaskGetToolName, false},
		{"write_file", true},
		{"edit_file", true},
		{"read_file", false},
		{"ls", false},
	}
	for _, tc := range cases {
		if got := isWriteLikeToolName(tc.name); got != tc.writeLike {
			t.Errorf("isWriteLikeToolName(%q) = %v, want %v", tc.name, got, tc.writeLike)
		}
	}
}
