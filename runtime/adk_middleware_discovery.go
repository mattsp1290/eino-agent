package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudwego/eino/adk"
	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/plantask"
	"github.com/cloudwego/eino/adk/middlewares/skill"
	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"
)

// discoveryProbeBudget bounds how long one handler factory's compile-time
// tool-discovery probe (factory construction plus its first BeforeAgent
// call) may run. A third-party factory that blocks -- on network I/O, say
// -- must not hang plan compilation for every run on that registry; a probe
// that exceeds this budget is treated as a construction error (see
// discoverHandlerTools).
const discoveryProbeBudget = 5 * time.Second

// HandlerToolSpec is one tool a typed ADK agent-handler middleware
// contributes, discovered once at plan-compile time (see
// discoverHandlerTools) and sealed into the plan fingerprint
// (session.AgentHandlerPlanIdentity.Tools). At real per-turn agent build
// time, adkEngine.buildAgent synthesizes a durable runtime.Tool for each
// sealed entry, whose Executor dispatches through the durable claim/
// permission/execute/settle pipeline to the live middleware's own tool
// instance (matched by name) -- so a handler tool is never bypassed the way
// an unsealed, ad-hoc BeforeAgent-injected tool would be, and
// prepareToolCalls/resolveToolCall resolves it like any other frozen tool.
type HandlerToolSpec struct {
	Name       string
	Info       *einoschema.ToolInfo
	SchemaHash string
	// WriteLike marks a tool this package's own bounded heuristic considers
	// state-changing (its name contains "write", "edit", "execute",
	// "shell", or "delete"), sealed with a Permissions tag
	// (handlerToolPermission) so the existing permission policy gates it
	// exactly like any other state-changing tool -- disabled unless a
	// permission explicitly grants it.
	WriteLike bool
}

// handlerToolPermission is the permission string a sealed, write-like
// handler tool is gated behind: disabled by default, exactly like any other
// state-changing tool, unless the host's permission policy explicitly
// grants it for this handler+tool.
func handlerToolPermission(handlerID, name string) string {
	return "handler:" + handlerID + ":" + name
}

// explicitWriteLikeTools names this package's own recipes' known tools
// whose write-like status the substring heuristic below gets wrong -- a
// converging finding from both round-one W6 reviewers (handler-authority
// reviewer S3, recipes-and-wit reviewer S3): plantask's TaskCreate/
// TaskUpdate genuinely change durable task state but contain none of
// isWriteLikeToolName's substring markers ("write", "edit", "execute",
// "shell", "delete"), so they were silently ungated even though a
// deny-by-default permission policy is configured. Consulted first, for
// exactness on tools this package actually knows about by name; every
// other tool (including any third-party Kind's) still falls back to the
// substring heuristic below.
var explicitWriteLikeTools = map[string]bool{
	plantask.TaskCreateToolName: true,
	plantask.TaskUpdateToolName: true,
}

func isWriteLikeToolName(name string) bool {
	if writeLike, known := explicitWriteLikeTools[name]; known {
		return writeLike
	}
	lower := strings.ToLower(name)
	for _, marker := range []string{"write", "edit", "execute", "shell", "delete"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// knownToolBearingHandlerKinds are this package's own recipe Kinds whose
// tool list is always non-empty once their required backend is available
// (filesystem always registers ls/read_file/..., plantask always registers
// its task tools, skill always registers "skill", toolsearch always
// registers its search tool) -- so a probe failure for one of these is a
// real misconfiguration, not a recipe (like summarization) that legitimately
// has no tools to discover in the first place. discoverHandlerTools fails
// plan compilation closed for these Kinds instead of silently sealing zero
// tools (see S2 in the W6 round-1 review).
var knownToolBearingHandlerKinds = map[string]bool{
	HandlerKindFilesystem: true, HandlerKindPlanTask: true, HandlerKindSkill: true, HandlerKindToolSearch: true,
}

// errHandlerDiscoveryFailed reports that compile-time tool discovery
// (discoverHandlerTools) failed for a handler Kind this package knows is
// always tool-bearing once configured correctly.
var errHandlerDiscoveryFailed = errors.New("agent handler tool discovery failed")

// discoverHandlerTools probes factory once, in a bounded (discoveryProbeBudget
// deadline), workspace- and session-independent construction context (stub
// backends and one stub deferred tool, no real store/model/IDs, no
// filesystem side effects -- see handlerProbeBuildContext), purely to
// enumerate the tools its middleware contributes via BeforeAgent. This is
// sufficient because every one of this package's own eight recipes' tool
// lists are static per configuration (e.g. filesystem always registers
// ls/read_file/write_file/edit_file/glob/grep once Backend is non-nil,
// regardless of what is actually in that backend).
//
// A factory or BeforeAgent call that errors or exceeds the deadline during
// probing fails plan compilation closed for a knownToolBearingHandlerKinds
// Kind (a misconfigured filesystem/plantask/skill/toolsearch recipe would
// otherwise be silently sealed with no tools and only fail at turn time --
// see S2 in the W6 round-1 review); for every other Kind (this package's
// own agentsmd/patchtoolcalls/reduction/summarization, and any third-party
// Kind) it degrades to "declares zero tools", matching the documented,
// bounded limitation below.
//
// This is a documented, bounded limitation of this discovery mechanism: a
// third-party handler Kind whose own tool list genuinely depends on data
// this stub probe cannot supply (a real session/model identity, live
// backend content) will be sealed with fewer tools than it might produce at
// real execution time, and any tool it then tries to add that is not in
// the sealed set fails that turn as a construction error (see
// adkEngine.buildAgentHandlers). Declaring tools statically in a future
// HandlerRegistration.Tools field (bypassing probing entirely) is the
// documented escape hatch for such a handler; it was not needed for this
// package's own eight recipes and was not added this pass.
func discoverHandlerTools(handlerID, kind string, factory HandlerFactory) ([]HandlerToolSpec, error) {
	if factory == nil {
		return nil, nil
	}
	strict := knownToolBearingHandlerKinds[kind]
	fail := func(stage string, err error) ([]HandlerToolSpec, error) {
		if strict {
			return nil, fmt.Errorf("%w: handler %q (%s) %s: %v", errHandlerDiscoveryFailed, handlerID, kind, stage, err)
		}
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), discoveryProbeBudget)
	defer cancel()
	build := handlerProbeBuildContext()
	mw, err := factory(ctx, build)
	if err != nil {
		return fail("factory construction", err)
	}
	if mw == nil {
		return fail("factory construction", errors.New("factory returned a nil middleware"))
	}
	if err := ctx.Err(); err != nil {
		return fail("factory construction", err)
	}
	_, runCtx, err := mw.BeforeAgent(ctx, &adk.ChatModelAgentContext{})
	if err != nil {
		return fail("BeforeAgent", err)
	}
	if runCtx == nil {
		return fail("BeforeAgent", errors.New("BeforeAgent returned a nil run context"))
	}
	if err := ctx.Err(); err != nil {
		return fail("BeforeAgent", err)
	}
	specs := make([]HandlerToolSpec, 0, len(runCtx.Tools))
	seen := make(map[string]bool, len(runCtx.Tools))
	for _, t := range runCtx.Tools {
		if t == nil {
			continue
		}
		info, infoErr := t.Info(ctx)
		if infoErr != nil || info == nil || info.Name == "" || info.Name == probeStubToolName || seen[info.Name] {
			continue
		}
		seen[info.Name] = true
		specs = append(specs, HandlerToolSpec{
			Name: info.Name, Info: info, SchemaHash: handlerToolSchemaHash(info), WriteLike: isWriteLikeToolName(info.Name),
		})
	}
	return specs, nil
}

func handlerToolSchemaHash(info *einoschema.ToolInfo) string {
	raw, err := json.Marshal(info)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

const probeStubToolName = "__handler_probe_stub__"

// handlerProbeBuildContext builds the bounded, stub-backed
// HandlerBuildContext discoverHandlerTools uses. Every backend is in-memory
// (stubFilesystemBackend/stubSkillBackend/probeWritableBackend): probing
// never touches the real filesystem, no temp directory is created or
// removed.
func handlerProbeBuildContext() HandlerBuildContext {
	return HandlerBuildContext{
		FilesystemBackend: stubFilesystemBackend{},
		SkillBackend:      stubSkillBackend{},
		DeferredTools:     []tool.BaseTool{probeStubTool{}},
		PlanTaskBackend:   newProbeWritableBackend(),
		ReductionBackend:  newProbeWritableBackend(),
	}
}

// probeWritableBackend is compile-time discovery's in-memory stand-in for
// writableWorkspaceBackend: it satisfies writableTaskBackend (plantask.Backend's
// full surface, and reduction.Backend's Write-only subset) purely in
// memory, via upstream's own adk/filesystem.InMemoryBackend plus a no-op
// Delete (InMemoryBackend has no Delete method of its own).
type probeWritableBackend struct {
	*adkfilesystem.InMemoryBackend
}

func newProbeWritableBackend() probeWritableBackend {
	return probeWritableBackend{InMemoryBackend: adkfilesystem.NewInMemoryBackend()}
}

func (probeWritableBackend) Delete(context.Context, *plantask.DeleteRequest) error { return nil }

var _ writableTaskBackend = probeWritableBackend{}

// probeStubTool is the single stub deferred tool handlerProbeBuildContext
// supplies so toolsearch's own "requires at least one deferred tool"
// precondition can be probed; its name is filtered out of
// discoverHandlerTools' results defensively (toolsearch's own tool list
// construction should never surface it directly, but the filter costs
// nothing and removes any doubt).
type probeStubTool struct{}

func (probeStubTool) Info(context.Context) (*einoschema.ToolInfo, error) {
	return &einoschema.ToolInfo{Name: probeStubToolName}, nil
}

func (probeStubTool) InvokableRun(context.Context, string, ...tool.Option) (string, error) {
	return "", errors.New("probe stub tool is not callable")
}

// stubFilesystemBackend/stubSkillBackend are non-functional Backend
// implementations used only to satisfy a recipe's "backend != nil"
// precondition during compile-time tool discovery -- see
// discoverHandlerTools' doc comment for why no real workspace content is
// needed for that purpose.
type stubFilesystemBackend struct{}

var (
	_ adkfilesystem.Backend = stubFilesystemBackend{}
	_ skill.Backend         = stubSkillBackend{}
)

func (stubFilesystemBackend) LsInfo(context.Context, *adkfilesystem.LsInfoRequest) ([]adkfilesystem.FileInfo, error) {
	return nil, nil
}
func (stubFilesystemBackend) Read(context.Context, *adkfilesystem.ReadRequest) (*adkfilesystem.FileContent, error) {
	return &adkfilesystem.FileContent{}, nil
}
func (stubFilesystemBackend) GrepRaw(context.Context, *adkfilesystem.GrepRequest) ([]adkfilesystem.GrepMatch, error) {
	return nil, nil
}
func (stubFilesystemBackend) GlobInfo(context.Context, *adkfilesystem.GlobInfoRequest) ([]adkfilesystem.FileInfo, error) {
	return nil, nil
}
func (stubFilesystemBackend) Write(context.Context, *adkfilesystem.WriteRequest) error {
	return errWorkspaceReadOnly
}
func (stubFilesystemBackend) Edit(context.Context, *adkfilesystem.EditRequest) error {
	return errWorkspaceReadOnly
}

// MultiModalRead makes the probe stub also satisfy
// adkfilesystem.MultiModalReader: filesystem.NewTyped's own Validate
// requires the backend to implement it whenever UseMultiModalRead is
// configured, so without this method probing would fail closed for that
// configuration and seal zero tools, leaving every real, runtime-built
// filesystem tool (ls, read_file, ...) unsealed and rejected by
// durableGuard.
func (stubFilesystemBackend) MultiModalRead(context.Context, *adkfilesystem.MultiModalReadRequest) (*adkfilesystem.MultiFileContent, error) {
	return &adkfilesystem.MultiFileContent{FileContent: &adkfilesystem.FileContent{}}, nil
}

var _ adkfilesystem.MultiModalReader = stubFilesystemBackend{}

type stubSkillBackend struct{}

func (stubSkillBackend) List(context.Context) ([]skill.FrontMatter, error) { return nil, nil }
func (stubSkillBackend) Get(context.Context, string) (skill.Skill, error) {
	return skill.Skill{}, errors.New("stub skill backend has no skills")
}
