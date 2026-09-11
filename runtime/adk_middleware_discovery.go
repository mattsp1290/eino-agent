package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/cloudwego/eino/adk"
	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/skill"
	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"
)

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

func isWriteLikeToolName(name string) bool {
	lower := strings.ToLower(name)
	for _, marker := range []string{"write", "edit", "execute", "shell", "delete"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// discoverHandlerTools probes factory once, in a bounded, workspace- and
// session-independent construction context (stub backends and one stub
// deferred tool, no real store/model/IDs), purely to enumerate the tools
// its middleware contributes via BeforeAgent. This is sufficient because
// every one of this package's own eight recipes' tool lists are static per
// configuration (e.g. filesystem always registers ls/read_file/write_file/
// edit_file/glob/grep once Backend is non-nil, regardless of what is
// actually in that backend); a factory or BeforeAgent call that errors
// during probing (e.g. summarization, which legitimately requires a real
// Model/Store and injects no tools anyway) is treated as "declares zero
// tools", not a plan-compile failure.
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
func discoverHandlerTools(handlerID string, factory HandlerFactory) []HandlerToolSpec {
	if factory == nil {
		return nil
	}
	ctx := context.Background()
	build, cleanup := handlerProbeBuildContext()
	defer cleanup()
	mw, err := factory(ctx, build)
	if err != nil || mw == nil {
		return nil
	}
	_, runCtx, err := mw.BeforeAgent(ctx, &adk.ChatModelAgentContext{})
	if err != nil || runCtx == nil {
		return nil
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
	_ = handlerID // reserved for a future per-handler probe scoping/log tag
	return specs
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
// HandlerBuildContext discoverHandlerTools uses; cleanup removes the
// temporary scratch directory the writable-backend stubs need (plantask/
// reduction backends require a real directory to construct against, even
// though probing never writes through them).
func handlerProbeBuildContext() (HandlerBuildContext, func()) {
	root, err := os.MkdirTemp("", "eino-agent-handler-probe-*")
	cleanup := func() {
		if root != "" {
			_ = os.RemoveAll(root)
		}
	}
	build := HandlerBuildContext{
		FilesystemBackend: stubFilesystemBackend{},
		SkillBackend:      stubSkillBackend{},
		DeferredTools:     []tool.BaseTool{probeStubTool{}},
	}
	if err == nil {
		if planTaskBackend, backendErr := newWritableWorkspaceBackend(root, "plantask"); backendErr == nil {
			build.PlanTaskBackend = planTaskBackend
		}
		if reductionBackend, backendErr := newWritableWorkspaceBackend(root, "reduction"); backendErr == nil {
			build.ReductionBackend = reductionBackend
		}
	}
	return build, cleanup
}

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

type stubSkillBackend struct{}

func (stubSkillBackend) List(context.Context) ([]skill.FrontMatter, error) { return nil, nil }
func (stubSkillBackend) Get(context.Context, string) (skill.Skill, error) {
	return skill.Skill{}, errors.New("stub skill backend has no skills")
}
