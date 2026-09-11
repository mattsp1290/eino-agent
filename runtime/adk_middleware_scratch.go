package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"time"

	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/plantask"

	"github.com/mattsp1290/eino-agent/internal/workspace"
)

// writableWorkspaceBackend is a private, writable scratch area used by
// recipes that need their own persistent state (plantask's task list,
// reduction's offloaded tool-output files) -- unlike
// workspaceFilesystemBackend/workspaceSkillBackend (read-only views over the
// admitted workspace's real content), this backend owns and may freely
// create/modify/delete files under its own subdirectory, which is still
// contained within the admitted canonical workspace root (see
// resolveWorkspacePath) so it can never escape it.
//
// It implements plantask.Backend (LsInfo/Read/Write/Delete, all type-aliased
// to adk/filesystem's request/response shapes -- see plantask/task.go) and,
// as a structural subset, reduction.Backend (Write only).
type writableWorkspaceBackend struct {
	root string
}

// newWritableWorkspaceBackend canonicalizes workspaceRoot, creates
// subdir under it if missing, and returns a backend rooted there.
func newWritableWorkspaceBackend(workspaceRoot, subdir string) (*writableWorkspaceBackend, error) {
	canonical, err := workspace.CanonicalRoot(workspaceRoot)
	if err != nil {
		return nil, err
	}
	root := filepath.Join(canonical, subdir)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	resolvedRoot, err := workspace.CanonicalRoot(root)
	if err != nil {
		return nil, err
	}
	return &writableWorkspaceBackend{root: resolvedRoot}, nil
}

var (
	_ plantask.Backend = (*writableWorkspaceBackend)(nil)
)

func (b *writableWorkspaceBackend) LsInfo(_ context.Context, req *adkfilesystem.LsInfoRequest) ([]adkfilesystem.FileInfo, error) {
	path := "."
	if req != nil {
		path = req.Path
	}
	resolved, err := resolveWorkspacePath(b.root, path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(resolved)
	if err != nil {
		return nil, err
	}
	result := make([]adkfilesystem.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		result = append(result, adkfilesystem.FileInfo{
			Path:  workspaceRelative(b.root, filepath.Join(resolved, entry.Name())),
			IsDir: entry.IsDir(), Size: info.Size(), ModifiedAt: info.ModTime().UTC().Format(time.RFC3339),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result, nil
}

func (b *writableWorkspaceBackend) Read(_ context.Context, req *adkfilesystem.ReadRequest) (*adkfilesystem.FileContent, error) {
	resolved, err := resolveWorkspacePath(b.root, req.FilePath)
	if err != nil {
		return nil, err
	}
	content, err := readLinesRange(resolved, req.Offset, req.Limit)
	if err != nil {
		return nil, err
	}
	return &adkfilesystem.FileContent{Content: content}, nil
}

func (b *writableWorkspaceBackend) Write(_ context.Context, req *adkfilesystem.WriteRequest) error {
	resolved, err := resolveWorkspacePath(b.root, req.FilePath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		return err
	}
	return os.WriteFile(resolved, []byte(req.Content), 0o644)
}

func (b *writableWorkspaceBackend) Delete(_ context.Context, req *plantask.DeleteRequest) error {
	resolved, err := resolveWorkspacePath(b.root, req.FilePath)
	if err != nil {
		return err
	}
	err = os.Remove(resolved)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
