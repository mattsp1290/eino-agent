package runtime

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/plantask"

	"github.com/mattsp1290/eino-agent/session"
)

// errScratchPathEscape reports a requested scratch-relative path that is
// absolute or otherwise cannot be safely joined under a session's scratch
// root -- os.Root itself independently rejects any path that would resolve
// outside it (including through a symlink), so this is a defensive,
// early rejection, not the sole enforcement.
var errScratchPathEscape = errors.New("path escapes scratch root")

// defaultScratchRootDir returns this process's default runtime-owned
// scratch root: <user cache dir>/eino-agent/scratch. Used by
// NewStreamingOrchestrator when WithScratchRoot is not supplied -- every
// orchestrator gets a private, non-workspace scratch area by default (round-
// two W6 review I5), never silently falling back to writing inside a host's
// workspace.
func defaultScratchRootDir() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve default scratch root: %w", err)
	}
	return filepath.Join(cacheDir, "eino-agent", "scratch"), nil
}

// sessionScratchRoot opens (creating if necessary) an *os.Root sandboxed to
// scratchRoot's own per-session subdirectory (sessionScratchDirName(sessionID),
// a hash of the opaque session id -- never the raw id, and never shared
// across sessions). Every scratchRootBackend I/O operation for this session
// goes through this single *os.Root, so no path -- including one that
// traverses a dangling or malicious symlink placed inside it -- can ever
// resolve outside this session's own subdirectory: os.Root enforces
// containment at the OS level for every Open/Create/Mkdir/Remove/Rename
// call (see the Go 1.24+ os.Root documentation), closing the dangling-
// symlink escape a plain filepath.EvalSymlinks-based check cannot (round-
// two W6 review I4).
//
// The per-session directory itself is created and opened THROUGH a parent
// *os.Root rooted at scratchRoot, never via a plain os.MkdirAll(dir, ...) +
// os.OpenRoot(dir) on the fully-joined path: that earlier version's
// os.MkdirAll on the joined path would happily create through, and
// os.OpenRoot would happily root itself AT, a symlink an attacker planted
// at exactly the per-session subdirectory name -- since containment was
// never established until AFTER that open, the "no escape" guarantee this
// function's own doc comment promises never actually applied to opening the
// root itself (round-three W6 authority-regression review S4, confirmed by
// probe). Routing both the directory's creation (parent.MkdirAll) and its
// opening (parent.OpenRoot) through the PARENT root closes this: os.Root's
// own methods refuse to follow a symlink that would resolve outside
// scratchRoot for either operation, so a pre-planted symlink at the
// per-session directory name is rejected instead of silently followed.
// scratchRoot itself (never attacker-influenced -- it is this process's own
// configured or default cache directory, not derived from any session-
// controlled input) is still created with a plain os.MkdirAll, since it is
// the trust anchor nothing else can sandbox it against.
func sessionScratchRoot(scratchRoot string, sessionID session.ID) (*os.Root, error) {
	if scratchRoot == "" {
		return nil, fmt.Errorf("%w: scratch root required", errScratchPathEscape)
	}
	if err := os.MkdirAll(scratchRoot, 0o700); err != nil {
		return nil, err
	}
	parent, err := os.OpenRoot(scratchRoot)
	if err != nil {
		return nil, err
	}
	defer func() { _ = parent.Close() }()
	name := sessionScratchDirName(sessionID)
	if err := parent.MkdirAll(name, 0o700); err != nil {
		return nil, err
	}
	// Lstat (not Stat) through the PARENT root before opening: os.Root
	// follows a symlink as long as it resolves within the root (see os.Root's
	// own doc comment -- "Methods on Root will follow symbolic links"), so
	// containment against escaping scratchRoot entirely is not automatically
	// the same guarantee as isolation BETWEEN sessions within it -- a
	// symlink planted at exactly this session's directory name that points
	// to ANOTHER session's directory INSIDE the same scratchRoot has no
	// reason to be refused on that basis alone (round-four W6
	// correlation-and-seal review, Suggestion #3). On this platform (darwin)
	// the preceding parent.MkdirAll already refuses to create through such a
	// pre-existing symlink regardless of what it points to (verified by
	// hand), so this Lstat check is confirmed defence in depth here rather
	// than closing a demonstrated live hole on every platform -- it is kept
	// because MkdirAll's "name already exists" behavior for a symlink is not
	// something this package controls, and a platform whose os.Root
	// tolerates an existing directory-like entry there would otherwise let
	// parent.OpenRoot below follow the symlink, giving this session
	// read/write access to that other session's offloads. Requiring a real
	// directory (Lstat's own mode bits, which -- unlike Stat -- never follow
	// the final symlink) closes this: a symlink here is rejected before
	// ever opening through it, regardless of where it points.
	info, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: session scratch directory %q is not a real directory (mode %s)", errScratchPathEscape, name, info.Mode())
	}
	return parent.OpenRoot(name)
}

// scratchRootBackend is a writable writableTaskBackend (plantask's full
// LsInfo/Read/Write/Delete surface, and reduction's Write-only subset)
// sandboxed to one session's own scratch *os.Root, under a fixed
// subdirectory (dir -- "plantask" or "reduction"). Unlike the pre-fix
// writableWorkspaceBackend, this backend's root is NEVER inside the
// admitted workspace: it is opened against a runtime-owned scratch root
// (see sessionScratchRoot), so a filesystem/skill/agentsmd recipe's
// read-only workspace view can never reach it regardless of any exclusion
// list, and its own containment comes from os.Root itself rather than a
// symlink-resolution check performed before the I/O call (see I4/I5 in the
// round-two W6 review).
type scratchRootBackend struct {
	root *os.Root
	dir  string
}

var _ writableTaskBackend = (*scratchRootBackend)(nil)

// newScratchRootBackend creates (if necessary) dir under root and returns a
// backend scoped to it.
func newScratchRootBackend(root *os.Root, dir string) (*scratchRootBackend, error) {
	if err := root.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &scratchRootBackend{root: root, dir: dir}, nil
}

// resolve joins requested (relative to this backend's own dir) into a path
// relative to the shared session *os.Root. Absolute paths are rejected
// outright; everything else is handed to os.Root's own methods, which
// independently refuse to resolve outside root regardless of "..” segments
// or symlinks.
func (b *scratchRootBackend) resolve(requested string) (string, error) {
	if filepath.IsAbs(requested) {
		return "", fmt.Errorf("%w: %s", errScratchPathEscape, requested)
	}
	clean := filepath.Clean(requested)
	if requested == "" {
		clean = "."
	}
	return filepath.Join(b.dir, clean), nil
}

func (b *scratchRootBackend) LsInfo(_ context.Context, req *adkfilesystem.LsInfoRequest) ([]adkfilesystem.FileInfo, error) {
	path := "."
	if req != nil {
		path = req.Path
	}
	rel, err := b.resolve(path)
	if err != nil {
		return nil, err
	}
	entries, err := fs.ReadDir(b.root.FS(), rel)
	if err != nil {
		return nil, err
	}
	result := make([]adkfilesystem.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		entryRel, err := filepath.Rel(b.dir, filepath.Join(rel, entry.Name()))
		if err != nil {
			continue
		}
		result = append(result, adkfilesystem.FileInfo{
			Path: filepath.ToSlash(entryRel), IsDir: entry.IsDir(), Size: info.Size(), ModifiedAt: info.ModTime().UTC().Format(time.RFC3339),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result, nil
}

func (b *scratchRootBackend) Read(_ context.Context, req *adkfilesystem.ReadRequest) (*adkfilesystem.FileContent, error) {
	if req == nil {
		return nil, errors.New("read request required")
	}
	rel, err := b.resolve(req.FilePath)
	if err != nil {
		return nil, err
	}
	data, err := b.root.ReadFile(rel)
	if err != nil {
		return nil, err
	}
	return &adkfilesystem.FileContent{Content: linesRange(string(data), req.Offset, req.Limit)}, nil
}

func (b *scratchRootBackend) Write(_ context.Context, req *adkfilesystem.WriteRequest) error {
	if req == nil {
		return errors.New("write request required")
	}
	rel, err := b.resolve(req.FilePath)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(rel); dir != "." {
		if err := b.root.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return b.root.WriteFile(rel, []byte(req.Content), 0o644)
}

func (b *scratchRootBackend) Delete(_ context.Context, req *plantask.DeleteRequest) error {
	if req == nil {
		return errors.New("delete request required")
	}
	rel, err := b.resolve(req.FilePath)
	if err != nil {
		return err
	}
	err = b.root.Remove(rel)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// linesRange is readLinesRange's in-memory counterpart: identical
// offset/limit semantics, applied to already-read content instead of a
// path (scratchRootBackend reads through *os.Root.ReadFile, which has no
// separate "open path, then read" step to hang a line-range read on).
func linesRange(data string, offset, limit int) string {
	if offset < 1 {
		offset = 1
	}
	lines := strings.Split(data, "\n")
	start := offset - 1
	if start > len(lines) {
		return ""
	}
	end := len(lines)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	return strings.Join(lines[start:end], "\n")
}
