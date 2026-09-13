package runtime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/skill"

	"github.com/mattsp1290/eino-agent/internal/workspace"
)

// errWorkspaceReadOnly is returned by every mutating workspaceFilesystemBackend
// operation: the backend a HandlerBuildContext hands to host middleware
// recipes is a read-only view over the admitted canonical workspace -- see
// the design's "Host supplies filesystem/skill backends scoped to the
// admitted canonical workspace. Use read-only views for context loading."
var errWorkspaceReadOnly = errors.New("workspace backend is read-only")

// errWorkspacePathEscape reports a requested path that resolves (directly,
// via "..", or through a symlink) outside the canonical workspace root.
var errWorkspacePathEscape = errors.New("path escapes workspace root")

// maxMultiModalReadBytes bounds a single MultiModalRead's raw file read.
// Handler tools are sealed with RetentionPolicy{MaxInlineBytes: -1}
// (adkEngine.sealHandlerTools) -- unbounded at the durable-settlement
// content-budget layer, which only clamps AFTER a result is already in
// memory -- so without a cap here, a single large PDF or image request
// would read the whole file into memory and base64-encode all of it into
// both the provider request and the durable settlement before any clamp
// ever has a chance to apply. This is a fixed, conservative ceiling
// (comfortably above session.DefaultContentLimits' 8MiB message budget,
// so an ordinarily-sized attachment is unaffected) rather than the
// caller's own configured content limits: MultiModalRead has no access to
// a specific run's ContentLimits (workspaceFilesystemBackend is
// constructed once per turn from only a workspace root -- see
// newWorkspaceBackends), and this suggestion (RW-S1 in the round-two W6
// review) is about closing the unbounded-read hazard itself, not about
// matching a dynamic per-run budget exactly.
const maxMultiModalReadBytes = 20 << 20

// errMultiModalReadTooLarge reports that MultiModalRead's source file
// exceeds maxMultiModalReadBytes.
var errMultiModalReadTooLarge = errors.New("multimodal read file exceeds the maximum bounded read size")

// newWorkspaceBackends canonicalizes root (see internal/workspace.CanonicalRoot
// -- symlink-resolved, absolute) and builds the shared, read-only
// filesystem/skill backend views every host agent-handler recipe for this
// turn receives via HandlerBuildContext. A malformed or unresolvable root
// fails closed here rather than silently constructing an unscoped backend.
func newWorkspaceBackends(root string) (adkfilesystem.Backend, skill.Backend, error) {
	canonical, err := workspace.CanonicalRoot(root)
	if err != nil {
		return nil, nil, err
	}
	backend := &workspaceFilesystemBackend{root: canonical}
	return backend, &workspaceSkillBackend{root: canonical}, nil
}

// resolveWorkspacePath resolves requested (relative or absolute) against
// root and rejects any result that is not contained in root, including
// after resolving symlinks -- a symlink placed inside the workspace that
// points outside it is rejected, not followed. When joined does not exist
// yet (e.g. a Write target that has not been created), EvalSymlinks fails
// outright and cannot check the eventual path; instead, the deepest
// existing ancestor directory is resolved and required to be contained, so
// a symlinked intermediate directory (e.g. a workspace-relative
// ".eino-agent" symlinked to somewhere outside root) cannot be used to
// smuggle a not-yet-existing file's containment check.
func resolveWorkspacePath(root, requested string) (string, error) {
	clean := filepath.Clean(requested)
	if requested == "" {
		clean = "."
	}
	joined := clean
	if !filepath.IsAbs(clean) {
		joined = filepath.Join(root, clean)
	}
	joined = filepath.Clean(joined)
	if err := requireContained(root, joined); err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(joined); err == nil {
		if err := requireContained(root, resolved); err != nil {
			return "", err
		}
		return resolved, nil
	}
	if err := requireAncestorContained(root, joined); err != nil {
		return "", err
	}
	return joined, nil
}

// requireAncestorContained walks upward from path (which itself does not
// exist, or filepath.EvalSymlinks would have succeeded) to the deepest
// existing ancestor directory, resolves ITS symlinks, and requires that
// resolved ancestor to be contained in root. It stops (without error) once
// it reaches root itself or the filesystem root, matching
// resolveWorkspacePath's existing-path behavior of trusting root as the
// already-canonicalized boundary.
func requireAncestorContained(root, path string) error {
	dir := filepath.Dir(path)
	for {
		resolved, err := filepath.EvalSymlinks(dir)
		if err == nil {
			return requireContained(root, resolved)
		}
		if dir == root || dir == filepath.Dir(dir) {
			// Reached the canonical root (already trusted) or the
			// filesystem root without finding an existing ancestor to
			// resolve: nothing further to check.
			return nil
		}
		dir = filepath.Dir(dir)
	}
}

func requireContained(root, path string) error {
	if path == root {
		return nil
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: %s", errWorkspacePathEscape, path)
	}
	return nil
}

// reservedWorkspacePrefix is the workspace-relative top-level path segment
// the read-only workspace/skill view refuses to surface, even though this
// runtime's own scratch/offload state no longer lives inside the workspace
// at all (see sessionScratchRoot) -- defense in depth (round-two W6 review
// I5) against a host-created ".eino-agent" directory in a real workspace,
// or a future regression that puts runtime state back under the workspace.
const reservedWorkspacePrefix = ".eino-agent"

// errReservedWorkspacePath reports a read-only workspace-view request whose
// resolved path falls under reservedWorkspacePrefix.
var errReservedWorkspacePath = fmt.Errorf("%w: path is reserved for runtime use", errWorkspacePathEscape)

// resolveReadOnlyWorkspacePath is resolveWorkspacePath plus the
// reservedWorkspacePrefix exclusion -- used by every workspaceFilesystemBackend/
// workspaceSkillBackend method (the READ-ONLY views host recipes receive),
// never by writableWorkspaceBackend (a separate, general-purpose writable
// backend that legitimately uses ".eino-agent"-named subdirectories of its
// own callers' choosing today, and is no longer used for plantask/reduction
// scratch at all -- see adk_middleware_scratch_root.go).
func resolveReadOnlyWorkspacePath(root, requested string) (string, error) {
	resolved, err := resolveWorkspacePath(root, requested)
	if err != nil {
		return "", err
	}
	rel := workspaceRelative(root, resolved)
	if rel == reservedWorkspacePrefix || strings.HasPrefix(rel, reservedWorkspacePrefix+"/") {
		return "", fmt.Errorf("%w: %s", errReservedWorkspacePath, rel)
	}
	return resolved, nil
}

func workspaceRelative(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(rel)
}

// workspaceFilesystemBackend is a read-only adk/filesystem.Backend rooted at
// a canonical workspace directory: every LsInfo/Read/GrepRaw/GlobInfo call is
// bounded to that root (see resolveWorkspacePath), and Write/Edit always
// fail closed with errWorkspaceReadOnly.
type workspaceFilesystemBackend struct {
	root string
}

var (
	_ adkfilesystem.Backend          = (*workspaceFilesystemBackend)(nil)
	_ adkfilesystem.MultiModalReader = (*workspaceFilesystemBackend)(nil)
)

func (b *workspaceFilesystemBackend) LsInfo(_ context.Context, req *adkfilesystem.LsInfoRequest) ([]adkfilesystem.FileInfo, error) {
	if req == nil {
		return nil, errors.New("ls request required")
	}
	resolved, err := resolveReadOnlyWorkspacePath(b.root, req.Path)
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

func readLinesRange(path string, offset, limit int) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return linesRange(string(data), offset, limit), nil
}

func (b *workspaceFilesystemBackend) Read(_ context.Context, req *adkfilesystem.ReadRequest) (*adkfilesystem.FileContent, error) {
	if req == nil {
		return nil, errors.New("read request required")
	}
	resolved, err := resolveReadOnlyWorkspacePath(b.root, req.FilePath)
	if err != nil {
		return nil, err
	}
	content, err := readLinesRange(resolved, req.Offset, req.Limit)
	if err != nil {
		return nil, err
	}
	return &adkfilesystem.FileContent{Content: content}, nil
}

func (b *workspaceFilesystemBackend) MultiModalRead(_ context.Context, req *adkfilesystem.MultiModalReadRequest) (*adkfilesystem.MultiFileContent, error) {
	if req == nil {
		return nil, errors.New("multimodal read request required")
	}
	resolved, err := resolveReadOnlyWorkspacePath(b.root, req.FilePath)
	if err != nil {
		return nil, err
	}
	mimeType, kind := workspaceMediaKind(resolved)
	if kind == "" {
		content, err := readLinesRange(resolved, req.Offset, req.Limit)
		if err != nil {
			return nil, err
		}
		return &adkfilesystem.MultiFileContent{FileContent: &adkfilesystem.FileContent{Content: content}}, nil
	}
	stat, err := os.Stat(resolved)
	if err != nil {
		return nil, err
	}
	if stat.Size() > maxMultiModalReadBytes {
		return nil, fmt.Errorf("%w: %q is %d bytes, max %d", errMultiModalReadTooLarge, req.FilePath, stat.Size(), int64(maxMultiModalReadBytes))
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, err
	}
	return &adkfilesystem.MultiFileContent{Parts: []adkfilesystem.FileContentPart{{Type: kind, MIMEType: mimeType, Data: data}}}, nil
}

func workspaceMediaKind(path string) (mimeType string, kind adkfilesystem.FileContentPartType) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".png":
		return "image/png", adkfilesystem.FileContentPartTypeImage
	case ".jpg", ".jpeg":
		return "image/jpeg", adkfilesystem.FileContentPartTypeImage
	case ".gif":
		return "image/gif", adkfilesystem.FileContentPartTypeImage
	case ".webp":
		return "image/webp", adkfilesystem.FileContentPartTypeImage
	case ".pdf":
		return "application/pdf", adkfilesystem.FileContentPartTypePDF
	default:
		return "", ""
	}
}

func (b *workspaceFilesystemBackend) GrepRaw(_ context.Context, req *adkfilesystem.GrepRequest) ([]adkfilesystem.GrepMatch, error) {
	if req == nil || req.Pattern == "" {
		return nil, errors.New("grep pattern required")
	}
	resolvedBase, err := resolveReadOnlyWorkspacePath(b.root, req.Path)
	if err != nil {
		return nil, err
	}
	pattern := req.Pattern
	if req.CaseInsensitive {
		pattern = "(?i)" + pattern
	}
	expr, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	var globExpr *regexp.Regexp
	if req.Glob != "" {
		globExpr = globToRegexp(req.Glob)
	}
	const matchCap = 2000
	var matches []adkfilesystem.GrepMatch
	walkErr := filepath.WalkDir(resolvedBase, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if len(matches) >= matchCap {
			return nil
		}
		rel := workspaceRelative(b.root, path)
		if req.FileType != "" && strings.TrimPrefix(filepath.Ext(path), ".") != req.FileType {
			return nil
		}
		if globExpr != nil && !globExpr.MatchString(filepath.ToSlash(path)) && !globExpr.MatchString(filepath.Base(path)) {
			return nil
		}
		// filepath.WalkDir reports a symlink (to a file OR a directory) as a
		// non-directory entry without following it, but os.ReadFile below
		// DOES follow symlinks -- so a checked-in symlink inside the
		// workspace pointing outside it (e.g. leak.txt -> ~/.ssh/id_rsa)
		// would otherwise let grep read arbitrary host files. Re-resolve
		// every walked path (symlink or not -- cheap, and uniform) through
		// the same containment check every other read goes through, and
		// silently skip an escaping entry rather than failing the whole
		// walk.
		safe, err := resolveReadOnlyWorkspacePath(b.root, path)
		if err != nil {
			return nil
		}
		data, err := os.ReadFile(safe)
		if err != nil {
			return nil
		}
		if req.EnableMultiline {
			if expr.Match(data) {
				matches = append(matches, adkfilesystem.GrepMatch{Content: string(data), Path: rel, Line: 1})
			}
			return nil
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		line := 0
		for scanner.Scan() {
			line++
			if expr.MatchString(scanner.Text()) {
				matches = append(matches, adkfilesystem.GrepMatch{Content: scanner.Text(), Path: rel, Line: line})
				if len(matches) >= matchCap {
					break
				}
			}
		}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return matches, nil
}

func (b *workspaceFilesystemBackend) GlobInfo(_ context.Context, req *adkfilesystem.GlobInfoRequest) ([]adkfilesystem.FileInfo, error) {
	if req == nil || req.Pattern == "" {
		return nil, errors.New("glob pattern required")
	}
	resolvedBase, err := resolveReadOnlyWorkspacePath(b.root, req.Path)
	if err != nil {
		return nil, err
	}
	expr := globToRegexp(req.Pattern)
	var result []adkfilesystem.FileInfo
	walkErr := filepath.WalkDir(resolvedBase, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel := workspaceRelative(resolvedBase, path)
		if !expr.MatchString(rel) && !expr.MatchString(filepath.Base(path)) {
			return nil
		}
		// See GrepRaw's identical containment re-check: a symlinked entry
		// must not surface metadata (or, through Read/MultiModalRead called
		// later with this Path) content from outside the workspace.
		if _, err := resolveReadOnlyWorkspacePath(b.root, path); err != nil {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		result = append(result, adkfilesystem.FileInfo{
			Path: workspaceRelative(b.root, path), IsDir: false, Size: info.Size(), ModifiedAt: info.ModTime().UTC().Format(time.RFC3339),
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result, nil
}

func (b *workspaceFilesystemBackend) Write(context.Context, *adkfilesystem.WriteRequest) error {
	return errWorkspaceReadOnly
}

func (b *workspaceFilesystemBackend) Edit(context.Context, *adkfilesystem.EditRequest) error {
	return errWorkspaceReadOnly
}

// globToRegexp translates a bounded glob subset ("*" within one path
// segment, "**" across segments, "?" one character) into an anchored
// regexp. This is a deliberately reduced-fidelity glob implementation (no
// character classes, no brace expansion) sufficient for the recipe
// backends' own bounded file discovery; it is not a drop-in for a full
// ripgrep/doublestar glob engine.
func globToRegexp(pattern string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString("(?i)^")
	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '*':
			if i+1 < len(runes) && runes[i+1] == '*' {
				b.WriteString(".*")
				i++
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '.', '+', '(', ')', '|', '^', '$', '{', '}', '[', ']', '\\':
			b.WriteString(regexp.QuoteMeta(string(runes[i])))
		default:
			b.WriteRune(runes[i])
		}
	}
	b.WriteString("$")
	expr, err := regexp.Compile(b.String())
	if err != nil {
		// A malformed pattern matches nothing rather than panicking or
		// silently matching everything.
		return regexp.MustCompile(`\A\z`)
	}
	return expr
}

// workspaceSkillBackend is a read-only skill.Backend that discovers skills as
// "<workspace root>/<skill-directory>/SKILL.md" files with a bounded flat
// key:value frontmatter block (name/description/context/agent/model),
// delimited by "---" lines -- the same fields upstream skill.FrontMatter
// declares. It intentionally does not depend on a general YAML library: the
// frontmatter shape it supports is flat scalars only.
type workspaceSkillBackend struct {
	root string
}

var _ skill.Backend = (*workspaceSkillBackend)(nil)

func (b *workspaceSkillBackend) List(context.Context) ([]skill.FrontMatter, error) {
	entries, err := os.ReadDir(b.root)
	if err != nil {
		return nil, err
	}
	var result []skill.FrontMatter
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		front, _, err := b.readSkillFile(entry.Name())
		if err != nil {
			continue
		}
		result = append(result, front)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (b *workspaceSkillBackend) Get(_ context.Context, name string) (skill.Skill, error) {
	if name == "" {
		return skill.Skill{}, errors.New("skill name required")
	}
	entries, err := os.ReadDir(b.root)
	if err != nil {
		return skill.Skill{}, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		front, content, err := b.readSkillFile(entry.Name())
		if err != nil {
			continue
		}
		if front.Name == name {
			return skill.Skill{FrontMatter: front, Content: content, BaseDirectory: filepath.Join(b.root, entry.Name())}, nil
		}
	}
	return skill.Skill{}, fmt.Errorf("skill %q not found", name)
}

func (b *workspaceSkillBackend) readSkillFile(dir string) (skill.FrontMatter, string, error) {
	resolved, err := resolveReadOnlyWorkspacePath(b.root, filepath.Join(dir, "SKILL.md"))
	if err != nil {
		return skill.FrontMatter{}, "", err
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return skill.FrontMatter{}, "", err
	}
	front, content := parseSkillFrontMatter(string(data))
	if front.Name == "" {
		front.Name = dir
	}
	return front, content, nil
}

// parseSkillFrontMatter parses a "---\nkey: value\n...\n---\n<content>"
// block into a flat FrontMatter. Unrecognized keys and any nested/complex
// YAML are ignored rather than rejected (this is a bounded, hand-written
// scanner, not a general YAML parser).
func parseSkillFrontMatter(raw string) (skill.FrontMatter, string) {
	var front skill.FrontMatter
	lines := strings.Split(raw, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return front, raw
	}
	end := -1
	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			end = i
			break
		}
	}
	if end == -1 {
		return front, raw
	}
	for _, line := range lines[1:end] {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		switch key {
		case "name":
			front.Name = value
		case "description":
			front.Description = value
		case "context":
			front.Context = skill.ContextMode(value)
		case "agent":
			front.Agent = value
		case "model":
			front.Model = value
		}
	}
	content := ""
	if end+1 < len(lines) {
		content = strings.Join(lines[end+1:], "\n")
	}
	return front, strings.TrimPrefix(content, "\n")
}
