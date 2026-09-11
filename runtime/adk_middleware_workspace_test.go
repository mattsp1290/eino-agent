package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	adkfilesystem "github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/middlewares/plantask"
)

func TestWorkspaceFilesystemBackendReadListGlobGrep(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "notes.md"), []byte("line one\nhello world\nline three\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "inner.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}
	backend, _, err := newWorkspaceBackends(root)
	if err != nil {
		t.Fatal(err)
	}

	ls, err := backend.LsInfo(context.Background(), &adkfilesystem.LsInfoRequest{Path: "."})
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 2 {
		t.Fatalf("LsInfo = %+v, want 2 entries", ls)
	}

	content, err := backend.Read(context.Background(), &adkfilesystem.ReadRequest{FilePath: "notes.md"})
	if err != nil {
		t.Fatal(err)
	}
	if content.Content != "line one\nhello world\nline three\n" && content.Content != "line one\nhello world\nline three" {
		t.Fatalf("Read content = %q", content.Content)
	}

	matches, err := backend.GrepRaw(context.Background(), &adkfilesystem.GrepRequest{Pattern: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].Path != "notes.md" || matches[0].Line != 2 {
		t.Fatalf("GrepRaw = %+v", matches)
	}

	globbed, err := backend.GlobInfo(context.Background(), &adkfilesystem.GlobInfoRequest{Pattern: "*.md"})
	if err != nil {
		t.Fatal(err)
	}
	if len(globbed) != 1 || globbed[0].Path != "notes.md" {
		t.Fatalf("GlobInfo = %+v", globbed)
	}

	if err := backend.Write(context.Background(), &adkfilesystem.WriteRequest{FilePath: "notes.md", Content: "x"}); err == nil {
		t.Fatal("Write on read-only backend succeeded")
	}
	if err := backend.Edit(context.Background(), &adkfilesystem.EditRequest{FilePath: "notes.md", OldString: "a", NewString: "b"}); err == nil {
		t.Fatal("Edit on read-only backend succeeded")
	}
}

func TestWorkspaceFilesystemBackendRejectsPathEscape(t *testing.T) {
	root := t.TempDir()
	backend, _, err := newWorkspaceBackends(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Read(context.Background(), &adkfilesystem.ReadRequest{FilePath: "../../etc/passwd"}); err == nil {
		t.Fatal("relative path escape was accepted")
	}
	if _, err := backend.Read(context.Background(), &adkfilesystem.ReadRequest{FilePath: filepath.Join(os.TempDir(), "definitely-outside")}); err == nil {
		t.Fatal("absolute path escape was accepted")
	}
}

// TestWorkspaceFilesystemBackendRejectsSymlinkEscape proves a symlink placed
// inside the workspace that points outside it is rejected, not followed --
// the acceptance criterion's "unauthorized path (symlink escape)" case.
func TestWorkspaceFilesystemBackendRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("top secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "escape")
	if err := os.Symlink(secret, link); err != nil {
		t.Skipf("symlink unsupported in this environment: %v", err)
	}
	backend, _, err := newWorkspaceBackends(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Read(context.Background(), &adkfilesystem.ReadRequest{FilePath: "escape"}); err == nil {
		t.Fatal("symlink escape was followed instead of rejected")
	}
}

// TestWorkspaceFilesystemBackendGrepRejectsSymlinkEscape proves C4/I1's fix:
// filepath.WalkDir reports a symlink as a non-directory entry without
// following it, but the pre-fix GrepRaw called os.ReadFile(path) directly,
// which DOES follow it -- letting a checked-in symlink inside an untrusted
// workspace leak arbitrary host file content through grep. The walked path
// must now be re-resolved through the same containment check every other
// read goes through, and an escaping entry must be silently skipped, not
// grepped.
func TestWorkspaceFilesystemBackendGrepRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("TOPSECRET-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leak.txt")
	if err := os.Symlink(secret, link); err != nil {
		t.Skipf("symlink unsupported in this environment: %v", err)
	}
	backend, _, err := newWorkspaceBackends(root)
	if err != nil {
		t.Fatal(err)
	}
	matches, err := backend.GrepRaw(context.Background(), &adkfilesystem.GrepRequest{Pattern: "TOPSECRET"})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("GrepRaw followed the symlink escape: matches = %+v", matches)
	}
}

// TestWorkspaceFilesystemBackendGlobRejectsSymlinkEscape is GlobInfo's
// counterpart to the Grep test above: an escaping symlink entry must not
// even surface as metadata.
func TestWorkspaceFilesystemBackendGlobRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leak.txt")
	if err := os.Symlink(secret, link); err != nil {
		t.Skipf("symlink unsupported in this environment: %v", err)
	}
	backend, _, err := newWorkspaceBackends(root)
	if err != nil {
		t.Fatal(err)
	}
	globbed, err := backend.GlobInfo(context.Background(), &adkfilesystem.GlobInfoRequest{Pattern: "*.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if len(globbed) != 0 {
		t.Fatalf("GlobInfo surfaced the symlink escape: globbed = %+v", globbed)
	}
}

// TestWritableWorkspaceBackendRejectsSymlinkedRoot proves I3's fix: a
// pre-existing symlink at the scratch subdirectory's own location (e.g. a
// checked-in ".eino-agent" symlinked to somewhere outside the workspace)
// must be rejected at construction, not silently followed by MkdirAll --
// which would let plantask/reduction state be written outside the
// workspace, potentially overwriting arbitrary host files.
func TestWritableWorkspaceBackendRejectsSymlinkedRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, ".eino-agent")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink unsupported in this environment: %v", err)
	}
	if _, err := newWritableWorkspaceBackend(root, filepath.Join(".eino-agent", "plantask")); err == nil {
		t.Fatal("writable backend construction under a symlinked ancestor succeeded instead of being rejected")
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("construction wrote into the symlink target outside the workspace: %+v", entries)
	}
}

// TestWritableWorkspaceBackendRejectsSymlinkedIntermediateDirectoryOnWrite
// proves resolveWorkspacePath's not-yet-existing-path ancestor check: a
// symlinked intermediate directory created AFTER the backend itself was
// constructed (so the backend's own root is legitimately contained) must
// still be rejected when a Write targets a new file underneath it.
func TestWritableWorkspaceBackendRejectsSymlinkedIntermediateDirectoryOnWrite(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	backend, err := newWritableWorkspaceBackend(root, "scratch")
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "scratch", "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink unsupported in this environment: %v", err)
	}
	if err := backend.Write(context.Background(), &adkfilesystem.WriteRequest{FilePath: "link/new.txt", Content: "x"}); err == nil {
		t.Fatal("write through a symlinked intermediate directory succeeded instead of being rejected")
	}
	if _, statErr := os.Stat(filepath.Join(outside, "new.txt")); statErr == nil {
		t.Fatal("write escaped through the symlinked intermediate directory")
	}
}

func TestWorkspaceFilesystemBackendMultiModalRead(t *testing.T) {
	root := t.TempDir()
	png := []byte{0x89, 'P', 'N', 'G', 1, 2, 3, 4}
	if err := os.WriteFile(filepath.Join(root, "pic.png"), png, 0o644); err != nil {
		t.Fatal(err)
	}
	backend, _, err := newWorkspaceBackends(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := backend.(adkfilesystem.MultiModalReader).MultiModalRead(context.Background(), &adkfilesystem.MultiModalReadRequest{ReadRequest: adkfilesystem.ReadRequest{FilePath: "pic.png"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Parts) != 1 || result.Parts[0].Type != adkfilesystem.FileContentPartTypeImage || result.Parts[0].MIMEType != "image/png" {
		t.Fatalf("MultiModalRead = %+v", result)
	}
	if len(result.Parts[0].Data) != len(png) {
		t.Fatalf("MultiModalRead data length = %d, want %d", len(result.Parts[0].Data), len(png))
	}
}

func TestWorkspaceSkillBackendListAndGet(t *testing.T) {
	root := t.TempDir()
	skillDir := filepath.Join(root, "greeter")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: greeter\ndescription: says hello\n---\nSay hello warmly.\n"
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	_, skillBackend, err := newWorkspaceBackends(root)
	if err != nil {
		t.Fatal(err)
	}
	list, err := skillBackend.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "greeter" || list[0].Description != "says hello" {
		t.Fatalf("List = %+v", list)
	}
	got, err := skillBackend.Get(context.Background(), "greeter")
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "Say hello warmly.\n" && got.Content != "Say hello warmly." {
		t.Fatalf("Get content = %q", got.Content)
	}
	if _, err := skillBackend.Get(context.Background(), "missing"); err == nil {
		t.Fatal("Get for a missing skill succeeded")
	}
}

func TestWritableWorkspaceBackendRoundTrip(t *testing.T) {
	root := t.TempDir()
	backend, err := newWritableWorkspaceBackend(root, "scratch")
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(context.Background(), &adkfilesystem.WriteRequest{FilePath: "task.json", Content: `{"id":1}`}); err != nil {
		t.Fatal(err)
	}
	content, err := backend.Read(context.Background(), &adkfilesystem.ReadRequest{FilePath: "task.json"})
	if err != nil {
		t.Fatal(err)
	}
	if content.Content != `{"id":1}` {
		t.Fatalf("Read = %q", content.Content)
	}
	list, err := backend.LsInfo(context.Background(), &adkfilesystem.LsInfoRequest{Path: "."})
	if err != nil || len(list) != 1 {
		t.Fatalf("LsInfo = %+v, err = %v", list, err)
	}
	if err := backend.Delete(context.Background(), &plantask.DeleteRequest{FilePath: "task.json"}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Read(context.Background(), &adkfilesystem.ReadRequest{FilePath: "task.json"}); err == nil {
		t.Fatal("Read after Delete succeeded")
	}
}
