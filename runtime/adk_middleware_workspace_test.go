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
