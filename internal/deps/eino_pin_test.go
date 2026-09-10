package deps

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Eino is pinned to exactly v0.9.19. The pin is a runtime contract: typed ADK,
// agentic messages, checkpoint and turn-loop semantics proven in runtime/adk_*
// tests are only valid for this version.
const (
	einoModulePath = "github.com/cloudwego/eino"
	einoPinVersion = "v0.9.19"
	einoPinSum     = "h1:i71YUBK3nwY4L53dkzRgZpAcPSZ4v4eRponN7W9sDtk="
)

func TestEinoIsPinnedExactlyWithoutReplacement(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-m", "-json", einoModulePath)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -m: %v\n%s", err, output)
	}
	var module struct {
		Path    string
		Version string
		Replace *struct{ Path, Version string }
	}
	if err := json.Unmarshal(output, &module); err != nil {
		t.Fatalf("decode module: %v\n%s", err, output)
	}
	if module.Path != einoModulePath || module.Version != einoPinVersion {
		t.Fatalf("eino module = %s %s, want %s %s", module.Path, module.Version, einoModulePath, einoPinVersion)
	}
	if module.Replace != nil {
		t.Fatalf("eino must not be replaced: %+v", module.Replace)
	}
	root, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/mattsp1290/eino-agent").Output()
	if err != nil {
		t.Fatal(err)
	}
	sum, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(root)), "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sum), einoModulePath+" "+einoPinVersion+" "+einoPinSum) {
		t.Fatalf("go.sum does not record %s %s %s", einoModulePath, einoPinVersion, einoPinSum)
	}
}
