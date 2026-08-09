package deps

import (
	"os/exec"
	"strings"
	"testing"
)

func TestDependencyAnchor(t *testing.T) {
	t.Parallel()
}

func TestCorePackagesDoNotDependOnWasmRuntimeOrBindings(t *testing.T) {
	t.Parallel()
	packages := []string{"runtime", "model", "config", "session", "tools", "permissions"}
	for _, name := range packages {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			command := exec.Command("go", "list", "-deps", "github.com/mattsp1290/eino-agent/"+name)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("go list -deps: %v\n%s", err, output)
			}
			dependencies := string(output)
			for _, forbidden := range []string{"github.com/bytecodealliance/wasmtime-go", "github.com/mattsp1290/eino-agent/wasmext/gen"} {
				if strings.Contains(dependencies, forbidden) {
					t.Errorf("%s depends on %s", name, forbidden)
				}
			}
		})
	}
}
