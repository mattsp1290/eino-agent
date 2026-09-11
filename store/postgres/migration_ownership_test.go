package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestMigrationOwnershipGuard is a lightweight structural check for the
// ownership invariant documented on migrationContext in
// migration_context.go: discardPGXConnection - and so conn.Raw - must only
// ever be called by the goroutine that owns the *sql.Conn, never from a
// closure that runs on an interrupt path (context.AfterFunc or
// time.AfterFunc). It is not a full call-graph analysis; it is a cheap
// tripwire that fails loudly, with a pointer back to the ownership-model
// doc, if a future edit adds a new discardPGXConnection call site or wires
// database/sql access into an AfterFunc/time.AfterFunc closure - the exact
// class of change that reintroduced github.com/mattsp1290/eino-agent issue
// eino-agent-f3i during this fix's own review.
func TestMigrationOwnershipGuard(t *testing.T) {
	const (
		contextSrc = "migration_context.go"
		lockSrc    = "migration_lock.go"
		dialectSrc = "dialect.go"
	)

	// 1. discardPGXConnection has exactly two call sites: the owner-side
	// choke point releaseConn (migration_context.go), and dialect.go's own
	// discardAndClose - a separate, unrelated caller that runs serially
	// under postgresTransaction.mu (see migration_lock.go's discardObserved
	// doc). A new call site anywhere else must be reviewed against the
	// ownership invariant before this count changes.
	total := 0
	for _, path := range []string{contextSrc, lockSrc, dialectSrc} {
		total += countCalls(t, path, "discardPGXConnection")
	}
	if got, want := total, 2; got != want {
		t.Fatalf("discardPGXConnection has %d call sites across %v, want %d "+
			"(releaseConn and dialect.go's discardAndClose); if this is a "+
			"deliberate new caller, confirm it runs on the goroutine that owns "+
			"the *sql.Conn before changing this count - see migration_context.go's "+
			"ownership-model doc", got, []string{contextSrc, lockSrc, dialectSrc}, want)
	}

	// 2. No closure passed to context.AfterFunc or time.AfterFunc in the
	// migration_* files may reach into database/sql on the bound *sql.Conn:
	// that is exactly the interrupt-goroutine-touches-the-Conn pattern the
	// ownership model exists to rule out.
	forbidden := []string{"discardPGXConnection", "sqlConn", ".Raw(", ".Close(", ".discard(", ".Query", ".Exec"}
	for _, path := range []string{contextSrc, lockSrc} {
		for _, body := range afterFuncClosures(t, path) {
			for _, bad := range forbidden {
				if strings.Contains(body, bad) {
					t.Fatalf("%s: a context.AfterFunc/time.AfterFunc closure references %q; "+
						"interrupt (the only thing such a closure may call) must never touch "+
						"database/sql on the bound *sql.Conn - see migration_context.go's "+
						"ownership-model doc. Closure body:\n%s", path, bad, body)
				}
			}
		}
	}
}

func countCalls(t *testing.T, path, name string) int {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	count := 0
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == name {
			count++
		}
		return true
	})
	return count
}

// afterFuncClosures returns the source text of every function-literal
// argument passed to context.AfterFunc or time.AfterFunc in path.
func afterFuncClosures(t *testing.T, path string) []string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var bodies []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok || sel.Sel.Name != "AfterFunc" || (pkgIdent.Name != "context" && pkgIdent.Name != "time") {
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.FuncLit)
			if !ok {
				continue
			}
			start := fset.Position(lit.Pos()).Offset
			end := fset.Position(lit.End()).Offset
			bodies = append(bodies, string(src[start:end]))
		}
		return true
	})
	return bodies
}
