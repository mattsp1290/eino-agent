package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestMigrationOwnershipGuard is a structural check for the ownership
// invariant documented on migrationContext in migration_context.go:
// discardPGXConnection - and so conn.Raw - and m.cancel must only ever be
// reached the way the type doc says. It is not a full call-graph analysis;
// it is a static tripwire that fails loudly, with a pointer back to the
// ownership-model doc, if a future edit adds a new discardPGXConnection call
// site, wires database/sql access into a non-owner goroutine (an
// AfterFunc/time.AfterFunc argument, or interrupt's own body), or calls
// m.cancel from anywhere but take/cancelIfUnbound - the exact class of
// change that reintroduced github.com/mattsp1290/eino-agent issue
// eino-agent-f3i during this fix's own review, more than once.
//
// This is the static half of the guard. It cannot see a violation routed
// through an intermediate helper this list does not yet know about, or
// catch every possible obfuscation - that is what
// testCanceledMigrationOwnerRace's runtime.Stack-based assertion in
// migration_lock_integration_test.go is for: it checks the actual calling
// goroutine of every discardPGXConnection call at runtime, which no static
// text scan can be fooled past. Keep both; they catch different mutants.
func TestMigrationOwnershipGuard(t *testing.T) {
	files := packageGoFiles(t)

	// 1. discardPGXConnection has exactly two call sites across every
	// production .go file in this package: the owner-side choke point
	// releaseConn (migration_context.go), and dialect.go's own
	// discardAndClose - a separate, unrelated caller that runs serially
	// under postgresTransaction.mu (see migration_lock.go's discardObserved
	// doc). A new call site anywhere else - including a new file - must be
	// reviewed against the ownership invariant before this count changes.
	total := 0
	for _, path := range files {
		total += countCalls(t, path, "discardPGXConnection")
	}
	if got, want := total, 2; got != want {
		t.Fatalf("discardPGXConnection has %d call sites across %v, want %d "+
			"(releaseConn and dialect.go's discardAndClose); if this is a "+
			"deliberate new caller, confirm it runs on the goroutine that owns "+
			"the *sql.Conn before changing this count - see migration_context.go's "+
			"ownership-model doc", got, files, want)
	}

	// 2. No argument passed to context.AfterFunc or time.AfterFunc anywhere
	// in the package may reach into database/sql on a bound *sql.Conn: that
	// callback always runs on a goroutine that does not own the connection.
	// This covers a function literal's body, a bare function value, and a
	// method value (for example time.AfterFunc(d, l.operation.discard)) -
	// none of them require a call-shaped "(" to be dangerous, so the
	// forbidden set below matches on the bare name.
	forbidden := []string{
		"discardPGXConnection", "sqlConn", ".Raw", ".Close",
		".discard", ".Query", ".Exec", ".Ping",
	}
	check := func(path, label, body string) {
		for _, bad := range forbidden {
			if strings.Contains(body, bad) {
				t.Fatalf("%s: %s references %q; a non-owner goroutine must never touch "+
					"database/sql on the bound *sql.Conn - see migration_context.go's "+
					"ownership-model doc. Text:\n%s", path, label, bad, body)
			}
		}
	}
	for _, path := range files {
		for _, arg := range afterFuncArgTexts(t, path) {
			check(path, "a context.AfterFunc/time.AfterFunc argument", arg)
		}
	}

	// 3. interrupt itself - the realistic regression site, since every
	// AfterFunc/time.AfterFunc argument above does nothing but call it -
	// must never touch database/sql on the bound *sql.Conn either. interrupt
	// delegates the transport close to closeTransportLocked and the
	// conditional cancel to cancelIfUnbound (see migration_context.go)
	// specifically so its own body can be held to the same forbidden set as
	// an AfterFunc argument, with no special-casing for legitimate
	// socket/sqlConn access - there is none left in interrupt's own text.
	interruptBody := funcBody(t, "migration_context.go", "interrupt")
	if interruptBody == "" {
		t.Fatal("migration_context.go: no interrupt function found; update this guard if it was renamed")
	}
	check("migration_context.go", "interrupt", interruptBody)

	// 4. m.cancel - which makes m.Context, the pgx-visible context, Done -
	// is referenced only from take (the owner's teardown, always safe: the
	// owner has already taken over whatever was bound) and cancelIfUnbound
	// (interrupt's delegate for the one case where a non-owner cancel is
	// safe - nothing bound yet; see the type doc). A reference anywhere else
	// could hand a dead-socket connection to database/sql while still bound,
	// or could silently reintroduce the pool-wait liveness regression this
	// guards if take or cancelIfUnbound stopped calling it.
	allowedCancelSites := map[string]bool{"take": true, "cancelIfUnbound": true}
	for _, path := range files {
		for _, site := range cancelCallSites(t, path) {
			if !allowedCancelSites[site.fn] {
				t.Fatalf("%s: m.cancel is referenced from %s, not just take/cancelIfUnbound; "+
					"canceling the pgx-visible context anywhere else risks handing a "+
					"dead-socket connection to database/sql while bound, or silently "+
					"dropping the pool-wait cancellation path - see migration_context.go's "+
					"ownership-model doc", path, site.fn)
			}
		}
	}
}

// packageGoFiles returns every non-test .go file in the current package
// directory (the test binary's working directory is the package directory).
// Scanning the whole package, rather than a fixed list of file names, is
// what lets this guard catch a new file adding a call site the original
// three-file version could not see.
func packageGoFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files = append(files, name)
	}
	if len(files) == 0 {
		t.Fatal("no production .go files found in package directory")
	}
	return files
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

// afterFuncArgTexts returns the source text of the function-shaped argument
// (always the last argument) of every context.AfterFunc or time.AfterFunc
// call in path - whatever form it takes: a function literal's full body, a
// bare function value, or a method value. Extracting by source position
// rather than requiring an *ast.FuncLit is what lets this catch
// time.AfterFunc(d, l.operation.discard) as well as a wrapping closure.
func afterFuncArgTexts(t *testing.T, path string) []string {
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
	var texts []string
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
		if len(call.Args) == 0 {
			return true
		}
		arg := call.Args[len(call.Args)-1]
		start := fset.Position(arg.Pos()).Offset
		end := fset.Position(arg.End()).Offset
		texts = append(texts, string(src[start:end]))
		return true
	})
	return texts
}

// funcBody returns the source text of the function or method declaration
// named name in path, matching by name only regardless of receiver. Returns
// "" if no such declaration exists.
func funcBody(t *testing.T, path, name string) string {
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
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name {
			continue
		}
		start := fset.Position(fn.Pos()).Offset
		end := fset.Position(fn.End()).Offset
		return string(src[start:end])
	}
	return ""
}

// cancelSite names the function or method enclosing a m.cancel-shaped call.
type cancelSite struct{ fn string }

// cancelCallSites returns one cancelSite per call whose selector is "cancel"
// (for example m.cancel(...)) in path, naming the enclosing function/method.
func cancelCallSites(t *testing.T, path string) []cancelSite {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var sites []cancelSite
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "cancel" {
				sites = append(sites, cancelSite{fn: fn.Name.Name})
			}
			return true
		})
	}
	return sites
}
