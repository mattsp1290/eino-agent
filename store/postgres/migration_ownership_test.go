package postgres

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
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
// through a helper this list does not yet resolve (afterFuncResolvedTexts
// follows one level of indirection - a bare function value, a method value,
// or a call to a function that returns a closure - but not further), or
// catch every possible obfuscation - that is what
// testCanceledMigrationOwnerRace's runtime.Stack-based assertion in
// migration_lock_integration_test.go is for: it checks the actual calling
// goroutine of every discardPGXConnection call at runtime, which no static
// text scan can be fooled past. Keep both; they catch different mutants.
func TestMigrationOwnershipGuard(t *testing.T) {
	files := packageGoFiles(t)
	pkg := loadOwnershipPackage(t, files)

	// 1. discardPGXConnection has exactly two call sites across every
	// production .go file in this package: the owner-side choke point
	// releaseConn (migration_context.go), and dialect.go's own
	// discardAndClose - a separate, unrelated caller that runs serially
	// under postgresTransaction.mu (see migration_lock.go's discardObserved
	// doc). A new call site anywhere else - including a new file - must be
	// reviewed against the ownership invariant before this count changes.
	total := 0
	for _, path := range files {
		total += countCalls(pkg.file(path), "discardPGXConnection")
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
	// The forbidden set matches bare names, not just call-shaped "foo(":
	// this text is produced by go/printer from a comment-free AST (see
	// loadOwnershipPackage), so it can never be a doc comment or an inline
	// comment that merely names one of these - only real, compiled code -
	// and afterFuncResolvedTexts resolves a function literal's full body, a
	// bare function value, a method value (time.AfterFunc(d,
	// l.operation.discard)), or one level through a helper that returns a
	// closure (time.AfterFunc(d, cleanupAbortFor(cleanup))), so none of
	// those forms need a call-shaped "(" to be caught either.
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
		for _, arg := range pkg.afterFuncResolvedTexts(path) {
			check(path, "a context.AfterFunc/time.AfterFunc argument (or the "+
				"function/closure it resolves to)", arg)
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
	interruptBody := pkg.funcDeclText("interrupt")
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
		for _, site := range cancelCallSites(pkg.file(path)) {
			if !allowedCancelSites[site] {
				t.Fatalf("%s: m.cancel is referenced from %s, not just take/cancelIfUnbound; "+
					"canceling the pgx-visible context anywhere else risks handing a "+
					"dead-socket connection to database/sql while bound, or silently "+
					"dropping the pool-wait cancellation path - see migration_context.go's "+
					"ownership-model doc", path, site)
			}
		}
	}

	// 5. bind must still refuse to bind an already-canceled m.Context, not
	// just an m.interrupted cause: take clears m.interrupted once it has
	// reported it (see take's doc), so after a plain discard/stop
	// m.interrupted alone no longer reflects that this context was ever
	// interrupted, even though cancelIfUnbound already canceled m.Context.
	// This is a structural check for that second gate so it cannot be
	// quietly dropped again the way base 10292e1's equivalent was.
	bindBody := pkg.funcDeclText("bind")
	if bindBody == "" {
		t.Fatal("migration_context.go: no bind function found; update this guard if it was renamed")
	}
	if !strings.Contains(bindBody, "context.Cause") {
		t.Fatalf("migration_context.go: bind no longer checks context.Cause(m.Context); "+
			"without that check, bind refuses only on m.interrupted, which take clears once reported, "+
			"so a future caller that rebinds after a discard could hand database/sql a *sql.Conn on an "+
			"already-canceled context - see migration_context.go's bind doc. Body:\n%s", bindBody)
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

// ownershipPackage is every production file in the package, parsed once
// with a shared *token.FileSet and without comments (parser mode 0): with
// nothing for the parser to attach, go/printer's output can never include a
// doc comment or an inline comment, only compiled code - which is what lets
// funcDeclText/afterFuncResolvedTexts be matched against forbidden without a
// comment that merely documents the invariant tripping the guard meant to
// enforce it.
type ownershipPackage struct {
	fset  *token.FileSet
	files map[string]*ast.File     // path -> parsed file
	decls map[string]*ast.FuncDecl // name only, regardless of receiver or file - see funcDeclText
}

func loadOwnershipPackage(t *testing.T, paths []string) *ownershipPackage {
	t.Helper()
	pkg := &ownershipPackage{
		fset:  token.NewFileSet(),
		files: make(map[string]*ast.File, len(paths)),
		decls: make(map[string]*ast.FuncDecl),
	}
	for _, path := range paths {
		file, err := parser.ParseFile(pkg.fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		pkg.files[path] = file
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				pkg.decls[fn.Name.Name] = fn
			}
		}
	}
	return pkg
}

func (p *ownershipPackage) file(path string) *ast.File { return p.files[path] }

// print renders node as comment-free source text via go/printer. Safe to
// call on any node from a file in p.files, since they share p.fset.
func (p *ownershipPackage) print(node ast.Node) string {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, p.fset, node); err != nil {
		return "<go/printer error: " + err.Error() + ">"
	}
	return buf.String()
}

// funcDeclText returns the printed, comment-free source of the function or
// method declaration named name, matching by name only regardless of
// receiver or which file declares it. Returns "" if no such declaration
// exists anywhere in the package.
func (p *ownershipPackage) funcDeclText(name string) string {
	fn, ok := p.decls[name]
	if !ok {
		return ""
	}
	return p.print(fn)
}

// afterFuncResolvedTexts returns the printed, comment-free text that the
// function-shaped last argument of every context.AfterFunc/time.AfterFunc
// call in path stands for. A function literal prints as its own body. A
// bare function value or a method value (an *ast.Ident or *ast.SelectorExpr
// naming a package-level function or method) resolves one level, to that
// declaration's own printed body. A call to a helper that returns a closure
// (time.AfterFunc(d, cleanupAbortFor(cleanup))) also resolves one level: to
// the helper's declaration, and if that declaration's body is exactly
// `return <func literal>`, to the returned literal's body specifically.
// Anything else falls back to printing the argument expression itself, so
// an unresolvable argument is still scanned as text rather than skipped.
func (p *ownershipPackage) afterFuncResolvedTexts(path string) []string {
	file := p.files[path]
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
		texts = append(texts, p.resolveCallableText(call.Args[len(call.Args)-1]))
		return true
	})
	return texts
}

// resolveCallableText implements afterFuncResolvedTexts' one-level
// resolution for a single argument expression; see that doc for the forms
// it handles.
func (p *ownershipPackage) resolveCallableText(arg ast.Expr) string {
	switch e := arg.(type) {
	case *ast.FuncLit:
		return p.print(e)
	case *ast.Ident:
		if body := p.funcDeclText(e.Name); body != "" {
			return body
		}
	case *ast.SelectorExpr:
		if body := p.funcDeclText(e.Sel.Name); body != "" {
			return body
		}
	case *ast.CallExpr:
		var name string
		switch fn := e.Fun.(type) {
		case *ast.Ident:
			name = fn.Name
		case *ast.SelectorExpr:
			name = fn.Sel.Name
		}
		if name == "" {
			break
		}
		fn, ok := p.decls[name]
		if !ok {
			break
		}
		if lit := lastReturnedFuncLit(fn); lit != nil {
			return p.print(lit)
		}
		return p.print(fn)
	}
	return p.print(arg)
}

// lastReturnedFuncLit returns the *ast.FuncLit a function declaration's body
// returns, when that body is exactly `return <func literal>` - the
// helper-returning-a-closure shape used to wire a non-owner discard into an
// AfterFunc call without any forbidden text appearing at the call site
// itself (for example cleanupAbortFor in the coordinator's mutant proof).
// Returns nil for any other body shape.
func lastReturnedFuncLit(fn *ast.FuncDecl) *ast.FuncLit {
	if fn.Body == nil {
		return nil
	}
	for _, stmt := range fn.Body.List {
		ret, ok := stmt.(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			continue
		}
		if lit, ok := ret.Results[0].(*ast.FuncLit); ok {
			return lit
		}
	}
	return nil
}

func countCalls(file *ast.File, name string) int {
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

// cancelCallSites returns the name of the enclosing function or method for
// every call whose selector is "cancel" (for example m.cancel(...)) in file.
func cancelCallSites(file *ast.File) []string {
	var sites []string
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
				sites = append(sites, fn.Name.Name)
			}
			return true
		})
	}
	return sites
}
