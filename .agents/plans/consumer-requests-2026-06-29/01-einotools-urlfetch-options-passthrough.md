# Plan 01 — `einotools.RegisterDefaults` url_fetch options passthrough

**Request:** `~/.agents/projects/eino-agent/requests/2026-06-29-einotools-registerdefaults-options-passthrough.md`
**Decision:** Add a typed `URLFetchOptions *urlfetch.Options` field to
`einotools.Options`, threaded into the `url_fetch` static factory. Mirrors the
existing `ShellOptions *shell.Options` precedent in the same struct.
**Scope:** one package — `tools/einotools`. Purely additive, no API break.

## Why this shape

- The struct **already has the exact pattern**: `ShellOptions *shell.Options`
  (`tools/einotools/einotools.go:47`) is an optional pointer threaded into
  `shell.New(root, *shellOptions)` only when non-nil
  (`einotools.go:99-104`). Copying it for url_fetch is the least-surprise design.
- `urlfetch.New` is variadic (`func New(opts ...Options)`) and
  `urlfetch.Options.HTTPClient *http.Client` is the documented injection point
  the consumer needs (host allowlist via a custom `http.Client.CheckRedirect`).
- A pointer (not a value) preserves "not configured ⇒ tool's own default
  client" — passing a zero `urlfetch.Options{}` would also work because
  `withDefaults()` fills a nil client, but the pointer keeps the einotools API
  honest about "unset vs set" and matches `ShellOptions`.

This resolves gap **T1**: the consumer no longer needs to capture the returned
`url_fetch` `Registration` and `Registry.Replace` it.

## Changes

All in `tools/einotools/einotools.go`.

### 1. Add the field to `Options` (around `:42`)

```go
// Options controls optional leaf-tool adapters.
type Options struct {
	Locker          *workspace.Locker
	UserSurface     userinteract.Surface
	UserStdin       io.Reader
	UserStderr      io.Writer
	ShellOptions    *shell.Options
	TrackerWriter   tracker.CloseWriter
	URLFetchOptions *urlfetch.Options // optional; threaded into urlfetch.New(...)
}
```

`urlfetch` is already imported (`einotools.go:20`) — no new import.

### 2. Thread it into the `url_fetch` static spec (`staticSpecs`, around `:121`)

Replace the current `url_fetch` entry:

```go
{name: urlfetch.Name, factory: func() (invokableTool, error) { return urlfetch.New() }},
```

with a closure that captures the option:

```go
urlFetchOptions := options.URLFetchOptions
// ...
specs := []staticSpec{
	{name: urlfetch.Name, factory: func() (invokableTool, error) {
		if urlFetchOptions == nil {
			return urlfetch.New()
		}
		return urlfetch.New(*urlFetchOptions)
	}},
	// userinteract entry unchanged
}
```

Capture `urlFetchOptions` into a local at the top of `staticSpecs` (next to the
existing `surface`/`stdin`/`stderr` locals) so the closure does not retain the
whole `options` value — matches how `shellOptions` is hoisted in
`workspaceSpecs` (`einotools.go:90`).

> Note: `registerStatic` calls the factory twice — once for `Info(ctx)` at
> registration time (`einotools.go:155`) and once per execution
> (`einotools.go:165`). Both go through the same closure, so the injected
> client is used at execute time too. No other change required.

## Optional: generalize later (out of scope for this pass)

The request mentions a per-tool options map or post-register hook would also
cover the shell case. **Do not build that now** — shell already has its own
`ShellOptions` field, so the only concrete unmet need is `url_fetch`. A typed
field keeps type-safety and discoverability; revisit a generic map only if a
third leaf tool needs config. Note this trade-off in the response file.

## Tests — `tools/einotools/einotools_test.go`

Add a test that proves the injected `HTTPClient` is actually used by the
registered `url_fetch` tool (not just that the field round-trips). The strongest
observable signal is a custom `http.RoundTripper` that records that it was hit.

Sketch:

```go
func TestRegisterDefaultsURLFetchOptionsHTTPClient(t *testing.T) {
	ctx := context.Background()

	// httptest server returns a known body; a sentinel transport flags use.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "from-injected-client")
	}))
	defer srv.Close()

	hit := false
	client := srv.Client() // trusts the test server's TLS cert
	base := client.Transport
	client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		hit = true
		return base.RoundTrip(req)
	})

	registry := agenttools.NewRegistry() // confirm constructor name in tools/registry.go
	regs, err := einotools.RegisterDefaults(ctx, registry, einotools.Options{
		URLFetchOptions: &urlfetch.Options{HTTPClient: client},
	})
	if err != nil {
		t.Fatalf("RegisterDefaults: %v", err)
	}
	_ = regs

	// Resolve + invoke the url_fetch tool against srv.URL and assert `hit`.
	// Drive it the same way existing einotools_test.go invokes a registered
	// static tool (follow the established harness in that file rather than
	// hand-rolling Execution wiring).
	if !hit {
		t.Fatal("injected HTTPClient was not used by url_fetch")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
```

**Before writing the test, read `tools/einotools/einotools_test.go`** to reuse
its existing pattern: `RegisterDefaults` → `registry.ResolveTools(ctx,
snapshot(root, id))` → `materialized[i].InputDecoder.DecodeToolInput(ctx, raw)`
→ `materialized[i].Executor.Execute(ctx, runtime.ToolCall{Input: normalized})`
(see `TestFileReadWrapperPreservesEinoToolsContract`, `einotools_test.go:80-91`,
and the `snapshot(...)` helper at `:316`).

Test mechanics that will bite if missed:

- **Select `url_fetch` by `tool.Name`, not by index.** `ResolveTools` iterates a
  map, so order is nondeterministic. The existing `materialized[0]` tests work
  only because they register a single tool; `RegisterDefaults` registers ~10, so
  find the entry whose `.Name == urlfetch.Name`.
- **Input key is `{"url": "<srv.URL>"}`** — that is urlfetch's schema field
  (`url string json:"url"`). Feed the test server's URL.
- **HTTPS only / default-deny:** use `httptest.NewTLSServer` + `srv.Client()`
  (the eino-tools urlfetch tests at `pkg/mod/.../urlfetch/urlfetch_test.go:142`
  do exactly this) so the request isn't rejected before the client is reached.
- **Assert the response body, not just `hit`.** Check the Execute output
  contains the server's body (e.g. `"from-injected-client"`) in addition to the
  `hit` transport flag — that proves the injected client is wired into the
  actual fetch path, not merely constructed.

Also add a cheap guard test: `URLFetchOptions: nil` still registers `url_fetch`
successfully (the default-client path), so the nil branch stays covered.

## Verification

```bash
cd /Users/punk1290/git/eino-agent
make fmt
go test ./tools/einotools/...   # fast inner loop
make vet && make test && make lint
```

Done means: new test passes (proves the client is used), existing einotools
tests still green, lint/vet clean.

## Acceptance deliverable

Write the response confirming the field name/shape — see file `03`.
</content>
