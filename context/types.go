package agentcontext

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// Kind classifies context material before it is converted into provider
// messages or attachment references.
type Kind string

const (
	// KindSystemPrompt is durable system instruction material.
	KindSystemPrompt Kind = "system_prompt"
	// KindProjectInstructions is host or project instruction material.
	KindProjectInstructions Kind = "project_instructions"
	// KindAttachment is user-visible attachment content or metadata.
	KindAttachment Kind = "attachment"
	// KindReference is a future reference handle, such as a vector-search hit.
	KindReference Kind = "reference"
)

var (
	// ErrTooLarge reports that context material exceeds configured bounds.
	ErrTooLarge = errors.New("context material exceeds bounds")
	// ErrLocalPath reports a local-only file path that should not be baked into
	// portable runtime context.
	ErrLocalPath = errors.New("context reference uses local-only path")
)

var windowsDrivePath = regexp.MustCompile(`^[A-Za-z]:[\\/]`)

// TraceContext contains correlation identifiers propagated through runtime
// context without tying the core packages to a tracing SDK.
type TraceContext struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Attributes   map[string]string
}

// Identity is the stable runtime identity visible to context sources, tools,
// model adapters, observability, and protocol adapters.
type Identity struct {
	SessionID          session.ID
	RunID              session.RunID
	AgentID            string
	AssistantMessageID session.MessageID
	ToolCallID         session.ToolCallID
	ProviderID         model.ProviderID
	ModelID            model.ID
	Trace              TraceContext
}

// Clone returns a defensive copy of mutable identity fields.
func (i Identity) Clone() Identity {
	next := i
	next.Trace.Attributes = cloneMap(i.Trace.Attributes)
	return next
}

// Source describes one configured context source without assuming a concrete
// file format, database, host route, or provider transport.
type Source struct {
	Name     string
	Kind     Kind
	Location string
	Options  map[string]string
}

// Request is passed to context source loaders.
type Request struct {
	Identity Identity
	Source   Source
	Bounds   Bounds
	Metadata map[string]string
}

// Clone returns a defensive copy of request maps and nested identity metadata.
func (r Request) Clone() Request {
	next := r
	next.Identity = r.Identity.Clone()
	next.Source.Options = cloneMap(r.Source.Options)
	next.Metadata = cloneMap(r.Metadata)
	return next
}

// Bounds limits context-source output before provider conversion.
type Bounds struct {
	MaxItems        int
	MaxBytesPerItem int
	MaxTotalBytes   int
}

// DefaultBounds returns conservative nonzero bounds for host integrations that
// do not supply source-specific limits.
func DefaultBounds() Bounds {
	return Bounds{
		MaxItems:        32,
		MaxBytesPerItem: 64 * 1024,
		MaxTotalBytes:   256 * 1024,
	}
}

// Item is one bounded unit of context material.
type Item struct {
	SourceName string
	Kind       Kind
	Content    string
	URI        string
	MIMEType   string
	Metadata   map[string]string
}

// Clone returns a defensive copy of item metadata.
func (i Item) Clone() Item {
	next := i
	next.Metadata = cloneMap(i.Metadata)
	return next
}

// Bundle is the assembled output of one or more context sources.
type Bundle struct {
	Items      []Item
	TotalBytes int
}

// Loader loads context material for one source.
type Loader interface {
	LoadContext(ctx context.Context, request Request) ([]Item, error)
}

// LoaderFunc adapts a function into a Loader.
type LoaderFunc func(ctx context.Context, request Request) ([]Item, error)

var _ Loader = LoaderFunc(nil)

// LoadContext calls fn.
func (fn LoaderFunc) LoadContext(ctx context.Context, request Request) ([]Item, error) {
	return fn(ctx, request)
}

// Assembler runs bounded source loaders.
type Assembler struct {
	Loaders []Loader
	Bounds  Bounds
}

// LoadContext loads all configured sources, enforcing cancellation and bounds
// between each loader and before returning material to runtime logic.
func (a Assembler) LoadContext(ctx context.Context, request Request) (Bundle, error) {
	if err := ctx.Err(); err != nil {
		return Bundle{}, err
	}
	bounds := a.Bounds.withDefaults()
	if err := validatePortableSource(request.Source); err != nil {
		return Bundle{}, err
	}
	var bundle Bundle
	for _, loader := range a.Loaders {
		if loader == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return Bundle{}, err
		}
		items, err := loader.LoadContext(ctx, request.Clone())
		if err != nil {
			return Bundle{}, err
		}
		if err := ctx.Err(); err != nil {
			return Bundle{}, err
		}
		if err := appendBounded(&bundle, items, bounds); err != nil {
			return Bundle{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return Bundle{}, err
	}
	return cloneBundle(bundle), nil
}

func appendBounded(bundle *Bundle, items []Item, bounds Bounds) error {
	for _, item := range items {
		next := item.Clone()
		if next.SourceName == "" {
			next.SourceName = "unknown"
		}
		if next.Kind == "" {
			next.Kind = KindReference
		}
		if err := validatePortableURI(next.URI); err != nil {
			return err
		}
		size := itemSize(next)
		if bounds.MaxItems > 0 && len(bundle.Items)+1 > bounds.MaxItems {
			return fmt.Errorf("%w: max items %d", ErrTooLarge, bounds.MaxItems)
		}
		if bounds.MaxBytesPerItem > 0 && size > bounds.MaxBytesPerItem {
			return fmt.Errorf("%w: item %q has %d bytes, max %d", ErrTooLarge, next.SourceName, size, bounds.MaxBytesPerItem)
		}
		if bounds.MaxTotalBytes > 0 && bundle.TotalBytes+size > bounds.MaxTotalBytes {
			return fmt.Errorf("%w: total %d bytes, max %d", ErrTooLarge, bundle.TotalBytes+size, bounds.MaxTotalBytes)
		}
		bundle.Items = append(bundle.Items, next)
		bundle.TotalBytes += size
	}
	return nil
}

func validatePortableSource(source Source) error {
	if err := validatePortableURI(source.Location); err != nil {
		return fmt.Errorf("source location: %w", err)
	}
	for key, value := range source.Options {
		if !isReferenceOption(key) {
			continue
		}
		if err := validatePortableURI(value); err != nil {
			return fmt.Errorf("source option %q: %w", key, err)
		}
	}
	return nil
}

func isReferenceOption(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	return normalized == "path" ||
		normalized == "uri" ||
		normalized == "url" ||
		strings.HasSuffix(normalized, "_path") ||
		strings.HasSuffix(normalized, "_uri") ||
		strings.HasSuffix(normalized, "_url")
}

func validatePortableURI(value string) error {
	if value == "" {
		return nil
	}
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(trimmed, "/") || strings.HasPrefix(trimmed, `\`) || windowsDrivePath.MatchString(trimmed) {
		return fmt.Errorf("%w: %s", ErrLocalPath, value)
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("parse context uri %q: %w", value, err)
	}
	if parsed.Scheme == "file" {
		return fmt.Errorf("%w: %s", ErrLocalPath, value)
	}
	return nil
}

func itemSize(item Item) int {
	size := len(item.Content) + len(item.URI) + len(item.MIMEType) + len(item.SourceName) + len(item.Kind)
	for key, value := range item.Metadata {
		size += len(key) + len(value)
	}
	return size
}

func (b Bounds) withDefaults() Bounds {
	defaults := DefaultBounds()
	if b.MaxItems <= 0 {
		b.MaxItems = defaults.MaxItems
	}
	if b.MaxBytesPerItem <= 0 {
		b.MaxBytesPerItem = defaults.MaxBytesPerItem
	}
	if b.MaxTotalBytes <= 0 {
		b.MaxTotalBytes = defaults.MaxTotalBytes
	}
	return b
}

func cloneBundle(src Bundle) Bundle {
	next := src
	next.Items = make([]Item, len(src.Items))
	for i, item := range src.Items {
		next.Items[i] = item.Clone()
	}
	return next
}

func cloneMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
