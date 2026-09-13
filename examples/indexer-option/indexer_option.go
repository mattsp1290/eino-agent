// Package indexeroption is a small, runnable proof of Eino v0.9.19's
// components/indexer.WithIndex call option (01-feature-inventory.md row 26,
// "Upstream through composition"). eino-agent's own runtime does not touch
// components/indexer at all -- this package adds no new indexing service or
// local wrapper API. It exists only to demonstrate the real upstream
// indexer.Indexer interface and indexer.WithIndex/indexer.GetCommonOptions
// call-option contract compiling and behaving as documented, against a
// minimal in-memory test indexer.
package indexeroption

import (
	"context"

	"github.com/cloudwego/eino/components/indexer"
	"github.com/cloudwego/eino/schema"
)

// TestIndexer is a minimal indexer.Indexer that records the resolved
// indexer.Options for its most recent Store call, so a caller can assert
// which call-time options actually reached the implementation -- exactly
// what a real indexer.Indexer implementation is required to do with
// indexer.GetCommonOptions (see that function's doc comment).
type TestIndexer struct {
	// LastOptions is the *indexer.Options resolved by the most recent Store
	// call, or nil if Store has never been called.
	LastOptions *indexer.Options
	// Documents accumulates every document ID minted across calls, keyed by
	// the resolved index name ("" when no indexer.WithIndex was passed).
	Documents map[string][]string
}

// NewTestIndexer returns a ready-to-use TestIndexer.
func NewTestIndexer() *TestIndexer {
	return &TestIndexer{Documents: map[string][]string{}}
}

// Store implements indexer.Indexer. It resolves opts through the real
// indexer.GetCommonOptions the same way a production indexer must, records
// the resolved Options, and mints one deterministic ID per document under
// the resolved index name.
func (t *TestIndexer) Store(_ context.Context, docs []*schema.Document, opts ...indexer.Option) ([]string, error) {
	options := indexer.GetCommonOptions(&indexer.Options{}, opts...)
	t.LastOptions = options

	indexName := ""
	if options.Index != nil {
		indexName = *options.Index
	}
	ids := make([]string, len(docs))
	for i, doc := range docs {
		id := doc.ID
		if id == "" {
			id = indexName + "-doc"
		}
		ids[i] = id
	}
	t.Documents[indexName] = append(t.Documents[indexName], ids...)
	return ids, nil
}

var _ indexer.Indexer = (*TestIndexer)(nil)
