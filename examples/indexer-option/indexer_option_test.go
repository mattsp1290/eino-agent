package indexeroption

import (
	"context"
	"testing"

	"github.com/cloudwego/eino/components/indexer"
	"github.com/cloudwego/eino/schema"
)

// TestIndexerWithIndexOptionReachesStore proves 01-feature-inventory.md row
// 26's acceptance bar directly: a small test indexer reads the ACTUAL
// upstream indexer.WithIndex call option through indexer.GetCommonOptions,
// with no new eino-agent indexing service or local wrapper API involved.
func TestIndexerWithIndexOptionReachesStore(t *testing.T) {
	idx := NewTestIndexer()
	docs := []*schema.Document{{ID: "doc-1", Content: "hello"}}

	ids, err := idx.Store(context.Background(), docs, indexer.WithIndex("customer-support"))
	if err != nil {
		t.Fatalf("Store error = %v", err)
	}
	if len(ids) != 1 || ids[0] != "doc-1" {
		t.Fatalf("Store ids = %#v, want [doc-1]", ids)
	}
	if idx.LastOptions == nil || idx.LastOptions.Index == nil {
		t.Fatal("indexer.WithIndex did not reach Store's resolved Options.Index")
	}
	if got := *idx.LastOptions.Index; got != "customer-support" {
		t.Fatalf("resolved Options.Index = %q, want %q", got, "customer-support")
	}
	if got := idx.Documents["customer-support"]; len(got) != 1 || got[0] != "doc-1" {
		t.Fatalf("Documents[customer-support] = %#v, want [doc-1]", got)
	}
}

// TestIndexerWithoutIndexOptionLeavesIndexNil proves the absence direction:
// omitting indexer.WithIndex leaves Options.Index nil, so a caller cannot
// mistake "no index requested" for an empty-string index.
func TestIndexerWithoutIndexOptionLeavesIndexNil(t *testing.T) {
	idx := NewTestIndexer()
	if _, err := idx.Store(context.Background(), []*schema.Document{{ID: "doc-2"}}); err != nil {
		t.Fatalf("Store error = %v", err)
	}
	if idx.LastOptions == nil {
		t.Fatal("Store never resolved Options")
	}
	if idx.LastOptions.Index != nil {
		t.Fatalf("resolved Options.Index = %q, want nil", *idx.LastOptions.Index)
	}
}
