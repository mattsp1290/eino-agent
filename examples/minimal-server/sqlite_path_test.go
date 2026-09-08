package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNewServerSQLitePaths(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, test := range []struct {
		name string
		path string
		want string
	}{
		{name: "default", want: "minimal-server.db"},
		{name: "relative", path: "relative store.db", want: "relative store.db"},
		{name: "special", path: "special #?.db", want: "special #?.db"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewServer(context.Background(), test.path)
			if err != nil {
				t.Fatalf("NewServer(%q): %v", test.path, err)
			}
			if err := server.Close(); err != nil {
				t.Fatalf("Close(%q): %v", test.path, err)
			}
			if _, err := os.Stat(filepath.Clean(test.want)); err != nil {
				t.Fatalf("SQLite file %q: %v", test.want, err)
			}
		})
	}
}
