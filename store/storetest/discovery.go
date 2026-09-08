package storetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// RunDiscovery tests the optional SessionDiscoveryReader capability. Factory
// must supply it explicitly; Run alone does not establish discovery support.
func RunDiscovery(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("discovery order scope and pages", func(t *testing.T) { discoveryPages(t, factory) })
	t.Run("discovery errors", func(t *testing.T) { discoveryErrors(t, factory) })
	t.Run("discovery between page writes", func(t *testing.T) { discoveryWrites(t, factory) })
	t.Run("discovery bounded private read only", func(t *testing.T) { discoveryPrivate(t, factory) })
}

func discoverySubject(t *testing.T, factory Factory) (session.Store, session.SessionDiscoveryReader) {
	t.Helper()
	st := setup(t, factory).Store
	reader, ok := st.(session.SessionDiscoveryReader)
	if !ok {
		t.Fatal("RunDiscovery requires SessionDiscoveryReader")
	}
	return st, reader
}
func discoveryCreate(t *testing.T, st session.Store, s session.Session) {
	t.Helper()
	if _, err := st.CreateSession(t.Context(), s); err != nil {
		t.Fatal(err)
	}
}
func discoveryList(t *testing.T, reader session.SessionDiscoveryReader, q session.SessionDiscoveryQuery) session.SessionDiscoveryPage {
	t.Helper()
	page, err := reader.ListSessions(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	return page
}
func discoveryIDs(p session.SessionDiscoveryPage) []session.ID {
	ids := make([]session.ID, len(p.Sessions))
	for i, s := range p.Sessions {
		ids[i] = s.ID
	}
	return ids
}
func discoveryPages(t *testing.T, factory Factory) {
	st, reader := discoverySubject(t, factory)
	at := time.Date(2026, 9, 8, 1, 2, 3, 4, time.UTC)
	seeds := []session.Session{
		{ID: "z", WorkspaceID: "A", CreatedAt: at}, {ID: "a", WorkspaceID: "A", CreatedAt: at.In(time.FixedZone("offset", 3600))},
		{ID: "n", WorkspaceID: "A", CreatedAt: at.Add(time.Nanosecond)}, {ID: "zero", WorkspaceID: "A"},
		{ID: "year-zero", WorkspaceID: "A", CreatedAt: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "other", WorkspaceID: "B", CreatedAt: at}, {ID: "unscoped"},
	}
	for _, s := range seeds {
		discoveryCreate(t, st, s)
	}
	want := []session.ID{"n", "z", "a", "year-zero", "zero"}
	for _, limit := range []int{0, 1, 2, 5, 100} {
		q := session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: limit}
		var ids []session.ID
		for i := 0; ; i++ {
			if i > len(want) {
				t.Fatal("pagination failed to terminate")
			}
			page := discoveryList(t, reader, q)
			ids = append(ids, discoveryIDs(page)...)
			for _, s := range page.Sessions {
				if s.WorkspaceID != "A" || s.CreatedAt.Location() != time.UTC || s.UpdatedAt.Location() != time.UTC {
					t.Fatal(s)
				}
			}
			if page.NextCursor == "" {
				break
			}
			q.Cursor = page.NextCursor
			if limit == 2 {
				q.Limit = 1
			}
		}
		if !reflect.DeepEqual(ids, want) {
			t.Fatal(limit, ids)
		}
	}
	for _, ws := range []string{"C", "*", "a", " A"} {
		p := discoveryList(t, reader, session.SessionDiscoveryQuery{WorkspaceID: ws})
		if len(p.Sessions) != 0 || p.NextCursor != "" {
			t.Fatal(p)
		}
	}
	// A full default page and full maximum page must distinguish their last row.
	for i := 0; i < 101; i++ {
		discoveryCreate(t, st, session.Session{ID: session.ID(fmt.Sprintf("bulk-%03d", i)), WorkspaceID: "bulk"})
	}
	for _, limit := range []int{0, 100} {
		p := discoveryList(t, reader, session.SessionDiscoveryQuery{WorkspaceID: "bulk", Limit: limit})
		n := limit
		if n == 0 {
			n = 50
		}
		if len(p.Sessions) != n || p.NextCursor == "" {
			t.Fatal(p)
		}
	}
}
func discoveryErrors(t *testing.T, factory Factory) {
	st, reader := discoverySubject(t, factory)
	for _, id := range []session.ID{"a", "b"} {
		discoveryCreate(t, st, session.Session{ID: id, WorkspaceID: "A"})
	}
	cursor := discoveryList(t, reader, session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1}).NextCursor
	for _, tc := range []struct {
		q    session.SessionDiscoveryQuery
		want error
	}{
		{session.SessionDiscoveryQuery{}, session.ErrDiscoveryQuery},
		{session.SessionDiscoveryQuery{WorkspaceID: "\xff"}, session.ErrDiscoveryQuery},
		{session.SessionDiscoveryQuery{WorkspaceID: strings.Repeat("x", 1025)}, session.ErrDiscoveryQuery},
		{session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: -1}, session.ErrDiscoveryQuery},
		{session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 101}, session.ErrDiscoveryQuery},
		{session.SessionDiscoveryQuery{WorkspaceID: "A", Cursor: "PRIVATE_BAD_CURSOR"}, session.ErrDiscoveryCursor},
		{session.SessionDiscoveryQuery{WorkspaceID: "A", Cursor: strings.Repeat("x", 8193)}, session.ErrDiscoveryCursor},
		{session.SessionDiscoveryQuery{WorkspaceID: "B", Cursor: cursor}, session.ErrDiscoveryCursor},
	} {
		p, err := reader.ListSessions(t.Context(), tc.q)
		discoveryErrorPage(t, p, err, tc.want)
	}
	_, other := discoverySubject(t, factory)
	p, err := other.ListSessions(t.Context(), session.SessionDiscoveryQuery{WorkspaceID: "A", Cursor: cursor})
	discoveryErrorPage(t, p, err, session.ErrDiscoveryCursor)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p, err = reader.ListSessions(ctx, session.SessionDiscoveryQuery{})
	discoveryErrorPage(t, p, err, context.Canceled)
	ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	p, err = reader.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: "A"})
	discoveryErrorPage(t, p, err, context.DeadlineExceeded)
}
func discoveryErrorPage(t *testing.T, p session.SessionDiscoveryPage, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || !reflect.DeepEqual(p, session.SessionDiscoveryPage{}) || err.Error() != want.Error() {
		t.Fatalf("page=%+v err=%v want=%v", p, err, want)
	}
}
func discoveryWrites(t *testing.T, factory Factory) {
	st, reader := discoverySubject(t, factory)
	for _, id := range []session.ID{"b", "d"} {
		discoveryCreate(t, st, session.Session{ID: id, WorkspaceID: "A"})
	}
	first := discoveryList(t, reader, session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1})
	for _, id := range []session.ID{"a", "z"} {
		discoveryCreate(t, st, session.Session{ID: id, WorkspaceID: "A"})
	}
	for _, id := range []session.ID{"b", "d"} {
		s, err := st.GetSession(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		s.Title = "new-" + string(id)
		s.UpdatedAt = time.Now()
		if err = st.UpdateSession(t.Context(), s); err != nil {
			t.Fatal(err)
		}
	}
	next := discoveryList(t, reader, session.SessionDiscoveryQuery{WorkspaceID: "A", Cursor: first.NextCursor})
	if !reflect.DeepEqual(discoveryIDs(next), []session.ID{"b", "a"}) || next.Sessions[0].Title != "new-b" {
		t.Fatal(next)
	}
	refresh := discoveryList(t, reader, session.SessionDiscoveryQuery{WorkspaceID: "A"})
	if !reflect.DeepEqual(discoveryIDs(refresh), []session.ID{"z", "d", "b", "a"}) || refresh.Sessions[1].Title != "new-d" {
		t.Fatal(refresh)
	}
}
