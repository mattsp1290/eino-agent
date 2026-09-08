package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
)

var _ session.SessionDiscoveryReader = (*Store)(nil)

// ListSessions reads bounded metadata from one committed workspace index range.
func (s *Store) ListSessions(ctx context.Context, q session.SessionDiscoveryQuery) (session.SessionDiscoveryPage, error) {
	var zero session.SessionDiscoveryPage
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := q.Validate(); err != nil {
		return zero, err
	}
	if s == nil || s.db == nil || s.pool == nil || s.tx != nil {
		return zero, session.ErrDiscoveryReader
	}
	var cursor DiscoveryPosition
	if q.Cursor != "" {
		var err error
		cursor, err = DecodeDiscoveryCursor(q.Cursor, q.WorkspaceID)
		if err != nil {
			return zero, err
		}
	}
	if q.Limit == 0 {
		q.Limit = session.DiscoveryDefaultLimit
	}
	var page session.SessionDiscoveryPage
	err := s.read(ctx, func(tx *Store) error {
		var incarnation sql.NullString
		if err := tx.queryRow(ctx, "SELECT "+tx.boundedColumn("incarnation")+" FROM observation_store WHERE singleton = 1", 32).Scan(&incarnation); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return session.ErrDiscoveryInvalid
			}
			return err
		}
		if !incarnation.Valid || !ValidDiscoveryIncarnation(incarnation.String) {
			return session.ErrDiscoveryInvalid
		}
		if q.Cursor != "" && cursor.StoreID != incarnation.String {
			return session.ErrDiscoveryCursor
		}
		var err error
		page, err = tx.discoveryPage(ctx, q, cursor, incarnation.String)
		return err
	})
	if err != nil {
		return zero, discoveryError(ctx, err)
	}
	return page, nil
}

// Separate first/continuation statements preserve the composite tuple seek.
func (s *Store) discoverySQL(continuation bool) string {
	query := "SELECT " + s.boundedColumn("id") + ", " + s.boundedColumn("workspace_id") + ", " + s.boundedColumn("title") + ", " + s.boundedColumn("created_at") + ", " + s.boundedColumn("updated_at") + ", (" + s.dialect.InvalidScalar("id", true) + " OR " + s.dialect.InvalidScalar("workspace_id", true) + " OR " + s.dialect.InvalidScalar("title", true) + " OR " + s.dialect.InvalidScalar("created_at", false) + " OR " + s.dialect.InvalidScalar("updated_at", false) + ") FROM sessions" + s.dialect.IndexHint("sessions_workspace_created_idx") + " WHERE workspace_id = ?"
	if continuation {
		query += " AND (created_at, id) < (?, ?)"
	}
	return query + " ORDER BY created_at DESC, id DESC LIMIT ?"
}

func (s *Store) discoveryPage(ctx context.Context, q session.SessionDiscoveryQuery, cursor DiscoveryPosition, incarnation string) (session.SessionDiscoveryPage, error) {
	args := []any{session.DiscoveryMaxIdentityBytes, session.DiscoveryMaxIdentityBytes, session.DiscoveryMaxTitleBytes, 30, 30, []byte(q.WorkspaceID)}
	if q.Cursor != "" {
		args = append(args, TimeText(cursor.CreatedAt), []byte(cursor.ID))
	}
	args = append(args, q.Limit+1)
	rows, err := s.query(ctx, s.discoverySQL(q.Cursor != ""), args...)
	if err != nil {
		return session.SessionDiscoveryPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := session.SessionDiscoveryPage{Sessions: make([]session.SessionSummary, 0, q.Limit)}
	for rows.Next() {
		if len(page.Sessions) == q.Limit {
			page.NextCursor = EncodeDiscoveryCursor(incarnation, q.WorkspaceID, page.Sessions[len(page.Sessions)-1])
			break
		}
		summary, err := scanDiscoverySummary(rows, q.WorkspaceID)
		if err != nil {
			return session.SessionDiscoveryPage{}, err
		}
		page.Sessions = append(page.Sessions, summary)
	}
	return page, errors.Join(rows.Err(), rows.Close())
}

func scanDiscoverySummary(row rowScanner, workspace string) (session.SessionSummary, error) {
	var id, ws, title, created, updated sql.NullString
	var summary session.SessionSummary
	var invalid bool
	if err := row.Scan(&id, &ws, &title, &created, &updated, &invalid); err != nil {
		return summary, err
	}
	if invalid {
		return summary, session.ErrDiscoveryInvalid
	}
	// NOT NULL schema columns mean NULL identity/title guards denote excess bytes.
	if !id.Valid || !ws.Valid || !title.Valid {
		return summary, session.ErrDiscoveryTooLarge
	}
	if !created.Valid || !updated.Valid || id.String == "" || ws.String != workspace || !utf8.ValidString(id.String) || !utf8.ValidString(ws.String) || !utf8.ValidString(title.String) {
		return summary, session.ErrDiscoveryInvalid
	}
	var err error
	summary.CreatedAt, err = DiscoveryTime(created.String)
	if err != nil {
		return summary, err
	}
	summary.UpdatedAt, err = DiscoveryTime(updated.String)
	if err != nil {
		return summary, err
	}
	summary.ID, summary.WorkspaceID, summary.Title = session.ID(id.String), ws.String, title.String
	return summary, nil
}

func discoveryError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, safe := range []error{session.ErrDiscoveryCursor, session.ErrDiscoveryTooLarge, session.ErrDiscoveryInvalid} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	return session.ErrDiscoveryStore
}
