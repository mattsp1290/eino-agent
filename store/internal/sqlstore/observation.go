package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
)

func observationError(err error) error {
	if err == nil {
		return nil
	}
	for _, safe := range []error{context.Canceled, context.DeadlineExceeded, session.ErrObservationLimits, session.ErrObservationTooLarge, session.ErrObservationInvalid, session.ErrObservationReader} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	return session.ErrObservationStore
}

func (s *Store) observationTx(ctx context.Context, id session.ID, fn func(*Store) error) error {
	if s == nil || s.tx != nil || s.db == nil || s.pool == nil || id == "" {
		return session.ErrObservationReader
	}
	return observationError(s.read(ctx, fn))
}

func (s *Store) revision(ctx context.Context, id session.ID) (session.ObservationWatermark, bool, error) {
	w := session.ObservationWatermark{SessionID: id}
	var exists bool
	var incarnation sql.NullString
	err := s.queryRow(ctx, "SELECT "+s.boundedColumn("incarnation")+", COALESCE((SELECT revision FROM observation_revisions WHERE session_id = ?), 0), EXISTS(SELECT 1 FROM sessions WHERE id = ?) FROM observation_store WHERE singleton = 1", 32, []byte(id), []byte(id)).Scan(&incarnation, &w.Revision, &exists)
	if err == nil && (!incarnation.Valid || !ValidDiscoveryIncarnation(incarnation.String)) {
		err = session.ErrObservationInvalid
	}
	w.StoreID = incarnation.String
	if err == nil && (!exists && w.Revision != 0 || exists && w.Revision <= 0) {
		err = session.ErrObservationInvalid
	}
	return w, exists, err
}
func (s *Store) ReadObservationRevision(ctx context.Context, id session.ID) (w session.ObservationWatermark, err error) {
	err = s.observationTx(ctx, id, func(tx *Store) error { var e error; w, _, e = tx.revision(ctx, id); return e })
	return
}
func (s *Store) ReadObservationSnapshot(ctx context.Context, id session.ID, limits session.ObservationLimits) (session.ObservationSnapshot, error) {
	if err := limits.Validate(); err != nil {
		return session.ObservationSnapshot{}, err
	}
	var result session.ObservationSnapshot
	err := s.observationTx(ctx, id, func(tx *Store) error {
		// BUSY retries restart the complete snapshot on a fresh read view.
		result = session.ObservationSnapshot{}
		var err error
		result.Watermark, result.Exists, err = tx.revision(ctx, id)
		if err != nil {
			return err
		}
		budget := &observationBudget{bytes: limits.MaxSnapshotBytes, text: limits.MaxTextBytes, parts: limits.MaxParts}
		if err := budget.record(len(result.Watermark.StoreID) + len(id)); err != nil {
			return err
		}
		if !result.Exists {
			return nil
		}
		if err = tx.observationMessages(ctx, &result, limits, budget); err != nil {
			return err
		}
		return tx.observationRuns(ctx, &result, limits, budget)
	})
	if err != nil {
		return session.ObservationSnapshot{}, err
	}
	return result, nil
}

type observationBudget struct{ bytes, text, parts int }

func (b *observationBudget) record(n int) error {
	if b.bytes < 256 || n > (b.bytes-256)/6 {
		return session.ErrObservationTooLarge
	}
	b.bytes -= 256 + 6*n
	return nil
}

// Bound each selected scalar at SQL's boundary before the driver materializes
// it. Record JSON (including private metadata) is never selected.
func (s *Store) boundedColumn(column string) string {
	return "CASE WHEN " + s.dialect.ByteLength(column) + " <= ? THEN " + column + " ELSE NULL END"
}
func (s *Store) observationMessages(ctx context.Context, out *session.ObservationSnapshot, l session.ObservationLimits, b *observationBudget) error {
	rows, err := s.query(ctx, "SELECT "+s.boundedColumn("m.id")+", "+s.boundedColumn("r.id")+", m.role, m.finalized, COALESCE(r.session_key = m.session_key, FALSE) FROM messages AS m"+s.dialect.IndexHint("messages_observation_idx")+" LEFT JOIN runs AS r ON r.row_key = m.run_key WHERE m.session_key = (SELECT row_key FROM sessions WHERE id = ?) AND m.role IN ('user', 'assistant') ORDER BY m.created_at DESC, m.id DESC LIMIT ?", b.bytes/6, b.bytes/6, []byte(out.Watermark.SessionID), l.MaxMessages+1)
	if err != nil {
		return err
	}
	for rows.Next() {
		if len(out.Messages) == l.MaxMessages {
			out.OmittedOlderMessages = true
			break
		}
		var id, run sql.NullString
		var m session.ObservationMessage
		var owner bool
		if err = rows.Scan(&id, &run, &m.Role, &m.Finalized, &owner); err != nil {
			break
		}
		if !owner {
			err = session.ErrObservationInvalid
			break
		}
		if !id.Valid || !run.Valid {
			err = session.ErrObservationTooLarge
			break
		}
		m.ID = session.MessageID(id.String)
		m.RunID = session.RunID(run.String)
		if id.String == "" || run.String == "" || !observationStringsValid(id.String, run.String) {
			err = session.ErrObservationInvalid
			break
		}
		if err = b.record(len(id.String) + len(run.String) + len(m.Role)); err != nil {
			break
		}
		out.Messages = append(out.Messages, m)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	for i, j := 0, len(out.Messages)-1; i < j; i, j = i+1, j-1 {
		out.Messages[i], out.Messages[j] = out.Messages[j], out.Messages[i]
	}
	for i := range out.Messages {
		if err = s.observationParts(ctx, out.Watermark.SessionID, &out.Messages[i], b); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) observationParts(ctx context.Context, id session.ID, m *session.ObservationMessage, b *observationBudget) error {
	rows, err := s.query(ctx, "SELECT "+s.boundedColumn("display_text")+", text_valid, COALESCE(run_key = (SELECT row_key FROM runs WHERE id = ?), FALSE) FROM parts"+s.dialect.IndexHint("parts_observation_idx")+" WHERE session_key = (SELECT row_key FROM sessions WHERE id = ?) AND message_key = (SELECT row_key FROM messages WHERE id = ?) AND kind = 'text' ORDER BY ordinal,id LIMIT ?", min(b.text, b.bytes/6), []byte(m.RunID), []byte(id), []byte(m.ID), b.parts+1)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var text strings.Builder
	for rows.Next() {
		if b.parts == 0 {
			return session.ErrObservationTooLarge
		}
		b.parts--
		var value sql.NullString
		var valid, owner bool
		if err = rows.Scan(&value, &valid, &owner); err != nil {
			return err
		}
		if !valid || !owner {
			return session.ErrObservationInvalid
		}
		if !value.Valid || len(value.String) > b.text {
			return session.ErrObservationTooLarge
		}
		if !utf8.ValidString(value.String) {
			return session.ErrObservationInvalid
		}
		if err = b.record(len(value.String)); err != nil {
			return err
		}
		b.text -= len(value.String)
		text.WriteString(value.String)
	}
	m.Text = text.String()
	return rows.Err()
}
func (s *Store) observationRuns(ctx context.Context, out *session.ObservationSnapshot, l session.ObservationLimits, b *observationBudget) error {
	ids := make(map[session.RunID]bool)
	for _, m := range out.Messages {
		ids[m.RunID] = true
	}
	var active sql.NullString
	if err := s.queryRow(ctx, "SELECT "+s.boundedColumn("id")+" FROM runs WHERE session_key = (SELECT row_key FROM sessions WHERE id = ?) AND status IN ('pending','running') LIMIT 1", b.bytes/6, []byte(out.Watermark.SessionID)).Scan(&active); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	} else if err == nil {
		if !active.Valid {
			return session.ErrObservationTooLarge
		}
		ids[session.RunID(active.String)] = true
	}
	ordered := make([]session.RunID, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, id := range ordered {
		r := session.ObservationRun{ID: id}
		var provider, model, status sql.NullString
		var lease int64
		err := s.queryRow(ctx, "SELECT "+s.boundedColumn("status")+", "+s.boundedColumn("provider_id")+", "+s.boundedColumn("model_id")+", lease_until FROM runs WHERE id = ? AND session_key = (SELECT row_key FROM sessions WHERE id = ?)", b.bytes/6, b.bytes/6, b.bytes/6, []byte(id), []byte(out.Watermark.SessionID)).Scan(&status, &provider, &model, &lease)
		if errors.Is(err, sql.ErrNoRows) {
			return session.ErrObservationInvalid
		}
		if err != nil {
			return err
		}
		if !provider.Valid || !model.Valid || !status.Valid {
			return session.ErrObservationTooLarge
		}
		r.Status = session.RunStatus(status.String)
		r.ProviderID = provider.String
		r.ModelID = model.String
		if !r.Terminal() && r.Status != session.RunPending && r.Status != session.RunRunning {
			return session.ErrObservationInvalid
		}
		if !r.Terminal() {
			r.LeaseUntil = time.UnixMicro(lease).UTC()
		}
		if id == "" || !observationStringsValid(string(id), r.ProviderID, r.ModelID) {
			return session.ErrObservationInvalid
		}
		if err = b.record(len(id) + len(r.ProviderID) + len(r.ModelID) + len(r.Status)); err != nil {
			return err
		}
		out.Runs = append(out.Runs, r)
		if err = s.observationTools(ctx, out, id, l, b); err != nil {
			return err
		}
	}
	sort.Slice(out.Tools, func(i, j int) bool { return out.Tools[i].ID < out.Tools[j].ID })
	return nil
}
func (s *Store) observationTools(ctx context.Context, out *session.ObservationSnapshot, id session.RunID, l session.ObservationLimits, b *observationBudget) error {
	rows, err := s.query(ctx, "SELECT "+s.boundedColumn("t.id")+", "+s.boundedColumn("m.id")+", "+s.boundedColumn("t.name")+", "+s.boundedColumn("t.status")+", COALESCE(m.session_key = t.session_key AND m.run_key = t.run_key, FALSE) FROM tool_calls AS t"+s.dialect.IndexHint("tools_observation_idx")+" LEFT JOIN messages AS m ON m.row_key = t.request_message_key WHERE t.session_key = (SELECT row_key FROM sessions WHERE id = ?) AND t.run_key = (SELECT row_key FROM runs WHERE id = ?) ORDER BY t.id LIMIT ?", b.bytes/6, b.bytes/6, b.bytes/6, b.bytes/6, []byte(out.Watermark.SessionID), []byte(id), l.MaxTools-len(out.Tools)+1)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if len(out.Tools) == l.MaxTools {
			return session.ErrObservationTooLarge
		}
		var tid, mid, name, status sql.NullString
		t := session.ObservationTool{RunID: id}
		var owner bool
		if err = rows.Scan(&tid, &mid, &name, &status, &owner); err != nil {
			return err
		}
		if !owner {
			return session.ErrObservationInvalid
		}
		if !tid.Valid || !mid.Valid || !name.Valid || !status.Valid {
			return session.ErrObservationTooLarge
		}
		t.Status = session.ToolCallStatus(status.String)
		t.ID = session.ToolCallID(tid.String)
		t.MessageID = session.MessageID(mid.String)
		t.Name = name.String
		if !observationStringsValid(tid.String, mid.String, name.String) || tid.String == "" || mid.String == "" || name.String == "" {
			return session.ErrObservationInvalid
		}
		if !session.TerminalToolCall(t.Status) && t.Status != session.ToolCallPending && t.Status != session.ToolCallRunning {
			return session.ErrObservationInvalid
		}
		if err = b.record(len(tid.String) + len(mid.String) + len(name.String) + len(id) + len(t.Status)); err != nil {
			return err
		}
		out.Tools = append(out.Tools, t)
	}
	return rows.Err()
}

var _ session.ObservationReader = (*Store)(nil)

func observationStringsValid(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return false
		}
	}
	return true
}
