package sqlite

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
	if s == nil || s.tx != nil || s.db == nil || id == "" {
		return session.ErrObservationReader
	}
	return observationError(s.WithinTx(ctx, func(ctx context.Context, st session.Store) error {
		return fn(st.(*Store))
	}))
}

func (s *Store) revision(ctx context.Context, id session.ID) (session.ObservationWatermark, bool, error) {
	w := session.ObservationWatermark{SessionID: id}
	var exists bool
	err := s.queryRow(ctx, `SELECT incarnation, COALESCE((SELECT revision FROM observation_revisions WHERE session_id = ?), 0), EXISTS(SELECT 1 FROM sessions WHERE id = ?) FROM observation_store WHERE singleton = 1`, id, id).Scan(&w.StoreID, &w.Revision, &exists)
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
func boundedColumn(column string) string {
	return "CASE WHEN length(CAST(" + column + " AS BLOB)) <= ? THEN " + column + " ELSE NULL END"
}
func (s *Store) observationMessages(ctx context.Context, out *session.ObservationSnapshot, l session.ObservationLimits, b *observationBudget) error {
	rows, err := s.query(ctx, "SELECT "+boundedColumn("id")+", "+boundedColumn("run_id")+", role, finalized FROM messages INDEXED BY messages_observation_idx WHERE session_id = ? AND role IN ('user', 'assistant') ORDER BY created_at DESC, id DESC LIMIT ?", b.bytes/6, b.bytes/6, out.Watermark.SessionID, l.MaxMessages+1)
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
		if err = rows.Scan(&id, &run, &m.Role, &m.Finalized); err != nil {
			break
		}
		if !id.Valid || !run.Valid {
			err = session.ErrObservationTooLarge
			break
		}
		m.ID = session.MessageID(id.String)
		m.RunID = session.RunID(run.String)
		if id.String == "" || run.String == "" || !utf8.ValidString(id.String+run.String) {
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
	rows, err := s.query(ctx, "SELECT "+boundedColumn("display_text")+", text_valid, run_id = ? FROM parts INDEXED BY parts_observation_idx WHERE session_id = ? AND message_id = ? AND kind = 'text' ORDER BY ordinal,id LIMIT ?", min(b.text, b.bytes/6), m.RunID, id, m.ID, b.parts+1)
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
	if err := s.queryRow(ctx, "SELECT "+boundedColumn("id")+" FROM runs WHERE session_id = ? AND status IN ('pending','running') LIMIT 1", b.bytes/6, out.Watermark.SessionID).Scan(&active); err != nil && !errors.Is(err, sql.ErrNoRows) {
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
		err := s.queryRow(ctx, "SELECT "+boundedColumn("status")+", "+boundedColumn("provider_id")+", "+boundedColumn("model_id")+", lease_until FROM runs WHERE id = ? AND session_id = ?", b.bytes/6, b.bytes/6, b.bytes/6, id, out.Watermark.SessionID).Scan(&status, &provider, &model, &lease)
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
		if !utf8.ValidString(string(id) + r.ProviderID + r.ModelID) {
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
	rows, err := s.query(ctx, "SELECT "+boundedColumn("id")+", "+boundedColumn("message_id")+", "+boundedColumn("name")+", "+boundedColumn("status")+" FROM tool_calls INDEXED BY tools_observation_idx WHERE session_id = ? AND run_id = ? ORDER BY id LIMIT ?", b.bytes/6, b.bytes/6, b.bytes/6, b.bytes/6, out.Watermark.SessionID, id, l.MaxTools-len(out.Tools)+1)
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
		if err = rows.Scan(&tid, &mid, &name, &status); err != nil {
			return err
		}
		if !tid.Valid || !mid.Valid || !name.Valid || !status.Valid {
			return session.ErrObservationTooLarge
		}
		t.Status = session.ToolCallStatus(status.String)
		t.ID = session.ToolCallID(tid.String)
		t.MessageID = session.MessageID(mid.String)
		t.Name = name.String
		if !utf8.ValidString(tid.String+mid.String+name.String) || tid.String == "" || mid.String == "" || name.String == "" {
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
