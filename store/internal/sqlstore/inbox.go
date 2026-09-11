package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

type inboxRow struct {
	RowKey         int64  `gorm:"column:row_key"`
	ID             []byte `gorm:"column:id"`
	SessionKey     int64  `gorm:"column:session_key"`
	SessionID      []byte `gorm:"column:session_id"`
	TurnKey        *int64 `gorm:"column:turn_key"`
	IdempotencyKey []byte `gorm:"column:idempotency_key"`
	State          string `gorm:"column:state"`
	Record         []byte `gorm:"column:record"`
	CreatedAt      string `gorm:"column:created_at"`
	UpdatedAt      string `gorm:"column:updated_at"`
}

func (s *Store) inboxQuery(ctx context.Context) *gorm.DB {
	return s.dbFor(ctx).Table(s.tableName("inbox")).
		Select("inbox.row_key, inbox.id, inbox.session_key, sessions.id AS session_id, inbox.turn_key, inbox.idempotency_key, inbox.state, inbox.record, inbox.created_at, inbox.updated_at").
		Joins("JOIN " + s.tableName("sessions") + " ON sessions.row_key = inbox.session_key")
}

func decodeInboxRow(row inboxRow) (session.InboxItem, error) {
	var value session.InboxItem
	if err := decodeStoredRecord(row.Record, &value); err != nil {
		return session.InboxItem{}, err
	}
	if string(row.ID) != string(value.ID) || string(row.SessionID) != string(value.SessionID) ||
		string(row.IdempotencyKey) != value.IdempotencyKey || row.State != string(value.State) {
		return session.InboxItem{}, session.ErrConflict
	}
	if (row.TurnKey == nil) && value.TurnID != "" {
		return session.InboxItem{}, session.ErrConflict
	}
	return value, nil
}

func (s *Store) inboxRowByID(ctx context.Context, id string) (inboxRow, error) {
	var row inboxRow
	err := s.inboxQuery(ctx).Where("inbox.id = ?", []byte(id)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return inboxRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

func (s *Store) inboxRowByIdempotencyKey(ctx context.Context, sessionKey int64, idempotencyKey string) (inboxRow, error) {
	var row inboxRow
	err := s.inboxQuery(ctx).Where("inbox.session_key = ? AND inbox.idempotency_key = ?", sessionKey, []byte(idempotencyKey)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return inboxRow{}, session.ErrNotFound
	}
	return row, s.mapErr(err)
}

// EnqueueInbox durably admits one idempotent submission for a session. A
// repeated call with the same (session, IdempotencyKey) and identical
// content blocks returns the existing row; a different payload under the
// same key reports ErrConflict.
func (s *Store) EnqueueInbox(ctx context.Context, item session.InboxItem, limits session.ContentLimits) (session.InboxItem, error) {
	if err := session.ValidateEnqueueInbox(item, limits); err != nil {
		return session.InboxItem{}, err
	}
	var result session.InboxItem
	err := s.atomic(ctx, func(st *Store) error {
		sessionKey, err := st.lockSessionKeyByID(ctx, string(item.SessionID))
		if err != nil {
			return relationError(err)
		}
		var err2 error
		result, _, err2 = st.enqueueInboxLocked(ctx, sessionKey, item)
		return err2
	})
	return result, err
}

// EnqueueInboxForRun is EnqueueInbox, additionally checked atomically against
// runID's current terminal status: lockRun locks the owning session row
// before the run row, the exact same lock SettleRun's fenced terminal
// transition takes via loadRunFence -- so this call and a concurrent
// terminal SettleRun for the same run always serialize on that lock,
// whichever commits first winning. If runID is already terminal once this
// transaction holds the lock, nothing is persisted and ErrRunClosed is
// returned; the residual case (this call's item commits first) is caught
// symmetrically by SettleRun's own queued-inbox check before it may settle a
// run RunCompleted (see execution.go's SettleRun).
func (s *Store) EnqueueInboxForRun(ctx context.Context, runID session.RunID, item session.InboxItem, limits session.ContentLimits) (session.InboxItem, bool, error) {
	if err := session.ValidateEnqueueInbox(item, limits); err != nil {
		return session.InboxItem{}, false, err
	}
	if runID == "" {
		return session.InboxItem{}, false, session.ErrConflict
	}
	var result session.InboxItem
	var created bool
	err := s.atomic(ctx, func(st *Store) error {
		row, err := st.lockRun(ctx, runID)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrRunClosed
			}
			return err
		}
		run, err := decodeRunRow(row)
		if err != nil {
			return err
		}
		if run.SessionID != item.SessionID {
			return session.ErrConflict
		}
		if run.Terminal() {
			return session.ErrRunClosed
		}
		var err2 error
		result, created, err2 = st.enqueueInboxLocked(ctx, row.SessionKey, item)
		return err2
	})
	if err != nil {
		return session.InboxItem{}, false, err
	}
	return result, created, nil
}

// enqueueInboxLocked is EnqueueInbox's core insert-or-find-existing logic,
// shared by EnqueueInbox and EnqueueInboxForRun: the caller has already
// locked sessionKey's session row (directly, or transitively via lockRun)
// before calling this. created reports whether this call durably inserted a
// new row (false for an idempotent replay of an existing item).
func (s *Store) enqueueInboxLocked(ctx context.Context, sessionKey int64, item session.InboxItem) (session.InboxItem, bool, error) {
	matchExisting := func(row inboxRow) (session.InboxItem, bool, error) {
		existing, err := decodeInboxRow(row)
		if err != nil {
			return session.InboxItem{}, false, err
		}
		// Ignore block ID: Enqueue's caller-facing entry point mints a
		// fresh ID for every block on every call (see runtime's
		// assignContentBlockIDs), including a genuine retry of the
		// identical logical request under the same IdempotencyKey --
		// comparing full equality (IDs included) would make every real
		// retry mismatch and spike a false conflict instead of
		// returning the original durably-admitted item, defeating the
		// point of an idempotency key.
		if !session.ContentBlocksEqualIgnoringID(existing.Blocks, item.Blocks) {
			return session.InboxItem{}, false, session.ErrConflict
		}
		return existing, false, nil
	}
	if row, err := s.inboxRowByIdempotencyKey(ctx, sessionKey, item.IdempotencyKey); err == nil {
		return matchExisting(row)
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.InboxItem{}, false, err
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return session.InboxItem{}, false, err
	}
	createdRow := s.dbFor(ctx).Table(s.tableName("inbox")).Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
		"id": []byte(item.ID), "session_key": sessionKey, "idempotency_key": []byte(item.IdempotencyKey),
		"state": string(item.State), "record": raw,
		"created_at": TimeText(item.CreatedAt), "updated_at": TimeText(item.UpdatedAt),
	})
	if err := s.mapErr(createdRow.Error); err != nil {
		return session.InboxItem{}, false, err
	}
	if createdRow.RowsAffected == 0 {
		row, err := s.inboxRowByIdempotencyKey(ctx, sessionKey, item.IdempotencyKey)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.InboxItem{}, false, session.ErrConflict
			}
			return session.InboxItem{}, false, err
		}
		return matchExisting(row)
	}
	return item, true, nil
}

func (s *Store) ListInbox(ctx context.Context, sessionID session.ID, states []session.InboxState) ([]session.InboxItem, error) {
	sessionKey, err := s.key(ctx, "sessions", string(sessionID))
	if err != nil {
		return nil, err
	}
	query := s.inboxQuery(ctx).Where("inbox.session_key = ?", sessionKey)
	if len(states) > 0 {
		values := make([]string, len(states))
		for i, state := range states {
			values[i] = string(state)
		}
		query = query.Where("inbox.state IN ?", values)
	}
	var rows []inboxRow
	if err := query.Order("inbox.created_at, inbox.id").Find(&rows).Error; err != nil {
		return nil, s.mapErr(err)
	}
	out := make([]session.InboxItem, 0, len(rows))
	for _, row := range rows {
		value, err := decodeInboxRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

// consumeInboxForTurn transitions queued -> consumed for the given inbox
// IDs, assigning turnID and turnKey. It is idempotent per ID (an item
// already consumed by this exact turn is left alone) but fails the whole
// call — rolling back the enclosing savepoint — if any ID does not belong to
// sessionKey or is not currently queued (or already consumed by this turn).
func (s *Store) consumeInboxForTurn(ctx context.Context, sessionKey, turnKey int64, turnID session.TurnID, ids []session.InboxID, at time.Time) error {
	for _, id := range ids {
		row, err := s.inboxRowByID(ctx, string(id))
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		if row.SessionKey != sessionKey {
			return session.ErrConflict
		}
		item, err := decodeInboxRow(row)
		if err != nil {
			return err
		}
		if item.State == session.InboxConsumed && item.TurnID == turnID {
			continue
		}
		if item.State != session.InboxQueued {
			return session.ErrConflict
		}
		item.State = session.InboxConsumed
		item.TurnID = turnID
		item.UpdatedAt = at.UTC()
		raw, err := json.Marshal(item)
		if err != nil {
			return err
		}
		db := s.dbFor(ctx).Table(s.tableName("inbox")).Where("row_key = ? AND state = ?", row.RowKey, string(session.InboxQueued)).Updates(map[string]any{
			"state": string(session.InboxConsumed), "turn_key": turnKey, "record": raw, "updated_at": TimeText(item.UpdatedAt),
		})
		if err := s.mapErr(db.Error); err != nil {
			return err
		}
		if db.RowsAffected == 0 {
			return session.ErrConflict
		}
	}
	return nil
}

// settleInboxForTurn transitions every inbox item currently in one of the
// `from` states and claimed by turnKey to the given terminal state. It is
// naturally idempotent: a replay finds no rows still in a `from` state and
// is a no-op.
func (s *Store) settleInboxForTurn(ctx context.Context, turnKey int64, from []session.InboxState, to session.InboxState, at time.Time) error {
	fromValues := make([]string, len(from))
	for i, state := range from {
		fromValues[i] = string(state)
	}
	var rows []inboxRow
	if err := s.inboxQuery(ctx).Where("inbox.turn_key = ? AND inbox.state IN ?", turnKey, fromValues).Find(&rows).Error; err != nil {
		return s.mapErr(err)
	}
	for _, row := range rows {
		item, err := decodeInboxRow(row)
		if err != nil {
			return err
		}
		fromState := item.State
		item.State = to
		item.UpdatedAt = at.UTC()
		raw, err := json.Marshal(item)
		if err != nil {
			return err
		}
		db := s.dbFor(ctx).Table(s.tableName("inbox")).Where("row_key = ? AND state = ?", row.RowKey, string(fromState)).Updates(map[string]any{
			"state": string(to), "record": raw, "updated_at": TimeText(item.UpdatedAt),
		})
		if err := s.mapErr(db.Error); err != nil {
			return err
		}
		if db.RowsAffected == 0 {
			return session.ErrConflict
		}
	}
	return nil
}

// requeueInboxForTurn transitions every InboxConsumed item claimed by
// turnKey back to InboxQueued, clearing its turn linkage (turn_key and
// TurnID) so it reads exactly like a fresh, never-claimed row -- used by
// ReconcileInterruptedTurn to requeue a crashed process's dangling turn's
// consumed items for a fresh AdmitTurn to re-consume on the next resume
// (unlike settleInboxForTurn, which only ever moves items to a TERMINAL
// state and never clears the turn link, since a terminal item's turn
// association remains a true historical fact).
func (s *Store) requeueInboxForTurn(ctx context.Context, turnKey int64, at time.Time) error {
	var rows []inboxRow
	if err := s.inboxQuery(ctx).Where("inbox.turn_key = ? AND inbox.state = ?", turnKey, string(session.InboxConsumed)).Find(&rows).Error; err != nil {
		return s.mapErr(err)
	}
	for _, row := range rows {
		item, err := decodeInboxRow(row)
		if err != nil {
			return err
		}
		item.State = session.InboxQueued
		item.TurnID = ""
		item.UpdatedAt = at.UTC()
		raw, err := json.Marshal(item)
		if err != nil {
			return err
		}
		db := s.dbFor(ctx).Table(s.tableName("inbox")).Where("row_key = ? AND state = ?", row.RowKey, string(session.InboxConsumed)).Updates(map[string]any{
			"state": string(session.InboxQueued), "turn_key": nil, "record": raw, "updated_at": TimeText(item.UpdatedAt),
		})
		if err := s.mapErr(db.Error); err != nil {
			return err
		}
		if db.RowsAffected == 0 {
			return session.ErrConflict
		}
	}
	return nil
}

// interruptInboxItems transitions the given inbox IDs (queued or consumed)
// directly to interrupted, as part of PromotePause. It is idempotent per ID.
func (s *Store) interruptInboxItems(ctx context.Context, sessionKey int64, ids []session.InboxID, at time.Time) error {
	for _, id := range ids {
		row, err := s.inboxRowByID(ctx, string(id))
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return session.ErrConflict
			}
			return err
		}
		if row.SessionKey != sessionKey {
			return session.ErrConflict
		}
		item, err := decodeInboxRow(row)
		if err != nil {
			return err
		}
		if item.State == session.InboxInterrupted {
			continue
		}
		if item.State != session.InboxQueued && item.State != session.InboxConsumed {
			return session.ErrConflict
		}
		fromState := item.State
		item.State = session.InboxInterrupted
		item.UpdatedAt = at.UTC()
		raw, err := json.Marshal(item)
		if err != nil {
			return err
		}
		db := s.dbFor(ctx).Table(s.tableName("inbox")).Where("row_key = ? AND state = ?", row.RowKey, string(fromState)).Updates(map[string]any{
			"state": string(session.InboxInterrupted), "record": raw, "updated_at": TimeText(item.UpdatedAt),
		})
		if err := s.mapErr(db.Error); err != nil {
			return err
		}
		if db.RowsAffected == 0 {
			return session.ErrConflict
		}
	}
	return nil
}
