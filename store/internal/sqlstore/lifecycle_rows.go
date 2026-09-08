package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// These rows deliberately contain only indexed/relational columns and the
// opaque record. Public session values must be reconstructed from record after
// validating the indexed columns; callers cannot select an authoritative owner
// from JSON.
type runRow struct {
	RowKey     int64
	ID         []byte
	SessionKey int64
	SessionID  []byte
	Status     string
	ProviderID []byte
	ModelID    []byte
	OwnerID    []byte
	ClaimToken []byte
	LeaseUntil int64
	Record     []byte
	CreatedAt  string
}

type toolCallRow struct {
	RowKey                   int64
	ID                       []byte
	SessionKey               int64
	RunKey                   int64
	SessionID                []byte
	RunID                    []byte
	RequestMessageKey        int64
	RequestPartKey           int64
	RequestMessageID         []byte
	RequestPartID            []byte
	RequestMessageSessionKey int64
	RequestMessageRunKey     int64
	RequestPartMessageKey    int64
	RequestPartSessionKey    int64
	RequestPartRunKey        int64
	ResultMessageID          []byte
	ResultPartID             []byte
	Status                   string
	Name                     []byte
	ClaimedBy                []byte
	ClaimToken               []byte
	LeaseUntil               int64
	Record                   []byte
}

type eventRow struct {
	RowKey         int64
	ID             []byte
	SessionKey     int64
	RunKey         int64
	SessionID      []byte
	RunID          []byte
	ToolKey        *int64
	ToolCallID     []byte
	ToolSessionKey int64
	ToolRunKey     int64
	Kind           []byte
	ToolTransition *string
	Record         []byte
	CreatedAt      string
}

func decodeRunRow(row runRow) (session.Run, error) {
	var value session.Run
	if err := json.Unmarshal(row.Record, &value); err != nil {
		return session.Run{}, fmt.Errorf("%w: malformed run record", session.ErrConflict)
	}
	if string(row.ID) != string(value.ID) || string(row.SessionID) != string(value.SessionID) ||
		string(row.Status) != string(value.Status) || string(row.ProviderID) != value.ProviderID ||
		string(row.ModelID) != value.ModelID || string(row.OwnerID) != value.OwnerID ||
		string(row.ClaimToken) != value.ClaimToken || TimeText(value.CreatedAt) != row.CreatedAt {
		return session.Run{}, session.ErrConflict
	}
	value.LeaseUntil = time.UnixMicro(row.LeaseUntil).UTC()
	return value, nil
}

func decodeToolCallRow(row toolCallRow) (session.ToolCall, error) {
	var value session.ToolCall
	if err := json.Unmarshal(row.Record, &value); err != nil {
		return session.ToolCall{}, fmt.Errorf("%w: malformed tool call record", session.ErrConflict)
	}
	if string(row.ID) != string(value.ID) || (len(row.SessionID) != 0 && string(row.SessionID) != string(value.SessionID)) || (len(row.RunID) != 0 && string(row.RunID) != string(value.RunID)) || (len(row.RequestMessageID) != 0 && string(row.RequestMessageID) != string(value.MessageID)) || (len(row.RequestPartID) != 0 && string(row.RequestPartID) != string(value.RequestPartID)) || (row.RequestMessageSessionKey != 0 && (row.RequestMessageSessionKey != row.SessionKey || row.RequestMessageRunKey != row.RunKey)) || (row.RequestPartMessageKey != 0 && (row.RequestPartMessageKey != row.RequestMessageKey || row.RequestPartSessionKey != row.SessionKey || row.RequestPartRunKey != row.RunKey)) || string(row.ResultMessageID) != string(value.ResultMessageID) ||
		string(row.ResultPartID) != string(value.ResultPartID) || string(row.Status) != string(value.Status) ||
		string(row.Name) != value.Name || string(row.ClaimedBy) != value.ClaimedBy || string(row.ClaimToken) != value.ClaimToken {
		return session.ToolCall{}, session.ErrConflict
	}
	return value, nil
}

func decodeEventRow(row eventRow) (session.EventRecord, error) {
	var value session.EventRecord
	if err := json.Unmarshal(row.Record, &value); err != nil {
		return session.EventRecord{}, fmt.Errorf("%w: malformed event record", session.ErrConflict)
	}
	if string(row.ID) != string(value.ID) || string(row.SessionID) != string(value.SessionID) || string(row.RunID) != string(value.RunID) || string(row.Kind) != value.Kind || TimeText(value.CreatedAt) != row.CreatedAt {
		return session.EventRecord{}, session.ErrConflict
	}
	if row.ToolTransition == nil {
		if value.ToolTransition != "" || len(row.ToolCallID) != 0 {
			return session.EventRecord{}, session.ErrConflict
		}
	} else if value.ToolTransition != session.ToolTransitionPhase(*row.ToolTransition) || string(row.ToolCallID) != string(value.ToolCallID) || row.ToolSessionKey != row.SessionKey || row.ToolRunKey != row.RunKey {
		return session.EventRecord{}, session.ErrConflict
	}
	return value, nil
}

func sameRun(left, right session.Run) bool { return SameRecord(left, right) }

func relationError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, session.ErrNotFound) {
		return session.ErrConflict
	}
	return err
}
