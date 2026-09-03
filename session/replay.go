package session

// ResolveReplayPartOwners returns one owner for every replay part. Stores that
// cannot expose an independent owner column may omit owners and fall back to
// each part's embedded MessageID; every non-empty owner vector must be exact.
func ResolveReplayPartOwners(parts []Part, owners []MessageID) ([]MessageID, error) {
	if len(owners) != 0 && len(owners) != len(parts) {
		return nil, ErrConflict
	}
	resolved := make([]MessageID, len(parts))
	if len(owners) != 0 {
		copy(resolved, owners)
		return resolved, nil
	}
	for index, part := range parts {
		resolved[index] = part.MessageID
	}
	return resolved, nil
}
