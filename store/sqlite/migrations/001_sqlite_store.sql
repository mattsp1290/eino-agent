CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  record BLOB NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  status TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  claim_token TEXT NOT NULL,
  lease_until INTEGER NOT NULL,
  record BLOB NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id)
);

CREATE INDEX IF NOT EXISTS runs_session_active_idx ON runs(session_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS runs_session_active_unique_idx ON runs(session_id) WHERE status IN ('pending', 'running');

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  role TEXT NOT NULL,
  record BLOB NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id)
);

CREATE INDEX IF NOT EXISTS messages_replay_idx ON messages(session_id, created_at, id);

CREATE TABLE IF NOT EXISTS parts (
  id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  record BLOB NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(message_id) REFERENCES messages(id)
);

CREATE INDEX IF NOT EXISTS parts_replay_idx ON parts(session_id, message_id, ordinal, id);

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  record BLOB NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id)
);

CREATE INDEX IF NOT EXISTS events_replay_idx ON events(session_id, created_at, id);

CREATE TABLE IF NOT EXISTS tool_calls (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  status TEXT NOT NULL,
  claimed_by TEXT NOT NULL,
  claim_token TEXT NOT NULL,
  record BLOB NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS tool_calls_unfinished_idx ON tool_calls(run_id, status);

CREATE TABLE IF NOT EXISTS context_epochs (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  record BLOB NOT NULL,
  closed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_requests (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  state TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  step INTEGER NOT NULL,
  record BLOB NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS model_requests_run_attempt_step_idx ON model_requests(run_id, attempt, step);
CREATE INDEX IF NOT EXISTS model_requests_run_created_idx ON model_requests(run_id, created_at, id);

INSERT OR IGNORE INTO schema_version(version, applied_at) VALUES (1, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
