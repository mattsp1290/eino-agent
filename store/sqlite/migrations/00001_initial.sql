-- +goose Up
-- Fresh baseline; production activation accompanies the shared-store cutover.

CREATE TABLE observation_store (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  incarnation TEXT NOT NULL CHECK (typeof(incarnation) = 'text' AND length(incarnation) = 32 AND incarnation NOT GLOB '*[^0-9a-f]*')
);
INSERT INTO observation_store VALUES (1, lower(hex(randomblob(16))));

CREATE TABLE observation_revisions (
  session_id BLOB PRIMARY KEY CHECK (typeof(session_id) = 'blob'),
  revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision >= 0)
);

CREATE TABLE sessions (
  row_key INTEGER PRIMARY KEY,
  id BLOB NOT NULL UNIQUE CHECK (typeof(id) = 'blob'),
  record BLOB NOT NULL CHECK (typeof(record) = 'blob'),
  workspace_id BLOB NOT NULL CHECK (typeof(workspace_id) = 'blob'),
  title BLOB NOT NULL CHECK (typeof(title) = 'blob'),
  created_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(created_at) = 'text'),
  updated_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(updated_at) = 'text')
);
CREATE INDEX sessions_workspace_created_idx ON sessions(workspace_id, created_at, id);

CREATE TABLE runs (
  row_key INTEGER PRIMARY KEY,
  id BLOB NOT NULL UNIQUE CHECK (typeof(id) = 'blob'),
  session_key INTEGER NOT NULL REFERENCES sessions(row_key),
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'interrupted', 'failed', 'completed')),
  provider_id BLOB NOT NULL CHECK (typeof(provider_id) = 'blob'),
  model_id BLOB NOT NULL CHECK (typeof(model_id) = 'blob'),
  owner_id BLOB NOT NULL CHECK (typeof(owner_id) = 'blob'),
  claim_token BLOB NOT NULL CHECK (typeof(claim_token) = 'blob'),
  lease_until INTEGER NOT NULL CHECK (typeof(lease_until) = 'integer'),
  record BLOB NOT NULL CHECK (typeof(record) = 'blob'),
  created_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(created_at) = 'text')
);
CREATE INDEX runs_session_status_idx ON runs(session_key, status);
CREATE UNIQUE INDEX runs_session_active_unique_idx ON runs(session_key) WHERE status IN ('pending', 'running');

CREATE TABLE messages (
  row_key INTEGER PRIMARY KEY,
  id BLOB NOT NULL UNIQUE CHECK (typeof(id) = 'blob'),
  session_key INTEGER NOT NULL REFERENCES sessions(row_key),
  run_key INTEGER NOT NULL REFERENCES runs(row_key),
  role TEXT NOT NULL CHECK (role IN ('system', 'user', 'assistant', 'tool')),
  finalized INTEGER NOT NULL CHECK (finalized IN (0,1)),
  record BLOB NOT NULL CHECK (typeof(record) = 'blob'),
  created_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(created_at) = 'text')
);
CREATE INDEX messages_replay_idx ON messages(session_key, created_at, id);
CREATE INDEX messages_run_key_idx ON messages(run_key);

CREATE TABLE parts (
  row_key INTEGER PRIMARY KEY,
  id BLOB NOT NULL UNIQUE CHECK (typeof(id) = 'blob'),
  message_key INTEGER NOT NULL REFERENCES messages(row_key),
  session_key INTEGER NOT NULL REFERENCES sessions(row_key),
  run_key INTEGER NOT NULL REFERENCES runs(row_key),
  ordinal INTEGER NOT NULL CHECK (typeof(ordinal) = 'integer'),
  kind TEXT NOT NULL CHECK (kind IN ('text', 'reasoning', 'tool_call', 'tool_result', 'file', 'step', 'compaction', 'state', 'provider_state')),
  display_text BLOB NOT NULL CHECK (typeof(display_text) = 'blob'),
  text_valid INTEGER NOT NULL CHECK (text_valid IN (0,1)),
  record BLOB NOT NULL CHECK (typeof(record) = 'blob'),
  created_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(created_at) = 'text')
);
CREATE INDEX parts_replay_idx ON parts(session_key, message_key, ordinal, id);
CREATE INDEX parts_message_key_idx ON parts(message_key);
CREATE INDEX parts_run_key_idx ON parts(run_key);

CREATE TABLE tool_calls (
  row_key INTEGER PRIMARY KEY,
  id BLOB NOT NULL UNIQUE CHECK (typeof(id) = 'blob'),
  session_key INTEGER NOT NULL REFERENCES sessions(row_key),
  run_key INTEGER NOT NULL REFERENCES runs(row_key),
  request_message_key INTEGER NOT NULL REFERENCES messages(row_key),
  request_part_key INTEGER NOT NULL REFERENCES parts(row_key),
  result_message_id BLOB NOT NULL CHECK (typeof(result_message_id) = 'blob'),
  result_part_id BLOB NOT NULL CHECK (typeof(result_part_id) = 'blob'),
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'interrupted')),
  name BLOB NOT NULL CHECK (typeof(name) = 'blob'),
  claimed_by BLOB NOT NULL CHECK (typeof(claimed_by) = 'blob'),
  claim_token BLOB NOT NULL CHECK (typeof(claim_token) = 'blob'),
  record BLOB NOT NULL CHECK (typeof(record) = 'blob')
);
CREATE INDEX tools_unfinished_idx ON tool_calls(run_key, status);
CREATE INDEX tool_calls_request_message_key_idx ON tool_calls(request_message_key);
CREATE INDEX tool_calls_request_part_key_idx ON tool_calls(request_part_key);

CREATE TABLE context_epochs (
  row_key INTEGER PRIMARY KEY,
  id BLOB NOT NULL UNIQUE CHECK (typeof(id) = 'blob'),
  session_key INTEGER NOT NULL REFERENCES sessions(row_key),
  record BLOB NOT NULL CHECK (typeof(record) = 'blob'),
  created_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(created_at) = 'text'),
  closed_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(closed_at) = 'text')
);
CREATE INDEX context_epochs_session_created_idx ON context_epochs(session_key, created_at, id);

CREATE TABLE model_requests (
  row_key INTEGER PRIMARY KEY,
  id BLOB NOT NULL UNIQUE CHECK (typeof(id) = 'blob'),
  session_key INTEGER NOT NULL REFERENCES sessions(row_key),
  run_key INTEGER NOT NULL REFERENCES runs(row_key),
  assistant_message_id BLOB NOT NULL CHECK (typeof(assistant_message_id) = 'blob'),
  state TEXT NOT NULL CHECK (state IN ('prepared', 'dispatch_started', 'completed', 'failed')),
  attempt INTEGER NOT NULL CHECK (typeof(attempt) = 'integer'),
  step INTEGER NOT NULL CHECK (typeof(step) = 'integer'),
  record BLOB NOT NULL CHECK (typeof(record) = 'blob'),
  created_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(created_at) = 'text')
);
CREATE UNIQUE INDEX model_requests_run_attempt_step_idx ON model_requests(run_key, attempt, step);
CREATE INDEX model_requests_run_created_idx ON model_requests(run_key, created_at, id);
CREATE INDEX model_requests_session_key_idx ON model_requests(session_key);

CREATE TABLE events (
  row_key INTEGER PRIMARY KEY,
  id BLOB NOT NULL UNIQUE CHECK (typeof(id) = 'blob'),
  session_key INTEGER NOT NULL REFERENCES sessions(row_key),
  run_key INTEGER NOT NULL REFERENCES runs(row_key),
  tool_key INTEGER REFERENCES tool_calls(row_key),
  kind BLOB NOT NULL CHECK (typeof(kind) = 'blob'),
  tool_transition TEXT CHECK (tool_transition IS NULL OR tool_transition IN ('pending', 'running', 'terminal')),
  record BLOB NOT NULL CHECK (typeof(record) = 'blob'),
  created_at TEXT NOT NULL COLLATE BINARY CHECK (typeof(created_at) = 'text'),
  CHECK ((tool_key IS NULL) = (tool_transition IS NULL))
);
CREATE INDEX events_replay_idx ON events(session_key, created_at, id);
CREATE INDEX events_run_key_idx ON events(run_key);
CREATE INDEX events_tool_key_idx ON events(tool_key);
CREATE UNIQUE INDEX events_tool_transition_unique_idx ON events(tool_key, tool_transition) WHERE tool_transition IS NOT NULL;
CREATE UNIQUE INDEX events_run_finished_unique_idx ON events(run_key, kind) WHERE kind = X'72756e5f66696e6973686564';
CREATE INDEX messages_observation_idx ON messages(session_key, created_at, id, run_key, role, finalized) WHERE role IN ('user', 'assistant');
CREATE INDEX parts_observation_idx ON parts(session_key, message_key, kind, ordinal, id, run_key, text_valid);
CREATE INDEX tools_observation_idx ON tool_calls(session_key, run_key, id);

-- +goose StatementBegin
CREATE TRIGGER sessions_observation_insert AFTER INSERT ON sessions BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.id,1)
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER sessions_observation_update AFTER UPDATE ON sessions BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.id,1)
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.id,1)
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER sessions_observation_delete AFTER DELETE ON sessions BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.id,1)
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER runs_observation_insert AFTER INSERT ON runs BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = NEW.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER runs_observation_update AFTER UPDATE ON runs BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = OLD.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = NEW.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER runs_observation_delete AFTER DELETE ON runs BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = OLD.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER messages_observation_insert AFTER INSERT ON messages BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = NEW.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER messages_observation_update AFTER UPDATE ON messages BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = OLD.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = NEW.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER messages_observation_delete AFTER DELETE ON messages BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = OLD.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER parts_observation_insert AFTER INSERT ON parts BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = NEW.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER parts_observation_update AFTER UPDATE ON parts BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = OLD.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = NEW.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER parts_observation_delete AFTER DELETE ON parts BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = OLD.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER tool_calls_observation_insert AFTER INSERT ON tool_calls BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = NEW.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER tool_calls_observation_update AFTER UPDATE ON tool_calls BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = OLD.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = NEW.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER tool_calls_observation_delete AFTER DELETE ON tool_calls BEGIN
  INSERT INTO observation_revisions(session_id, revision) SELECT id, 1 FROM sessions
  WHERE row_key = OLD.session_key
  ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
-- +goose StatementEnd
