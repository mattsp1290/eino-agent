CREATE TABLE observation_store (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), incarnation TEXT NOT NULL);
INSERT INTO observation_store VALUES (1, lower(hex(randomblob(16))));
CREATE TABLE observation_revisions (session_id TEXT PRIMARY KEY, revision INTEGER NOT NULL CHECK(typeof(revision) = 'integer' AND revision >= 0));

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  record BLOB NOT NULL,
  workspace_id TEXT NOT NULL,
  title TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX sessions_workspace_created_idx ON sessions(workspace_id COLLATE BINARY, created_at COLLATE BINARY, id COLLATE BINARY);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  status TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  model_id TEXT NOT NULL,
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
  finalized INTEGER NOT NULL CHECK(finalized IN (0, 1)),
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
  kind TEXT NOT NULL,
  display_text BLOB NOT NULL,
  text_valid INTEGER NOT NULL CHECK(text_valid IN (0, 1)),
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
  tool_call_id TEXT,
  tool_transition TEXT,
  record BLOB NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id)
);

CREATE INDEX IF NOT EXISTS events_replay_idx ON events(session_id, created_at, id);
CREATE UNIQUE INDEX IF NOT EXISTS events_tool_transition_unique_idx ON events(tool_call_id, tool_transition) WHERE tool_transition IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS events_run_finished_unique_idx ON events(run_id, kind) WHERE kind = 'run_finished';

CREATE TABLE IF NOT EXISTS tool_calls (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  status TEXT NOT NULL,
  name TEXT NOT NULL,
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

CREATE INDEX messages_observation_idx ON messages(session_id, created_at, id, run_id, role, finalized) WHERE role IN ('user', 'assistant');
CREATE INDEX parts_observation_idx ON parts(session_id, message_id, kind, ordinal, id, run_id, text_valid);
CREATE INDEX tools_observation_idx ON tool_calls(session_id, run_id, id);

CREATE TRIGGER sessions_observation_insert AFTER INSERT ON sessions BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER sessions_observation_update AFTER UPDATE ON sessions BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER sessions_observation_delete AFTER DELETE ON sessions BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER runs_observation_insert AFTER INSERT ON runs BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER runs_observation_update AFTER UPDATE ON runs BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER runs_observation_delete AFTER DELETE ON runs BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER messages_observation_insert AFTER INSERT ON messages BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER messages_observation_update AFTER UPDATE ON messages BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER messages_observation_delete AFTER DELETE ON messages BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER parts_observation_insert AFTER INSERT ON parts BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER parts_observation_update AFTER UPDATE ON parts BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER parts_observation_delete AFTER DELETE ON parts BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER tool_calls_observation_insert AFTER INSERT ON tool_calls BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER tool_calls_observation_update AFTER UPDATE ON tool_calls BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
  INSERT INTO observation_revisions(session_id, revision) VALUES (NEW.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER tool_calls_observation_delete AFTER DELETE ON tool_calls BEGIN
  INSERT INTO observation_revisions(session_id, revision) VALUES (OLD.session_id, 1) ON CONFLICT(session_id) DO UPDATE SET revision = revision + 1;
END;
