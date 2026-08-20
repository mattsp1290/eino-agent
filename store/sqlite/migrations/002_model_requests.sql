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

INSERT OR IGNORE INTO schema_version(version, applied_at) VALUES (2, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
