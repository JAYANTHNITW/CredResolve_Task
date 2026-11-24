CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL,
  state TEXT NOT NULL, pending_state TEXT,
  version INTEGER NOT NULL DEFAULT 0,
  current_call_id TEXT, available_since INTEGER,
  wrap_up_until INTEGER, last_heartbeat_at INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS borrowers (
  id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, phone TEXT NOT NULL,
  status TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3,
  next_attempt_at INTEGER NOT NULL DEFAULT 0, active_call_id TEXT);

CREATE TABLE IF NOT EXISTS calls (
  id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL,
  borrower_id TEXT NOT NULL, agent_id TEXT NOT NULL,
  provider TEXT, state TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT NOT NULL UNIQUE, fail_reason TEXT,
  lease_expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL, initiated_at INTEGER, ringing_at INTEGER,
  answered_at INTEGER, ended_at INTEGER);

-- INVARIANTS enforced by the database, not just by code:
CREATE UNIQUE INDEX IF NOT EXISTS one_active_call_per_agent ON calls(agent_id)
  WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED');
CREATE UNIQUE INDEX IF NOT EXISTS one_active_call_per_borrower ON calls(borrower_id)
  WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED');

CREATE TABLE IF NOT EXISTS call_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, call_id TEXT NOT NULL,
  provider TEXT NOT NULL, provider_event_id TEXT, type TEXT NOT NULL,
  occurred_at INTEGER, received_at INTEGER NOT NULL, outcome TEXT NOT NULL,
  UNIQUE(provider, provider_event_id));   -- NULL ids are allowed; forward-only rule covers them

CREATE TABLE IF NOT EXISTS safety_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, campaign_id TEXT,
  requested INTEGER, approved INTEGER, action TEXT, reason_codes TEXT, snapshot TEXT);

CREATE TABLE IF NOT EXISTS incidents (            -- compliance incidents, e.g. ABANDONED_AGENT_LOST
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, call_id TEXT, kind TEXT, detail TEXT);

CREATE INDEX IF NOT EXISTS idx_agents_avail ON agents(campaign_id, state, available_since);
CREATE INDEX IF NOT EXISTS idx_borrowers_next ON borrowers(campaign_id, status, next_attempt_at, priority);
CREATE INDEX IF NOT EXISTS idx_calls_state_lease ON calls(state, lease_expires_at);

CREATE TABLE IF NOT EXISTS safety_permits (
  token TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, approved INTEGER NOT NULL,
  expires_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_permit_expiry ON safety_permits(expires_at);

CREATE TABLE IF NOT EXISTS placement_attempts (
  provider TEXT NOT NULL, ts INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_placement_rate ON placement_attempts(provider, ts);

CREATE TABLE IF NOT EXISTS dialing_switches (
  scope TEXT PRIMARY KEY, stopped INTEGER NOT NULL CHECK(stopped IN (0,1)));

CREATE INDEX IF NOT EXISTS idx_active_calls_provider ON calls(provider)
  WHERE state NOT IN ('COMPLETED','CANCELLED','FAILED');

CREATE TABLE IF NOT EXISTS pending_hangups (
  call_id TEXT PRIMARY KEY, provider TEXT NOT NULL, campaign_id TEXT NOT NULL);
