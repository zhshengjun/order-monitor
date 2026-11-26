PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS settings (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS targets (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
  initialized INTEGER NOT NULL DEFAULT 0, detail TEXT NOT NULL DEFAULT '{}',
  balance TEXT, last_check REAL, next_check REAL NOT NULL DEFAULT 0,
  failures INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS records (
  target TEXT NOT NULL, kind TEXT NOT NULL, key TEXT NOT NULL, body TEXT NOT NULL,
  stamp INTEGER NOT NULL, PRIMARY KEY(target,kind,key));
CREATE INDEX IF NOT EXISTS records_target_time ON records(target,kind,stamp DESC);
CREATE TABLE IF NOT EXISTS order_discovery (
  target TEXT NOT NULL, key TEXT NOT NULL, discovered_at_ms INTEGER NOT NULL,
  discovery_delay_ms INTEGER NOT NULL, PRIMARY KEY(target,key));
CREATE TABLE IF NOT EXISTS leader_history (
  target TEXT NOT NULL, key TEXT NOT NULL, stamp INTEGER NOT NULL, body TEXT NOT NULL,
  PRIMARY KEY(target,key));
CREATE TABLE IF NOT EXISTS leader_history_stage (
  target TEXT NOT NULL, key TEXT NOT NULL, stamp INTEGER NOT NULL, body TEXT NOT NULL,
  PRIMARY KEY(target,key));
CREATE INDEX IF NOT EXISTS leader_history_time ON leader_history(target,stamp);
CREATE TABLE IF NOT EXISTS leader_history_sync (
  target TEXT PRIMARY KEY, page INTEGER NOT NULL DEFAULT 1, complete INTEGER NOT NULL DEFAULT 0,
  total INTEGER NOT NULL DEFAULT 0, next_check REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', updated REAL);
CREATE TABLE IF NOT EXISTS app_migrations (name TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS baseline_watermarks (
  target TEXT NOT NULL, kind TEXT NOT NULL, stamp INTEGER NOT NULL,
  PRIMARY KEY(target,kind));
CREATE TABLE IF NOT EXISTS monitor_start (target TEXT PRIMARY KEY, started_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, target TEXT NOT NULL, name TEXT NOT NULL,
  kind TEXT NOT NULL, body TEXT NOT NULL, stamp INTEGER NOT NULL, historical INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS event_time ON events(stamp DESC,id DESC);
CREATE TABLE IF NOT EXISTS futures_accounts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
  api_key TEXT NOT NULL, api_secret TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS display_order (
  kind TEXT NOT NULL, item_id TEXT NOT NULL, position INTEGER NOT NULL,
  PRIMARY KEY(kind, item_id));
CREATE TABLE IF NOT EXISTS copy_bindings (
  account_id INTEGER NOT NULL, target_id TEXT NOT NULL,
  PRIMARY KEY(account_id, target_id));
CREATE TABLE IF NOT EXISTS copy_multipliers (
  account_id INTEGER NOT NULL, target_id TEXT NOT NULL, multiplier TEXT NOT NULL,
  PRIMARY KEY(account_id,target_id));
CREATE TABLE IF NOT EXISTS copy_task_multipliers (
  task_id INTEGER PRIMARY KEY, multiplier TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS copy_multiplier_settings_v2 (
  account_id INTEGER NOT NULL, target_id TEXT NOT NULL, multiplier TEXT NOT NULL,
  PRIMARY KEY(account_id,target_id));
CREATE TABLE IF NOT EXISTS copy_task_multiplier_v2 (
  task_id INTEGER PRIMARY KEY, multiplier TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS virtual_sessions (
  account_id INTEGER NOT NULL, target_id TEXT NOT NULL, started_at INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(account_id,target_id));
CREATE TABLE IF NOT EXISTS virtual_positions (
  account_id INTEGER NOT NULL, target_id TEXT NOT NULL, symbol TEXT NOT NULL,
  position_side TEXT NOT NULL, quantity TEXT NOT NULL, PRIMARY KEY(account_id,target_id,symbol,position_side));
CREATE TABLE IF NOT EXISTS virtual_events (
  account_id INTEGER NOT NULL, target_id TEXT NOT NULL, order_key TEXT NOT NULL,
  symbol TEXT NOT NULL, position_side TEXT NOT NULL, stamp INTEGER NOT NULL,
  action TEXT NOT NULL, quantity TEXT NOT NULL, before_qty TEXT NOT NULL, after_qty TEXT NOT NULL,
  PRIMARY KEY(account_id,target_id,order_key));
CREATE TABLE IF NOT EXISTS copy_live (
  account_id INTEGER NOT NULL, target_id TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 0,
  started_at INTEGER NOT NULL, PRIMARY KEY(account_id,target_id));
CREATE TABLE IF NOT EXISTS copy_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL, target_id TEXT NOT NULL,
  order_key TEXT NOT NULL, body TEXT NOT NULL, action TEXT NOT NULL, before_qty TEXT NOT NULL,
  lead_balance TEXT NOT NULL, client_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL DEFAULT 'pending',
  quantity TEXT, result TEXT, error TEXT NOT NULL DEFAULT '', created INTEGER NOT NULL, next_check REAL NOT NULL DEFAULT 0,
  UNIQUE(account_id,target_id,order_key));
CREATE TABLE IF NOT EXISTS copy_task_audit (
  task_id INTEGER PRIMARY KEY, balance_time INTEGER NOT NULL, asset TEXT NOT NULL,
  wallet_balance TEXT, available_balance TEXT, lead_balance TEXT NOT NULL,
  reference_price TEXT NOT NULL, planned_quantity TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS copied_positions (
  account_id INTEGER NOT NULL, target_id TEXT NOT NULL, symbol TEXT NOT NULL,
  position_side TEXT NOT NULL, quantity TEXT NOT NULL,
  PRIMARY KEY(account_id,target_id,symbol,position_side));
CREATE TABLE IF NOT EXISTS copy_close_snapshots (
  task_id INTEGER PRIMARY KEY, tracked_quantity TEXT NOT NULL, actual_quantity TEXT NOT NULL,
  leader_full_close INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS manual_closes (
  id TEXT PRIMARY KEY, account_id INTEGER NOT NULL, target_id TEXT NOT NULL,
  symbol TEXT NOT NULL, side TEXT NOT NULL, percent INTEGER NOT NULL,
  preview TEXT NOT NULL, client_id TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL, result TEXT, error TEXT NOT NULL DEFAULT '',
  created REAL NOT NULL, updated REAL NOT NULL, next_check REAL NOT NULL DEFAULT 0,
  unknown_notified INTEGER NOT NULL DEFAULT 0);
CREATE UNIQUE INDEX IF NOT EXISTS manual_close_one_active_account
  ON manual_closes(account_id) WHERE state IN ('submitted','needs_review');
CREATE TABLE IF NOT EXISTS close_watch (
  id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL, target_id TEXT NOT NULL,
  symbol TEXT NOT NULL, position_side TEXT NOT NULL, order_key TEXT NOT NULL,
  task_id INTEGER, state TEXT NOT NULL DEFAULT 'pending', next_check REAL NOT NULL DEFAULT 0,
  error_notified INTEGER NOT NULL DEFAULT 0,
  UNIQUE(account_id,target_id,order_key));
CREATE TABLE IF NOT EXISTS margin_alert_state (
  account_id INTEGER NOT NULL, risk_key TEXT NOT NULL, active INTEGER NOT NULL,
  PRIMARY KEY(account_id, risk_key));
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT, event INTEGER NOT NULL, channel TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
  next_try REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', UNIQUE(event,channel));
