PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS artikel (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  afs_artikel_id TEXT UNIQUE,
  afs_key TEXT UNIQUE,
  artikelnummer TEXT,
  name TEXT,
  warengruppe_id TEXT,
  price REAL,
  stock INTEGER,
  online INTEGER,
  last_seen_at TEXT,
  changed INTEGER DEFAULT 0,
  change_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_artikel_changed ON artikel(changed);
CREATE INDEX IF NOT EXISTS idx_artikel_last_seen ON artikel(last_seen_at);

CREATE TABLE IF NOT EXISTS warengruppe (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  afs_wg_id TEXT UNIQUE,
  name TEXT,
  parent_id TEXT,
  path TEXT,
  path_ids TEXT,
  last_seen_at TEXT,
  changed INTEGER DEFAULT 0,
  change_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_warengruppe_changed ON warengruppe(changed);

CREATE TABLE IF NOT EXISTS media_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT,
  rel_path TEXT UNIQUE,
  abs_path TEXT,
  size INTEGER,
  mtime INTEGER,
  checksum TEXT,
  changed INTEGER DEFAULT 0,
  last_seen_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_media_changed ON media_files(changed);

CREATE TABLE IF NOT EXISTS sync_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT,
  finished_at TEXT,
  status TEXT,
  message TEXT
);
