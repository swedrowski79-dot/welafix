PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS artikel (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  afs_artikel_id TEXT UNIQUE,
  artikelnummer TEXT,
  warengruppe_id TEXT,
  seo_url TEXT,
  master_modell TEXT,
  is_master INTEGER,
  is_deleted INTEGER DEFAULT 0,
  row_hash TEXT,
  changed_fields TEXT,
  last_synced_at TEXT,
  last_seen_at TEXT,
  changed INTEGER DEFAULT 0,
  change_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_artikel_changed ON artikel(changed);
CREATE INDEX IF NOT EXISTS idx_artikel_last_seen ON artikel(last_seen_at);

CREATE TABLE IF NOT EXISTS warengruppe (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  afs_wg_id INTEGER UNIQUE,
  name TEXT,
  parent_id INTEGER,
  path TEXT,
  path_ids TEXT,
  seo_url TEXT,
  changed_fields TEXT,
  last_synced_at TEXT,
  last_seen_at TEXT,
  changed INTEGER DEFAULT 0,
  change_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_warengruppe_changed ON warengruppe(changed);

CREATE TABLE IF NOT EXISTS documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  source_id TEXT NOT NULL,
  doc_type TEXT NOT NULL,
  changed INTEGER DEFAULT 0,
  UNIQUE(source, source_id)
);

CREATE TABLE IF NOT EXISTS media (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  source TEXT NULL,
  created_at TEXT NULL,
  type TEXT NULL,
  storage_path TEXT NULL,
  checksum TEXT NULL,
  changed INTEGER DEFAULT 0,
  last_checked_at TEXT NULL,
  is_deleted INTEGER DEFAULT 0
);

DROP INDEX IF EXISTS idx_media_filename_nocase;
DROP INDEX IF EXISTS idx_media_filename_source_nocase;
CREATE UNIQUE INDEX IF NOT EXISTS idx_media_filename_nocase ON media(lower(filename));

CREATE TABLE IF NOT EXISTS attributes (
  attributes_id INTEGER PRIMARY KEY AUTOINCREMENT,
  attributes_parent INTEGER NOT NULL DEFAULT 0,
  attributes_model TEXT NOT NULL,
  attributes_image TEXT NULL,
  attributes_color INTEGER NOT NULL DEFAULT 0,
  sort_order INTEGER NOT NULL DEFAULT 1,
  status INTEGER NOT NULL DEFAULT 1,
  attributes_templates_id INTEGER NOT NULL DEFAULT 1,
  bw_id INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_attributes_parent_model_nocase
ON attributes(attributes_parent, lower(attributes_model));
