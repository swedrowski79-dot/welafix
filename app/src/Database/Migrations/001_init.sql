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

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS artikel_attribute_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  afs_artikel_id TEXT NOT NULL,
  attributes_parent_id INTEGER NOT NULL,
  attributes_id INTEGER NOT NULL,
  position INTEGER NOT NULL DEFAULT 0,
  attribute_name TEXT NULL,
  attribute_value TEXT NULL,
  UNIQUE(afs_artikel_id, attributes_parent_id, attributes_id)
);
CREATE INDEX IF NOT EXISTS idx_artikel_attribute_map_artikel
ON artikel_attribute_map(afs_artikel_id);

CREATE TABLE IF NOT EXISTS artikel_media_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  afs_artikel_id TEXT NOT NULL,
  media_id INTEGER NULL,
  filename TEXT NOT NULL,
  position INTEGER NOT NULL DEFAULT 0,
  is_main INTEGER NOT NULL DEFAULT 0,
  source_field TEXT NULL,
  UNIQUE(afs_artikel_id, position, filename)
);
CREATE INDEX IF NOT EXISTS idx_artikel_media_map_artikel
ON artikel_media_map(afs_artikel_id);

CREATE TABLE IF NOT EXISTS artikel_extra_data (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  Artikelnummer TEXT NOT NULL UNIQUE,
  updated_at TEXT NULL
);

CREATE TABLE IF NOT EXISTS warengruppe_extra_data (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  warengruppenname TEXT NOT NULL UNIQUE,
  updated_at TEXT NULL
);

CREATE TABLE IF NOT EXISTS Meta_Data_Artikel (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  afs_artikel_id TEXT NOT NULL UNIQUE,
  artikelnummer TEXT NOT NULL,
  meta_title TEXT NULL,
  meta_description TEXT NULL,
  updated INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS Meta_Data_Waregruppen (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  afs_wg_id INTEGER NOT NULL UNIQUE,
  warengruppenname TEXT NOT NULL,
  meta_title TEXT NULL,
  meta_description TEXT NULL,
  updated INTEGER NOT NULL DEFAULT 0
);

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
