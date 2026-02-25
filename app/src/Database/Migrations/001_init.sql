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
  seo_url TEXT,
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

CREATE TABLE IF NOT EXISTS documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  source_id TEXT NOT NULL,
  doc_type TEXT NOT NULL,
  doc_no TEXT,
  doc_date TEXT,
  customer_no TEXT,
  total_gross REAL,
  currency TEXT,
  updated_at TEXT,
  synced_at TEXT,
  UNIQUE(source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_documents_doc_no ON documents(doc_no);
CREATE INDEX IF NOT EXISTS idx_documents_doc_date ON documents(doc_date);

CREATE TABLE IF NOT EXISTS document_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  document_id INTEGER NOT NULL,
  line_no INTEGER NOT NULL,
  article_no TEXT,
  title TEXT,
  qty REAL,
  unit_price REAL,
  total REAL,
  vat REAL,
  FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_items_document_id ON document_items(document_id);
CREATE INDEX IF NOT EXISTS idx_document_items_article_no ON document_items(article_no);

CREATE TABLE IF NOT EXISTS document_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  document_id INTEGER NOT NULL,
  file_name TEXT NOT NULL,
  mime_type TEXT,
  storage_path TEXT NOT NULL,
  checksum TEXT,
  created_at TEXT,
  FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_files_document_id ON document_files(document_id);

CREATE TABLE IF NOT EXISTS change_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type TEXT NOT NULL,
  entity_key TEXT NOT NULL,
  changed_at TEXT NOT NULL,
  diff_json TEXT NOT NULL,
  source TEXT NULL
);
