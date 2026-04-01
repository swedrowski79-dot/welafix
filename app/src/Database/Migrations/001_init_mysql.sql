CREATE TABLE IF NOT EXISTS artikel (
  id INT AUTO_INCREMENT PRIMARY KEY,
  afs_artikel_id VARCHAR(255) UNIQUE,
  artikelnummer VARCHAR(255),
  warengruppe_id VARCHAR(255),
  seo_url TEXT,
  master_modell VARCHAR(255),
  is_master TINYINT DEFAULT NULL,
  is_deleted TINYINT NOT NULL DEFAULT 0,
  row_hash TEXT,
  changed_fields LONGTEXT,
  last_synced_at VARCHAR(64),
  last_seen_at VARCHAR(64),
  changed TINYINT NOT NULL DEFAULT 0,
  change_reason TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_artikel_changed ON artikel(changed);
CREATE INDEX idx_artikel_last_seen ON artikel(last_seen_at(32));
CREATE INDEX idx_artikel_changed_afs_artikel_id ON artikel(changed, afs_artikel_id);
CREATE INDEX idx_artikel_changed_warengruppe_id ON artikel(changed, warengruppe_id);

CREATE TABLE IF NOT EXISTS warengruppe (
  id INT AUTO_INCREMENT PRIMARY KEY,
  afs_wg_id INT UNIQUE,
  name VARCHAR(255),
  parent_id INT NULL,
  path TEXT,
  path_ids TEXT,
  seo_url TEXT,
  is_deleted TINYINT NOT NULL DEFAULT 0,
  changed_fields LONGTEXT,
  last_synced_at VARCHAR(64),
  last_seen_at VARCHAR(64),
  changed TINYINT NOT NULL DEFAULT 0,
  change_reason TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_warengruppe_changed ON warengruppe(changed);
CREATE INDEX idx_warengruppe_changed_afs_wg_id ON warengruppe(changed, afs_wg_id);

CREATE TABLE IF NOT EXISTS documents (
  id INT AUTO_INCREMENT PRIMARY KEY,
  source VARCHAR(64) NOT NULL,
  source_id VARCHAR(255) NOT NULL,
  doc_type VARCHAR(255) NOT NULL,
  last_seen_at VARCHAR(64) NULL,
  is_deleted TINYINT NOT NULL DEFAULT 0,
  changed TINYINT NOT NULL DEFAULT 0,
  UNIQUE KEY uniq_documents_source_source_id (source, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_documents_changed_source_id ON documents(changed, source_id);

CREATE TABLE IF NOT EXISTS media (
  id INT AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(512) NOT NULL,
  source VARCHAR(64) NULL,
  created_at VARCHAR(64) NULL,
  type VARCHAR(64) NULL,
  storage_path TEXT NULL,
  checksum VARCHAR(255) NULL,
  changed TINYINT NOT NULL DEFAULT 0,
  last_checked_at VARCHAR(64) NULL,
  is_deleted TINYINT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE UNIQUE INDEX idx_media_filename_nocase ON media(filename);
CREATE INDEX idx_media_changed_id ON media(changed, id);

CREATE TABLE IF NOT EXISTS settings (
  `key` VARCHAR(190) PRIMARY KEY,
  `value` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS artikel_attribute_map (
  id INT AUTO_INCREMENT PRIMARY KEY,
  afs_artikel_id VARCHAR(255) NOT NULL,
  attributes_parent_id INT NOT NULL,
  attributes_id INT NOT NULL,
  position INT NOT NULL DEFAULT 0,
  attribute_name VARCHAR(255) NULL,
  attribute_value VARCHAR(255) NULL,
  changed TINYINT NOT NULL DEFAULT 0,
  UNIQUE KEY uniq_artikel_attribute_map (afs_artikel_id, attributes_parent_id, attributes_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_artikel_attribute_map_artikel ON artikel_attribute_map(afs_artikel_id);
CREATE INDEX idx_artikel_attribute_map_changed ON artikel_attribute_map(changed);
CREATE INDEX idx_artikel_attribute_map_changed_artikel ON artikel_attribute_map(changed, afs_artikel_id);
CREATE INDEX idx_artikel_attribute_map_artikel_parent_child ON artikel_attribute_map(afs_artikel_id, attributes_parent_id, attributes_id);

CREATE TABLE IF NOT EXISTS artikel_media_map (
  id INT AUTO_INCREMENT PRIMARY KEY,
  afs_artikel_id VARCHAR(255) NOT NULL,
  media_id INT NULL,
  filename VARCHAR(512) NOT NULL,
  position INT NOT NULL DEFAULT 0,
  is_main TINYINT NOT NULL DEFAULT 0,
  source_field VARCHAR(255) NULL,
  changed TINYINT NOT NULL DEFAULT 0,
  UNIQUE KEY uniq_artikel_media_map (afs_artikel_id, position, filename(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_artikel_media_map_artikel ON artikel_media_map(afs_artikel_id);
CREATE INDEX idx_artikel_media_map_changed ON artikel_media_map(changed);
CREATE INDEX idx_artikel_media_map_changed_artikel ON artikel_media_map(changed, afs_artikel_id);
CREATE INDEX idx_artikel_media_map_artikel_position ON artikel_media_map(afs_artikel_id, position);

CREATE TABLE IF NOT EXISTS artikel_warengruppe (
  id INT AUTO_INCREMENT PRIMARY KEY,
  afs_artikel_id VARCHAR(255) NOT NULL,
  afs_wg_id INT NOT NULL,
  position INT NOT NULL DEFAULT 0,
  source_field VARCHAR(255) NULL,
  changed TINYINT NOT NULL DEFAULT 0,
  UNIQUE KEY uniq_artikel_warengruppe (afs_artikel_id, afs_wg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_artikel_warengruppe_artikel ON artikel_warengruppe(afs_artikel_id);
CREATE INDEX idx_artikel_warengruppe_changed ON artikel_warengruppe(changed);
CREATE INDEX idx_artikel_warengruppe_changed_artikel ON artikel_warengruppe(changed, afs_artikel_id);
CREATE INDEX idx_artikel_warengruppe_artikel_wg ON artikel_warengruppe(afs_artikel_id, afs_wg_id);

CREATE TABLE IF NOT EXISTS artikel_extra_data (
  id INT AUTO_INCREMENT PRIMARY KEY,
  Artikelnummer VARCHAR(255) NOT NULL UNIQUE,
  updated_at VARCHAR(64) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS warengruppe_extra_data (
  id INT AUTO_INCREMENT PRIMARY KEY,
  warengruppenname VARCHAR(255) NOT NULL UNIQUE,
  updated_at VARCHAR(64) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Meta_Data_Artikel (
  id INT AUTO_INCREMENT PRIMARY KEY,
  afs_artikel_id VARCHAR(255) NOT NULL UNIQUE,
  artikelnummer VARCHAR(255) NOT NULL,
  meta_title TEXT NULL,
  meta_description LONGTEXT NULL,
  updated TINYINT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Meta_Data_Waregruppen (
  id INT AUTO_INCREMENT PRIMARY KEY,
  afs_wg_id INT NOT NULL UNIQUE,
  warengruppenname VARCHAR(255) NOT NULL,
  meta_title TEXT NULL,
  meta_description LONGTEXT NULL,
  updated TINYINT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS attributes (
  attributes_id INT AUTO_INCREMENT PRIMARY KEY,
  attributes_parent INT NOT NULL DEFAULT 0,
  attributes_model VARCHAR(255) NOT NULL,
  attributes_image VARCHAR(255) NULL,
  attributes_color INT NOT NULL DEFAULT 0,
  sort_order INT NOT NULL DEFAULT 1,
  status INT NOT NULL DEFAULT 1,
  attributes_templates_id INT NOT NULL DEFAULT 1,
  bw_id INT NOT NULL DEFAULT 0,
  changed TINYINT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE UNIQUE INDEX idx_attributes_parent_model_nocase ON attributes(attributes_parent, attributes_model);
CREATE INDEX idx_attributes_changed ON attributes(changed);

CREATE TABLE IF NOT EXISTS afs_update_pending (
  entity VARCHAR(64) NOT NULL,
  source_id VARCHAR(255) NOT NULL,
  PRIMARY KEY(entity, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS xt_products_to_categories (
  products_id VARCHAR(255) NOT NULL,
  categories_id VARCHAR(255) NOT NULL,
  master_link VARCHAR(64) NULL,
  store_id VARCHAR(64) NULL,
  changed TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY(products_id, categories_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_xt_products_to_categories_changed ON xt_products_to_categories(changed);
CREATE INDEX idx_xt_products_to_categories_changed_product ON xt_products_to_categories(changed, products_id);
CREATE INDEX idx_xt_products_to_categories_category ON xt_products_to_categories(categories_id);

CREATE TABLE IF NOT EXISTS app_schema_changes (
  id INT AUTO_INCREMENT PRIMARY KEY,
  table_name VARCHAR(255) NOT NULL,
  column_name VARCHAR(255) NOT NULL,
  added_at VARCHAR(64) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
