<?php
declare(strict_types=1);

namespace Welafix\Database;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Database\SqliteGuardedPdo;

final class ConnectionFactory
{
    private ?PDO $sqlite = null;
    private ?PDO $mysql = null;
    private ?PDO $mssql = null;

    public function sqlite(): PDO
    {
        if ($this->sqlite) return $this->sqlite;

        $path = (string)env('SQLITE_PATH', '');
        if ($path === '') {
            throw new RuntimeException('SQLITE_PATH ist nicht gesetzt.');
        }
        $dir = dirname($path);
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }

        $pdo = new SqliteGuardedPdo('sqlite:' . $path);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        // reduce "database is locked" errors under concurrent access
        $pdo->exec('PRAGMA busy_timeout = 10000');
        $pdo->exec('PRAGMA journal_mode = WAL');
        $pdo->exec('PRAGMA synchronous = NORMAL');
        $this->sqlite = $pdo;
        return $pdo;
    }

    public function mysql(): PDO
    {
        if ($this->mysql) return $this->mysql;

        $host = trim((string)env('LOCAL_DB_HOST', '127.0.0.1'));
        $port = trim((string)env('LOCAL_DB_PORT', '3306'));
        $db   = trim((string)env('LOCAL_DB_NAME', 'welafix'));
        $user = trim((string)env('LOCAL_DB_USER', 'welafix'));
        $pass = trim((string)env('LOCAL_DB_PASS', 'welafix'));

        $attempts = $this->mysqlConnectionAttempts($host, $port, $db);
        $lastError = null;

        foreach ($attempts as [$tryHost, $tryPort, $tryDb]) {
            $dsn = "mysql:host={$tryHost};port={$tryPort};dbname={$tryDb};charset=utf8mb4";
            try {
                $pdo = new PDO($dsn, $user, $pass, [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                ]);
                $this->mysql = $pdo;
                return $pdo;
            } catch (\Throwable $e) {
                $lastError = $e;
            }
        }

        throw new RuntimeException(
            'Lokale MySQL/MariaDB Verbindung fehlgeschlagen: ' . ($lastError?->getMessage() ?? 'unbekannter Fehler'),
            0,
            $lastError
        );
    }

    public function localDriver(): string
    {
        $driver = strtolower(trim((string)env('LOCAL_DB_DRIVER', 'sqlite')));
        return in_array($driver, ['sqlite', 'mysql', 'mariadb'], true) ? $driver : 'sqlite';
    }

    public function localDb(): PDO
    {
        $driver = $this->localDriver();
        if ($driver === 'mysql' || $driver === 'mariadb') {
            $pdo = $this->mysql();
            $this->ensureMysqlMigrated();
            return $pdo;
        }
        $pdo = $this->sqlite();
        $this->ensureSqliteMigrated();
        return $pdo;
    }

    public function mssql(): PDO
    {
        if ($this->mssql) return $this->mssql;

        $host = trim((string)env('MSSQL_HOST', 'localhost'));
        $port = trim((string)env('MSSQL_PORT', '1433'));
        $db   = trim((string)env('MSSQL_DB', ''));
        $user = trim((string)env('MSSQL_USER', ''));
        $pass = trim((string)env('MSSQL_PASS', ''));

        $encrypt = (strtolower(trim((string)env('MSSQL_ENCRYPT', ''))) === 'true') ? 'yes' : 'no';
        $trust   = (strtolower(trim((string)env('MSSQL_TRUST_CERT', ''))) === 'true') ? 'yes' : 'no';

        $this->logMssqlConfigOnce($host, $port, $db, $user);

        $dsn = "sqlsrv:Server={$host},{$port};Database={$db};Encrypt={$encrypt};TrustServerCertificate={$trust}";

        try {
            $pdo = new PDO($dsn, $user, $pass, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            ]);
        } catch (\Throwable $e) {
            throw new RuntimeException('MSSQL Verbindung fehlgeschlagen: ' . $e->getMessage(), 0, $e);
        }

        $this->mssql = $pdo;
        return $pdo;
    }

    public function disconnectAll(): void
    {
        $this->sqlite = null;
        $this->mysql = null;
        $this->mssql = null;
    }


    public function ensureSqliteMigrated(): void
    {
        $pdo = $this->sqlite();
        // if tables exist, skip
        $exists = (int)$pdo->query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='artikel'")->fetchColumn();
        if ($exists === 0) {
            $migrationFile = __DIR__ . '/Migrations/001_init.sql';
            $sql = file_get_contents($migrationFile);
            if ($sql === false) {
                throw new RuntimeException('Migration nicht gefunden: ' . $migrationFile);
            }
            $pdo->exec($sql);
        }

        $this->ensureColumns($pdo, 'artikel', [
            'afs_artikel_id' => 'TEXT',
            'warengruppe_id' => 'TEXT',
            'seo_url' => 'TEXT',
            'master_modell' => 'TEXT',
            'is_master' => 'INTEGER',
            'is_deleted' => 'INTEGER DEFAULT 0',
            'changed_fields' => 'TEXT',
            'last_synced_at' => 'TEXT',
            'last_seen_at' => 'TEXT',
            'changed' => 'INTEGER DEFAULT 0',
            'change_reason' => 'TEXT',
            'row_hash' => 'TEXT',
        ]);
        $pdo->exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_artikel_afs_artikel_id ON artikel(afs_artikel_id)');

        $this->ensureColumns($pdo, 'warengruppe', [
            'afs_wg_id' => 'TEXT',
            'name' => 'TEXT',
            'parent_id' => 'TEXT',
            'path' => 'TEXT',
            'path_ids' => 'TEXT',
            'seo_url' => 'TEXT',
            'is_deleted' => 'INTEGER DEFAULT 0',
            'changed_fields' => 'TEXT',
            'last_synced_at' => 'TEXT',
            'last_seen_at' => 'TEXT',
            'changed' => 'INTEGER DEFAULT 0',
            'change_reason' => 'TEXT',
        ]);
        $this->dropColumnsIfExist($pdo, 'artikel', ['meta_title', 'meta_description']);
        $this->dropColumnsIfExist($pdo, 'warengruppe', ['meta_title', 'meta_description']);

        $this->ensureDocumentSchema($pdo);
        $this->ensureMediaFilenameTable($pdo);
        $this->ensureColumns($pdo, 'media', [
            'type' => 'TEXT',
            'storage_path' => 'TEXT',
            'checksum' => 'TEXT',
            'changed' => 'INTEGER DEFAULT 0',
            'last_checked_at' => 'TEXT',
            'is_deleted' => 'INTEGER DEFAULT 0',
        ]);
        $this->ensureDocumentsExtra($pdo);
        $this->ensureAttributesSchema($pdo);
        $this->ensureArtikelAttributeMapTable($pdo);
        $this->ensureArtikelMediaMapTable($pdo);
        $this->ensureArtikelWarengruppeTable($pdo);
        $this->ensureSettingsTable($pdo);
        $this->ensureArtikelExtraDataTable($pdo);
        $this->ensureWarengruppeExtraDataTable($pdo);
        $this->ensureMetaDataArtikelTable($pdo);
        $this->ensureMetaDataWarengruppenTable($pdo);
        $this->ensureAfsUpdatePendingTable($pdo);
        $this->ensureXtProductsToCategoriesTable($pdo);
        $this->ensurePerformanceIndexes($pdo);
    }

    public function ensureMysqlMigrated(): void
    {
        $pdo = $this->mysql();
        $exists = (int)$pdo->query("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'artikel'")->fetchColumn();
        if ($exists === 0) {
            $migrationFile = __DIR__ . '/Migrations/001_init_mysql.sql';
            $sql = file_get_contents($migrationFile);
            if ($sql === false) {
                throw new RuntimeException('Migration nicht gefunden: ' . $migrationFile);
            }
            foreach ($this->splitSqlStatements($sql) as $statement) {
                $pdo->exec($statement);
            }
        }

        $this->ensureColumns($pdo, 'artikel', [
            'afs_artikel_id' => 'VARCHAR(255)',
            'warengruppe_id' => 'VARCHAR(255)',
            'seo_url' => 'TEXT',
            'master_modell' => 'VARCHAR(255)',
            'is_master' => 'TINYINT',
            'is_deleted' => 'TINYINT DEFAULT 0',
            'changed_fields' => 'LONGTEXT',
            'last_synced_at' => 'VARCHAR(64)',
            'last_seen_at' => 'VARCHAR(64)',
            'changed' => 'TINYINT DEFAULT 0',
            'change_reason' => 'TEXT',
            'row_hash' => 'TEXT',
        ]);
        $this->createIndexIfMissing($pdo, 'idx_artikel_afs_artikel_id', 'artikel', ['afs_artikel_id'], true);

        $this->ensureColumns($pdo, 'warengruppe', [
            'afs_wg_id' => 'VARCHAR(255)',
            'name' => 'VARCHAR(255)',
            'parent_id' => 'VARCHAR(255)',
            'path' => 'TEXT',
            'path_ids' => 'TEXT',
            'seo_url' => 'TEXT',
            'is_deleted' => 'TINYINT DEFAULT 0',
            'changed_fields' => 'LONGTEXT',
            'last_synced_at' => 'VARCHAR(64)',
            'last_seen_at' => 'VARCHAR(64)',
            'changed' => 'TINYINT DEFAULT 0',
            'change_reason' => 'TEXT',
        ]);
        $this->dropColumnsIfExist($pdo, 'artikel', ['meta_title', 'meta_description']);
        $this->dropColumnsIfExist($pdo, 'warengruppe', ['meta_title', 'meta_description']);

        $this->ensureDocumentSchema($pdo);
        $this->ensureMediaFilenameTable($pdo);
        $this->ensureColumns($pdo, 'media', [
            'type' => 'VARCHAR(64)',
            'storage_path' => 'TEXT',
            'checksum' => 'VARCHAR(255)',
            'changed' => 'TINYINT DEFAULT 0',
            'last_checked_at' => 'VARCHAR(64)',
            'is_deleted' => 'TINYINT DEFAULT 0',
        ]);
        $this->ensureDocumentsExtra($pdo);
        $this->ensureAttributesSchema($pdo);
        $this->ensureArtikelAttributeMapTable($pdo);
        $this->ensureArtikelMediaMapTable($pdo);
        $this->ensureArtikelWarengruppeTable($pdo);
        $this->ensureSettingsTable($pdo);
        $this->ensureArtikelExtraDataTable($pdo);
        $this->ensureWarengruppeExtraDataTable($pdo);
        $this->ensureMetaDataArtikelTable($pdo);
        $this->ensureMetaDataWarengruppenTable($pdo);
        $this->ensureAfsUpdatePendingTable($pdo);
        $this->ensureXtProductsToCategoriesTable($pdo);
        $this->ensurePerformanceIndexes($pdo);
    }

    public function ensureLocalMigrated(): void
    {
        $driver = $this->localDriver();
        if ($driver === 'mysql' || $driver === 'mariadb') {
            $this->ensureMysqlMigrated();
            return;
        }
        $this->ensureSqliteMigrated();
    }

    /**
     * @param array<string, string> $columns
     */
    private function ensureColumns(PDO $pdo, string $table, array $columns): void
    {
        $stmt = $this->describeTable($pdo, $table);
        $existing = [];
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $name = (string)($row['name'] ?? $row['Field'] ?? '');
            if ($name !== '') {
                $existing[strtolower($name)] = true;
            }
        }

        foreach ($columns as $name => $type) {
            if (isset($existing[strtolower($name)])) {
                continue;
            }
            $pdo->exec("ALTER TABLE {$table} ADD COLUMN {$name} {$type}");
        }
    }

    /**
     * @param array<int, string> $columns
     */
    private function dropColumnsIfExist(PDO $pdo, string $table, array $columns): void
    {
        $stmt = $this->describeTable($pdo, $table);
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $existing = [];
        foreach ($rows as $row) {
            $name = strtolower((string)($row['name'] ?? $row['Field'] ?? ''));
            if ($name !== '') {
                $existing[$name] = true;
            }
        }

        foreach ($columns as $column) {
            $key = strtolower($column);
            if (!isset($existing[$key])) {
                continue;
            }
            $pdo->exec(
                'ALTER TABLE ' . $this->quoteIdentifier($table) .
                ' DROP COLUMN ' . $this->quoteIdentifier($column)
            );
        }
    }

    private function ensureDocumentSchema(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS documents (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    source VARCHAR(64) NOT NULL,
                    source_id VARCHAR(255) NOT NULL,
                    doc_type VARCHAR(255) NOT NULL,
                    last_seen_at VARCHAR(64) NULL,
                    is_deleted TINYINT DEFAULT 0,
                    changed TINYINT DEFAULT 0,
                    UNIQUE KEY uniq_documents_source_source_id (source, source_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
        } else {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    doc_type TEXT NOT NULL,
                    last_seen_at TEXT NULL,
                    is_deleted INTEGER DEFAULT 0,
                    changed INTEGER DEFAULT 0,
                    UNIQUE(source, source_id)
                )'
            );
        }
        $this->ensureColumns($pdo, 'documents', [
            'last_seen_at' => $this->isMysql($pdo) ? 'VARCHAR(64) NULL' : 'TEXT NULL',
            'is_deleted' => $this->isMysql($pdo) ? 'TINYINT DEFAULT 0' : 'INTEGER DEFAULT 0',
            'changed' => $this->isMysql($pdo) ? 'TINYINT DEFAULT 0' : 'INTEGER DEFAULT 0',
        ]);
    }

    private function ensureMediaFilenameTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS media (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    filename VARCHAR(512) NOT NULL,
                    source VARCHAR(64) NULL,
                    created_at VARCHAR(64) NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->createIndexIfMissing($pdo, 'idx_media_filename_nocase', 'media', ['filename'], true);
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                source TEXT NULL,
                created_at TEXT NULL
            )'
        );
        $pdo->exec('DROP INDEX IF EXISTS idx_media_filename_source_nocase');
        $pdo->exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_media_filename_nocase ON media(lower(filename))');
    }

    private function ensureDocumentsExtra(PDO $pdo): void
    {
        $this->ensureColumns($pdo, 'documents', [
            'changed' => 'INTEGER DEFAULT 0',
        ]);
    }

    private function ensureAttributesSchema(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS attributes (
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
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->createIndexIfMissing($pdo, 'idx_attributes_parent_model_nocase', 'attributes', ['attributes_parent', 'attributes_model'], true);
        } else {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS attributes (
                    attributes_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attributes_parent INTEGER NOT NULL DEFAULT 0,
                    attributes_model TEXT NOT NULL,
                    attributes_image TEXT NULL,
                    attributes_color INTEGER NOT NULL DEFAULT 0,
                    sort_order INTEGER NOT NULL DEFAULT 1,
                    status INTEGER NOT NULL DEFAULT 1,
                    attributes_templates_id INTEGER NOT NULL DEFAULT 1,
                    bw_id INTEGER NOT NULL DEFAULT 0,
                    changed INTEGER NOT NULL DEFAULT 0
                )'
            );
            $pdo->exec(
                'CREATE UNIQUE INDEX IF NOT EXISTS idx_attributes_parent_model_nocase
                 ON attributes(attributes_parent, lower(attributes_model))'
            );
        }
        $this->ensureColumns($pdo, 'attributes', [
            'changed' => $this->isMysql($pdo) ? 'TINYINT NOT NULL DEFAULT 0' : 'INTEGER NOT NULL DEFAULT 0',
        ]);
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_attributes_changed ON attributes(changed)');
    }

    private function ensureSettingsTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS settings (
                    `key` VARCHAR(190) PRIMARY KEY,
                    `value` TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )'
        );
    }

    private function ensureArtikelAttributeMapTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS artikel_attribute_map (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    afs_artikel_id VARCHAR(255) NOT NULL,
                    attributes_parent_id INT NOT NULL,
                    attributes_id INT NOT NULL,
                    position INT NOT NULL DEFAULT 0,
                    attribute_name VARCHAR(255) NULL,
                    attribute_value VARCHAR(255) NULL,
                    changed TINYINT NOT NULL DEFAULT 0,
                    UNIQUE KEY uniq_artikel_attribute_map (afs_artikel_id, attributes_parent_id, attributes_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->createIndexIfMissing($pdo, 'idx_artikel_attribute_map_artikel', 'artikel_attribute_map', ['afs_artikel_id']);
            $this->ensureColumns($pdo, 'artikel_attribute_map', [
                'changed' => 'TINYINT NOT NULL DEFAULT 0',
            ]);
            $this->createIndexIfMissing($pdo, 'idx_artikel_attribute_map_changed', 'artikel_attribute_map', ['changed']);
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS artikel_attribute_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                afs_artikel_id TEXT NOT NULL,
                attributes_parent_id INTEGER NOT NULL,
                attributes_id INTEGER NOT NULL,
                position INTEGER NOT NULL DEFAULT 0,
                attribute_name TEXT NULL,
                attribute_value TEXT NULL,
                changed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(afs_artikel_id, attributes_parent_id, attributes_id)
            )'
        );
        $pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_attribute_map_artikel
             ON artikel_attribute_map(afs_artikel_id)'
        );
        $this->ensureColumns($pdo, 'artikel_attribute_map', [
            'changed' => 'INTEGER NOT NULL DEFAULT 0',
        ]);
        $pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_attribute_map_changed
             ON artikel_attribute_map(changed)'
        );
    }

    private function ensureArtikelMediaMapTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS artikel_media_map (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    afs_artikel_id VARCHAR(255) NOT NULL,
                    media_id INT NULL,
                    filename VARCHAR(512) NOT NULL,
                    position INT NOT NULL DEFAULT 0,
                    is_main TINYINT NOT NULL DEFAULT 0,
                    source_field VARCHAR(255) NULL,
                    changed TINYINT NOT NULL DEFAULT 0,
                    UNIQUE KEY uniq_artikel_media_map (afs_artikel_id, position, filename(191))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->createIndexIfMissing($pdo, 'idx_artikel_media_map_artikel', 'artikel_media_map', ['afs_artikel_id']);
            $this->ensureColumns($pdo, 'artikel_media_map', [
                'changed' => 'TINYINT NOT NULL DEFAULT 0',
            ]);
            $this->createIndexIfMissing($pdo, 'idx_artikel_media_map_changed', 'artikel_media_map', ['changed']);
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS artikel_media_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                afs_artikel_id TEXT NOT NULL,
                media_id INTEGER NULL,
                filename TEXT NOT NULL,
                position INTEGER NOT NULL DEFAULT 0,
                is_main INTEGER NOT NULL DEFAULT 0,
                source_field TEXT NULL,
                changed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(afs_artikel_id, position, filename)
            )'
        );
        $pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_media_map_artikel
             ON artikel_media_map(afs_artikel_id)'
        );
        $this->ensureColumns($pdo, 'artikel_media_map', [
            'changed' => 'INTEGER NOT NULL DEFAULT 0',
        ]);
        $pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_media_map_changed
             ON artikel_media_map(changed)'
        );
    }

    private function ensureArtikelWarengruppeTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS artikel_warengruppe (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    afs_artikel_id VARCHAR(255) NOT NULL,
                    afs_wg_id INT NOT NULL,
                    position INT NOT NULL DEFAULT 0,
                    source_field VARCHAR(255) NULL,
                    changed TINYINT NOT NULL DEFAULT 0,
                    UNIQUE KEY uniq_artikel_warengruppe (afs_artikel_id, afs_wg_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->createIndexIfMissing($pdo, 'idx_artikel_warengruppe_artikel', 'artikel_warengruppe', ['afs_artikel_id']);
            $this->ensureColumns($pdo, 'artikel_warengruppe', [
                'position' => 'INT NOT NULL DEFAULT 0',
                'source_field' => 'VARCHAR(255) NULL',
                'changed' => 'TINYINT NOT NULL DEFAULT 0',
            ]);
            $this->createIndexIfMissing($pdo, 'idx_artikel_warengruppe_changed', 'artikel_warengruppe', ['changed']);
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS artikel_warengruppe (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                afs_artikel_id TEXT NOT NULL,
                afs_wg_id INTEGER NOT NULL,
                position INTEGER NOT NULL DEFAULT 0,
                source_field TEXT NULL,
                changed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(afs_artikel_id, afs_wg_id)
            )'
        );
        $pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_warengruppe_artikel
             ON artikel_warengruppe(afs_artikel_id)'
        );
        $this->ensureColumns($pdo, 'artikel_warengruppe', [
            'position' => 'INTEGER NOT NULL DEFAULT 0',
            'source_field' => 'TEXT NULL',
            'changed' => 'INTEGER NOT NULL DEFAULT 0',
        ]);
        $pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_warengruppe_changed
             ON artikel_warengruppe(changed)'
        );
    }

    private function ensureArtikelExtraDataTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS artikel_extra_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    Artikelnummer VARCHAR(255) NOT NULL UNIQUE,
                    updated_at VARCHAR(64) NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->renameColumnIfExists($pdo, 'artikel_extra_data', 'source_dir', 'Artikelnummer');
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS artikel_extra_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Artikelnummer TEXT NOT NULL UNIQUE,
                updated_at TEXT NULL
            )'
        );
        $this->renameColumnIfExists($pdo, 'artikel_extra_data', 'source_dir', 'Artikelnummer');
    }

    private function ensureWarengruppeExtraDataTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS warengruppe_extra_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    warengruppenname VARCHAR(255) NOT NULL UNIQUE,
                    updated_at VARCHAR(64) NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->renameColumnIfExists($pdo, 'warengruppe_extra_data', 'source_dir', 'warengruppenname');
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS warengruppe_extra_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                warengruppenname TEXT NOT NULL UNIQUE,
                updated_at TEXT NULL
            )'
        );
        $this->renameColumnIfExists($pdo, 'warengruppe_extra_data', 'source_dir', 'warengruppenname');
    }

    private function ensureMetaDataArtikelTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS Meta_Data_Artikel (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    afs_artikel_id VARCHAR(255) NOT NULL UNIQUE,
                    artikelnummer VARCHAR(255) NOT NULL,
                    meta_title TEXT NULL,
                    meta_description LONGTEXT NULL,
                    updated TINYINT NOT NULL DEFAULT 0
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->ensureColumns($pdo, 'Meta_Data_Artikel', [
                'updated' => 'TINYINT NOT NULL DEFAULT 0',
            ]);
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS Meta_Data_Artikel (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                afs_artikel_id TEXT NOT NULL UNIQUE,
                artikelnummer TEXT NOT NULL,
                meta_title TEXT NULL,
                meta_description TEXT NULL,
                updated INTEGER NOT NULL DEFAULT 0
            )'
        );
        $this->ensureColumns($pdo, 'Meta_Data_Artikel', [
            'updated' => 'INTEGER NOT NULL DEFAULT 0',
        ]);
    }

    private function ensureMetaDataWarengruppenTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS Meta_Data_Waregruppen (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    afs_wg_id INT NOT NULL UNIQUE,
                    warengruppenname VARCHAR(255) NOT NULL,
                    meta_title TEXT NULL,
                    meta_description LONGTEXT NULL,
                    updated TINYINT NOT NULL DEFAULT 0
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->ensureColumns($pdo, 'Meta_Data_Waregruppen', [
                'updated' => 'TINYINT NOT NULL DEFAULT 0',
            ]);
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS Meta_Data_Waregruppen (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                afs_wg_id INTEGER NOT NULL UNIQUE,
                warengruppenname TEXT NOT NULL,
                meta_title TEXT NULL,
                meta_description TEXT NULL,
                updated INTEGER NOT NULL DEFAULT 0
            )'
        );
        $this->ensureColumns($pdo, 'Meta_Data_Waregruppen', [
            'updated' => 'INTEGER NOT NULL DEFAULT 0',
        ]);
    }

    private function ensureAfsUpdatePendingTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS afs_update_pending (
                    entity VARCHAR(64) NOT NULL,
                    source_id VARCHAR(255) NOT NULL,
                    PRIMARY KEY(entity, source_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS afs_update_pending (
                entity TEXT NOT NULL,
                source_id TEXT NOT NULL,
                PRIMARY KEY(entity, source_id)
            )'
        );
    }

    private function ensureXtProductsToCategoriesTable(PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS xt_products_to_categories (
                    products_id VARCHAR(255) NOT NULL,
                    categories_id VARCHAR(255) NOT NULL,
                    master_link VARCHAR(64) NULL,
                    store_id VARCHAR(64) NULL,
                    changed TINYINT NOT NULL DEFAULT 0,
                    PRIMARY KEY(products_id, categories_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->ensureColumns($pdo, 'xt_products_to_categories', [
                'master_link' => 'VARCHAR(64) NULL',
                'store_id' => 'VARCHAR(64) NULL',
                'changed' => 'TINYINT NOT NULL DEFAULT 0',
            ]);
            $this->createIndexIfMissing($pdo, 'idx_xt_products_to_categories_changed', 'xt_products_to_categories', ['changed']);
            if ($this->tableExists($pdo, 'products_to_categories')) {
                $pdo->exec(
                    'INSERT IGNORE INTO xt_products_to_categories (products_id, categories_id, master_link, store_id, changed)
                     SELECT products_id, categories_id, master_link, store_id, COALESCE(changed, 0)
                     FROM products_to_categories'
                );
            }
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS xt_products_to_categories (
                products_id TEXT NOT NULL,
                categories_id TEXT NOT NULL,
                master_link TEXT NULL,
                store_id TEXT NULL,
                changed INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(products_id, categories_id)
            )'
        );
        $this->ensureColumns($pdo, 'xt_products_to_categories', [
            'master_link' => 'TEXT NULL',
            'store_id' => 'TEXT NULL',
            'changed' => 'INTEGER NOT NULL DEFAULT 0',
        ]);
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_xt_products_to_categories_changed ON xt_products_to_categories(changed)');
        if ($this->tableExists($pdo, 'products_to_categories')) {
            $pdo->exec(
                "INSERT OR IGNORE INTO xt_products_to_categories (products_id, categories_id, master_link, store_id, changed)
                 SELECT products_id, categories_id, master_link, store_id, COALESCE(changed, 0)
                 FROM products_to_categories"
            );
        }
    }

    private function ensurePerformanceIndexes(PDO $pdo): void
    {
        $this->createIndexIfTableExists($pdo, 'idx_artikel_changed_afs_artikel_id', 'artikel', ['changed', 'afs_artikel_id']);
        $this->createIndexIfTableExists($pdo, 'idx_artikel_changed_warengruppe_id', 'artikel', ['changed', 'warengruppe_id']);
        $this->createIndexIfTableExists($pdo, 'idx_warengruppe_changed_afs_wg_id', 'warengruppe', ['changed', 'afs_wg_id']);
        $this->createIndexIfTableExists($pdo, 'idx_documents_changed_source_id', 'documents', ['changed', 'source_id']);
        $this->createIndexIfTableExists($pdo, 'idx_media_changed_id', 'media', ['changed', 'id']);
        $this->createIndexIfTableExists($pdo, 'idx_artikel_attribute_map_changed_artikel', 'artikel_attribute_map', ['changed', 'afs_artikel_id']);
        $this->createIndexIfTableExists($pdo, 'idx_artikel_attribute_map_artikel_parent_child', 'artikel_attribute_map', ['afs_artikel_id', 'attributes_parent_id', 'attributes_id']);
        $this->createIndexIfTableExists($pdo, 'idx_artikel_media_map_changed_artikel', 'artikel_media_map', ['changed', 'afs_artikel_id']);
        $this->createIndexIfTableExists($pdo, 'idx_artikel_media_map_artikel_position', 'artikel_media_map', ['afs_artikel_id', 'position']);
        $this->createIndexIfTableExists($pdo, 'idx_artikel_warengruppe_changed_artikel', 'artikel_warengruppe', ['changed', 'afs_artikel_id']);
        $this->createIndexIfTableExists($pdo, 'idx_artikel_warengruppe_artikel_wg', 'artikel_warengruppe', ['afs_artikel_id', 'afs_wg_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_products_external_id', 'xt_products', ['external_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_products_changed_status', 'xt_products', ['changed', 'products_status']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_categories_external_id', 'xt_categories', ['external_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_categories_changed_status', 'xt_categories', ['changed', 'categories_status']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_media_external_id', 'xt_media', ['external_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_media_changed_status', 'xt_media', ['changed', 'status']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_pta_changed_product', 'xt_plg_products_to_attributes', ['changed', 'products_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_pta_product_parent', 'xt_plg_products_to_attributes', ['products_id', 'attributes_parent_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_products_to_categories_changed_product', 'xt_products_to_categories', ['changed', 'products_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_products_to_categories_category', 'xt_products_to_categories', ['categories_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_media_link_changed_link', 'xt_media_link', ['changed', 'link_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_media_link_m_id', 'xt_media_link', ['m_id']);
        $this->createIndexIfTableExists($pdo, 'idx_xt_seo_url_link', 'xt_seo_url', ['link_type', 'link_id']);
    }

    /**
     * @param array<int, string> $columns
     */
    private function createIndexIfTableExists(PDO $pdo, string $indexName, string $table, array $columns): void
    {
        if (!$this->tableExists($pdo, $table) || $columns === []) {
            return;
        }
        try {
            $this->createIndexIfMissing($pdo, $indexName, $table, $columns, false);
        } catch (\Throwable $e) {
            $this->logOptionalIndexError($table, $indexName, $e);
        }
    }

    private function renameColumnIfExists(PDO $pdo, string $table, string $from, string $to): void
    {
        $stmt = $this->describeTable($pdo, $table);
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $hasFrom = false;
        $hasTo = false;
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? $row['Field'] ?? '');
            if (strcasecmp($name, $from) === 0) {
                $hasFrom = true;
            }
            if (strcasecmp($name, $to) === 0) {
                $hasTo = true;
            }
        }

        if ($hasFrom && !$hasTo) {
            $pdo->exec(
                'ALTER TABLE ' . $this->quoteIdentifier($table) .
                ' RENAME COLUMN ' . $this->quoteIdentifier($from) .
                ' TO ' . $this->quoteIdentifier($to)
            );
        }
    }

    private function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }

    private function logMssqlConfigOnce(string $host, string $port, string $db, string $user): void
    {
        if (!is_dev_env()) {
            return;
        }

        static $logged = false;
        if ($logged) {
            return;
        }
        $logged = true;

        $timestamp = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $line = "[{$timestamp}] mssql_debug host={$host} port={$port} db={$db} user={$user}\n";
        $path = __DIR__ . '/../../logs/app.log';
        @file_put_contents($path, $line, FILE_APPEND);
    }

    private function isMysql(PDO $pdo): bool
    {
        return (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql';
    }

    private function describeTable(PDO $pdo, string $table): \PDOStatement
    {
        if ($this->isMysql($pdo)) {
            return $pdo->query('DESCRIBE ' . $this->quoteIdentifier($table));
        }
        return $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
    }

    private function tableExists(PDO $pdo, string $table): bool
    {
        if ($this->isMysql($pdo)) {
            $stmt = $pdo->prepare('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :table');
            $stmt->execute([':table' => $table]);
            return (int)$stmt->fetchColumn() > 0;
        }
        return (int)$pdo->query(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=" . $pdo->quote($table)
        )->fetchColumn() > 0;
    }

    private function indexExists(PDO $pdo, string $table, string $indexName): bool
    {
        if ($this->isMysql($pdo)) {
            $stmt = $pdo->prepare(
                'SELECT COUNT(*) FROM information_schema.statistics
                 WHERE table_schema = DATABASE() AND table_name = :table AND index_name = :index_name'
            );
            $stmt->execute([
                ':table' => $table,
                ':index_name' => $indexName,
            ]);
            return (int)$stmt->fetchColumn() > 0;
        }

        $stmt = $pdo->prepare(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name = :table AND name = :index_name"
        );
        $stmt->execute([
            ':table' => $table,
            ':index_name' => $indexName,
        ]);
        return (int)$stmt->fetchColumn() > 0;
    }

    /**
     * @param array<int, string> $columns
     */
    private function createIndexIfMissing(PDO $pdo, string $indexName, string $table, array $columns, bool $unique = false): void
    {
        if ($columns === [] || !$this->tableExists($pdo, $table) || $this->indexExists($pdo, $table, $indexName)) {
            return;
        }

        $quotedColumns = implode(', ', array_map(
            fn(string $column): string => $this->indexColumnSql($pdo, $table, $column),
            $columns
        ));
        $sql = 'CREATE ' . ($unique ? 'UNIQUE ' : '') . 'INDEX ' . $this->quoteIdentifier($indexName) .
            ' ON ' . $this->quoteIdentifier($table) . '(' . $quotedColumns . ')';
        if (!$this->isMysql($pdo)) {
            $sql = 'CREATE ' . ($unique ? 'UNIQUE ' : '') . 'INDEX IF NOT EXISTS ' . $this->quoteIdentifier($indexName) .
                ' ON ' . $this->quoteIdentifier($table) . '(' . $quotedColumns . ')';
        }
        $pdo->exec($sql);
    }

    private function logOptionalIndexError(string $table, string $indexName, \Throwable $e): void
    {
        $dir = dirname(__DIR__, 2) . '/logs';
        if (!is_dir($dir)) {
            @mkdir($dir, 0777, true);
        }
        @file_put_contents(
            $dir . '/app.log',
            '[' . gmdate(DATE_ATOM) . '] optional_index_failed table=' . $table . ' index=' . $indexName . ' error=' . $e->getMessage() . PHP_EOL,
            FILE_APPEND
        );
    }

    private function indexColumnSql(PDO $pdo, string $table, string $column): string
    {
        $quoted = $this->quoteIdentifier($column);
        if (!$this->isMysql($pdo)) {
            return $quoted;
        }

        $stmt = $this->describeTable($pdo, $table);
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? $row['Field'] ?? '');
            if (strcasecmp($name, $column) !== 0) {
                continue;
            }
            $type = strtolower((string)($row['type'] ?? $row['Type'] ?? ''));
            if (
                str_contains($type, 'text') ||
                str_contains($type, 'blob') ||
                preg_match('/^varchar\((\d+)\)/', $type, $m)
            ) {
                $length = isset($m[1]) ? min((int)$m[1], 191) : 191;
                return $quoted . '(' . $length . ')';
            }
            return $quoted;
        }

        return $quoted;
    }

    /**
     * @return array<int, string>
     */
    private function splitSqlStatements(string $sql): array
    {
        $parts = preg_split('/;\s*(?:\r?\n|$)/', $sql) ?: [];
        return array_values(array_filter(array_map('trim', $parts), static fn(string $part): bool => $part !== ''));
    }

    /**
     * @return array<int, array{0:string,1:string,2:string}>
     */
    private function mysqlConnectionAttempts(string $host, string $port, string $db): array
    {
        $attempts = [[$host, $port, $db]];
        $isDocker = is_file('/.dockerenv');

        if ($isDocker && in_array($host, ['127.0.0.1', 'localhost'], true)) {
            $attempts[] = ['welafix-db', '3306', $db];
        }
        if (!$isDocker && $host === 'welafix-db') {
            $attempts[] = ['127.0.0.1', '3307', $db];
        }

        $unique = [];
        $out = [];
        foreach ($attempts as $attempt) {
            $key = implode('|', $attempt);
            if (isset($unique[$key])) {
                continue;
            }
            $unique[$key] = true;
            $out[] = $attempt;
        }
        return $out;
    }

}
