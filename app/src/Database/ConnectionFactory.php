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
        $this->ensureSettingsTable($pdo);
        $this->ensureArtikelExtraDataTable($pdo);
        $this->ensureWarengruppeExtraDataTable($pdo);
        $this->ensureMetaDataArtikelTable($pdo);
        $this->ensureMetaDataWarengruppenTable($pdo);
        $this->ensureAfsUpdatePendingTable($pdo);
    }

    /**
     * @param array<string, string> $columns
     */
    private function ensureColumns(PDO $pdo, string $table, array $columns): void
    {
        $stmt = $pdo->query("PRAGMA table_info({$table})");
        $existing = [];
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $name = (string)($row['name'] ?? '');
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
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $existing = [];
        foreach ($rows as $row) {
            $name = strtolower((string)($row['name'] ?? ''));
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
        $this->ensureColumns($pdo, 'documents', [
            'last_seen_at' => 'TEXT NULL',
            'is_deleted' => 'INTEGER DEFAULT 0',
            'changed' => 'INTEGER DEFAULT 0',
        ]);
    }

    private function ensureMediaFilenameTable(PDO $pdo): void
    {
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
        $this->ensureColumns($pdo, 'attributes', [
            'changed' => 'INTEGER NOT NULL DEFAULT 0',
        ]);
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_attributes_changed ON attributes(changed)');
    }

    private function ensureSettingsTable(PDO $pdo): void
    {
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )'
        );
    }

    private function ensureArtikelAttributeMapTable(PDO $pdo): void
    {
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

    private function ensureArtikelExtraDataTable(PDO $pdo): void
    {
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
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS afs_update_pending (
                entity TEXT NOT NULL,
                source_id TEXT NOT NULL,
                PRIMARY KEY(entity, source_id)
            )'
        );
    }

    private function renameColumnIfExists(PDO $pdo, string $table, string $from, string $to): void
    {
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $hasFrom = false;
        $hasTo = false;
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
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
        return '"' . str_replace('"', '""', $name) . '"';
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

}
