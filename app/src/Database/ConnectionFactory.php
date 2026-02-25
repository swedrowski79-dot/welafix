<?php
declare(strict_types=1);

namespace Welafix\Database;

use PDO;
use RuntimeException;

final class ConnectionFactory
{
    private ?PDO $sqlite = null;
    private ?PDO $mssql = null;
    private ?PDO $media = null;

    public function sqlite(): PDO
    {
        if ($this->sqlite) return $this->sqlite;

        $path = getenv('SQLITE_PATH') ?: (__DIR__ . '/../../storage/app.db');
        $dir = dirname($path);
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }

        $pdo = new PDO('sqlite:' . $path);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->sqlite = $pdo;
        return $pdo;
    }

    public function mssql(): PDO
    {
        if ($this->mssql) return $this->mssql;

        $host = getenv('MSSQL_HOST') ?: 'localhost';
        $port = getenv('MSSQL_PORT') ?: '1433';
        $db   = getenv('MSSQL_DB') ?: '';
        $user = getenv('MSSQL_USER') ?: '';
        $pass = getenv('MSSQL_PASS') ?: '';

        $encrypt = (getenv('MSSQL_ENCRYPT') === 'true') ? 'yes' : 'no';
        $trust   = (getenv('MSSQL_TRUST_CERT') === 'true') ? 'yes' : 'no';

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

    public function media(): PDO
    {
        if ($this->media) return $this->media;

        $path = getenv('MEDIA_DB_PATH') ?: (__DIR__ . '/../../storage/media.db');
        $dir = dirname($path);
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }

        $pdo = new PDO('sqlite:' . $path);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->media = $pdo;
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
            'afs_key' => 'TEXT',
            'afs_artikel_id' => 'TEXT',
            'warengruppe_id' => 'TEXT',
            'price' => 'REAL',
            'stock' => 'INTEGER',
            'online' => 'INTEGER',
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
            'last_seen_at' => 'TEXT',
            'changed' => 'INTEGER DEFAULT 0',
            'change_reason' => 'TEXT',
        ]);

        $this->ensureDocumentSchema($pdo);
    }

    public function ensureMediaMigrated(): void
    {
        $pdo = $this->media();

        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS media_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_key TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                filename TEXT NULL,
                mime_type TEXT NULL,
                storage_path TEXT NOT NULL,
                checksum TEXT NULL,
                size_bytes INTEGER NULL,
                updated_at TEXT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(source, source_key)
            )'
        );

        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS media_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                field_name TEXT NULL,
                FOREIGN KEY(asset_id) REFERENCES media_assets(id) ON DELETE CASCADE
            )'
        );

        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_media_links_entity ON media_links(entity_type, entity_id)');
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_media_assets_type ON media_assets(asset_type)');
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_media_assets_checksum ON media_assets(checksum)');
        $pdo->exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_media_links_unique ON media_links(asset_id, entity_type, entity_id, field_name)');
    }

    /**
     * @param array<string, string> $columns
     */
    private function ensureColumns(PDO $pdo, string $table, array $columns): void
    {
        $stmt = $pdo->query("PRAGMA table_info({$table})");
        $existing = [];
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $existing[$row['name']] = true;
        }

        foreach ($columns as $name => $type) {
            if (isset($existing[$name])) {
                continue;
            }
            $pdo->exec("ALTER TABLE {$table} ADD COLUMN {$name} {$type}");
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
                doc_no TEXT,
                doc_date TEXT,
                customer_no TEXT,
                total_gross REAL,
                currency TEXT,
                updated_at TEXT,
                synced_at TEXT,
                UNIQUE(source, source_id)
            )'
        );

        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_documents_doc_no ON documents(doc_no)');
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_documents_doc_date ON documents(doc_date)');

        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS document_items (
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
            )'
        );
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_document_items_document_id ON document_items(document_id)');
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_document_items_article_no ON document_items(article_no)');

        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS document_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                mime_type TEXT,
                storage_path TEXT NOT NULL,
                checksum TEXT,
                created_at TEXT,
                FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
            )'
        );
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_document_files_document_id ON document_files(document_id)');
    }

}
