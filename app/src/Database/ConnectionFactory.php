<?php
declare(strict_types=1);

namespace Welafix\Database;

use PDO;
use RuntimeException;

final class ConnectionFactory
{
    private ?PDO $sqlite = null;
    private ?PDO $mssql = null;

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
            'warengruppe_id' => 'TEXT',
            'price' => 'REAL',
            'stock' => 'INTEGER',
            'online' => 'INTEGER',
            'last_seen_at' => 'TEXT',
            'changed' => 'INTEGER DEFAULT 0',
            'change_reason' => 'TEXT',
        ]);

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
}
