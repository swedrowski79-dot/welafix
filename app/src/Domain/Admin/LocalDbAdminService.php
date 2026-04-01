<?php
declare(strict_types=1);

namespace Welafix\Domain\Admin;

use PDO;
use RuntimeException;
use Welafix\Database\ConnectionFactory;

final class LocalDbAdminService
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function resetAndRecreate(): array
    {
        $driver = $this->factory->localDriver();
        return match ($driver) {
            'mysql', 'mariadb' => $this->resetMysql(),
            default => $this->resetSqlite(),
        };
    }

    /**
     * @return array<string, mixed>
     */
    private function resetSqlite(): array
    {
        $path = trim((string)env('SQLITE_PATH', ''));
        if ($path === '') {
            throw new RuntimeException('SQLITE_PATH ist nicht gesetzt.');
        }

        $this->factory->disconnectAll();

        $removed = [];
        foreach ([$path, $path . '-wal', $path . '-shm'] as $file) {
            if (!file_exists($file)) {
                continue;
            }
            if (!@unlink($file)) {
                throw new RuntimeException('Konnte SQLite-Datei nicht löschen: ' . $file);
            }
            $removed[] = $file;
        }

        $this->factory->ensureSqliteMigrated();

        return [
            'ok' => true,
            'driver' => 'sqlite',
            'path' => $path,
            'removed' => $removed,
            'recreated' => file_exists($path),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function resetMysql(): array
    {
        $host = trim((string)env('LOCAL_DB_HOST', '127.0.0.1'));
        $port = trim((string)env('LOCAL_DB_PORT', '3306'));
        $db   = trim((string)env('LOCAL_DB_NAME', 'welafix'));
        $rootPass = (string)env('LOCAL_DB_ROOT_PASSWORD', 'root');

        if ($db === '') {
            throw new RuntimeException('LOCAL_DB_NAME ist nicht gesetzt.');
        }

        $this->factory->disconnectAll();
        $pdo = null;
        $lastError = null;
        foreach ($this->mysqlConnectionAttempts($host, $port) as [$tryHost, $tryPort]) {
            $dsn = "mysql:host={$tryHost};port={$tryPort};charset=utf8mb4";
            try {
                $pdo = new PDO($dsn, 'root', $rootPass, [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                ]);
                $host = $tryHost;
                $port = $tryPort;
                break;
            } catch (\Throwable $e) {
                $lastError = $e;
            }
        }
        if (!$pdo instanceof PDO) {
            throw new RuntimeException('Admin-Verbindung zu MySQL/MariaDB fehlgeschlagen: ' . ($lastError?->getMessage() ?? 'unbekannter Fehler'), 0, $lastError);
        }

        $quotedDb = '`' . str_replace('`', '``', $db) . '`';
        $pdo->exec('DROP DATABASE IF EXISTS ' . $quotedDb);
        $pdo->exec('CREATE DATABASE ' . $quotedDb . ' CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci');

        $this->factory->disconnectAll();

        return [
            'ok' => true,
            'driver' => 'mysql',
            'host' => $host,
            'port' => $port,
            'database' => $db,
            'recreated' => true,
        ];
    }

    /**
     * @return array<int, array{0:string,1:string}>
     */
    private function mysqlConnectionAttempts(string $host, string $port): array
    {
        $attempts = [[$host, $port]];
        $isDocker = is_file('/.dockerenv');

        if ($isDocker && in_array($host, ['127.0.0.1', 'localhost'], true)) {
            $attempts[] = ['welafix-db', '3306'];
        }
        if (!$isDocker && $host === 'welafix-db') {
            $attempts[] = ['127.0.0.1', '3307'];
        }

        $seen = [];
        $out = [];
        foreach ($attempts as $attempt) {
            $key = implode('|', $attempt);
            if (isset($seen[$key])) {
                continue;
            }
            $seen[$key] = true;
            $out[] = $attempt;
        }
        return $out;
    }
}
