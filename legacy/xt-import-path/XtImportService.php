<?php
declare(strict_types=1);

namespace Welafix\Domain\Xt;

use PDO;
use RuntimeException;
use Welafix\Config\MappingLoader;
use Welafix\Database\Db;

final class XtImportService
{
    /**
     * Legacy archivierter Service.
     *
     * Dieser Pfad ist absichtlich nicht mehr aktiv verdrahtet.
     * Siehe legacy/xt-import-path/README.md
     *
     * @return array<string, mixed>
     */
    public function import(string $mappingName, int $pageSize = 500): array
    {
        $mapping = (new MappingLoader())->load($mappingName);
        $source = $mapping['source'] ?? [];
        $db = (string)($source['db'] ?? '');
        if ($db !== 'xt_api' && $db !== 'xt_mysql') {
            throw new RuntimeException('Mapping ist nicht xt_api/xt_mysql.');
        }

        $table = (string)($source['table'] ?? '');
        $key = (string)($source['key'] ?? '');
        $select = $mapping['select'] ?? [];
        if ($table === '' || !is_array($select) || $select === []) {
            throw new RuntimeException('Mapping unvollständig.');
        }

        $sqlite = Db::guardSqlite(Db::sqlite(), __METHOD__);
        $this->ensureTable($sqlite, $table, $select, $key);

        $stats = [
            'ok' => true,
            'inserted' => 0,
            'updated' => 0,
            'pages' => 0,
            'batch_size' => 0,
        ];

        $pageSize = $this->loadBatchSize($pageSize);
        $stats['batch_size'] = $pageSize;

        if ($db === 'xt_api') {
            $page = 1;
            while (true) {
                $payload = $this->fetchFromXtApi($mappingName, $page, $pageSize);
                $rows = $payload['rows'] ?? [];
                $this->upsertRows($sqlite, $table, $select, $key, $rows, $stats);
                $stats['pages']++;
                if (!($payload['has_more'] ?? false)) {
                    break;
                }
                $page++;
            }
        } else {
            $rows = $this->fetchFromXtMysql($mapping, $pageSize);
            $this->upsertRows($sqlite, $table, $select, $key, $rows, $stats);
            $stats['pages'] = 1;
        }

        return $stats;
    }

    /**
     * @return array<string, mixed>
     */
    private function fetchFromXtApi(string $mappingName, int $page, int $pageSize): array
    {
        $base = trim((string)env('XT_API_BASE_URL', (string)env('XT_API_BASE', '')));
        if ($base === '') {
            throw new RuntimeException('XT_API_BASE_URL fehlt.');
        }
        $key = (string)env('XT_API_KEY', '');
        if ($key === '') {
            throw new RuntimeException('XT_API_KEY fehlt.');
        }

        $mapping = (new MappingLoader())->load($mappingName);
        $source = $mapping['source'] ?? [];
        $payload = [
            'table' => (string)($source['table'] ?? ''),
            'select' => $mapping['select'] ?? [],
            'where' => (string)($source['where'] ?? '1=1'),
            'key' => (string)($source['key'] ?? ''),
            'page' => $page,
            'page_size' => $pageSize,
        ];

        $url = rtrim($base, "/\\") . '/export';
        $body = json_encode($payload);
        if ($body === false) {
            throw new RuntimeException('XT-API payload ungültig.');
        }
        $ts = (string)time();
        $path = parse_url($url, PHP_URL_PATH) ?: '/export';
        $baseString = "POST\n" . $path . "\n" . $ts . "\n" . $body;
        $sig = hash_hmac('sha256', $baseString, $key);

        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, 30);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'X-API-KEY: default',
            'X-API-TS: ' . $ts,
            'X-API-SIG: ' . $sig,
            'Accept: application/json',
            'Content-Type: application/json',
        ]);
        $resp = curl_exec($ch);
        $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $err = curl_error($ch);
        curl_close($ch);

        if ($resp === false) {
            throw new RuntimeException('XT-API unreachable: ' . $err);
        }
        if ($code >= 400) {
            $body = is_string($resp) ? trim($resp) : '';
            if (strlen($body) > 300) {
                $body = substr($body, 0, 300) . '...';
            }
            throw new RuntimeException('XT-API http error: ' . $code . ' url=' . $url . ' body=' . $body);
        }

        $json = json_decode((string)$resp, true);
        if (!is_array($json)) {
            throw new RuntimeException('XT-API response ungültig.');
        }
        return $json;
    }

    private function loadBatchSize(int $fallback): int
    {
        $fallback = $fallback > 0 ? $fallback : 500;
        try {
            $pdo = Db::guardSqlite(Db::sqlite(), __METHOD__);
            $pdo->exec('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)');
            $stmt = $pdo->prepare('SELECT value FROM settings WHERE key = :key');
            $stmt->execute([':key' => 'xt_import_batch_size']);
            $value = $stmt->fetchColumn();
            if ($value !== false) {
                $num = (int)$value;
                if ($num >= 50 && $num <= 5000) {
                    return $num;
                }
            }
        } catch (\Throwable $e) {
            // ignore and use fallback
        }
        return $fallback;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function fetchFromXtMysql(array $mapping, int $pageSize): array
    {
        $host = (string)env('XT_DB_HOST', '');
        $port = (string)env('XT_DB_PORT', '3306');
        $db = (string)env('XT_DB_NAME', '');
        $user = (string)env('XT_DB_USER', '');
        $pass = (string)env('XT_DB_PASS', '');
        if ($host === '' || $db === '') {
            throw new RuntimeException('XT_DB_HOST/XT_DB_NAME fehlen.');
        }
        $pdo = new PDO(
            "mysql:host={$host};port={$port};dbname={$db};charset=utf8mb4",
            $user,
            $pass,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );

        $source = $mapping['source'] ?? [];
        $table = (string)($source['table'] ?? '');
        $where = (string)($source['where'] ?? '1=1');
        $key = (string)($source['key'] ?? '');
        $select = $mapping['select'] ?? [];
        $cols = array_map([$this, 'quoteIdentifierMySql'], $select);
        $sql = 'SELECT ' . implode(', ', $cols) . ' FROM ' . $this->quoteIdentifierMySql($table) . ' WHERE ' . $where;
        if ($key !== '') {
            $sql .= ' ORDER BY ' . $this->quoteIdentifierMySql($key);
        }
        $sql .= ' LIMIT ' . (int)$pageSize;
        $stmt = $pdo->query($sql);
        return $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<string, mixed> $stats
     */
    private function upsertRows(PDO $sqlite, string $table, array $select, string $key, array $rows, array &$stats): void
    {
        if ($rows === []) {
            return;
        }
        $cols = $select;
        $params = array_map(static fn(string $c): string => ':' . $c, $cols);
        $setParts = [];
        foreach ($cols as $col) {
            if ($col === $key) {
                continue;
            }
            $setParts[] = $this->quoteIdentifierSqlite($col) . ' = excluded.' . $this->quoteIdentifierSqlite($col);
        }
        $sql = 'INSERT INTO ' . $this->quoteIdentifierSqlite($table) .
            ' (' . implode(',', array_map([$this, 'quoteIdentifierSqlite'], $cols)) . ')' .
            ' VALUES (' . implode(',', $params) . ')';
        if ($key !== '') {
            $sql .= ' ON CONFLICT(' . $this->quoteIdentifierSqlite($key) . ') DO UPDATE SET ' . implode(', ', $setParts);
        }

        $stmt = $sqlite->prepare($sql);
        $sqlite->beginTransaction();
        try {
            foreach ($rows as $row) {
                foreach ($cols as $col) {
                    $stmt->bindValue(':' . $col, $row[$col] ?? null);
                }
                $stmt->execute();
                if ($stmt->rowCount() > 0) {
                    $stats['inserted']++;
                } else {
                    $stats['updated']++;
                }
            }
            $sqlite->commit();
        } catch (\Throwable $e) {
            $sqlite->rollBack();
            throw $e;
        }
    }

    /**
     * @param array<int, string> $columns
     */
    private function ensureTable(PDO $pdo, string $table, array $columns, string $key): void
    {
        $pdo->exec('CREATE TABLE IF NOT EXISTS ' . $this->quoteIdentifierSqlite($table) .
            ' (' . $this->quoteIdentifierSqlite($columns[0]) . ' TEXT)');

        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifierSqlite($table) . ')');
        $existing = [];
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $existing[strtolower($name)] = true;
            }
        }
        foreach ($columns as $col) {
            if (isset($existing[strtolower($col)])) {
                continue;
            }
            $pdo->exec('ALTER TABLE ' . $this->quoteIdentifierSqlite($table) . ' ADD COLUMN ' . $this->quoteIdentifierSqlite($col) . ' TEXT');
        }
        if ($key !== '') {
            $pdo->exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_' . $table . '_' . $key . ' ON ' .
                $this->quoteIdentifierSqlite($table) . '(' . $this->quoteIdentifierSqlite($key) . ')');
        }
    }

    private function quoteIdentifierSqlite(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    private function quoteIdentifierMySql(string $name): string
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $name)) {
            throw new RuntimeException('Ungueltiger Identifier');
        }
        return '`' . $name . '`';
    }
}
