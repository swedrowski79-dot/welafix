<?php
declare(strict_types=1);

namespace Welafix\Domain\Xt;

use PDO;
use RuntimeException;
use Welafix\Database\Db;

final class XtTableImportService
{
    /**
     * @return array<int, string>
     */
    public function fetchTables(): array
    {
        $data = $this->request('GET', '/schema/tables');
        return $data['tables'] ?? [];
    }

    /**
     * @return array<string, mixed>
     */
    public function fetchTableSchema(string $table): array
    {
        $data = $this->request('GET', '/schema/table/' . rawurlencode($table));
        return $data;
    }

    /**
     * @return array<string, mixed>
     */
    public function importTable(string $table, int $pageSize = 2000): array
    {
        $pageSize = max(1, min(10000, $pageSize));
        $schema = $this->fetchTableSchema($table);
        if (!($schema['ok'] ?? false)) {
            throw new RuntimeException('Schema not available.');
        }
        $columns = $schema['columns'] ?? [];
        if (!is_array($columns) || $columns === []) {
            throw new RuntimeException('No columns found.');
        }

        $sqlite = Db::guardSqlite(Db::sqlite(), __METHOD__);
        $this->recreateTable($sqlite, $table, $columns);

        $page = 1;
        $total = 0;
        $pages = 0;
        while (true) {
            $data = $this->request('GET', '/export/table/' . rawurlencode($table), [
                'page' => $page,
                'page_size' => $pageSize,
            ]);
            $rows = $data['rows'] ?? [];
            if (!is_array($rows)) {
                $rows = [];
            }
            $this->insertRows($sqlite, $table, $columns, $rows);
            $count = count($rows);
            $total += $count;
            $pages++;
            if (empty($data['has_more'])) {
                break;
            }
            $page++;
        }

        return [
            'ok' => true,
            'table' => $table,
            'page_size' => $pageSize,
            'pages' => $pages,
            'rows' => $total,
        ];
    }

    /**
     * @param array<int, array<string, mixed>> $columns
     */
    private function recreateTable(PDO $pdo, string $table, array $columns): void
    {
        if (!$this->isSafeIdentifier($table)) {
            throw new RuntimeException('Invalid table name.');
        }
        $pdo->exec('DROP TABLE IF EXISTS ' . $this->quoteIdentifier($table));

        $colDefs = [];
        foreach ($columns as $col) {
            $name = (string)($col['name'] ?? '');
            $type = (string)($col['type'] ?? '');
            if ($name === '' || !$this->isSafeIdentifier($name)) {
                continue;
            }
            $sqliteType = $this->mapType($type);
            $colDefs[] = $this->quoteIdentifier($name) . ' ' . $sqliteType;
        }
        if ($colDefs === []) {
            throw new RuntimeException('No valid columns.');
        }
        $pdo->exec('CREATE TABLE ' . $this->quoteIdentifier($table) . ' (' . implode(', ', $colDefs) . ')');
    }

    /**
     * @param array<int, array<string, mixed>> $columns
     * @param array<int, array<string, mixed>> $rows
     */
    private function insertRows(PDO $pdo, string $table, array $columns, array $rows): void
    {
        if ($rows === []) {
            return;
        }
        $colNames = [];
        foreach ($columns as $col) {
            $name = (string)($col['name'] ?? '');
            if ($name === '' || !$this->isSafeIdentifier($name)) {
                continue;
            }
            $colNames[] = $name;
        }
        if ($colNames === []) {
            return;
        }
        $placeholders = array_map(static fn(string $c): string => ':' . $c, $colNames);
        $sql = 'INSERT INTO ' . $this->quoteIdentifier($table) .
            ' (' . implode(',', array_map([$this, 'quoteIdentifier'], $colNames)) . ')' .
            ' VALUES (' . implode(',', $placeholders) . ')';
        $stmt = $pdo->prepare($sql);

        $pdo->beginTransaction();
        try {
            foreach ($rows as $row) {
                foreach ($colNames as $col) {
                    $stmt->bindValue(':' . $col, $row[$col] ?? null);
                }
                $stmt->execute();
            }
            $pdo->commit();
        } catch (\Throwable $e) {
            $pdo->rollBack();
            throw $e;
        }
    }

    /**
     * @param array<string, mixed>|null $query
     * @return array<string, mixed>
     */
    private function request(string $method, string $path, ?array $query = null): array
    {
        $base = trim((string)env('XT_API_BASE_URL', (string)env('XT_API_BASE', '')));
        if ($base === '') {
            throw new RuntimeException('XT_API_BASE_URL fehlt.');
        }
        $key = (string)env('XT_API_KEY', '');
        if ($key === '') {
            throw new RuntimeException('XT_API_KEY fehlt.');
        }

        $url = rtrim($base, "/\\") . $path;
        if ($query) {
            $url .= '?' . http_build_query($query);
        }

        $ts = (string)time();
        $signPath = parse_url($url, PHP_URL_PATH) ?: $path;
        $baseString = $method . "\n" . $signPath . "\n" . $ts . "\n";
        $sig = hash_hmac('sha256', $baseString, $key);

        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, 60);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'X-API-KEY: default',
            'X-API-TS: ' . $ts,
            'X-API-SIG: ' . $sig,
            'Accept: application/json',
        ]);
        $resp = curl_exec($ch);
        $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $err = curl_error($ch);
        curl_close($ch);

        if ($resp === false) {
            throw new RuntimeException('XT-API unreachable: ' . $err);
        }
        if ($code >= 400) {
            $snippet = substr((string)$resp, 0, 300);
            throw new RuntimeException('XT-API http error: ' . $code . ' url=' . $url . ' body=' . $snippet);
        }

        $json = json_decode((string)$resp, true);
        if (!is_array($json)) {
            throw new RuntimeException('XT-API response ungültig.');
        }
        return $json;
    }

    private function isSafeIdentifier(string $name): bool
    {
        return (bool)preg_match('/^[A-Za-z0-9_]+$/', $name);
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    private function mapType(string $mysqlType): string
    {
        $t = strtolower($mysqlType);
        if (str_contains($t, 'int')) {
            return 'INTEGER';
        }
        if (str_contains($t, 'decimal') || str_contains($t, 'numeric') || str_contains($t, 'float') || str_contains($t, 'double') || str_contains($t, 'real')) {
            return 'REAL';
        }
        if (str_contains($t, 'blob') || str_contains($t, 'binary')) {
            return 'BLOB';
        }
        return 'TEXT';
    }
}
