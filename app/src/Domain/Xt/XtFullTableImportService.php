<?php
declare(strict_types=1);

namespace Welafix\Domain\Xt;

use PDO;
use RuntimeException;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;

final class XtFullTableImportService
{
    /**
     * @return array<string, mixed>
     */
    public function run(string $mappingName, ?string $jobId = null, int $batchSize = 500): array
    {
        $mapping = $this->loadMapping($mappingName);
        $jobs = $mapping['jobs'] ?? [];
        if (!is_array($jobs) || $jobs === []) {
            throw new RuntimeException('Keine Jobs im Mapping.');
        }

        $batchSize = max(1, min(10000, $batchSize));
        $stats = [
            'ok' => true,
            'mapping' => $mappingName,
            'batch_size' => $batchSize,
            'jobs' => [],
        ];

        foreach ($jobs as $job) {
            if (!is_array($job)) {
                continue;
            }
            $id = (string)($job['id'] ?? '');
            if ($jobId !== null && $jobId !== '' && $id !== $jobId) {
                continue;
            }
            $sourceTable = (string)($job['source']['table'] ?? '');
            $targetTable = (string)($job['target']['table'] ?? $sourceTable);
            if ($sourceTable === '' || $targetTable === '') {
                continue;
            }

            $stats['jobs'][$id ?: $sourceTable] = $this->importTable($sourceTable, $targetTable, $batchSize, $mapping);
        }

        return $stats;
    }

    /**
     * @return array<string, mixed>
     */
    private function importTable(string $sourceTable, string $targetTable, int $batchSize, array $mapping): array
    {
        $schema = $this->request('GET', '/schema/table/' . rawurlencode($sourceTable));
        if (!($schema['ok'] ?? false)) {
            throw new RuntimeException('Schema fehlt: ' . $sourceTable);
        }
        $columns = $schema['columns'] ?? [];
        if (!is_array($columns) || $columns === []) {
            throw new RuntimeException('Keine Spalten für ' . $sourceTable);
        }
        $pkCols = $this->detectPk($columns);

        $pdo = (new ConnectionFactory())->localDb();
        $this->ensureTable($pdo, $targetTable, $columns, $pkCols);
        $this->ensureChangedColumn($pdo, $targetTable);

        $noKeyStrategy = (string)($mapping['defaults']['no_key_strategy'] ?? 'truncate_insert');
        $truncateAll = (bool)($mapping['defaults']['truncate_before_import'] ?? false);
        $jobTruncate = isset($mapping['jobs']) && is_array($mapping['jobs']) ? (bool)($mapping['jobs'][$sourceTable]['truncate_before_import'] ?? false) : false;
        if ($truncateAll || $jobTruncate || ($pkCols === [] && $noKeyStrategy === 'truncate_insert')) {
            $pdo->exec('DELETE FROM ' . $this->quoteIdentifier($targetTable));
        }

        $selectCols = array_map(static fn(array $c): string => (string)$c['name'], $columns);
        $stats = [
            'table' => $targetTable,
            'rows' => 0,
            'pages' => 0,
            'inserted' => 0,
            'updated' => 0,
            'unchanged' => 0,
        ];

        $page = 1;
        while (true) {
            $data = $this->request('GET', '/export/table/' . rawurlencode($sourceTable), [
                'page' => $page,
                'page_size' => $batchSize,
            ]);
            $rows = $data['rows'] ?? [];
            if (!is_array($rows)) {
                $rows = [];
            }
            $this->applyRows($pdo, $targetTable, $selectCols, $pkCols, $rows, $stats);
            $stats['pages']++;
            if (empty($data['has_more'])) {
                break;
            }
            $page++;
        }

        // XT Full-Table Import: always reset changed to 0
        $pdo->exec('UPDATE ' . $this->quoteIdentifier($targetTable) . ' SET changed = 0');

        return $stats;
    }

    /**
     * @param array<int, array<string, mixed>> $columns
     * @return array<int, string>
     */
    private function detectPk(array $columns): array
    {
        $pk = [];
        foreach ($columns as $col) {
            if (($col['key'] ?? '') === 'PRI') {
                $pk[] = (string)($col['name'] ?? '');
            }
        }
        return array_values(array_filter($pk, static fn(string $c): bool => $c !== ''));
    }

    /**
     * @param array<int, string> $columns
     * @param array<int, array<string, mixed>> $rows
     * @param array<string, mixed> $stats
     */
    private function applyRows(PDO $pdo, string $table, array $columns, array $pkCols, array $rows, array &$stats): void
    {
        if ($rows === []) {
            return;
        }
        $colIds = array_map([$this, 'quoteIdentifier'], $columns);
        $placeholders = array_map(static fn(string $c): string => ':' . $c, $columns);

        $insertSql = 'INSERT INTO ' . $this->quoteIdentifier($table) .
            ' (' . implode(',', $colIds) . ', changed)' .
            ' VALUES (' . implode(',', $placeholders) . ', :changed)';
        $insertStmt = $pdo->prepare($insertSql);

        $updateSets = [];
        foreach ($columns as $col) {
            $updateSets[] = $this->quoteIdentifier($col) . ' = :' . $col;
        }
        $updateSql = 'UPDATE ' . $this->quoteIdentifier($table) .
            ' SET ' . implode(',', $updateSets) . ', changed = 1';

        $selectSql = 'SELECT ' . implode(',', $colIds) . ' FROM ' . $this->quoteIdentifier($table);
        $whereSql = '';
        if ($pkCols !== []) {
            $parts = [];
            foreach ($pkCols as $pk) {
                $parts[] = $this->quoteIdentifier($pk) . ' = :pk_' . $pk;
            }
            $whereSql = ' WHERE ' . implode(' AND ', $parts);
        }

        $selectStmt = $pkCols !== [] ? $pdo->prepare($selectSql . $whereSql) : null;
        $updateStmt = $pkCols !== [] ? $pdo->prepare($updateSql . $whereSql) : null;

        $pdo->beginTransaction();
        try {
            foreach ($rows as $row) {
                $stats['rows']++;
                if ($pkCols === []) {
                    foreach ($columns as $col) {
                        $insertStmt->bindValue(':' . $col, $row[$col] ?? null);
                    }
                    $insertStmt->bindValue(':changed', 1, PDO::PARAM_INT);
                    $insertStmt->execute();
                    $stats['inserted']++;
                    continue;
                }

                foreach ($pkCols as $pk) {
                    $selectStmt->bindValue(':pk_' . $pk, $row[$pk] ?? null);
                }
                $selectStmt->execute();
                $existing = $selectStmt->fetch(PDO::FETCH_ASSOC) ?: null;

                if ($existing === null) {
                    foreach ($columns as $col) {
                        $insertStmt->bindValue(':' . $col, $row[$col] ?? null);
                    }
                    $insertStmt->bindValue(':changed', 1, PDO::PARAM_INT);
                    $insertStmt->execute();
                    $stats['inserted']++;
                    continue;
                }

                if (!$this->diffRow($existing, $row, $columns)) {
                    $stats['unchanged']++;
                    continue;
                }

                foreach ($columns as $col) {
                    $updateStmt->bindValue(':' . $col, $row[$col] ?? null);
                }
                foreach ($pkCols as $pk) {
                    $updateStmt->bindValue(':pk_' . $pk, $row[$pk] ?? null);
                }
                $updateStmt->execute();
                $stats['updated']++;
            }
            $pdo->commit();
        } catch (\Throwable $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }
    }

    private function diffRow(array $existing, array $row, array $columns): bool
    {
        foreach ($columns as $col) {
            $old = $existing[$col] ?? null;
            $new = $row[$col] ?? null;
            if ($this->normalizeCompare($old) !== $this->normalizeCompare($new)) {
                return true;
            }
        }
        return false;
    }

    private function normalizeCompare(mixed $value): string
    {
        if ($value === null || $value === '') {
            return '';
        }
        return (string)$value;
    }

    /**
     * @param array<int, array<string, mixed>> $columns
     * @param array<int, string> $pkCols
     */
    private function ensureTable(PDO $pdo, string $table, array $columns, array $pkCols): void
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $table)) {
            throw new RuntimeException('Ungültige Tabelle');
        }
        $exists = $this->tableExists($pdo, $table);
        if (!$exists) {
            $defs = [];
            foreach ($columns as $col) {
                $name = (string)($col['name'] ?? '');
                if ($name === '' || !preg_match('/^[A-Za-z0-9_]+$/', $name)) {
                    continue;
                }
                $defs[] = $this->quoteIdentifier($name) . ' ' . $this->mapType((string)($col['type'] ?? ''));
            }
            $defs[] = 'changed ' . ($this->isMysql($pdo) ? 'TINYINT NOT NULL DEFAULT 0' : 'INTEGER NOT NULL DEFAULT 0');
            if (count($pkCols) === 1) {
                $defs[] = 'PRIMARY KEY (' . $this->quoteIdentifier($pkCols[0]) . ')';
            }
            $suffix = $this->isMysql($pdo) ? ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci' : '';
            $pdo->exec('CREATE TABLE ' . $this->quoteIdentifier($table) . ' (' . implode(', ', $defs) . ')' . $suffix);
        } else {
            $existing = $this->getExistingColumns($pdo, $table);
            foreach ($columns as $col) {
                $name = (string)($col['name'] ?? '');
                if ($name === '' || isset($existing[strtolower($name)])) {
                    continue;
                }
                $pdo->exec('ALTER TABLE ' . $this->quoteIdentifier($table) . ' ADD COLUMN ' . $this->quoteIdentifier($name) . ' ' . $this->mapType((string)($col['type'] ?? '')));
            }
        }
        if (count($pkCols) > 1) {
            $idx = $this->buildIndexName($table, $pkCols, 'uniq');
            $cols = implode(',', array_map([$this, 'quoteIdentifier'], $pkCols));
            if (!$this->indexExists($pdo, $table, $idx)) {
                $pdo->exec(($this->isMysql($pdo) ? 'CREATE UNIQUE INDEX ' : 'CREATE UNIQUE INDEX IF NOT EXISTS ') . $this->quoteIdentifier($idx) . ' ON ' . $this->quoteIdentifier($table) . '(' . $cols . ')');
            }
        }
    }

    private function ensureChangedColumn(PDO $pdo, string $table): void
    {
        $cols = $this->getExistingColumns($pdo, $table);
        if (!isset($cols['changed'])) {
            $pdo->exec('ALTER TABLE ' . $this->quoteIdentifier($table) . ' ADD COLUMN changed ' . ($this->isMysql($pdo) ? 'TINYINT NOT NULL DEFAULT 0' : 'INTEGER NOT NULL DEFAULT 0'));
        }
        $indexName = $this->buildIndexName($table, ['changed'], 'idx');
        if (!$this->indexExists($pdo, $table, $indexName)) {
            $pdo->exec(($this->isMysql($pdo) ? 'CREATE INDEX ' : 'CREATE INDEX IF NOT EXISTS ') . $this->quoteIdentifier($indexName) . ' ON ' . $this->quoteIdentifier($table) . '(changed)');
        }
    }

    /**
     * @return array<string, true>
     */
    private function getExistingColumns(PDO $pdo, string $table): array
    {
        $stmt = $this->isMysql($pdo)
            ? $pdo->query('DESCRIBE ' . $this->quoteIdentifier($table))
            : $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $cols = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? $row['Field'] ?? '');
            if ($name !== '') {
                $cols[strtolower($name)] = true;
            }
        }
        return $cols;
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

    /**
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

    private function loadMapping(string $name): array
    {
        $path = __DIR__ . '/../../Config/mappings/' . $name . '.php';
        if (!is_file($path)) {
            throw new RuntimeException('Mapping nicht gefunden: ' . $name);
        }
        $mapping = require $path;
        if (!is_array($mapping)) {
            throw new RuntimeException('Mapping ungültig: ' . $name);
        }
        return $mapping;
    }

    private function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }

    /**
     * @param array<int, string> $columns
     */
    private function buildIndexName(string $table, array $columns, string $prefix): string
    {
        $base = $prefix . '_' . $table . '_' . implode('_', $columns);
        if (strlen($base) <= 60) {
            return $base;
        }

        return substr($prefix . '_' . $table, 0, 40) . '_' . substr(sha1($base), 0, 16);
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

    private function isMysql(PDO $pdo): bool
    {
        return (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql';
    }

    private function tableExists(PDO $pdo, string $table): bool
    {
        if ($this->isMysql($pdo)) {
            $stmt = $pdo->prepare('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :table');
            $stmt->execute([':table' => $table]);
            return (int)$stmt->fetchColumn() > 0;
        }
        return (bool)$pdo->query('SELECT name FROM sqlite_master WHERE type="table" AND name=' . $pdo->quote($table))->fetchColumn();
    }
}
