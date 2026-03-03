<?php
declare(strict_types=1);

namespace Welafix\Domain\Xt;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Database\Db;
use Welafix\Database\ConnectionFactory;

final class XtMappingSyncService
{
    /** @var array<string, array<string, int>> */
    private array $targetMaps = [];
    /** @var array<string, bool> */
    private array $targetMapsLoaded = [];
    /** @var array<string, array<string, bool>> */
    private array $loggedMissing = [];
    private ?PDO $pdo = null;

    /**
     * @return array<string, mixed>
     */
    public function run(string $mappingName = 'welafix_xt'): array
    {
        $mapping = $this->loadMapping($mappingName);
        $targets = $mapping['targets'] ?? [];
        if (!is_array($targets) || $targets === []) {
            throw new RuntimeException('Keine Targets im Mapping.');
        }

        $pdo = Db::guardSqlite(Db::sqlite(), __METHOD__ . ':write');
        $this->pdo = $pdo;
        // separate read connection to avoid write-locks while iterating source rows
        $readPdo = Db::guardSqlite((new ConnectionFactory())->sqlite(), __METHOD__ . ':read');
        $batchSize = $this->loadBatchSize($pdo);
        $debugMeta = $this->loadDebugEnabled($pdo);
        $debugEnabled = $debugMeta['enabled'];

        $stats = [
            'ok' => true,
            'targets' => [],
            'nested_set_rebuild' => 0,
            'batch_size' => $batchSize,
            'debug' => $debugEnabled,
            'debug_setting' => $debugMeta['setting'],
            'debug_env' => $debugMeta['env'],
        ];

        // pre-load target maps for stable FK resolution
        $this->loadTargetMapFromDb($pdo, 'xt_products', 'external_id', 'products_id');
        $this->loadTargetMapFromDb($pdo, 'xt_media', 'external_id', 'id');
        $this->loadTargetMapFromDb($pdo, 'xt_categories', 'external_id', 'categories_id');
        $stats['map_sizes'] = [
            'xt_categories' => isset($this->targetMaps['xt_categories']) ? count($this->targetMaps['xt_categories']) : 0,
            'xt_products' => isset($this->targetMaps['xt_products']) ? count($this->targetMaps['xt_products']) : 0,
            'xt_media' => isset($this->targetMaps['xt_media']) ? count($this->targetMaps['xt_media']) : 0,
        ];

        foreach ($targets as $targetName => $target) {
            if (!is_array($target)) {
                continue;
            }
            $table = (string)($target['table'] ?? $targetName);
            $primaryKey = $target['primary_key'] ?? null;
            $columns = $target['columns'] ?? [];
            if ($table === '' || !is_array($columns) || $columns === []) {
                continue;
            }

            $this->ensureChangedColumn($pdo, $table);
            $existingCols = $this->getExistingColumns($pdo, $table);
            if ($existingCols === []) {
                $this->createTable($pdo, $table, $columns, $primaryKey);
                $existingCols = $this->getExistingColumns($pdo, $table);
            } else {
                $this->ensureColumns($pdo, $table, $columns, $existingCols);
                $existingCols = $this->getExistingColumns($pdo, $table);
            }

            $base = $this->resolveBaseSource($columns);
            if ($base === '') {
                $stats['targets'][$table] = ['skipped' => true, 'reason' => 'no_base_source'];
                continue;
            }

            $offset = 0;
            $totals = [
                'inserted' => 0,
                'updated' => 0,
                'unchanged' => 0,
                'needs_nested_set' => false,
            ];
            while (true) {
                $sourceRows = $this->fetchSourceRowsBatch($readPdo, $base, $batchSize, $offset);
                if ($sourceRows === []) {
                    break;
                }
                $result = $this->applyTarget($pdo, $table, $columns, $primaryKey, $existingCols, $base, $sourceRows, true);
                $totals['inserted'] += (int)($result['inserted'] ?? 0);
                $totals['updated'] += (int)($result['updated'] ?? 0);
                $totals['unchanged'] += (int)($result['unchanged'] ?? 0);
                if (!empty($result['needs_nested_set'])) {
                    $totals['needs_nested_set'] = true;
                }
                $offset += $batchSize;
            }

            $stats['targets'][$table] = $totals;

            if ($table === 'xt_categories' && !empty($totals['needs_nested_set'])) {
                $rebuilt = $this->rebuildNestedSet($pdo);
                $stats['nested_set_rebuild'] = $rebuilt;
            }
        }

        if ($debugEnabled) {
            $path = $this->getLogsDir() . '/xt_mapping_debug.json';
            $dir = dirname($path);
            if (!is_dir($dir)) {
                @mkdir($dir, 0777, true);
            }
            $ok = @file_put_contents($path, json_encode($stats, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
            $stats['debug_path'] = $path;
            $stats['debug_written'] = $ok !== false;
        }
        return $stats;
    }

    /**
     * @param array<string, string> $columns
     * @param array<string, true> $existingCols
     * @param array<int, array<string, mixed>> $sourceRows
     * @return array<string, mixed>
     */
    private function applyTarget(PDO $pdo, string $table, array $columns, mixed $primaryKey, array $existingCols, string $base, iterable $sourceRows, bool $useTransaction = false): array
    {
        $inserted = 0;
        $updated = 0;
        $unchanged = 0;
        $needsNestedSet = false;

        $pkCols = $this->normalizePk($primaryKey);
        $compareCols = $this->getCompareColumns($columns, $existingCols);
        if ($pkCols !== []) {
            $this->ensurePkIndex($pdo, $table, $pkCols);
        }
        $upsert = $this->buildUpsertStatement($pdo, $table, $columns, $existingCols, $pkCols, $compareCols);
        $upsertCols = $this->getUpsertColumns($table, $columns, $existingCols);
        $selectByPk = $pkCols !== [] ? $this->buildSelectByPk($pdo, $table, $pkCols, $compareCols) : null;
        $selectByExternal = $this->buildSelectByExternal($pdo, $table, $compareCols, $existingCols);
        $updateStmt = $pkCols !== [] ? $this->buildUpdateStatement($pdo, $table, $columns, $existingCols, $pkCols) : null;
        $updateCols = $pkCols !== [] ? $this->getUpdateColumns($table, $columns, $existingCols, $pkCols) : [];

        if ($useTransaction) {
            $pdo->beginTransaction();
        }
        try {
            foreach ($sourceRows as $sourceRow) {
                $context = [$base => $sourceRow];
                $values = [];
                $autoCols = [];
                foreach ($columns as $col => $expr) {
                    if (!isset($existingCols[strtolower($col)])) {
                        $this->warn($table, $this->pkJson($primaryKey, $values), (string)$expr, 'missing_column');
                        continue;
                    }
                    $exprValue = $this->evalExpr((string)$expr, $context, $table, $primaryKey, $values);
                    if ($exprValue === '__AUTO__') {
                        $autoCols[$col] = true;
                        $values[$col] = null;
                    } else {
                        $values[$col] = $exprValue;
                    }
                }

                $pkValues = $this->buildPkValues($pkCols, $values);

                $hasPk = $pkCols !== [] && $this->hasNonAutoPk($pkValues);
                // If we can compare, prefer select+diff to avoid pointless updates.
                if ($hasPk && $upsert && $compareCols === []) {
                    $this->bindValues($upsert, $values, $columns, $existingCols, $pkCols, $autoCols, $upsertCols);
                    $upsert->execute();
                    $count = $upsert->rowCount();
                    if ($count > 0) {
                        $updated++;
                        if ($table === 'xt_categories') {
                            $needsNestedSet = true;
                        }
                    } else {
                        $unchanged++;
                    }
                    $this->updateTargetMap($table, $values, $pkCols, $pdo);
                    continue;
                }

                // fallback: external_id or no PK
                $existing = null;
                $lookupPk = $pkValues;
                if ($hasPk && $selectByPk) {
                    $this->bindPk($selectByPk, $pkCols, $pkValues);
                    $selectByPk->execute();
                    $existing = $selectByPk->fetch(PDO::FETCH_ASSOC) ?: null;
                }
                if ($existing === null && isset($values['external_id']) && $selectByExternal) {
                    $selectByExternal->execute([':external_id' => $values['external_id']]);
                    $existing = $selectByExternal->fetch(PDO::FETCH_ASSOC) ?: null;
                    if ($existing) {
                        $lookupPk = $this->buildPkValues($pkCols, $existing);
                    }
                }

                if ($existing === null) {
                    $this->insertRow($pdo, $table, $values, $existingCols, $pkCols, $autoCols);
                    $inserted++;
                    $this->setChanged($pdo, $table, $pkCols, $this->buildPkValues($pkCols, $values));
                    if ($table === 'xt_categories') {
                        $needsNestedSet = true;
                    }
                    $this->updateTargetMap($table, $values, $pkCols, $pdo);
                    continue;
                }

                $diff = $this->diffValues($existing, $values, $compareCols);
                if ($diff) {
                    if ($updateStmt) {
                        $this->bindValues($updateStmt, $values, $columns, $existingCols, $pkCols, $autoCols, $updateCols);
                        $this->bindPk($updateStmt, $pkCols, $lookupPk);
                        $updateStmt->execute();
                    } else {
                        $this->updateRow($pdo, $table, $values, $existingCols, $pkCols, $lookupPk, $autoCols, $columns);
                    }
                    $updated++;
                    $this->setChanged($pdo, $table, $pkCols, $lookupPk);
                    if ($table === 'xt_categories') {
                        $needsNestedSet = true;
                    }
                    $this->updateTargetMap($table, $values, $pkCols, $pdo);
            } else {
                $unchanged++;
                // keep target maps warm even if nothing changed
                $this->updateTargetMap($table, $values, $pkCols, $pdo);
            }
        }
            if ($useTransaction) {
                $pdo->commit();
            }
        } catch (\Throwable $e) {
            if ($useTransaction && $pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }

        return [
            'inserted' => $inserted,
            'updated' => $updated,
            'unchanged' => $unchanged,
            'needs_nested_set' => $needsNestedSet,
        ];
    }

    private function ensureChangedColumn(PDO $pdo, string $table): void
    {
        $info = $this->getExistingColumns($pdo, $table);
        if ($info === []) {
            return;
        }
        if (!isset($info['changed'])) {
            $pdo->exec('ALTER TABLE ' . $this->quoteIdentifier($table) . ' ADD COLUMN changed INTEGER NOT NULL DEFAULT 0');
        }
        $pdo->exec('CREATE INDEX IF NOT EXISTS idx_' . $table . '_changed ON ' . $this->quoteIdentifier($table) . '(changed)');
    }

    /**
     * @return array<string, mixed>
     */
    private function loadMapping(string $name): array
    {
        $path = __DIR__ . '/../../Config/mappings/' . $name . '.php.php';
        if (!is_file($path)) {
            $path = __DIR__ . '/../../Config/mappings/' . $name . '.php';
        }
        if (!is_file($path)) {
            throw new RuntimeException('Mapping nicht gefunden: ' . $name);
        }
        $mapping = require $path;
        if (!is_array($mapping)) {
            throw new RuntimeException('Mapping ungültig: ' . $name);
        }
        return $mapping;
    }

    /**
     * @param array<string, string> $columns
     */
    private function resolveBaseSource(array $columns): string
    {
        foreach ($columns as $expr) {
            if (preg_match('/^(artikel|warengruppe|media)\./', (string)$expr, $m)) {
                return $m[1];
            }
        }
        foreach ($columns as $expr) {
            if (preg_match('/^(xt_[a-z0-9_]+)\./i', (string)$expr, $m)) {
                return $m[1];
            }
        }
        return '';
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function fetchSourceRows(PDO $pdo, string $source): iterable
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $source)) {
            return [];
        }
        $stmt = $pdo->query('SELECT * FROM ' . $this->quoteIdentifier($source));
        return $stmt ?: [];
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function fetchSourceRowsBatch(PDO $pdo, string $source, int $limit, int $offset): array
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $source)) {
            return [];
        }
        $limit = max(1, $limit);
        $offset = max(0, $offset);
        $sql = 'SELECT * FROM ' . $this->quoteIdentifier($source) . ' LIMIT :limit OFFSET :offset';
        $stmt = $pdo->prepare($sql);
        $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $stmt->execute();
        return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
    }

    private function loadBatchSize(PDO $pdo): int
    {
        $env = (int)env('XT_MAPPING_BATCH_SIZE', 0);
        $fallback = $env > 0 ? $env : 2000;
        try {
            $pdo->exec('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)');
            $stmt = $pdo->prepare('SELECT value FROM settings WHERE key = :key');
            $stmt->execute([':key' => 'xt_mapping_batch_size']);
            $value = $stmt->fetchColumn();
            if ($value !== false) {
                $num = (int)$value;
                if ($num >= 100 && $num <= 10000) {
                    return $num;
                }
            }
        } catch (\Throwable $e) {
            // ignore and use fallback
        }
        return $fallback;
    }

    /**
     * @return array{enabled:bool, setting:?string, env:string}
     */
    private function loadDebugEnabled(PDO $pdo): array
    {
        $env = (string)env('DEBUG', '');
        try {
            $pdo->exec('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)');
            $stmt = $pdo->prepare('SELECT value FROM settings WHERE key = :key');
            $stmt->execute([':key' => 'xt_debug_enabled']);
            $value = $stmt->fetchColumn();
            if ($value === false || $value === null || $value === '') {
                return ['enabled' => false, 'setting' => null, 'env' => $env];
            }
            return ['enabled' => (string)$value === '1', 'setting' => (string)$value, 'env' => $env];
        } catch (\Throwable $e) {
            return ['enabled' => false, 'setting' => null, 'env' => $env];
        }
    }

    private function evalExpr(string $expr, array $context, string $table, mixed $primaryKey, array $values): mixed
    {
        $expr = trim($expr);
        if ($expr === '' || strtolower($expr) === 'leer') {
            return '';
        }
        if (str_starts_with($expr, 'default:')) {
            return $this->parseDefault(substr($expr, 8));
        }
        if (str_starts_with($expr, 'calc:')) {
            return $this->calcValue(substr($expr, 5));
        }
        if ($expr === 'auto') {
            return '__AUTO__';
        }
        if ($expr === 'filename') {
            return $context['media']['filename'] ?? null;
        }
        if (preg_match('/^md5\((.+)\)$/', $expr, $m)) {
            $inner = $this->evalExpr($m[1], $context, $table, $primaryKey, $values);
            return md5((string)($inner ?? ''));
        }
        if (str_contains($expr, ' oder ')) {
            $parts = explode(' oder ', $expr);
            foreach ($parts as $part) {
                $val = $this->evalExpr($part, $context, $table, $primaryKey, $values);
                if ($val !== null && $val !== '') {
                    return $val;
                }
            }
            return '';
        }
        if (preg_match('/^(.+)\+(\d+)$/', $expr, $m)) {
            $base = $this->evalExpr($m[1], $context, $table, $primaryKey, $values);
            return (int)$base + (int)$m[2];
        }
        if (preg_match('/^(xt_[a-z0-9_]+)\.([A-Za-z0-9_]+)$/i', $expr, $m)) {
            $tableRef = $m[1];
            $field = $m[2];
            return $this->resolveTargetReference($tableRef, $field, $context);
        }
        if (preg_match('/^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$/', $expr, $m)) {
            $entity = $m[1];
            $field = $m[2];
            if (!isset($context[$entity])) {
                $this->warn($table, $this->pkJson($primaryKey, $values), $expr, 'missing_source_field');
                return '';
            }
            if (!array_key_exists($field, $context[$entity])) {
                $this->warn($table, $this->pkJson($primaryKey, $values), $expr, 'missing_source_field');
                return '';
            }
            return $context[$entity][$field];
        }
        return $expr;
    }

    private function parseDefault(string $raw): mixed
    {
        $raw = trim($raw);
        if ($raw === '') {
            return '';
        }
        if (($raw[0] === '"' && str_ends_with($raw, '"')) || ($raw[0] === "'" && str_ends_with($raw, "'"))) {
            return substr($raw, 1, -1);
        }
        if (is_numeric($raw)) {
            return strpos($raw, '.') !== false ? (float)$raw : (int)$raw;
        }
        if (strtolower($raw) === 'true') return true;
        if (strtolower($raw) === 'false') return false;
        return $raw;
    }

    private function calcValue(string $name): mixed
    {
        if ($name === 'now') {
            return (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format('Y-m-d H:i:s');
        }
        if ($name === 'nested_set_left' || $name === 'nested_set_right') {
            return null;
        }
        return null;
    }

    private function resolveTargetReference(string $table, string $field, array $context): mixed
    {
        if ($table === 'xt_categories' && $field === 'categories_id') {
            $wg = $context['warengruppe'] ?? null;
            if (is_array($wg)) {
                $key = $wg['afs_wg_id'] ?? $wg['afs_id'] ?? null;
                if ($key !== null) {
                    $map = $this->targetMaps['xt_categories'] ?? [];
                    $id = $map[(string)$key] ?? null;
                    if ($id !== null) {
                        return $id;
                    }
                    $id = $this->lookupTargetId('xt_categories', 'external_id', 'categories_id', (string)$key);
                    if ($id !== null) {
                        return $id;
                    }
                }
            }
        }
        if ($table === 'xt_products' && $field === 'products_id') {
            $artikel = $context['artikel'] ?? null;
            if (is_array($artikel)) {
                $key = $artikel['afs_artikel_id'] ?? null;
                if ($key !== null && $key !== '') {
                    $map = $this->targetMaps['xt_products'] ?? [];
                    $id = $map[(string)$key] ?? null;
                    if ($id !== null) {
                        return $id;
                    }
                    $id = $this->lookupTargetId('xt_products', 'external_id', 'products_id', (string)$key);
                    if ($id !== null) {
                        return $id;
                    }
                }
            }
        }
        if ($table === 'xt_media' && $field === 'id') {
            $media = $context['media'] ?? null;
            if (is_array($media)) {
                $key = $media['id'] ?? null;
                if ($key !== null && $key !== '') {
                    $map = $this->targetMaps['xt_media'] ?? [];
                    $id = $map[(string)$key] ?? null;
                    if ($id !== null) {
                        return $id;
                    }
                    $id = $this->lookupTargetId('xt_media', 'external_id', 'id', (string)$key);
                    if ($id !== null) {
                        return $id;
                    }
                }
            }
        }
        if (isset($context[$table]) && is_array($context[$table])) {
            return $context[$table][$field] ?? null;
        }
        return null;
    }

    /**
     * @param array<int, string> $pkCols
     * @param array<string, mixed> $values
     * @return array<string, mixed>
     */
    private function buildPkValues(array $pkCols, array $values): array
    {
        $out = [];
        foreach ($pkCols as $col) {
            if (array_key_exists($col, $values)) {
                $out[$col] = $values[$col];
            }
        }
        return $out;
    }

    /**
     * @return array<int, string>
     */
    private function normalizePk(mixed $primaryKey): array
    {
        if (is_array($primaryKey)) {
            return array_values(array_map('strval', $primaryKey));
        }
        if (is_string($primaryKey) && $primaryKey !== '') {
            return [$primaryKey];
        }
        return [];
    }

    /**
     * @param array<string, mixed> $values
     * @return array{row:?array, pk:array<string,mixed>}
     */
    private function findExistingRow(PDO $pdo, string $table, array $pkCols, array $pkValues, array $values): array
    {
        if ($pkCols !== [] && $pkValues !== [] && $this->hasNonAutoPk($pkValues)) {
            $where = [];
            $params = [];
            foreach ($pkCols as $col) {
                $where[] = $this->quoteIdentifier($col) . ' = :' . $col;
                $params[':' . $col] = $pkValues[$col] ?? null;
            }
            $sql = 'SELECT * FROM ' . $this->quoteIdentifier($table) . ' WHERE ' . implode(' AND ', $where) . ' LIMIT 1';
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);
            $row = $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
            return ['row' => $row, 'pk' => $pkValues];
        }

        if (isset($values['external_id']) && $values['external_id'] !== null && $values['external_id'] !== '') {
            $stmt = $pdo->prepare(
                'SELECT * FROM ' . $this->quoteIdentifier($table) . ' WHERE external_id = :external_id LIMIT 1'
            );
            $stmt->execute([':external_id' => $values['external_id']]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
            if ($row) {
                $pk = [];
                foreach ($pkCols as $col) {
                    if (isset($row[$col])) {
                        $pk[$col] = $row[$col];
                    }
                }
                return ['row' => $row, 'pk' => $pk];
            }
        }

        return ['row' => null, 'pk' => []];
    }

    /**
     * @param array<string, mixed> $values
     */
    private function hasNonAutoPk(array $values): bool
    {
        foreach ($values as $v) {
            if ($v !== null && $v !== '' && $v !== 'auto') {
                return true;
            }
        }
        return false;
    }

    /**
     * @param array<string, string> $columns
     * @param array<string, true> $existing
     * @return array<int, string>
     */
    private function getCompareColumns(array $columns, array $existing): array
    {
        $result = [];
        foreach ($columns as $col => $expr) {
            if (strtolower($col) === 'changed') {
                continue;
            }
            if (strtolower((string)$expr) === 'auto') {
                continue;
            }
            if ($this->isDefaultExpr((string)$expr)) {
                continue;
            }
            if (str_starts_with((string)$expr, 'calc:')) {
                continue;
            }
            if (!isset($existing[strtolower($col)])) {
                continue;
            }
            $result[] = $col;
        }
        return $result;
    }

    private function diffValues(array $existing, array $values, array $compareCols): bool
    {
        foreach ($compareCols as $col) {
            $old = $existing[$col] ?? null;
            $new = $values[$col] ?? null;
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
        if (is_bool($value)) {
            return $value ? '1' : '0';
        }
        $str = is_string($value) ? trim($value) : (string)$value;
        // normalize numeric strings to avoid 1 vs 1.0 vs 1.00 diffs
        if ($str !== '' && is_numeric($str)) {
            if (str_contains($str, '.') || str_contains($str, ',')) {
                $str = str_replace(',', '.', $str);
                $num = (float)$str;
                $str = rtrim(rtrim(sprintf('%.10F', $num), '0'), '.');
                if ($str === '-0') {
                    $str = '0';
                }
                return $str;
            }
            return (string)((int)$str);
        }
        return $str;
    }

    private function insertRow(PDO $pdo, string $table, array $values, array $existingCols, array $pkCols, array $autoCols): void
    {
        $cols = [];
        $params = [];
        foreach ($values as $col => $val) {
            if (!isset($existingCols[strtolower($col)])) {
                continue;
            }
            if (isset($autoCols[$col])) {
                continue;
            }
            if (in_array($col, $pkCols, true) && ($val === null || $val === '' || $val === 'auto')) {
                continue;
            }
            $cols[] = $col;
            $params[':' . $col] = $val;
        }
        $cols[] = 'changed';
        $params[':changed'] = 1;

        $sql = 'INSERT INTO ' . $this->quoteIdentifier($table) .
            ' (' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) . ')'
            . ' VALUES (' . implode(',', array_keys($params)) . ')';
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
    }

    private function updateRow(PDO $pdo, string $table, array $values, array $existingCols, array $pkCols, array $pkValues, array $autoCols, array $columns): void
    {
        $sets = [];
        $params = [];
        foreach ($values as $col => $val) {
            if (!isset($existingCols[strtolower($col)])) {
                continue;
            }
            if (isset($autoCols[$col])) {
                continue;
            }
            if (isset($columns[$col]) && $this->isDefaultExpr((string)$columns[$col])) {
                continue;
            }
            if (in_array($col, $pkCols, true)) {
                continue;
            }
            $sets[] = $this->quoteIdentifier($col) . ' = :' . $col;
            $params[':' . $col] = $val;
        }
        $sets[] = 'changed = 1';

        $where = [];
        foreach ($pkValues as $col => $val) {
            $where[] = $this->quoteIdentifier($col) . ' = :pk_' . $col;
            $params[':pk_' . $col] = $val;
        }
        if ($where === []) {
            return;
        }
        $sql = 'UPDATE ' . $this->quoteIdentifier($table) . ' SET ' . implode(',', $sets) . ' WHERE ' . implode(' AND ', $where);
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
    }

    private function setChanged(PDO $pdo, string $table, array $pkCols, array $pkValues): void
    {
        if ($pkCols === [] || $pkValues === []) {
            return;
        }
        $where = [];
        $params = [];
        foreach ($pkValues as $col => $val) {
            $where[] = $this->quoteIdentifier($col) . ' = :' . $col;
            $params[':' . $col] = $val;
        }
        $sql = 'UPDATE ' . $this->quoteIdentifier($table) . ' SET changed = 1 WHERE ' . implode(' AND ', $where);
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
    }

    private function updateTargetMap(string $table, array $values, array $pkCols, PDO $pdo): void
    {
        if ($table === 'xt_categories') {
            if (!isset($values['external_id'])) {
                return;
            }
            $external = (string)$values['external_id'];
            if ($external === '') {
                return;
            }
            $id = null;
            if (isset($values['categories_id'])) {
                $id = $values['categories_id'];
            }
            if (($id === null || $id === '' || $id === 'auto')) {
                $stmt = $pdo->prepare('SELECT categories_id FROM xt_categories WHERE external_id = :ext LIMIT 1');
                $stmt->execute([':ext' => $external]);
                $id = $stmt->fetchColumn();
            }
            if ($id !== null && $id !== '') {
                $this->targetMaps['xt_categories'][(string)$external] = (int)$id;
            }
        }
        if ($table === 'xt_products') {
            if (!isset($values['external_id'])) {
                return;
            }
            $external = (string)$values['external_id'];
            if ($external === '') {
                return;
            }
            $id = null;
            if (isset($values['products_id'])) {
                $id = $values['products_id'];
            }
            if (($id === null || $id === '' || $id === 'auto')) {
                $stmt = $pdo->prepare('SELECT products_id FROM xt_products WHERE external_id = :ext LIMIT 1');
                $stmt->execute([':ext' => $external]);
                $id = $stmt->fetchColumn();
            }
            if ($id !== null && $id !== '') {
                $this->targetMaps['xt_products'][(string)$external] = (int)$id;
            }
        }
        if ($table === 'xt_media') {
            if (!isset($values['external_id'])) {
                return;
            }
            $external = (string)$values['external_id'];
            if ($external === '') {
                return;
            }
            $id = null;
            if (isset($values['id'])) {
                $id = $values['id'];
            }
            if (($id === null || $id === '' || $id === 'auto')) {
                $stmt = $pdo->prepare('SELECT id FROM xt_media WHERE external_id = :ext LIMIT 1');
                $stmt->execute([':ext' => $external]);
                $id = $stmt->fetchColumn();
            }
            if ($id !== null && $id !== '') {
                $this->targetMaps['xt_media'][(string)$external] = (int)$id;
            }
        }
    }

    private function loadTargetMapFromDb(PDO $pdo, string $table, string $keyCol, string $idCol): void
    {
        if (isset($this->targetMapsLoaded[$table])) {
            return;
        }
        $cols = $this->getExistingColumns($pdo, $table);
        if ($cols === [] || !isset($cols[strtolower($keyCol)]) || !isset($cols[strtolower($idCol)])) {
            $this->targetMapsLoaded[$table] = true;
            return;
        }
        $sql = 'SELECT ' . $this->quoteIdentifier($keyCol) . ' AS k, ' . $this->quoteIdentifier($idCol) . ' AS v'
            . ' FROM ' . $this->quoteIdentifier($table)
            . ' WHERE ' . $this->quoteIdentifier($keyCol) . ' IS NOT NULL AND ' . $this->quoteIdentifier($keyCol) . " != ''";
        $stmt = $pdo->query($sql);
        if ($stmt) {
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
            $map = [];
            foreach ($rows as $row) {
                $k = (string)($row['k'] ?? '');
                $v = $row['v'] ?? null;
                if ($k !== '' && $v !== null && $v !== '') {
                    $map[$k] = (int)$v;
                }
            }
            $this->targetMaps[$table] = $map;
        }
        $this->targetMapsLoaded[$table] = true;
    }

    private function lookupTargetId(string $table, string $keyCol, string $idCol, string $key): ?int
    {
        if ($this->pdo === null || $key === '') {
            return null;
        }
        try {
            $stmt = $this->pdo->prepare(
                'SELECT ' . $this->quoteIdentifier($idCol) . ' FROM ' . $this->quoteIdentifier($table) .
                ' WHERE ' . $this->quoteIdentifier($keyCol) . ' = :key LIMIT 1'
            );
            $stmt->execute([':key' => $key]);
            $id = $stmt->fetchColumn();
            if ($id !== false && $id !== null && $id !== '') {
                $this->targetMaps[$table][$key] = (int)$id;
                return (int)$id;
            }
        } catch (\Throwable $e) {
            // ignore
        }
        return null;
    }

    private function rebuildNestedSet(PDO $pdo): int
    {
        $stmt = $pdo->prepare('UPDATE xt_categories SET categories_left = :l, categories_right = :r WHERE categories_id = :id');
        $childrenStmt = $pdo->prepare('SELECT categories_id, sort_order FROM xt_categories WHERE parent_id = :pid ORDER BY sort_order ASC, categories_id ASC');
        $counter = 1;
        $updated = 0;

        $stack = [
            ['id' => 0, 'idx' => 0, 'started' => false, 'left' => 0, 'children' => null],
        ];

        while ($stack !== []) {
            $topIndex = count($stack) - 1;
            $node = &$stack[$topIndex];
            if (!$node['started']) {
                $node['left'] = $counter++;
                $node['started'] = true;
            }

            if ($node['children'] === null) {
                $childrenStmt->execute([':pid' => $node['id']]);
                $rows = $childrenStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
                $childIds = [];
                foreach ($rows as $row) {
                    $childIds[] = (int)($row['categories_id'] ?? 0);
                }
                $node['children'] = $childIds;
            }

            $children = $node['children'];
            if ($node['idx'] < count($children)) {
                $childId = $children[$node['idx']];
                $node['idx']++;
                $stack[] = ['id' => $childId, 'idx' => 0, 'started' => false, 'left' => 0, 'children' => null];
                unset($node);
                continue;
            }

            $right = $counter++;
            if ($node['id'] !== 0) {
                $stmt->execute([':l' => $node['left'], ':r' => $right, ':id' => $node['id']]);
                $updated++;
            }
            array_pop($stack);
            unset($node);
        }

        return $updated;
    }

    private function createTable(PDO $pdo, string $table, array $columns, mixed $primaryKey): void
    {
        $defs = [];
        foreach ($columns as $col => $expr) {
            if (!preg_match('/^[A-Za-z0-9_]+$/', $col)) {
                continue;
            }
            $defs[] = $this->quoteIdentifier($col) . ' TEXT';
        }
        $defs[] = 'changed INTEGER NOT NULL DEFAULT 0';
        $pk = $this->normalizePk($primaryKey);
        if (count($pk) === 1 && $pk[0] !== '') {
            $defs[] = 'PRIMARY KEY (' . $this->quoteIdentifier($pk[0]) . ')';
        }
        $pdo->exec('CREATE TABLE IF NOT EXISTS ' . $this->quoteIdentifier($table) . ' (' . implode(',', $defs) . ')');
    }

    private function ensureColumns(PDO $pdo, string $table, array $columns, array $existing): void
    {
        foreach ($columns as $col => $expr) {
            if (!preg_match('/^[A-Za-z0-9_]+$/', $col)) {
                continue;
            }
            if (isset($existing[strtolower($col)])) {
                continue;
            }
            $pdo->exec('ALTER TABLE ' . $this->quoteIdentifier($table) . ' ADD COLUMN ' . $this->quoteIdentifier($col) . ' TEXT');
        }
    }

    /**
     * @return array<string, true>
     */
    private function getExistingColumns(PDO $pdo, string $table): array
    {
        $stmt = $pdo->query('SELECT name FROM sqlite_master WHERE type = "table" AND name = ' . $pdo->quote($table));
        $exists = $stmt ? $stmt->fetchColumn() : false;
        if (!$exists) {
            return [];
        }
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $cols = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $cols[strtolower($name)] = true;
            }
        }
        return $cols;
    }

    private function warn(string $table, string $pkJson, string $mapping, string $reason): void
    {
        $key = $table . '|' . $mapping . '|' . $reason;
        if (isset($this->loggedMissing[$key])) {
            return;
        }
        $this->loggedMissing[$key] = true;
        $line = 'WARNING table=' . $table . ' pk=' . $pkJson . ' mapping=' . $mapping . ' reason=' . $reason;
        $this->logWarning($line);
    }

    private function pkJson(mixed $primaryKey, array $values): string
    {
        $pkCols = $this->normalizePk($primaryKey);
        $data = [];
        foreach ($pkCols as $col) {
            if (array_key_exists($col, $values)) {
                $data[$col] = $values[$col];
            }
        }
        return json_encode($data);
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    private function buildUpsertStatement(PDO $pdo, string $table, array $columns, array $existingCols, array $pkCols, array $compareCols): ?\PDOStatement
    {
        if ($pkCols === []) {
            return null;
        }
        $cols = [];
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) {
                continue;
            }
            if (strtolower((string)$expr) === 'auto') {
                continue;
            }
            $cols[] = $col;
        }
        $cols[] = 'changed';
        $params = array_map(static fn(string $c): string => ':' . $c, $cols);
        $insert = 'INSERT INTO ' . $this->quoteIdentifier($table) .
            ' (' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) . ')' .
            ' VALUES (' . implode(',', $params) . ')';

        $setParts = [];
        foreach ($columns as $col => $expr) {
            if (!in_array($col, $cols, true)) {
                continue;
            }
            if ($col === 'changed') {
                continue;
            }
            if ($this->isDefaultExpr((string)$expr)) {
                continue;
            }
            $setParts[] = $this->quoteIdentifier($col) . ' = excluded.' . $this->quoteIdentifier($col);
        }
        $setParts[] = 'changed = 1';

        $whereParts = [];
        foreach ($compareCols as $col) {
            $whereParts[] = 'COALESCE(excluded.' . $this->quoteIdentifier($col) . ', \'\') <> COALESCE(' . $this->quoteIdentifier($col) . ', \'\')';
        }
        $where = $whereParts !== [] ? (' WHERE ' . implode(' OR ', $whereParts)) : '';

        $sql = $insert . ' ON CONFLICT(' . implode(',', array_map([$this, 'quoteIdentifier'], $pkCols)) . ') DO UPDATE SET ' .
            implode(', ', $setParts) . $where;
        return $pdo->prepare($sql);
    }

    /**
     * @return array<int, string>
     */
    private function getUpsertColumns(string $table, array $columns, array $existingCols): array
    {
        $cols = [];
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) continue;
            if (strtolower((string)$expr) === 'auto') continue;
            $cols[] = $col;
        }
        $cols[] = 'changed';
        return $cols;
    }

    /**
     * @return array<int, string>
     */
    private function getUpdateColumns(string $table, array $columns, array $existingCols, array $pkCols): array
    {
        $cols = [];
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) continue;
            if (strtolower((string)$expr) === 'auto') continue;
            if ($this->isDefaultExpr((string)$expr)) continue;
            if (in_array($col, $pkCols, true)) continue;
            $cols[] = $col;
        }
        $cols[] = 'changed';
        return $cols;
    }

    private function isDefaultExpr(string $expr): bool
    {
        $expr = trim($expr);
        return str_starts_with($expr, 'default:');
    }

    private function ensurePkIndex(PDO $pdo, string $table, array $pkCols): void
    {
        $idx = 'idx_' . $table . '_pk';
        $desired = array_map('strtolower', $pkCols);
        $existing = null;

        $list = $pdo->query('PRAGMA index_list(' . $this->quoteIdentifier($table) . ')');
        if ($list) {
            $rows = $list->fetchAll(PDO::FETCH_ASSOC) ?: [];
            foreach ($rows as $row) {
                if (($row['name'] ?? '') === $idx) {
                    $info = $pdo->query('PRAGMA index_info(' . $this->quoteIdentifier($idx) . ')');
                    if ($info) {
                        $cols = $info->fetchAll(PDO::FETCH_ASSOC) ?: [];
                        $existing = [];
                        foreach ($cols as $c) {
                            $existing[] = strtolower((string)($c['name'] ?? ''));
                        }
                    }
                    break;
                }
            }
        }

        if ($existing !== null && $existing !== $desired) {
            $pdo->exec('DROP INDEX IF EXISTS ' . $this->quoteIdentifier($idx));
            $existing = null;
        }

        if ($existing === null) {
            $cols = implode(',', array_map([$this, 'quoteIdentifier'], $pkCols));
            $pdo->exec('CREATE UNIQUE INDEX IF NOT EXISTS ' . $idx . ' ON ' . $this->quoteIdentifier($table) . '(' . $cols . ')');
        }
    }

    private function buildSelectByPk(PDO $pdo, string $table, array $pkCols, array $compareCols): \PDOStatement
    {
        $cols = array_values(array_unique(array_merge($pkCols, $compareCols)));
        $select = 'SELECT ' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) .
            ' FROM ' . $this->quoteIdentifier($table) .
            ' WHERE ' . implode(' AND ', array_map(fn(string $c): string => $this->quoteIdentifier($c) . ' = :' . $c, $pkCols)) .
            ' LIMIT 1';
        return $pdo->prepare($select);
    }

    private function buildSelectByExternal(PDO $pdo, string $table, array $compareCols, array $existingCols): ?\PDOStatement
    {
        if (!isset($existingCols['external_id'])) {
            return null;
        }
        $cols = array_values(array_unique(array_merge(['external_id'], $compareCols)));
        $select = 'SELECT ' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) .
            ' FROM ' . $this->quoteIdentifier($table) . ' WHERE external_id = :external_id LIMIT 1';
        return $pdo->prepare($select);
    }

    private function buildUpdateStatement(PDO $pdo, string $table, array $columns, array $existingCols, array $pkCols): ?\PDOStatement
    {
        if ($pkCols === []) {
            return null;
        }
        $sets = [];
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) continue;
            if (strtolower((string)$expr) === 'auto') continue;
            if (in_array($col, $pkCols, true)) continue;
            $sets[] = $this->quoteIdentifier($col) . ' = :' . $col;
        }
        $sets[] = 'changed = 1';
        $where = implode(' AND ', array_map(fn(string $c): string => $this->quoteIdentifier($c) . ' = :pk_' . $c, $pkCols));
        if ($sets === ['changed = 1']) {
            return null;
        }
        $sql = 'UPDATE ' . $this->quoteIdentifier($table) . ' SET ' . implode(', ', $sets) . ' WHERE ' . $where;
        return $pdo->prepare($sql);
    }

    private function bindValues(\PDOStatement $stmt, array $values, array $columns, array $existingCols, array $pkCols, array $autoCols, array $allowedCols = []): void
    {
        $allowed = $allowedCols !== [] ? array_flip($allowedCols) : null;
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) continue;
            if (isset($autoCols[$col])) continue;
            if (in_array($col, $pkCols, true) && ($values[$col] ?? null) === null) continue;
            if ($allowed !== null && !isset($allowed[$col])) continue;
            $stmt->bindValue(':' . $col, $values[$col] ?? null);
        }
        if (str_contains($stmt->queryString, ':changed')) {
            $stmt->bindValue(':changed', 1, PDO::PARAM_INT);
        }
    }

    private function bindPk(\PDOStatement $stmt, array $pkCols, array $pkValues): void
    {
        $sql = $stmt->queryString ?? '';
        foreach ($pkCols as $pk) {
            $val = $pkValues[$pk] ?? null;
            if (str_contains($sql, ':pk_' . $pk)) {
                $stmt->bindValue(':pk_' . $pk, $val);
            }
            if (str_contains($sql, ':' . $pk)) {
                $stmt->bindValue(':' . $pk, $val);
            }
        }
    }

    private function logWarning(string $line): void
    {
        $path = $this->getLogsDir() . '/xt_mapping_warnings.log';
        @file_put_contents($path, $line . PHP_EOL, FILE_APPEND);
    }

    private function getLogsDir(): string
    {
        return __DIR__ . '/../../../logs';
    }
}
