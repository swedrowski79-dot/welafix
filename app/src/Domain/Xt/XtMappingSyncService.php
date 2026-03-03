<?php
declare(strict_types=1);

namespace Welafix\Domain\Xt;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Database\Db;

final class XtMappingSyncService
{
    /** @var array<string, array<string, int>> */
    private array $targetMaps = [];
    /** @var array<string, array<string, bool>> */
    private array $loggedMissing = [];

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

        $pdo = Db::guardSqlite(Db::sqlite(), __METHOD__);

        $stats = [
            'ok' => true,
            'targets' => [],
            'nested_set_rebuild' => 0,
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
            }

            $base = $this->resolveBaseSource($columns);
            if ($base === '') {
                $stats['targets'][$table] = ['skipped' => true, 'reason' => 'no_base_source'];
                continue;
            }

            $sourceRows = $this->fetchSourceRows($pdo, $base);
            $result = $this->applyTarget($pdo, $table, $columns, $primaryKey, $existingCols, $base, $sourceRows);

            $stats['targets'][$table] = $result;

            if ($table === 'xt_categories' && !empty($result['needs_nested_set'])) {
                $rebuilt = $this->rebuildNestedSet($pdo);
                $stats['nested_set_rebuild'] = $rebuilt;
            }
        }

        return $stats;
    }

    /**
     * @param array<string, string> $columns
     * @param array<string, true> $existingCols
     * @param array<int, array<string, mixed>> $sourceRows
     * @return array<string, mixed>
     */
    private function applyTarget(PDO $pdo, string $table, array $columns, mixed $primaryKey, array $existingCols, string $base, iterable $sourceRows): array
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
            if ($hasPk && $upsert) {
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
                    $this->updateRow($pdo, $table, $values, $existingCols, $pkCols, $lookupPk, $autoCols);
                }
                $updated++;
                $this->setChanged($pdo, $table, $pkCols, $lookupPk);
                if ($table === 'xt_categories') {
                    $needsNestedSet = true;
                }
                $this->updateTargetMap($table, $values, $pkCols, $pdo);
            } else {
                $unchanged++;
            }
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
        return (string)$value;
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

    private function updateRow(PDO $pdo, string $table, array $values, array $existingCols, array $pkCols, array $pkValues, array $autoCols): void
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
        foreach ($cols as $col) {
            if ($col === 'changed') continue;
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
            if (in_array($col, $pkCols, true)) continue;
            $cols[] = $col;
        }
        $cols[] = 'changed';
        return $cols;
    }

    private function ensurePkIndex(PDO $pdo, string $table, array $pkCols): void
    {
        $idx = 'idx_' . $table . '_pk';
        $cols = implode(',', array_map([$this, 'quoteIdentifier'], $pkCols));
        $pdo->exec('CREATE UNIQUE INDEX IF NOT EXISTS ' . $idx . ' ON ' . $this->quoteIdentifier($table) . '(' . $cols . ')');
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
        $path = __DIR__ . '/../../../logs/xt_mapping_warnings.log';
        @file_put_contents($path, $line . PHP_EOL, FILE_APPEND);
    }
}
