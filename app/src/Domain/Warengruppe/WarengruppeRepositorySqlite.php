<?php
declare(strict_types=1);

namespace Welafix\Domain\Warengruppe;

use Welafix\Domain\ChangeTracking\ChangeTracker;
use PDO;

final class WarengruppeRepositorySqlite
{
    private PDO $pdo;
    /** @var array<int, string>|null */
    private ?array $columnCache = null;
    private ChangeTracker $tracker;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
        $this->tracker = new ChangeTracker();
    }

    /**
     * @return array<string, mixed>|null
     */
    public function findById(int $afsWgId): ?array
    {
        $columns = $this->getColumns();
        $selectList = implode(', ', array_map([$this, 'quoteIdentifier'], $columns));
        $stmt = $this->pdo->prepare('SELECT ' . $selectList . ' FROM warengruppe WHERE afs_wg_id = :id LIMIT 1');
        $stmt->bindValue(':id', $afsWgId, PDO::PARAM_INT);
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return $row ?: null;
    }

    /**
     * @return array{inserted:bool, updated:bool, unchanged:bool}
     */
    /**
     * @param array<string, mixed> $extraFields
     */
    public function upsert(int $afsWgId, string $name, ?int $parentId, array $extraFields, string $seenAtIso, array $diffColumns = []): array
    {
        $existing = $this->findById($afsWgId);
        $parentIdNormalized = $this->normalizeParentId($parentId);
        $extraKeys = array_keys($extraFields);
        $diff = $this->tracker->buildDiff($existing, $extraFields, $diffColumns);

        if ($existing === null) {
            $columns = ['afs_wg_id', 'name', 'parent_id'];
            $params = [':id', ':name', ':parent_id'];
            foreach ($extraKeys as $key) {
                $columns[] = $this->quoteIdentifier($key);
                $params[] = ':' . $key;
            }
            $columns[] = 'last_seen_at';
            $columns[] = 'last_synced_at';
            $columns[] = 'changed';
            $columns[] = 'changed_fields';
            $columns[] = 'change_reason';
            $params[] = ':last_seen_at';
            $params[] = ':last_synced_at';
            $params[] = ':changed';
            $params[] = ':changed_fields';
            $params[] = ':change_reason';

            $stmt = $this->pdo->prepare(
                'INSERT INTO warengruppe (' . implode(',', $columns) . ')
                 VALUES (' . implode(',', $params) . ')'
            );
            $stmt->bindValue(':id', $afsWgId, PDO::PARAM_INT);
            $stmt->bindValue(':name', $name, PDO::PARAM_STR);
            if ($parentIdNormalized === null) {
                $stmt->bindValue(':parent_id', null, PDO::PARAM_NULL);
            } else {
                $stmt->bindValue(':parent_id', $parentIdNormalized, PDO::PARAM_INT);
            }
            foreach ($extraKeys as $key) {
                $this->bindNullableText($stmt, ':' . $key, $extraFields[$key] ?? null);
            }
            $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':last_synced_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':changed', 1, PDO::PARAM_INT);
            $stmt->bindValue(':changed_fields', $this->tracker->encodeDiff($diff), PDO::PARAM_STR);
            $stmt->bindValue(':change_reason', 'new', PDO::PARAM_STR);
            $stmt->execute();
            return ['inserted' => true, 'updated' => false, 'unchanged' => false, 'diff' => $diff];
        }

        $reasons = [];
        if ((string)$existing['name'] !== $name) {
            $reasons[] = 'name';
        }

        $existingParent = $this->normalizeParentId($existing['parent_id'] ?? null);
        if ($existingParent !== $parentIdNormalized) {
            $reasons[] = 'parent';
        }

        if ($this->extrasChanged($existing, $extraFields, $extraKeys)) {
            $reasons[] = 'extras';
        }

        $changeReason = '';
        if ($diff !== [] && $reasons !== []) {
            $changeReason = $this->mergeReasons((string)($existing['change_reason'] ?? ''), $reasons);
        }

        $setParts = [
            'name = :name',
            'parent_id = :parent_id',
        ];
        foreach ($extraKeys as $key) {
            $setParts[] = $this->quoteIdentifier($key) . ' = :' . $key;
        }
        $setParts[] = 'last_seen_at = :last_seen_at';
        $setParts[] = 'last_synced_at = :last_synced_at';
        if ($diff !== []) {
            $setParts[] = 'changed = 1';
            $setParts[] = 'changed_fields = :changed_fields';
        }
        $setParts[] = 'change_reason = :change_reason';

        $stmt = $this->pdo->prepare(
            'UPDATE warengruppe
             SET ' . implode(', ', $setParts) . '
             WHERE afs_wg_id = :id'
        );
        $stmt->bindValue(':id', $afsWgId, PDO::PARAM_INT);
        $stmt->bindValue(':name', $name, PDO::PARAM_STR);
        if ($parentIdNormalized === null) {
            $stmt->bindValue(':parent_id', null, PDO::PARAM_NULL);
        } else {
            $stmt->bindValue(':parent_id', $parentIdNormalized, PDO::PARAM_INT);
        }
        foreach ($extraKeys as $key) {
            $this->bindNullableText($stmt, ':' . $key, $extraFields[$key] ?? null);
        }
        $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
        $stmt->bindValue(':last_synced_at', $seenAtIso, PDO::PARAM_STR);
        if ($diff !== []) {
            $stmt->bindValue(':changed_fields', $this->tracker->encodeDiff($diff), PDO::PARAM_STR);
        }
        if ($changeReason !== '') {
            $stmt->bindValue(':change_reason', $changeReason, PDO::PARAM_STR);
        } else {
            $stmt->bindValue(':change_reason', null, PDO::PARAM_NULL);
        }
        $stmt->execute();

        if ($reasons !== []) {
            return ['inserted' => false, 'updated' => true, 'unchanged' => false, 'diff' => $diff];
        }

        return ['inserted' => false, 'updated' => false, 'unchanged' => true, 'diff' => $diff];
    }

    private function normalizeParentId(int|string|null $value): ?int
    {
        if ($value === null) {
            return null;
        }
        $trimmed = trim((string)$value);
        if ($trimmed === '' || $trimmed === '0') {
            return null;
        }
        return (int)$trimmed;
    }

    private function bindNullableText(\PDOStatement $stmt, string $param, ?string $value): void
    {
        if ($value === null || $value === '') {
            $stmt->bindValue($param, null, PDO::PARAM_NULL);
            return;
        }
        $stmt->bindValue($param, $value, PDO::PARAM_STR);
    }

    /**
     * @param array<string, mixed> $existing
     * @param array<string, mixed> $extraFields
     * @param array<int, string> $extraKeys
     */
    private function extrasChanged(array $existing, array $extraFields, array $extraKeys): bool
    {
        foreach ($extraKeys as $key) {
            $existingValue = (string)($existing[$key] ?? '');
            $newValue = (string)($extraFields[$key] ?? '');
            if ($existingValue !== $newValue) {
                return true;
            }
        }
        return false;
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    /**
     * @return array<int, string>
     */
    private function getColumns(): array
    {
        if ($this->columnCache !== null) {
            return $this->columnCache;
        }
        $stmt = $this->pdo->query('PRAGMA table_info(warengruppe)');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $cols = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $cols[] = $name;
            }
        }
        if ($cols === []) {
            $cols = ['afs_wg_id', 'name', 'parent_id', 'path', 'path_ids', 'last_seen_at', 'changed', 'change_reason'];
        }
        $this->columnCache = $cols;
        return $cols;
    }

    /**
     * @param string[] $reasons
     */
    private function mergeReasons(string $existing, array $reasons): string
    {
        $items = [];
        if ($existing !== '') {
            foreach (explode(',', $existing) as $item) {
                $item = trim($item);
                if ($item !== '') {
                    $items[$item] = true;
                }
            }
        }
        foreach ($reasons as $reason) {
            $items[$reason] = true;
        }
        return implode(',', array_keys($items));
    }

    public function encodeDiff(array $diff): string
    {
        return $this->tracker->encodeDiff($diff);
    }
}
