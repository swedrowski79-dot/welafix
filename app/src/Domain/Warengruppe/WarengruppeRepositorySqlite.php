<?php
declare(strict_types=1);

namespace Welafix\Domain\Warengruppe;

use PDO;

final class WarengruppeRepositorySqlite
{
    private PDO $pdo;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @return array<string, mixed>|null
     */
    public function findById(int $afsWgId): ?array
    {
        $stmt = $this->pdo->prepare('SELECT * FROM warengruppe WHERE afs_wg_id = :id LIMIT 1');
        $stmt->bindValue(':id', $afsWgId, PDO::PARAM_INT);
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return $row ?: null;
    }

    /**
     * @return array{inserted:bool, updated:bool, unchanged:bool}
     */
    public function upsert(int $afsWgId, string $name, ?int $parentId, string $seenAtIso): array
    {
        $existing = $this->findById($afsWgId);
        $parentIdNormalized = $this->normalizeParentId($parentId);

        if ($existing === null) {
            $stmt = $this->pdo->prepare(
                'INSERT INTO warengruppe (afs_wg_id, name, parent_id, last_seen_at, changed, change_reason)
                 VALUES (:id, :name, :parent_id, :last_seen_at, :changed, :change_reason)'
            );
            $stmt->bindValue(':id', $afsWgId, PDO::PARAM_INT);
            $stmt->bindValue(':name', $name, PDO::PARAM_STR);
            if ($parentIdNormalized === null) {
                $stmt->bindValue(':parent_id', null, PDO::PARAM_NULL);
            } else {
                $stmt->bindValue(':parent_id', $parentIdNormalized, PDO::PARAM_INT);
            }
            $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':changed', 1, PDO::PARAM_INT);
            $stmt->bindValue(':change_reason', 'new', PDO::PARAM_STR);
            $stmt->execute();
            return ['inserted' => true, 'updated' => false, 'unchanged' => false];
        }

        $reasons = [];
        if ((string)$existing['name'] !== $name) {
            $reasons[] = 'name';
        }

        $existingParent = $this->normalizeParentId($existing['parent_id'] ?? null);
        if ($existingParent !== $parentIdNormalized) {
            $reasons[] = 'parent';
        }

        $changed = (int)$existing['changed'];
        $changeReason = (string)($existing['change_reason'] ?? '');

        if ($reasons !== []) {
            $changed = 1;
            $changeReason = $this->mergeReasons($changeReason, $reasons);
        }

        $stmt = $this->pdo->prepare(
            'UPDATE warengruppe
             SET name = :name,
                 parent_id = :parent_id,
                 last_seen_at = :last_seen_at,
                 changed = :changed,
                 change_reason = :change_reason
             WHERE afs_wg_id = :id'
        );
        $stmt->bindValue(':id', $afsWgId, PDO::PARAM_INT);
        $stmt->bindValue(':name', $name, PDO::PARAM_STR);
        if ($parentIdNormalized === null) {
            $stmt->bindValue(':parent_id', null, PDO::PARAM_NULL);
        } else {
            $stmt->bindValue(':parent_id', $parentIdNormalized, PDO::PARAM_INT);
        }
        $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
        $stmt->bindValue(':changed', $changed, PDO::PARAM_INT);
        if ($changeReason !== '') {
            $stmt->bindValue(':change_reason', $changeReason, PDO::PARAM_STR);
        } else {
            $stmt->bindValue(':change_reason', null, PDO::PARAM_NULL);
        }
        $stmt->execute();

        if ($reasons !== []) {
            return ['inserted' => false, 'updated' => true, 'unchanged' => false];
        }

        return ['inserted' => false, 'updated' => false, 'unchanged' => true];
    }

    public function updatePath(int $afsWgId, string $path, string $pathIds): bool
    {
        $existing = $this->findById($afsWgId);
        if ($existing === null) {
            return false;
        }

        $pathChanged = (string)($existing['path'] ?? '') !== $path;
        $pathIdsChanged = (string)($existing['path_ids'] ?? '') !== $pathIds;

        if (!$pathChanged && !$pathIdsChanged) {
            return false;
        }

        $reasons = [];
        if ($pathChanged || $pathIdsChanged) {
            $reasons[] = 'path';
        }

        $changeReason = $this->mergeReasons((string)($existing['change_reason'] ?? ''), $reasons);

        $stmt = $this->pdo->prepare(
            'UPDATE warengruppe
             SET path = :path,
                 path_ids = :path_ids,
                 changed = 1,
                 change_reason = :change_reason
             WHERE afs_wg_id = :id'
        );
        $stmt->bindValue(':id', $afsWgId, PDO::PARAM_INT);
        $stmt->bindValue(':path', $path, PDO::PARAM_STR);
        $stmt->bindValue(':path_ids', $pathIds, PDO::PARAM_STR);
        if ($changeReason !== '') {
            $stmt->bindValue(':change_reason', $changeReason, PDO::PARAM_STR);
        } else {
            $stmt->bindValue(':change_reason', null, PDO::PARAM_NULL);
        }
        $stmt->execute();
        return true;
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
}
