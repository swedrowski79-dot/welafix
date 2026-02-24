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
    public function findById(string $afsWgId): ?array
    {
        $stmt = $this->pdo->prepare('SELECT * FROM warengruppe WHERE afs_wg_id = :id LIMIT 1');
        $stmt->execute([':id' => $afsWgId]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return $row ?: null;
    }

    /**
     * @return array{inserted:bool, updated:bool, unchanged:bool}
     */
    public function upsert(string $afsWgId, string $name, ?string $parentId, string $seenAtIso): array
    {
        $existing = $this->findById($afsWgId);
        $parentIdNormalized = $this->normalizeParentId($parentId);

        if ($existing === null) {
            $stmt = $this->pdo->prepare(
                'INSERT INTO warengruppe (afs_wg_id, name, parent_id, last_seen_at, changed, change_reason)
                 VALUES (:id, :name, :parent_id, :last_seen_at, :changed, :change_reason)'
            );
            $stmt->execute([
                ':id' => $afsWgId,
                ':name' => $name,
                ':parent_id' => $parentIdNormalized,
                ':last_seen_at' => $seenAtIso,
                ':changed' => 1,
                ':change_reason' => 'new',
            ]);
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
        $stmt->execute([
            ':id' => $afsWgId,
            ':name' => $name,
            ':parent_id' => $parentIdNormalized,
            ':last_seen_at' => $seenAtIso,
            ':changed' => $changed,
            ':change_reason' => $changeReason !== '' ? $changeReason : null,
        ]);

        if ($reasons !== []) {
            return ['inserted' => false, 'updated' => true, 'unchanged' => false];
        }

        return ['inserted' => false, 'updated' => false, 'unchanged' => true];
    }

    public function updatePath(string $afsWgId, string $path, string $pathIds): bool
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
        $stmt->execute([
            ':id' => $afsWgId,
            ':path' => $path,
            ':path_ids' => $pathIds,
            ':change_reason' => $changeReason !== '' ? $changeReason : null,
        ]);
        return true;
    }

    private function normalizeParentId(?string $value): ?string
    {
        if ($value === null) {
            return null;
        }
        $trimmed = trim($value);
        if ($trimmed === '' || $trimmed === '0') {
            return null;
        }
        return $trimmed;
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
