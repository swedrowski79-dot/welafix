<?php
declare(strict_types=1);

namespace Welafix\Domain\ChangeTracking;

use PDO;

final class ChangeTracker
{
    /**
     * @param array<string, mixed>|null $existing
     * @param array<string, mixed> $newValues
     * @param array<int, string> $fields
     * @return array<string, array{old:mixed,new:mixed}>
     */
    public function buildDiff(?array $existing, array $newValues, array $fields): array
    {
        if ($fields === []) {
            return [];
        }
        $diff = [];
        foreach ($fields as $field) {
            $old = $existing[$field] ?? null;
            $new = $newValues[$field] ?? null;
            if ((string)($old ?? '') !== (string)($new ?? '')) {
                $diff[$field] = ['old' => $old, 'new' => $new];
            }
        }
        return $diff;
    }

    /**
     * @param array<string, array{old:mixed,new:mixed}> $diff
     */
    public function encodeDiff(array $diff): string
    {
        $json = json_encode($diff, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($json === false) {
            return '{}';
        }
        return $json;
    }

    /**
     * @param array<string, array{old:mixed,new:mixed}> $diff
     */
    public function writeHistory(PDO $pdo, string $entityType, string $entityKey, string $changedAt, array $diff, ?string $source): void
    {
        if ($diff === []) {
            return;
        }
        $stmt = $pdo->prepare(
            'INSERT INTO change_history (entity_type, entity_key, changed_at, diff_json, source)
             VALUES (:entity_type, :entity_key, :changed_at, :diff_json, :source)'
        );
        $stmt->bindValue(':entity_type', $entityType, PDO::PARAM_STR);
        $stmt->bindValue(':entity_key', $entityKey, PDO::PARAM_STR);
        $stmt->bindValue(':changed_at', $changedAt, PDO::PARAM_STR);
        $stmt->bindValue(':diff_json', $this->encodeDiff($diff), PDO::PARAM_STR);
        if ($source === null || $source === '') {
            $stmt->bindValue(':source', null, PDO::PARAM_NULL);
        } else {
            $stmt->bindValue(':source', $source, PDO::PARAM_STR);
        }
        $stmt->execute();
    }
}
