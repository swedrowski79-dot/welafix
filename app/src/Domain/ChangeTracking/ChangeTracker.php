<?php
declare(strict_types=1);

namespace Welafix\Domain\ChangeTracking;

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

}
