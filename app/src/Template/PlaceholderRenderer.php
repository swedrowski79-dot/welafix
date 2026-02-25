<?php
declare(strict_types=1);

namespace Welafix\Template;

final class PlaceholderRenderer
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, mixed> $rowIdentity
     * @return array{rendered:string, missing: array<int, string>}
     */
    public function render(string $template, array $row, string $tableName, array $rowIdentity): array
    {
        $missing = [];

        $rendered = preg_replace_callback('/\{\{\s*([A-Za-z0-9_]+)\s*\}\}/', function (array $matches) use ($row, &$missing): string {
            $key = $matches[1] ?? '';
            if ($key === '') {
                return '';
            }
            if (array_key_exists($key, $row) && $row[$key] !== null) {
                return (string)$row[$key];
            }
            $missing[$key] = true;
            return '';
        }, $template);

        return [
            'rendered' => $rendered ?? '',
            'missing' => array_keys($missing),
        ];
    }
}
