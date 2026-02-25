<?php
declare(strict_types=1);

namespace Welafix\Config;

use RuntimeException;

final class MappingService
{
    private MappingLoader $loader;
    /** @var array<string, array<int, string>> */
    private array $allowedCache = [];

    public function __construct(?MappingLoader $loader = null)
    {
        $this->loader = $loader ?? new MappingLoader();
    }

    /**
     * @return array<int, string>
     */
    public function getAllowedColumns(string $name): array
    {
        if (!isset($this->allowedCache[$name])) {
            $this->allowedCache[$name] = $this->loader->getAllowedColumns($name);
        }
        return $this->allowedCache[$name];
    }

    /**
     * @param array<int, string> $columns
     */
    public function validateColumns(array $columns, string $context): void
    {
        $this->loader->validateColumns($columns, $context);
    }

    public function escapeMssqlIdentifier(string $column): string
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $column)) {
            throw new RuntimeException("Ungueltiger Spaltenname '{$column}'.");
        }
        return '[' . str_replace(']', ']]', $column) . ']';
    }

    /**
     * @param array<int, string> $allowed
     */
    public function buildMssqlSelectList(array $allowed, string $alias = ''): string
    {
        $this->validateColumns($allowed, 'select');
        $prefix = $alias !== '' ? $alias . '.' : '';
        return implode(', ', array_map(fn(string $col): string => $prefix . $this->escapeMssqlIdentifier($col), $allowed));
    }

    /**
     * @param array<string, mixed> $row
     * @param array<int, string> $allowed
     * @return array<string, mixed>
     */
    public function filterRow(array $row, array $allowed): array
    {
        $filtered = [];
        foreach ($allowed as $col) {
            if (array_key_exists($col, $row)) {
                $filtered[$col] = $row[$col];
            }
        }
        return $filtered;
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<int, string> $allowed
     * @return array<int, array<string, mixed>>
     */
    public function filterRows(array $rows, array $allowed): array
    {
        return array_map(fn(array $row): array => $this->filterRow($row, $allowed), $rows);
    }
}
