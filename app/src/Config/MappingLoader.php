<?php
declare(strict_types=1);

namespace Welafix\Config;

use RuntimeException;

final class MappingLoader
{
    private string $directory;

    public function __construct(?string $directory = null)
    {
        $this->directory = $directory ?? (__DIR__ . '/mappings');
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    public function loadAll(): array
    {
        if (!is_dir($this->directory)) {
            throw new RuntimeException("Mapping-Verzeichnis nicht gefunden: {$this->directory}");
        }

        $files = glob($this->directory . '/*.php');
        if ($files === false) {
            throw new RuntimeException("Mapping-Verzeichnis konnte nicht gelesen werden: {$this->directory}");
        }

        sort($files);

        $mappings = [];
        foreach ($files as $file) {
            $name = basename($file, '.php');

            $mapping = require $file;
            if (!is_array($mapping)) {
                throw new RuntimeException("Mapping-Datei '{$file}' muss ein Array zurueckgeben.");
            }

            $mappings[$name] = $this->validateMapping($mapping, $name, $file);
        }

        return $mappings;
    }

    /**
     * @return array<int, string>
     */
    public function getAllowedColumns(string $name): array
    {
        $mappings = $this->loadAll();
        if (!isset($mappings[$name])) {
            throw new RuntimeException("Mapping '{$name}' nicht gefunden.");
        }
        $select = $mappings[$name]['select'] ?? [];
        if (!is_array($select)) {
            return [];
        }
        $allowed = array_values(array_filter($select, static fn($v): bool => is_string($v) && $v !== ''));
        $this->validateColumns($allowed, $name);
        return $allowed;
    }

    /**
     * @param array<int, string> $columns
     */
    public function validateColumns(array $columns, string $context): void
    {
        foreach ($columns as $column) {
            if (!preg_match('/^[A-Za-z0-9_]+$/', $column)) {
                throw new RuntimeException("Mapping '{$context}': Ungueltiger Spaltenname '{$column}'.");
            }
        }
    }

    /**
     * @param array<int, string> $allowed
     */
    public function buildSelectList(array $allowed, string $alias = ''): string
    {
        $this->validateColumns($allowed, 'select');
        $prefix = $alias !== '' ? $alias . '.' : '';
        return implode(', ', array_map(static fn(string $col): string => $prefix . $col, $allowed));
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

    /**
     * @param array<string, mixed> $mapping
     * @return array<string, mixed>
     */
    private function validateMapping(array $mapping, string $name, string $file): array
    {
        if (!isset($mapping['source']) || !is_array($mapping['source'])) {
            throw new RuntimeException("Mapping '{$name}': Feld 'source' fehlt oder ist kein Array ({$file}).");
        }

        $source = $mapping['source'];

        if (!isset($source['table']) || !is_string($source['table']) || $source['table'] === '') {
            throw new RuntimeException("Mapping '{$name}': Pflichtfeld 'source.table' fehlt oder ist kein String ({$file}).");
        }

        if (!isset($source['key']) || !is_string($source['key']) || $source['key'] === '') {
            throw new RuntimeException("Mapping '{$name}': Pflichtfeld 'source.key' fehlt oder ist kein String ({$file}).");
        }

        if (!isset($mapping['select']) || !is_array($mapping['select']) || $mapping['select'] === []) {
            throw new RuntimeException("Mapping '{$name}': Pflichtfeld 'select' fehlt oder ist leer ({$file}).");
        }

        if (!isset($source['where'])) {
            $source['where'] = '1=1';
        } elseif (!is_string($source['where']) || $source['where'] === '') {
            throw new RuntimeException("Mapping '{$name}': Feld 'source.where' muss ein String sein ({$file}).");
        }

        $mapping['source'] = $source;

        return $mapping;
    }
}
