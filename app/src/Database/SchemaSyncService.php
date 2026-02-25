<?php
declare(strict_types=1);

namespace Welafix\Database;

use PDO;
use RuntimeException;

final class SchemaSyncService
{
    public function ensureSqliteColumnsMatchMssql(PDO $mssql, PDO $sqlite, string $mssqlTable, string $sqliteTable): void
    {
        $mssqlCols = $this->fetchMssqlColumns($mssql, $mssqlTable);
        if ($mssqlCols === []) {
            throw new RuntimeException('Keine MSSQL-Spalten gefunden fuer ' . $mssqlTable);
        }

        $sqliteCols = $this->fetchSqliteColumns($sqlite, $sqliteTable);
        $sqliteLookup = array_fill_keys($sqliteCols, true);

        $added = [];
        foreach ($mssqlCols as $col) {
            if (!isset($sqliteLookup[$col])) {
                $sqlite->exec(
                    'ALTER TABLE ' . $this->quoteIdentifier($sqliteTable) .
                    ' ADD COLUMN ' . $this->quoteIdentifier($col) . ' TEXT'
                );
                $added[] = $col;
            }
        }

        if ($added !== []) {
            $this->ensureChangeLog($sqlite);
            $stmt = $sqlite->prepare(
                'INSERT INTO app_schema_changes (table_name, column_name, added_at)
                 VALUES (:table_name, :column_name, :added_at)'
            );
            $now = gmdate(DATE_ATOM);
            foreach ($added as $col) {
                $stmt->execute([
                    ':table_name' => $sqliteTable,
                    ':column_name' => $col,
                    ':added_at' => $now,
                ]);
            }
        }
    }

    /**
     * @return array<int, string>
     */
    private function fetchMssqlColumns(PDO $mssql, string $mssqlTable): array
    {
        [$schema, $table] = $this->splitMssqlTable($mssqlTable);
        $stmt = $mssql->prepare(
            'SELECT COLUMN_NAME
             FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
             ORDER BY ORDINAL_POSITION'
        );
        $stmt->execute([
            ':schema' => $schema,
            ':table' => $table,
        ]);
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
        return array_values(array_filter(array_map('strval', $rows), static fn(string $v): bool => $v !== ''));
    }

    /**
     * @return array<int, string>
     */
    private function fetchSqliteColumns(PDO $sqlite, string $sqliteTable): array
    {
        $stmt = $sqlite->query('PRAGMA table_info(' . $this->quoteIdentifier($sqliteTable) . ')');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $cols = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $cols[] = $name;
            }
        }
        return $cols;
    }

    /**
     * @return array{0:string,1:string}
     */
    private function splitMssqlTable(string $mssqlTable): array
    {
        $parts = explode('.', $mssqlTable);
        if (count($parts) === 1) {
            return ['dbo', $parts[0]];
        }
        if (count($parts) >= 2) {
            return [$parts[0], $parts[1]];
        }
        return ['dbo', $mssqlTable];
    }

    private function ensureChangeLog(PDO $sqlite): void
    {
        $sqlite->exec(
            'CREATE TABLE IF NOT EXISTS app_schema_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                added_at TEXT NOT NULL
            )'
        );
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
