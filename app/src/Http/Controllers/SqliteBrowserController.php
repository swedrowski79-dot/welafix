<?php
declare(strict_types=1);

namespace Welafix\Http\Controllers;

use PDO;
use Welafix\Database\ConnectionFactory;

final class SqliteBrowserController
{
    public function __construct(private ConnectionFactory $factory) {}

    public function tables(): void
    {
        try {
            $pdo = $this->factory->sqlite();
            $stmt = $pdo->query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name");
            $tables = $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
            $this->jsonResponse($tables);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    public function table(): void
    {
        try {
            $pdo = $this->factory->sqlite();

            $name = isset($_GET['name']) ? trim((string)$_GET['name']) : '';
            if ($name === '') {
                $this->jsonResponse(['error' => 'Tabellenname fehlt.', 'sql' => null, 'params' => null], 400);
                return;
            }

            $allowedTables = $this->getAllowedTables($pdo);
            if (!isset($allowedTables[$name])) {
                $this->jsonResponse(['error' => 'Unbekannte Tabelle.', 'sql' => null, 'params' => null], 400);
                return;
            }

            $tableInfo = $this->getTableInfo($pdo, $name);
            if ($tableInfo === []) {
                $this->jsonResponse(['error' => 'Keine Spalten gefunden.', 'sql' => null, 'params' => null], 400);
                return;
            }
            $columns = array_values(array_map(
                static fn(array $row): string => (string)($row['name'] ?? ''),
                $tableInfo
            ));
            $columns = array_values(array_filter($columns, static fn(string $col): bool => $col !== ''));
            if ($columns === []) {
                $this->jsonResponse(['error' => 'Keine Spalten gefunden.', 'sql' => null, 'params' => null], 400);
                return;
            }

            $textColumns = $this->getTextColumns($tableInfo);

            $q = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
            $page = isset($_GET['page']) ? (int)$_GET['page'] : 1;
            $perPage = isset($_GET['per_page']) ? (int)$_GET['per_page'] : 50;
            $page = max(1, $page);
            $perPage = max(1, min(200, $perPage));
            $offset = ($page - 1) * $perPage;

            $search = $this->buildSearch($columns, $textColumns, $q);
            $whereSql = $search['where'];
            $params = $search['params'];

            $tableId = $this->quoteIdentifier($name);
            $columnList = array_map([$this, 'quoteIdentifier'], $columns);
            $selectSql = 'SELECT ' . implode(', ', $columnList) . " FROM {$tableId}";
            $countSql = "SELECT COUNT(*) FROM {$tableId}";
            if ($whereSql !== '') {
                $selectSql .= ' WHERE ' . $whereSql;
                $countSql .= ' WHERE ' . $whereSql;
            }
            $selectSql .= ' LIMIT :limit OFFSET :offset';

            $countStmt = $pdo->prepare($countSql);
            foreach ($params as $key => $value) {
                $countStmt->bindValue($key, $value, PDO::PARAM_STR);
            }
            $countStmt->execute();
            $totalRows = (int)$countStmt->fetchColumn();

            $stmt = $pdo->prepare($selectSql);
            foreach ($params as $key => $value) {
                $stmt->bindValue($key, $value, PDO::PARAM_STR);
            }
            $stmt->bindValue(':limit', $perPage, PDO::PARAM_INT);
            $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
            $stmt->execute();
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];

            $this->jsonResponse([
                'table' => $name,
                'columns' => $columns,
                'rows' => $rows,
                'page' => $page,
                'per_page' => $perPage,
                'totalRows' => $totalRows,
                'query' => $q,
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'error' => $e->getMessage(),
                'sql' => null,
                'params' => null,
            ], 500);
        }
    }

    /**
     * @return array<string, true>
     */
    private function getAllowedTables(PDO $pdo): array
    {
        $stmt = $pdo->query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
        $tables = $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
        $allowed = [];
        foreach ($tables as $table) {
            $allowed[(string)$table] = true;
        }
        return $allowed;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function getTableInfo(PDO $pdo, string $table): array
    {
        $tableId = $this->quoteIdentifier($table);
        $stmt = $pdo->query("PRAGMA table_info({$tableId})");
        return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
    }

    /**
     * @param array<int, string> $columns
     * @param array<int, string> $textColumns
     * @return array{where: string, params: array<string, string>}
     */
    private function buildSearch(array $columns, array $textColumns, string $query): array
    {
        if ($query === '') {
            return ['where' => '', 'params' => []];
        }

        $searchable = $textColumns;
        $useCastFallback = false;
        if ($searchable === []) {
            if (count($columns) <= 5) {
                $searchable = $columns;
                $useCastFallback = true;
            } else {
                return ['where' => '', 'params' => []];
            }
        }

        $parts = [];
        foreach ($searchable as $column) {
            $colId = $this->quoteIdentifier($column);
            if ($useCastFallback) {
                $parts[] = "CAST({$colId} AS TEXT) LIKE :q";
            } else {
                $parts[] = "{$colId} LIKE :q";
            }
        }

        return [
            'where' => implode(' OR ', $parts),
            'params' => [':q' => '%' . $query . '%'],
        ];
    }

    /**
     * @param array<int, array<string, mixed>> $tableInfo
     * @return array<int, string>
     */
    private function getTextColumns(array $tableInfo): array
    {
        $textColumns = [];
        foreach ($tableInfo as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name === '') {
                continue;
            }
            $type = strtoupper((string)($row['type'] ?? ''));
            if ($type === '') {
                continue;
            }
            if (str_contains($type, 'CHAR') || str_contains($type, 'CLOB') || str_contains($type, 'TEXT')) {
                $textColumns[] = $name;
            }
        }
        return $textColumns;
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    private function jsonResponse(array $payload, int $status = 200): void
    {
        http_response_code($status);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($payload, JSON_PRETTY_PRINT);
    }
}
