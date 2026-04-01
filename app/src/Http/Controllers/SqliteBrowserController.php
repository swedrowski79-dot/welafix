<?php
declare(strict_types=1);

namespace Welafix\Http\Controllers;

use PDO;
use Welafix\Config\MappingService;
use Welafix\Database\ConnectionFactory;

final class SqliteBrowserController
{
    public function __construct(private ConnectionFactory $factory) {}

    public function tables(): void
    {
        try {
            $pdo = $this->factory->localDb();
            $tables = $this->listTables($pdo);
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
            $pdo = $this->factory->localDb();

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

            $showAll = isset($_GET['all']) && (string)$_GET['all'] === '1';
            if (!$showAll && ($name === 'artikel' || $name === 'warengruppe')) {
                $mapping = new MappingService();
                $allowed = array_values(array_unique(array_merge(
                    $mapping->getAllowedColumns($name === 'artikel' ? 'artikel' : 'warengruppe'),
                    ['seo_url']
                )));
                $allowedLookup = [];
                foreach ($allowed as $col) {
                    $allowedLookup[strtolower($col)] = true;
                }
                $columns = array_values(array_filter(
                    $columns,
                    static fn(string $col): bool => isset($allowedLookup[strtolower($col)])
                ));
                if ($columns === []) {
                    $this->jsonResponse(['error' => 'Keine freigegebenen Spalten gefunden.', 'sql' => null, 'params' => null], 400);
                    return;
                }
            }

            $textColumns = $this->getTextColumns($tableInfo);
            if ($columns !== []) {
                $colLookup = [];
                foreach ($columns as $col) {
                    $colLookup[strtolower($col)] = true;
                }
                $textColumns = array_values(array_filter(
                    $textColumns,
                    static fn(string $col): bool => isset($colLookup[strtolower($col)])
                ));
            }

            $q = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
            $sort = isset($_GET['sort']) ? trim((string)$_GET['sort']) : '';
            $dir = isset($_GET['dir']) ? strtolower((string)$_GET['dir']) : 'asc';
            if ($dir !== 'asc' && $dir !== 'desc') {
                $dir = 'asc';
            }
            $page = isset($_GET['page']) ? (int)$_GET['page'] : 1;
            $perPage = isset($_GET['per_page']) ? (int)$_GET['per_page'] : 50;
            $page = max(1, $page);
            $perPage = max(1, min(200, $perPage));
            $offset = ($page - 1) * $perPage;
            $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
            $primaryKey = $this->resolvePrimaryKeyColumn($tableInfo);

            $search = $this->buildSearch($columns, $textColumns, $q, $driver);
            $whereSql = $search['where'];
            $params = $search['params'];

            $tableId = $this->quoteIdentifier($name);
            $columnList = array_map([$this, 'quoteIdentifier'], $columns);
            $selectPrefix = $primaryKey !== null
                ? $this->quoteIdentifier($primaryKey) . ' AS "__rowid", '
                : '';
            $selectSql = 'SELECT ' . $selectPrefix . implode(', ', $columnList) . " FROM {$tableId}";
            $countSql = "SELECT COUNT(*) FROM {$tableId}";
            if ($whereSql !== '') {
                $selectSql .= ' WHERE ' . $whereSql;
                $countSql .= ' WHERE ' . $whereSql;
            }
            if ($sort !== '') {
                $lower = strtolower($sort);
                $allowedCols = array_change_key_case(array_flip($columns), CASE_LOWER);
                if (isset($allowedCols[$lower])) {
                    $selectSql .= ' ORDER BY ' . $this->quoteIdentifier($sort) . ' ' . strtoupper($dir);
                }
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
                'primary_key' => $primaryKey,
                'page' => $page,
                'per_page' => $perPage,
                'totalRows' => $totalRows,
                'query' => $q,
                'sort' => $sort,
                'dir' => $dir,
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'error' => $e->getMessage(),
                'sql' => null,
                'params' => null,
            ], 500);
        }
    }

    public function clearTable(): void
    {
        try {
            $pdo = $this->factory->localDb();

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

            $pdo->exec('DELETE FROM ' . $this->quoteIdentifier($name));

            $this->jsonResponse([
                'ok' => true,
                'table' => $name,
                'message' => 'Tabelle geleert.',
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'error' => $e->getMessage(),
                'sql' => null,
                'params' => null,
            ], 500);
        }
    }

    public function dropTable(): void
    {
        try {
            $pdo = $this->factory->localDb();

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

            $pdo->exec('DROP TABLE ' . $this->quoteIdentifier($name));

            $this->jsonResponse([
                'ok' => true,
                'table' => $name,
                'message' => 'Tabelle gelöscht.',
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'error' => $e->getMessage(),
                'sql' => null,
                'params' => null,
            ], 500);
        }
    }

    public function updateCell(): void
    {
        try {
            $pdo = $this->factory->localDb();

            $name = isset($_POST['name']) ? trim((string)$_POST['name']) : '';
            $column = isset($_POST['column']) ? trim((string)$_POST['column']) : '';
            $rowIdRaw = isset($_POST['rowid']) ? trim((string)$_POST['rowid']) : '';
            $value = $_POST['value'] ?? '';

            if ($name === '' || $column === '' || $rowIdRaw === '') {
                $this->jsonResponse(['error' => 'name, column oder rowid fehlt.', 'sql' => null, 'params' => null], 400);
                return;
            }

            $allowedTables = $this->getAllowedTables($pdo);
            if (!isset($allowedTables[$name])) {
                $this->jsonResponse(['error' => 'Unbekannte Tabelle.', 'sql' => null, 'params' => null], 400);
                return;
            }

            $tableInfo = $this->getTableInfo($pdo, $name);
            $allowedColumns = [];
            foreach ($tableInfo as $row) {
                $colName = (string)($row['name'] ?? '');
                if ($colName !== '') {
                    $allowedColumns[strtolower($colName)] = $colName;
                }
            }

            $columnKey = strtolower($column);
            if (!isset($allowedColumns[$columnKey])) {
                $this->jsonResponse(['error' => 'Unbekannte Spalte.', 'sql' => null, 'params' => null], 400);
                return;
            }

            $primaryKey = $this->resolvePrimaryKeyColumn($tableInfo);
            if ($primaryKey === null) {
                $this->jsonResponse(['error' => 'Tabelle hat keinen bearbeitbaren Primärschlüssel.', 'sql' => null, 'params' => null], 400);
                return;
            }
            if ($rowIdRaw === '') {
                $this->jsonResponse(['error' => 'Ungültige rowid.', 'sql' => null, 'params' => null], 400);
                return;
            }

            $actualColumn = $allowedColumns[$columnKey];
            $stmt = $pdo->prepare(
                'UPDATE ' . $this->quoteIdentifier($name) .
                ' SET ' . $this->quoteIdentifier($actualColumn) . ' = :value WHERE ' . $this->quoteIdentifier($primaryKey) . ' = :rowid'
            );
            $stmt->bindValue(':value', (string)$value, PDO::PARAM_STR);
            $stmt->bindValue(':rowid', $rowIdRaw, PDO::PARAM_STR);
            $stmt->execute();

            $this->jsonResponse([
                'ok' => true,
                'table' => $name,
                'column' => $actualColumn,
                'rowid' => $rowIdRaw,
                'primary_key' => $primaryKey,
                'value' => (string)$value,
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
        $allowed = [];
        foreach ($this->listTables($pdo) as $table) {
            $allowed[(string)$table] = true;
        }
        return $allowed;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function getTableInfo(PDO $pdo, string $table): array
    {
        $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $tableId = $this->quoteIdentifier($table);

        if ($driver === 'mysql') {
            $stmt = $pdo->query('DESCRIBE ' . $tableId);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
            return array_map(static function (array $row): array {
                return [
                    'name' => (string)($row['Field'] ?? ''),
                    'type' => (string)($row['Type'] ?? ''),
                    'pk' => (string)($row['Key'] ?? '') === 'PRI' ? 1 : 0,
                ];
            }, $rows);
        }

        $stmt = $pdo->query("PRAGMA table_info({$tableId})");
        return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
    }

    /**
     * @param array<int, string> $columns
     * @param array<int, string> $textColumns
     * @return array{where: string, params: array<string, string>}
     */
    private function buildSearch(array $columns, array $textColumns, string $query, string $driver): array
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
                $parts[] = $driver === 'mysql'
                    ? "CAST({$colId} AS CHAR) LIKE :q"
                    : "CAST({$colId} AS TEXT) LIKE :q";
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
        return '`' . str_replace('`', '``', $name) . '`';
    }

    /**
     * @return array<int, string>
     */
    private function listTables(PDO $pdo): array
    {
        $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        if ($driver === 'mysql') {
            $stmt = $pdo->query('SHOW TABLES');
            $tables = $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
            sort($tables, SORT_STRING);
            return $tables;
        }

        $stmt = $pdo->query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name");
        return $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
    }

    private function resolvePrimaryKeyColumn(array $tableInfo): ?string
    {
        foreach ($tableInfo as $row) {
            if ((int)($row['pk'] ?? 0) > 0) {
                $name = trim((string)($row['name'] ?? ''));
                if ($name !== '') {
                    return $name;
                }
            }
        }
        return null;
    }

    private function jsonResponse(array $payload, int $status = 200): void
    {
        http_response_code($status);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($payload, JSON_PRETTY_PRINT);
    }
}
