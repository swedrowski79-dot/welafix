<?php
declare(strict_types=1);

namespace XtApi\Controller;

use PDO;
use XtApi\Db\MySql;
use XtApi\Http\Response;

final class ApplyController
{
    public function applyFromBody(): void
    {
        $raw = file_get_contents('php://input') ?: '';
        $payload = json_decode($raw, true);
        if (!is_array($payload)) {
            Response::json(['ok' => false, 'error' => 'Invalid JSON'], 400);
            return;
        }

        $table = (string)($payload['table'] ?? '');
        $mode = (string)($payload['mode'] ?? '');
        if (!$this->isSafeIdentifier($table) || $mode === '') {
            Response::json(['ok' => false, 'error' => 'Payload unvollständig'], 400);
            return;
        }

        try {
            $pdo = MySql::connect();
            $tables = $this->listTables($pdo);
            if (!in_array($table, $tables, true)) {
                Response::json(['ok' => false, 'error' => 'Tabelle nicht erlaubt'], 404);
                return;
            }

            $columns = $this->loadColumns($pdo, $table);
            $validColumns = array_fill_keys(array_column($columns, 'name'), true);

            $result = match ($mode) {
                'upsert' => $this->applyUpsert($pdo, $table, $payload, $validColumns),
                'delete_rows' => $this->applyDeleteRows($pdo, $table, $payload),
                'delete_where_in' => $this->applyDeleteWhereIn($pdo, $table, $payload),
                default => throw new \RuntimeException('Ungültiger Modus'),
            };

            Response::json(['ok' => true] + $result);
        } catch (\Throwable $e) {
            error_log('xt_api apply error: ' . $e->getMessage());
            Response::json(['ok' => false, 'error' => 'apply failed'], 500);
        }
    }

    /**
     * @param array<string, bool> $validColumns
     * @return array<string, mixed>
     */
    private function applyUpsert(PDO $pdo, string $table, array $payload, array $validColumns): array
    {
        $rows = $payload['rows'] ?? [];
        $keyColumns = $payload['key_columns'] ?? [];
        if (!is_array($rows) || !is_array($keyColumns) || $rows === [] || $keyColumns === []) {
            throw new \RuntimeException('upsert payload ungültig');
        }
        $keyColumns = array_values(array_filter(array_map('strval', $keyColumns), fn(string $c): bool => $this->isSafeIdentifier($c)));
        $count = 0;

        $pdo->beginTransaction();
        try {
            foreach ($rows as $row) {
                if (!is_array($row)) {
                    continue;
                }
                $filtered = [];
                foreach ($row as $column => $value) {
                    if (isset($validColumns[$column])) {
                        $filtered[$column] = $value;
                    }
                }
                if ($filtered === []) {
                    continue;
                }
                $columns = array_keys($filtered);
                $placeholders = array_map(static fn(string $c): string => ':' . $c, $columns);
                $updateParts = [];
                foreach ($columns as $column) {
                    if (in_array($column, $keyColumns, true)) {
                        continue;
                    }
                    $updateParts[] = $this->quoteIdentifier($column) . ' = VALUES(' . $this->quoteIdentifier($column) . ')';
                }
                $sql = 'INSERT INTO ' . $this->quoteIdentifier($table) .
                    ' (' . implode(',', array_map([$this, 'quoteIdentifier'], $columns)) . ')' .
                    ' VALUES (' . implode(',', $placeholders) . ')';
                if ($updateParts !== []) {
                    $sql .= ' ON DUPLICATE KEY UPDATE ' . implode(', ', $updateParts);
                }
                $stmt = $pdo->prepare($sql);
                foreach ($filtered as $column => $value) {
                    $stmt->bindValue(':' . $column, $value);
                }
                $stmt->execute();
                $count++;
            }
            $pdo->commit();
        } catch (\Throwable $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }

        return ['mode' => 'upsert', 'table' => $table, 'rows' => $count];
    }

    /**
     * @return array<string, mixed>
     */
    private function applyDeleteRows(PDO $pdo, string $table, array $payload): array
    {
        $rows = $payload['rows'] ?? [];
        $keyColumns = $payload['key_columns'] ?? [];
        if (!is_array($rows) || !is_array($keyColumns) || $rows === [] || $keyColumns === []) {
            throw new \RuntimeException('delete_rows payload ungültig');
        }
        $keyColumns = array_values(array_filter(array_map('strval', $keyColumns), fn(string $c): bool => $this->isSafeIdentifier($c)));
        $count = 0;

        $pdo->beginTransaction();
        try {
            foreach ($rows as $row) {
                if (!is_array($row)) {
                    continue;
                }
                $where = [];
                $params = [];
                foreach ($keyColumns as $column) {
                    $where[] = $this->quoteIdentifier($column) . ' = :' . $column;
                    $params[':' . $column] = $row[$column] ?? null;
                }
                $sql = 'DELETE FROM ' . $this->quoteIdentifier($table) . ' WHERE ' . implode(' AND ', $where);
                $stmt = $pdo->prepare($sql);
                $stmt->execute($params);
                $count += $stmt->rowCount();
            }
            $pdo->commit();
        } catch (\Throwable $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }

        return ['mode' => 'delete_rows', 'table' => $table, 'rows' => $count];
    }

    /**
     * @return array<string, mixed>
     */
    private function applyDeleteWhereIn(PDO $pdo, string $table, array $payload): array
    {
        $column = (string)($payload['column'] ?? '');
        $values = $payload['values'] ?? [];
        if (!$this->isSafeIdentifier($column) || !is_array($values) || $values === []) {
            throw new \RuntimeException('delete_where_in payload ungültig');
        }
        $values = array_values(array_filter($values, static fn($value): bool => $value !== null && $value !== ''));
        if ($values === []) {
            return ['mode' => 'delete_where_in', 'table' => $table, 'rows' => 0];
        }

        $count = 0;
        foreach (array_chunk($values, 500) as $chunk) {
            $placeholders = implode(',', array_fill(0, count($chunk), '?'));
            $sql = 'DELETE FROM ' . $this->quoteIdentifier($table) . ' WHERE ' . $this->quoteIdentifier($column) . ' IN (' . $placeholders . ')';
            $stmt = $pdo->prepare($sql);
            $stmt->execute($chunk);
            $count += $stmt->rowCount();
        }

        return ['mode' => 'delete_where_in', 'table' => $table, 'rows' => $count];
    }

    /**
     * @return array<int, array{name:string,type:string,nullable:bool,key:string}>
     */
    private function loadColumns(PDO $pdo, string $table): array
    {
        $stmt = $pdo->query('DESCRIBE ' . $this->quoteIdentifier($table));
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $cols = [];
        foreach ($rows as $row) {
            $cols[] = [
                'name' => (string)($row['Field'] ?? ''),
                'type' => (string)($row['Type'] ?? ''),
                'nullable' => ($row['Null'] ?? '') === 'YES',
                'key' => (string)($row['Key'] ?? ''),
            ];
        }
        return $cols;
    }

    /**
     * @return array<int, string>
     */
    private function listTables(PDO $pdo): array
    {
        $stmt = $pdo->query('SHOW TABLES');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_NUM) : [];
        $tables = [];
        foreach ($rows as $row) {
            if (isset($row[0])) {
                $tables[] = (string)$row[0];
            }
        }
        return $tables;
    }

    private function isSafeIdentifier(string $name): bool
    {
        return (bool)preg_match('/^[A-Za-z0-9_]+$/', $name);
    }

    private function quoteIdentifier(string $name): string
    {
        if (!$this->isSafeIdentifier($name)) {
            throw new \RuntimeException('Ungueltiger Identifier');
        }
        return '`' . $name . '`';
    }
}
