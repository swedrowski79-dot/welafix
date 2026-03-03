<?php
declare(strict_types=1);

namespace XtApi\Controller;

use PDO;
use XtApi\Db\MySql;
use XtApi\Http\Response;

final class SchemaController
{
    public function tables(): void
    {
        try {
            $pdo = MySql::connect();
            $tables = $this->listTables($pdo);
            Response::json(['ok' => true, 'tables' => $tables]);
        } catch (\Throwable $e) {
            error_log('xt_api schema tables error: ' . $e->getMessage());
            Response::json(['ok' => false, 'error' => 'schema tables failed'], 500);
        }
    }

    public function table(string $table): void
    {
        if (!$this->isSafeIdentifier($table)) {
            Response::json(['ok' => false, 'error' => 'Ungültige Tabelle'], 400);
            return;
        }
        try {
            $pdo = MySql::connect();
            $tables = $this->listTables($pdo);
            if (!in_array($table, $tables, true)) {
                Response::json(['ok' => false, 'error' => 'Tabelle nicht erlaubt'], 404);
                return;
            }
            $stmt = $pdo->query('DESCRIBE ' . $this->quoteIdentifier($table));
            $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
            $cols = [];
            foreach ($rows as $row) {
                $cols[] = [
                    'name' => $row['Field'] ?? '',
                    'type' => $row['Type'] ?? '',
                    'nullable' => ($row['Null'] ?? '') === 'YES',
                    'key' => $row['Key'] ?? '',
                ];
            }
            Response::json(['ok' => true, 'table' => $table, 'columns' => $cols]);
        } catch (\Throwable $e) {
            error_log('xt_api schema table error: ' . $e->getMessage());
            Response::json(['ok' => false, 'error' => 'schema table failed'], 500);
        }
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
        sort($tables, SORT_STRING);
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
