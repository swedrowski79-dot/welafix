<?php
declare(strict_types=1);

namespace XtApi\Controller;

use PDO;
use XtApi\Db\MySql;
use XtApi\Http\Response;

final class ExportTableController
{
    public function export(string $table, int $page, int $pageSize): void
    {
        if (!$this->isSafeIdentifier($table)) {
            Response::json(['ok' => false, 'error' => 'Ungültige Tabelle'], 400);
            return;
        }
        $page = max(1, $page);
        $pageSize = max(1, min(10000, $pageSize));
        $offset = ($page - 1) * $pageSize;
        $limit = $pageSize + 1;

        try {
            $pdo = MySql::connect();
            $tables = $this->listTables($pdo);
            if (!in_array($table, $tables, true)) {
                Response::json(['ok' => false, 'error' => 'Tabelle nicht erlaubt'], 404);
                return;
            }

            $sql = 'SELECT * FROM ' . $this->quoteIdentifier($table) . ' LIMIT ' . (int)$limit . ' OFFSET ' . (int)$offset;
            $stmt = $pdo->query($sql);
            $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        } catch (\Throwable $e) {
            error_log('xt_api export table error: ' . $e->getMessage());
            Response::json(['ok' => false, 'error' => 'export failed'], 500);
            return;
        }

        $hasMore = count($rows) > $pageSize;
        if ($hasMore) {
            $rows = array_slice($rows, 0, $pageSize);
        }

        Response::json([
            'ok' => true,
            'table' => $table,
            'page' => $page,
            'page_size' => $pageSize,
            'rows' => $rows,
            'has_more' => $hasMore,
        ]);
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
