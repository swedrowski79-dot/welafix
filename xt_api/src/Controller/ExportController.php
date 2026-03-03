<?php
declare(strict_types=1);

namespace XtApi\Controller;

use PDO;
use XtApi\Db\MySql;
use XtApi\Http\Response;

final class ExportController
{
    public function exportFromBody(): void
    {
        $raw = file_get_contents('php://input') ?: '';
        $payload = json_decode($raw, true);
        if (!is_array($payload)) {
            Response::json(['ok' => false, 'error' => 'Invalid JSON'], 400);
            return;
        }

        $table = (string)($payload['table'] ?? '');
        $select = $payload['select'] ?? [];
        $where = (string)($payload['where'] ?? '1=1');
        $key = (string)($payload['key'] ?? '');
        $page = (int)($payload['page'] ?? 1);
        $pageSize = (int)($payload['page_size'] ?? 500);

        if ($table === '' || !is_array($select) || $select === []) {
            Response::json(['ok' => false, 'error' => 'Payload unvollständig'], 400);
            return;
        }
        if (!$this->isSafeIdentifier($table)) {
            Response::json(['ok' => false, 'error' => 'Ungültige Tabelle'], 400);
            return;
        }
        foreach ($select as $col) {
            if (!$this->isSafeIdentifier((string)$col)) {
                Response::json(['ok' => false, 'error' => 'Ungültige Spalte'], 400);
                return;
            }
        }
        if ($key !== '' && !$this->isSafeIdentifier($key)) {
            Response::json(['ok' => false, 'error' => 'Ungültiger Key'], 400);
            return;
        }
        if (!$this->isSafeWhere($where)) {
            Response::json(['ok' => false, 'error' => 'Ungültiger Where-Filter'], 400);
            return;
        }

        try {
            $pdo = MySql::connect();
        $rows = $this->fetchPage($pdo, $table, $select, $where, $key, $page, $pageSize);
        } catch (\Throwable $e) {
            error_log('xt_api export error: ' . $e->getMessage());
            Response::json(['ok' => false, 'error' => 'export failed'], 500);
            return;
        }

        $hasMore = count($rows) > $pageSize;
        if ($hasMore) {
            $rows = array_slice($rows, 0, $pageSize);
        }

        Response::json([
            'ok' => true,
            'rows' => $rows,
            'has_more' => $hasMore,
            'page' => $page,
        ]);
    }

    /**
     * @param array<int, string> $select
     * @return array<int, array<string, mixed>>
     */
    private function fetchPage(PDO $pdo, string $table, array $select, string $where, string $key, int $page, int $pageSize): array
    {
        $page = max(1, $page);
        $pageSize = max(1, min(5000, $pageSize));
        $offset = ($page - 1) * $pageSize;
        $limit = $pageSize + 1;

        $cols = array_map([$this, 'quoteIdentifier'], $select);
        $sql = 'SELECT ' . implode(', ', $cols) . ' FROM ' . $this->quoteIdentifier($table) . ' WHERE ' . $where;
        if ($key !== '') {
            $sql .= ' ORDER BY ' . $this->quoteIdentifier($key);
        }
        $sql .= ' LIMIT ' . (int)$limit . ' OFFSET ' . (int)$offset;

        $stmt = $pdo->query($sql);
        return $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
    }

    private function quoteIdentifier(string $name): string
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $name)) {
            throw new \RuntimeException('Ungueltiger Identifier');
        }
        return '`' . $name . '`';
    }

    private function isSafeIdentifier(string $name): bool
    {
        return (bool)preg_match('/^[A-Za-z0-9_]+$/', $name);
    }

    private function isSafeWhere(string $where): bool
    {
        $where = trim($where);
        if ($where === '' || $where === '1=1') {
            return true;
        }
        if (str_contains($where, ';')) {
            return false;
        }
        return (bool)preg_match('/^[A-Za-z0-9_\\s\\=\\!\\<\\>\\\'"\\.\\(\\),%+-]+$/', $where);
    }
}
