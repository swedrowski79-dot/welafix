<?php
declare(strict_types=1);

namespace Welafix\Infrastructure\Sqlite;

use PDO;

final class SqliteSchemaHelper
{
    public function columnExists(PDO $db, string $table, string $column): bool
    {
        $driver = (string)$db->getAttribute(PDO::ATTR_DRIVER_NAME);
        if ($driver === 'mysql') {
            $stmt = $db->query('DESCRIBE ' . $this->quoteIdentifier($table));
        } else {
            $stmt = $db->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        }
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? $row['Field'] ?? '');
            if ($name !== '' && strcasecmp($name, $column) === 0) {
                return true;
            }
        }
        return false;
    }

    private function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }
}
