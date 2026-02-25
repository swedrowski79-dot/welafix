<?php
declare(strict_types=1);

namespace Welafix\Database;

use PDO;

final class SchemaHelper
{
    public static function columnExists(PDO $db, string $table, string $column): bool
    {
        $stmt = $db->query('PRAGMA table_info(' . self::quoteIdentifier($table) . ')');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '' && strcasecmp($name, $column) === 0) {
                return true;
            }
        }
        return false;
    }

    private static function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
