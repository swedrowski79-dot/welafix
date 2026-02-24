<?php
declare(strict_types=1);

namespace Welafix\Domain\Warengruppe;

use PDO;
use RuntimeException;

final class WarengruppeRepositoryMssql
{
    private PDO $pdo;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @param array<string, mixed> $mapping
     * @return array<int, array<string, mixed>>
     */
    public function fetchAllByMapping(array $mapping): array
    {
        $source = $mapping['source'] ?? null;
        if (!is_array($source)) {
            throw new RuntimeException('Mapping: Feld "source" fehlt.');
        }
        $table = $source['table'] ?? null;
        if (!is_string($table) || $table === '') {
            throw new RuntimeException('Mapping: Feld "source.table" fehlt.');
        }

        $select = $mapping['select'] ?? null;
        if (!is_array($select) || $select === []) {
            throw new RuntimeException('Mapping: Feld "select" fehlt oder ist leer.');
        }

        $where = $source['where'] ?? '1=1';
        if (!is_string($where) || $where === '') {
            $where = '1=1';
        }

        $columns = implode(', ', $select);
        $sql = "SELECT {$columns} FROM {$table} WHERE {$where}";

        $stmt = $this->pdo->query($sql);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }
}
