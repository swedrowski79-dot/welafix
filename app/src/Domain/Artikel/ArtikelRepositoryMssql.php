<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use PDO;
use RuntimeException;

final class ArtikelRepositoryMssql
{
    private PDO $pdo;
    private ?string $lastSql = null;
    private array $lastParams = [];

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function fetchAfterByMapping(array $mapping, string $afterKey, int $limit = 500): array
    {
        $limit = max(1, min(1000, $limit));

        $source = $mapping['source'] ?? [];
        $table = $source['table'] ?? 'dbo.Artikel';
        $where = $source['where'] ?? '';
        $key = $source['key'] ?? 'Artikel';
        $select = $mapping['select'] ?? [];

        $select = $this->ensureKeySelected($select, $key);
        $selectSql = implode(', ', array_map([$this, 'quoteIdentifier'], $select));

        $whereParts = [];
        if (trim($where) !== '') {
            $whereParts[] = '(' . $where . ')';
        }
        $whereParts[] = '(? = \'\' OR ' . $this->quoteIdentifier($key) . ' > ?)';
        $whereSql = implode(' AND ', $whereParts);

        $sql = "SELECT TOP {$limit} {$selectSql}
          FROM {$table}
          WHERE {$whereSql}
          ORDER BY " . $this->quoteIdentifier($key) . " ASC";

        $this->lastSql = $sql;
        $this->lastParams = [$afterKey, $afterKey];

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($this->lastParams);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Throwable $e) {
            throw new RuntimeException('MSSQL Query fehlgeschlagen: ' . $e->getMessage(), 0, $e);
        }
    }

    public function getLastSql(): ?string
    {
        return $this->lastSql;
    }

    /**
     * @return array<int, mixed>
     */
    public function getLastParams(): array
    {
        return $this->lastParams;
    }

    /**
     * @param array<int, string> $select
     * @return array<int, string>
     */
    private function ensureKeySelected(array $select, string $key): array
    {
        foreach ($select as $field) {
            if ($field === $key) {
                return $select;
            }
        }
        $select[] = $key;
        return $select;
    }

    private function quoteIdentifier(string $name): string
    {
        if (str_contains($name, '[') || str_contains($name, ']')) {
            return $name;
        }
        return '[' . str_replace(']', ']]', $name) . ']';
    }
}
