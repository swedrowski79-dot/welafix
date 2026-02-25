<?php
declare(strict_types=1);

namespace Welafix\Domain\Warengruppe;

use PDO;
use RuntimeException;
use Welafix\Config\MappingService;

final class WarengruppeRepositoryMssql
{
    private PDO $pdo;
    private ?string $lastSql = null;

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

        $mappingService = new MappingService();
        $select = array_values(array_filter($select, static fn($value): bool => is_string($value) && $value !== ''));
        $columns = $mappingService->buildMssqlSelectList($select, 'w');
        $tableEscaped = $this->escapeIdentifier($table);
        $sql = "SELECT {$columns} FROM {$tableEscaped} w WHERE {$where}";
        $this->lastSql = $sql;

        try {
            $stmt = $this->pdo->query($sql);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Throwable $e) {
            throw new RuntimeException('MSSQL Query fehlgeschlagen: ' . $e->getMessage(), 0, $e);
        }
    }

    public function getLastSql(): ?string
    {
        return $this->lastSql;
    }

    private function escapeIdentifier(string $identifier): string
    {
        $identifier = trim($identifier);
        if ($identifier === '') {
            return $identifier;
        }
        if (preg_match('/[\\s()]/', $identifier)) {
            return $identifier;
        }
        $parts = explode('.', $identifier);
        $escaped = array_map(static fn(string $part): string => '[' . $part . ']', $parts);
        return implode('.', $escaped);
    }
}
