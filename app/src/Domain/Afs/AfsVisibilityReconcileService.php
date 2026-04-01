<?php
declare(strict_types=1);

namespace Welafix\Domain\Afs;

use PDO;
use RuntimeException;
use Welafix\Config\MappingLoader;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;
use Welafix\Config\MappingService;

final class AfsVisibilityReconcileService
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function run(): array
    {
        $mssql = Db::guardMssql($this->factory->mssql(), __METHOD__ . ':mssql');
        $sqlite = $this->factory->localDb();
        $mappings = (new MappingLoader())->loadAll();

        $stats = [
            'ok' => true,
            'artikel' => $this->reconcileEntity($mssql, $sqlite, $mappings['artikel'] ?? null, 'artikel', 'Artikel', 'afs_artikel_id'),
            'warengruppe' => $this->reconcileEntity($mssql, $sqlite, $mappings['warengruppe'] ?? null, 'warengruppe', 'Warengruppe', 'afs_wg_id'),
            'dokument' => $this->reconcileEntity($mssql, $sqlite, $mappings['dokument'] ?? null, 'documents', 'Zaehler', 'source_id', 'AFS_DOKUMENT'),
        ];

        return $stats;
    }

    /**
     * @param array<string, mixed>|null $mapping
     * @return array<string, int>
     */
    private function reconcileEntity(PDO $mssql, PDO $sqlite, ?array $mapping, string $sqliteTable, string $mssqlKey, string $sqliteKey, ?string $sqliteSource = null): array
    {
        if (!is_array($mapping)) {
            throw new RuntimeException('Mapping fehlt fuer ' . $sqliteTable);
        }
        $visibleKeys = $this->fetchVisibleKeys($mssql, $mapping, $mssqlKey);
        $localKeys = $this->fetchLocalActiveKeys($sqlite, $sqliteTable, $sqliteKey, $sqliteSource);
        $visibleSet = array_fill_keys($visibleKeys, true);

        $missing = [];
        foreach ($localKeys as $key) {
            if (!isset($visibleSet[$key])) {
                $missing[] = $key;
            }
        }

        if ($missing === []) {
            return ['visible' => count($visibleKeys), 'marked_deleted' => 0];
        }

        $marked = 0;
        foreach (array_chunk($missing, 500) as $chunk) {
            $placeholders = implode(',', array_fill(0, count($chunk), '?'));
            if ($sqliteTable === 'documents') {
                $sql = 'UPDATE documents
                        SET is_deleted = 1, changed = 1
                        WHERE source = ? AND source_id IN (' . $placeholders . ') AND COALESCE(is_deleted,0) = 0';
                $stmt = $sqlite->prepare($sql);
                $stmt->execute(array_merge([$sqliteSource], $chunk));
            } else {
                $sql = 'UPDATE ' . $this->quoteIdentifier($sqliteTable) . '
                        SET is_deleted = 1, changed = 1
                        WHERE ' . $this->quoteIdentifier($sqliteKey) . ' IN (' . $placeholders . ')
                          AND COALESCE(is_deleted,0) = 0';
                $stmt = $sqlite->prepare($sql);
                $stmt->execute($chunk);
            }
            $marked += $stmt->rowCount();
        }

        return ['visible' => count($visibleKeys), 'marked_deleted' => $marked];
    }

    /**
     * @param array<string, mixed> $mapping
     * @return array<int, string>
     */
    private function fetchVisibleKeys(PDO $pdo, array $mapping, string $key): array
    {
        $source = $mapping['source'] ?? [];
        $table = (string)($source['table'] ?? '');
        $where = (string)($source['where'] ?? '1=1');
        if ($table === '') {
            return [];
        }
        $mappingService = new MappingService();
        $keySql = $mappingService->escapeMssqlIdentifier($key);
        $alias = 's';
        $sql = 'SELECT ' . $alias . '.' . $keySql . ' AS source_key FROM ' . $table . ' ' . $alias . ' WHERE ' . $where;
        $stmt = $pdo->query($sql);
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $keys = [];
        foreach ($rows as $row) {
            $value = trim((string)($row['source_key'] ?? ''));
            if ($value !== '') {
                $keys[] = $value;
            }
        }
        return $keys;
    }

    /**
     * @return array<int, string>
     */
    private function fetchLocalActiveKeys(PDO $pdo, string $table, string $key, ?string $source = null): array
    {
        $sql = 'SELECT ' . $this->quoteIdentifier($key) . ' AS source_key FROM ' . $this->quoteIdentifier($table) . ' WHERE COALESCE(is_deleted,0) = 0';
        $params = [];
        if ($source !== null) {
            $sql .= ' AND source = :source';
            $params[':source'] = $source;
        }
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        return array_values(array_filter(array_map(static fn(array $row): string => trim((string)($row['source_key'] ?? '')), $rows)));
    }

    private function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }
}
