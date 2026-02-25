<?php
declare(strict_types=1);

namespace Welafix\Domain\Warengruppe;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Config\MappingLoader;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;
use Welafix\Database\SchemaSyncService;
use Welafix\Domain\FileDb\FileDbCache;

final class WarengruppeSyncService
{
    private ConnectionFactory $factory;
    private ?string $lastSql = null;
    private ?array $lastContext = null;
    private FileDbCache $fileDbCache;
    private SchemaSyncService $schemaSync;
    private const BASE_COLUMNS = [
        'afs_wg_id',
        'name',
        'parent_id',
        'path',
        'path_ids',
        'last_seen_at',
        'changed',
        'change_reason',
        'id',
    ];

    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
        $this->fileDbCache = new FileDbCache();
        $this->schemaSync = new SchemaSyncService();
    }

    /**
     * @return array{total_fetched:int,inserted:int,updated:int,unchanged:int,paths_updated:int,errors_count:int}
     */
    public function runImportAndBuildPaths(): array
    {
        $loader = new MappingLoader();
        $mappings = $loader->loadAll();
        if (!isset($mappings['warengruppe'])) {
            throw new RuntimeException('Mapping "warengruppe" nicht gefunden.');
        }

        $mapping = $mappings['warengruppe'];

        $mssql = Db::guardMssql(Db::mssql(), __METHOD__);
        $sqlite = Db::guardSqlite(Db::sqlite(), __METHOD__);
        $mssqlRepo = new WarengruppeRepositoryMssql($mssql);
        $sqliteRepo = new WarengruppeRepositorySqlite($sqlite);
        $this->schemaSync->ensureSqliteColumnsMatchMssql($mssql, $sqlite, 'dbo.Warengruppe', 'warengruppe');

        try {
            $rows = $mssqlRepo->fetchAllByMapping($mapping);
            $this->lastSql = $mssqlRepo->getLastSql();
        } catch (\Throwable $e) {
            $this->lastSql = $mssqlRepo->getLastSql();
            throw $e;
        }
        $seenAt = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);

        $stats = [
            'total_fetched' => count($rows),
            'inserted' => 0,
            'updated' => 0,
            'unchanged' => 0,
            'paths_updated' => 0,
            'errors_count' => 0,
        ];

        $pdo = $sqlite;
        $existingColumns = $this->loadExistingColumns($pdo, 'warengruppe');
        $preparedRows = [];
        $newColumns = [];

        foreach ($rows as $row) {
            $afsWgId = isset($row['Warengruppe']) ? (string)$row['Warengruppe'] : '';
            if ($afsWgId === '') {
                $preparedRows[] = ['error' => 'missing_key', 'row' => $row];
                continue;
            }
            $extras = $this->fileDbCache->getMerged('Warengruppen', $afsWgId);
            $normalizedExtras = $this->normalizeExtras($extras);
            $normalizedExtras = $this->filterBaseColumns($normalizedExtras, self::BASE_COLUMNS);
            foreach (array_keys($normalizedExtras) as $field) {
                if (!isset($existingColumns[$field])) {
                    $newColumns[$field] = true;
                }
            }
            $preparedRows[] = [
                'row' => $row,
                'extras' => $normalizedExtras,
            ];
        }

        if ($newColumns !== []) {
            foreach (array_keys($newColumns) as $column) {
                if (!isset($existingColumns[$column])) {
                    $pdo->exec('ALTER TABLE warengruppe ADD COLUMN ' . $this->quoteIdentifier($column) . ' TEXT');
                    $existingColumns[$column] = true;
                }
            }
        }

        $pdo->beginTransaction();
        try {
            foreach ($preparedRows as $prepared) {
                if (isset($prepared['error'])) {
                    $stats['errors_count']++;
                    continue;
                }
                $row = $prepared['row'];
                try {
                    $afsWgId = $this->requireIntField($row, 'Warengruppe');
                    $name = $this->requireStringField($row, 'Bezeichnung');
                    $parentId = $this->optionalParentId($row['Anhang'] ?? null);

                    $extras = $prepared['extras'];

                    $result = $sqliteRepo->upsert($afsWgId, $name, $parentId, $extras, $seenAt);
                    if ($result['inserted']) $stats['inserted']++;
                    if ($result['updated']) $stats['updated']++;
                    if ($result['unchanged']) $stats['unchanged']++;
                } catch (\Throwable $e) {
                    $stats['errors_count']++;
                    $this->log('Import-Fehler: ' . $e->getMessage());
                }
            }
            $pdo->commit();
        } catch (\Throwable $e) {
            $pdo->rollBack();
            throw $e;
        }

        $stats['paths_updated'] = $this->buildAndStorePaths($sqliteRepo, $this->factory->sqlite());
        return $stats;
    }

    public function getLastSql(): ?string
    {
        return $this->lastSql;
    }

    public function getLastContext(): ?array
    {
        return $this->lastContext;
    }

    private function buildAndStorePaths(WarengruppeRepositorySqlite $repo, PDO $pdo): int
    {
        $items = $this->loadAllWarengruppen($pdo);
        $updated = 0;
        $pdo->beginTransaction();
        try {
            foreach ($items as $id => $item) {
                $idInt = (int)$id;
                [$path, $pathIds] = $this->buildPath($idInt, $items);
                if ($repo->updatePath($idInt, $path, $pathIds)) {
                    $updated++;
                }
            }
            $pdo->commit();
        } catch (\Throwable $e) {
            $pdo->rollBack();
            throw $e;
        }
        return $updated;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function loadAllWarengruppen(PDO $pdo): array
    {
        $stmt = $pdo->query('SELECT afs_wg_id, name, parent_id, path, path_ids FROM warengruppe');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $map = [];
        foreach ($rows as $row) {
            $id = (int)$row['afs_wg_id'];
            $row['parent_id'] = $this->optionalParentId($row['parent_id'] ?? null);
            $map[$id] = $row;
        }
        return $map;
    }

    /**
     * @param array<string, array<string, mixed>> $items
     * @return array{0:string,1:string}
     */
    private function buildPath(int $startId, array $items): array
    {
        $names = [];
        $ids = [];
        $seen = [];
        $current = $startId;
        $depth = 0;
        $parent = null;

        while ($current !== 0) {
            if ($depth >= 50) {
                $this->log("Max-Tiefe erreicht bei Warengruppe {$startId}.");
                break;
            }

            if (isset($seen[$current])) {
                $this->log("Zyklus erkannt bei Warengruppe {$startId} (ID {$current}).");
                break;
            }
            $seen[$current] = true;

            if (!isset($items[$current])) {
                break;
            }

            $node = $items[$current];
            $name = (string)($node['name'] ?? '');
            $names[] = $name;
            $ids[] = $current;

            $parent = $node['parent_id'] ?? null;
            if ($parent === null) {
                break;
            }

            $current = $parent;
            $depth++;
        }

        $names = array_reverse($names);
        $ids = array_reverse($ids);
        $trimmedNames = array_map(static fn(string $value): string => trim($value), $names);
        $filteredNames = array_values(array_filter($trimmedNames, static fn(string $value): bool => $value !== ''));

        $path = implode('/', $filteredNames);
        $pathIds = implode('/', array_map(static fn(int $value): string => (string)$value, $ids));

        $this->lastContext = [
            'currentId' => $startId,
            'parentId' => $parent,
            'computedPath' => $path,
        ];

        return [$path, $pathIds];
    }

    private function optionalParentId(int|string|null $value): ?int
    {
        if ($value === null) {
            return null;
        }
        $trimmed = trim((string)$value);
        if ($trimmed === '' || $trimmed === '0') {
            return null;
        }
        return (int)$trimmed;
    }

    /**
     * @param array<string, mixed> $row
     */
    private function requireStringField(array $row, string $field): string
    {
        if (!array_key_exists($field, $row)) {
            throw new RuntimeException("Pflichtfeld fehlt: {$field}");
        }
        $value = trim((string)$row[$field]);
        if ($value === '') {
            throw new RuntimeException("Pflichtfeld leer: {$field}");
        }
        return $value;
    }

    /**
     * @param array<string, mixed> $row
     */
    private function requireIntField(array $row, string $field): int
    {
        if (!array_key_exists($field, $row)) {
            throw new RuntimeException("Pflichtfeld fehlt: {$field}");
        }
        $value = trim((string)$row[$field]);
        if ($value === '') {
            throw new RuntimeException("Pflichtfeld leer: {$field}");
        }
        return (int)$value;
    }

    /**
     * @return array<string, true>
     */
    private function loadExistingColumns(PDO $pdo, string $table): array
    {
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $columns = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $columns[$name] = true;
            }
        }
        return $columns;
    }

    /**
     * @param array<string, string> $extras
     * @return array<string, string>
     */
    private function normalizeExtras(array $extras): array
    {
        $normalized = [];
        foreach ($extras as $field => $value) {
            $key = $this->normalizeFieldName($field);
            if ($key === '') {
                continue;
            }
            $normalized[$key] = $value;
        }
        if ($normalized !== []) {
            ksort($normalized, SORT_STRING);
        }
        return $normalized;
    }

    /**
     * @param array<string, string> $extras
     * @param array<int, string> $baseColumns
     * @return array<string, string>
     */
    private function filterBaseColumns(array $extras, array $baseColumns): array
    {
        if ($extras === []) {
            return $extras;
        }
        $base = array_fill_keys($baseColumns, true);
        foreach (array_keys($extras) as $key) {
            if (isset($base[$key])) {
                unset($extras[$key]);
            }
        }
        return $extras;
    }

    private function normalizeFieldName(string $field): string
    {
        $field = strtolower(trim($field));
        if ($field === '') {
            return '';
        }
        $replacements = [
            'ä' => 'ae',
            'ö' => 'oe',
            'ü' => 'ue',
            'ß' => 'ss',
        ];
        $field = strtr($field, $replacements);
        $field = preg_replace('/\\s+/', '_', $field) ?? $field;
        $field = preg_replace('/[^a-z0-9_]/', '', $field) ?? $field;
        $field = trim($field, '_');
        return $field;
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    private function log(string $message): void
    {
        $timestamp = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $line = "[{$timestamp}] {$message}\n";
        $path = __DIR__ . '/../../../logs/app.log';
        @file_put_contents($path, $line, FILE_APPEND);
    }
}
