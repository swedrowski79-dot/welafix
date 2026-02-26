<?php
declare(strict_types=1);

namespace Welafix\Domain\Warengruppe;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Config\MappingService;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;
use Welafix\Database\SchemaSyncService;
use Welafix\Domain\ChangeTracking\ChangeTracker;
use Welafix\Domain\FileDb\FileDbTemplateApplier;
use Welafix\Infrastructure\Sqlite\SqliteSchemaHelper;

final class WarengruppeSyncService
{
    private ConnectionFactory $factory;
    private ?string $lastSql = null;
    private ?array $lastContext = null;
    private SchemaSyncService $schemaSync;
    private SqliteSchemaHelper $schemaHelper;

    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
        $this->schemaSync = new SchemaSyncService();
        $this->schemaHelper = new SqliteSchemaHelper();
    }

    /**
     * @return array{total_fetched:int,inserted:int,updated:int,unchanged:int,paths_updated:int,errors_count:int}
     */
    public function runImportAndBuildPaths(): array
    {
        $mappingLoader = new \Welafix\Config\MappingLoader();
        $mappings = $mappingLoader->loadAll();
        if (!isset($mappings['warengruppe'])) {
            throw new RuntimeException('Mapping "warengruppe" nicht gefunden.');
        }

        $mapping = $mappings['warengruppe'];
        $selectFields = $this->getSelectFields($mapping);
        $mapping['select'] = $selectFields;

        $mssql = Db::guardMssql(Db::mssql(), __METHOD__);
        $sqlite = Db::guardSqlite(Db::sqlite(), __METHOD__);
        $mssqlRepo = new WarengruppeRepositoryMssql($mssql);
        $sqliteRepo = new WarengruppeRepositorySqlite($sqlite);
        $this->schemaSync->ensureSqliteColumnsMatchMssql($mssql, $sqlite, 'dbo.Warengruppe', 'warengruppe', $selectFields);

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
        $allowedLookup = array_flip($selectFields);
        $applyFileDb = $this->shouldApplyFileDbOnSync();
        $fileDbApplier = $applyFileDb ? new FileDbTemplateApplier() : null;
        $diffColumns = $selectFields;

        foreach ($rows as $row) {
            $row = array_intersect_key($row, $allowedLookup);
            $afsWgId = isset($row['Warengruppe']) ? (string)$row['Warengruppe'] : '';
            if ($afsWgId === '') {
                $preparedRows[] = ['error' => 'missing_key', 'row' => $row];
                continue;
            }
            $preparedRows[] = [
                'row' => $row,
                'extras' => $this->extractRawFields($row, $this->getRawFieldNames($selectFields)),
            ];
        }

        foreach ($this->buildDesiredColumns($selectFields) as $column => $type) {
            if (!isset($existingColumns[strtolower($column)])) {
                $newColumns[$column] = $type;
            }
        }

        if ($newColumns !== []) {
            foreach ($newColumns as $column => $type) {
                if (!$this->schemaHelper->columnExists($pdo, 'warengruppe', $column)) {
                    $pdo->exec('ALTER TABLE warengruppe ADD COLUMN ' . $this->quoteIdentifier($column) . ' ' . $type);
                    $existingColumns[strtolower($column)] = $column;
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

                    $extras = $this->fillMissingExtras($prepared['extras'], $this->getRawFieldNames($selectFields));

                    $result = $sqliteRepo->upsert($afsWgId, $name, $parentId, $extras, $seenAt, $diffColumns);
                    if ($result['inserted']) $stats['inserted']++;
                    if ($result['updated']) $stats['updated']++;
                    if ($result['unchanged']) $stats['unchanged']++;

                    if ($fileDbApplier) {
                        $context = array_merge($row, $extras);
                        $context['Bezeichnung'] = $name;
                        $fileDbApplier->applyWarengruppe($pdo, $afsWgId, $name, $context);
                    }

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

        $stats['paths_updated'] = $this->buildAndStorePathsAndSeo($this->factory->sqlite());
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

    private function buildAndStorePathsAndSeo(PDO $pdo): int
    {
        $items = $this->loadAllWarengruppen($pdo);
        $updated = 0;
        $seoCache = [];
        $seoBuilding = [];
        $tracker = new ChangeTracker();

        $computeSeo = function (int $id) use (&$computeSeo, &$items, &$seoCache, &$seoBuilding): string {
            if (isset($seoCache[$id])) {
                return $seoCache[$id];
            }
            if (isset($seoBuilding[$id])) {
                return 'de';
            }
            $seoBuilding[$id] = true;
            $name = (string)($items[$id]['name'] ?? '');
            $slug = strtolower(xt_filterAutoUrlText_inline($name, 'de'));
            $parent = $items[$id]['parent_id'] ?? null;

            if ($parent === null || !isset($items[$parent])) {
                $seo = 'de' . ($slug !== '' ? '/' . $slug : '');
            } else {
                $parentSeo = $computeSeo($parent);
                $seo = rtrim($parentSeo, '/') . ($slug !== '' ? '/' . $slug : '');
            }

            $seoCache[$id] = $seo;
            unset($seoBuilding[$id]);
            return $seo;
        };

        $pdo->beginTransaction();
        try {
            foreach ($items as $id => $item) {
                $idInt = (int)$id;
                [$path, $pathIds] = $this->buildPath($idInt, $items);
                $seoUrl = $computeSeo($idInt);
                $pathChanged = (string)($item['path'] ?? '') !== $path;
                $pathIdsChanged = (string)($item['path_ids'] ?? '') !== $pathIds;
                $seoChanged = (string)($item['seo_url'] ?? '') !== $seoUrl;

                if (!$pathChanged && !$pathIdsChanged && !$seoChanged) {
                    continue;
                }

                $reasons = [];
                if ($pathChanged || $pathIdsChanged) {
                    $reasons[] = 'path';
                }
                if ($seoChanged) {
                    $reasons[] = 'seo_url';
                }

                $changeReason = $this->mergeReasons((string)($item['change_reason'] ?? ''), $reasons);
                $mergedDiff = [];
                $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
                if ($seoChanged) {
                    $existingDiff = [];
                    $raw = (string)($item['changed_fields'] ?? '');
                    if ($raw !== '') {
                        $decoded = json_decode($raw, true);
                        if (is_array($decoded)) {
                            $existingDiff = $decoded;
                        }
                    }
                    $existingDiff['seo_url'] = [
                        'old' => (string)($item['seo_url'] ?? ''),
                        'new' => $seoUrl,
                    ];
                    $mergedDiff = $existingDiff;
                }

                $stmt = $pdo->prepare(
                    'UPDATE warengruppe
                     SET path = :path,
                         path_ids = :path_ids,
                         seo_url = :seo_url,
                         changed = 1,
                         changed_fields = :changed_fields,
                         last_synced_at = :last_synced_at,
                         change_reason = :change_reason
                     WHERE afs_wg_id = :id'
                );
                $stmt->execute([
                    ':path' => $path,
                    ':path_ids' => $pathIds,
                    ':seo_url' => $seoUrl,
                    ':changed_fields' => $mergedDiff !== [] ? $tracker->encodeDiff($mergedDiff) : null,
                    ':last_synced_at' => $now,
                    ':change_reason' => $changeReason,
                    ':id' => $idInt,
                ]);

                $updated++;
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
        $stmt = $pdo->query('SELECT afs_wg_id, name, parent_id, path, path_ids, seo_url, change_reason FROM warengruppe');
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
     * @param array<string, mixed> $mapping
     * @return array<int, string>
     */
    private function getSelectFields(array $mapping): array
    {
        $mapping = new MappingService();
        return $mapping->getAllowedColumns('warengruppe');
    }

    /**
     * @param array<int, string> $selectFields
     * @return array<int, string>
     */
    private function getRawFieldNames(array $selectFields): array
    {
        return $selectFields;
    }

    /**
     * @param array<string, mixed> $row
     * @param array<int, string> $rawFieldNames
     * @return array<string, mixed>
     */
    private function extractRawFields(array $row, array $rawFieldNames): array
    {
        $raw = [];
        foreach ($rawFieldNames as $field) {
            $raw[$field] = $row[$field] ?? null;
        }
        return $raw;
    }

    /**
     * @param array<string, mixed> $extras
     * @param array<int, string> $extraKeys
     * @return array<string, mixed>
     */
    private function fillMissingExtras(array $extras, array $extraKeys): array
    {
        foreach ($extraKeys as $key) {
            if (!array_key_exists($key, $extras)) {
                $extras[$key] = null;
            }
        }
        return $extras;
    }

    /**
     * @return array<string, string>
     */
    private function buildDesiredColumns(array $selectFields): array
    {
        $columns = [
            'afs_wg_id' => 'INTEGER',
            'name' => 'TEXT',
            'parent_id' => 'INTEGER',
            'path' => 'TEXT',
            'path_ids' => 'TEXT',
            'seo_url' => 'TEXT',
            'last_seen_at' => 'TEXT',
            'changed' => 'INTEGER',
            'change_reason' => 'TEXT',
        ];

        foreach ($selectFields as $field) {
            if (!isset($columns[$field])) {
                $columns[$field] = 'TEXT';
            }
        }
        return $columns;
    }

    /**
     * @param string[] $reasons
     */
    private function mergeReasons(string $existing, array $reasons): string
    {
        $items = [];
        if ($existing !== '') {
            foreach (explode(',', $existing) as $item) {
                $item = trim($item);
                if ($item !== '') {
                    $items[$item] = true;
                }
            }
        }
        foreach ($reasons as $reason) {
            $items[$reason] = true;
        }
        return implode(',', array_keys($items));
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

    private function shouldApplyFileDbOnSync(): bool
    {
        $flag = strtolower((string)env('FILEDB_APPLY_ON_SYNC', 'false'));
        return $flag === '1' || $flag === 'true' || $flag === 'yes';
    }
}
