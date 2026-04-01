<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use DateTimeImmutable;
use DateTimeZone;
use RuntimeException;
use Welafix\Config\MappingService;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;
use Welafix\Database\SchemaSyncService;
use Welafix\Domain\Afs\AfsUpdateQueue;
use Welafix\Domain\Attribute\AttributesBuilder;
use Welafix\Infrastructure\Sqlite\SqliteSchemaHelper;

final class ArtikelSyncService
{
    private ConnectionFactory $factory;
    private ?string $lastSql = null;
    private ?array $lastContext = null;
    private array $lastParams = [];
    private const STATE_TYPE = 'artikel';
    private SchemaSyncService $schemaSync;
    private SqliteSchemaHelper $schemaHelper;

    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
        $this->schemaSync = new SchemaSyncService();
        $this->schemaHelper = new SqliteSchemaHelper();
    }

    /**
     * @return array{done:bool,batch_size:int,batch_fetched:int,total_fetched:int,inserted:int,updated:int,unchanged:int,errors_count:int,batches:int,last_key:string}
     */
    public function processBatch(string $afterKey, int $batchSize = 500): array
    {
        $pdo = $this->factory->localDb();
        $this->ensureStateTable($pdo);

        if ($afterKey === '') {
            $this->resetState($pdo);
        }

        $mssql = Db::guardMssql(Db::mssql(), __METHOD__);
        $mssqlRepo = new ArtikelRepositoryMssql($mssql);
        $sqliteRepo = new ArtikelRepositorySqlite($pdo);
        $sqliteRepo->ensureTable();
        $relationWriter = new ArtikelRelationWriter($pdo);
        $relationWriter->ensureSchema();

        $batchSize = max(1, min(10000, $batchSize));
        $seenAt = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);

        $mapping = $this->loadArtikelMapping();
        $selectFields = $this->getSelectFields($mapping);
        $mapping['select'] = $selectFields;
        $this->schemaSync->ensureSqliteColumnsMatchMssql($mssql, $pdo, 'dbo.Artikel', 'artikel', $selectFields);
        $keyField = (string)(($mapping['source']['key'] ?? 'Artikel') ?: 'Artikel');
        $deltaOnly = $this->resolveDeltaOnlyMode($mapping, $pdo, 'artikel', $afterKey);
        $afsQueue = new AfsUpdateQueue($pdo);
        $afsQueue->ensureTable();

        try {
            $rows = $mssqlRepo->fetchAfterByMapping($mapping, $afterKey, $batchSize, $deltaOnly);
            $this->lastSql = $mssqlRepo->getLastSql();
            $this->lastParams = $mssqlRepo->getLastParams();
        } catch (\Throwable $e) {
            $this->lastSql = $mssqlRepo->getLastSql();
            $this->lastParams = $mssqlRepo->getLastParams();
            throw $e;
        }

        $batchStats = [
            'batch_fetched' => count($rows),
            'inserted' => 0,
            'updated' => 0,
            'unchanged' => 0,
            'errors_count' => 0,
            'last_key' => $afterKey,
        ];

        if ($rows !== []) {
            $existingColumns = $this->loadExistingColumns($pdo, 'artikel');
            $preparedRows = [];
            $newColumns = [];
            $allowedLookup = array_flip($selectFields);

            $desiredColumns = $this->buildDesiredColumns($selectFields);
            foreach ($desiredColumns as $column => $type) {
                if (!isset($existingColumns[strtolower($column)])) {
                    $newColumns[$column] = $type;
                }
            }

            $extraKeys = array_values(array_unique(array_merge(
                $this->getRawFieldNames($selectFields),
                ['seo_url', 'master_modell', 'is_master']
            )));
            sort($extraKeys, SORT_STRING);
            $diffColumns = $extraKeys;

            $wgSeoMap = $this->loadWarengruppeSeoMap($pdo);
            $attributesBuilder = new AttributesBuilder($pdo);
            $masterCount = 0;
            $slaveCount = 0;
            $normalCount = 0;
            $attributeAssignmentsWritten = 0;
            $mediaAssignmentsWritten = 0;
            $warengruppeAssignmentsWritten = 0;

            foreach ($rows as $row) {
                $row = array_intersect_key($row, $allowedLookup);
                $afsArtikelId = isset($row[$keyField]) ? (string)$row[$keyField] : '';
                if ($afsArtikelId === '') {
                    $preparedRows[] = ['error' => 'missing_key', 'row' => $row];
                    continue;
                }
                $preparedRows[] = [
                    'row' => $row,
                    'extras' => $this->extractRawFields($row, $extraKeys),
                ];
            }

            if ($newColumns !== []) {
                foreach ($newColumns as $column => $type) {
                    if (!$this->schemaHelper->columnExists($pdo, 'artikel', $column)) {
                        $pdo->exec('ALTER TABLE artikel ADD COLUMN ' . $this->quoteIdentifier($column) . ' ' . $type);
                        $existingColumns[strtolower($column)] = $column;
                    }
                }
            }

            $pdo->beginTransaction();
            try {
                foreach ($preparedRows as $prepared) {
                    if (isset($prepared['error'])) {
                        $batchStats['errors_count']++;
                        continue;
                    }
                    $row = $prepared['row'];
                    try {
                        $afsArtikelId = $this->requireStringField($row, $keyField);
                        $artikelnummer = $this->requireStringField($row, 'Artikelnummer');
                        $name = $this->requireStringField($row, 'Bezeichnung');
                        $warengruppeId = $this->optionalIntField($row['Warengruppe'] ?? null);

                        $extras = $this->fillMissingExtras($prepared['extras'], $extraKeys);
                        $rawLangtext = (string)($row['Langtext'] ?? '');
                        if ($rawLangtext !== '') {
                            $converted = rtfToHtmlSimple($rawLangtext);
                            $row['Langtext'] = $converted;
                            $extras['Langtext'] = $converted;
                        }

                        $artikelSlug = strtolower(xt_filterAutoUrlText_inline($name, 'de'));
                        $wgSeo = '';
                        if ($warengruppeId !== null && isset($wgSeoMap[$warengruppeId])) {
                            $wgSeo = (string)$wgSeoMap[$warengruppeId];
                        }
                        if ($wgSeo === '') {
                            if ($warengruppeId !== null) {
                                $this->logMissingWarengruppeOnce($warengruppeId, $artikelnummer);
                            }
                            $wgSeo = 'de';
                        }
                        $extras['seo_url'] = rtrim($wgSeo, '/') . ($artikelSlug !== '' ? '/' . $artikelSlug : '');

                        $masterInfo = $this->resolveMasterInfo($row['Zusatzfeld07'] ?? null);
                        $extras['master_modell'] = $masterInfo['master_modell'];
                        $extras['is_master'] = $masterInfo['is_master'] ? 1 : null;
                        if ($masterInfo['is_master']) {
                            $masterCount++;
                        } elseif ($masterInfo['master_modell'] !== null) {
                            $slaveCount++;
                        } else {
                            $normalCount++;
                        }

                        $data = [
                            'afs_artikel_id' => $afsArtikelId,
                            'artikelnummer' => $artikelnummer,
                            'warengruppe_id' => $warengruppeId,
                            'master_modell' => $masterInfo['master_modell'],
                            'is_master' => $masterInfo['is_master'] ? 1 : null,
                        ];

                        $rowHash = $this->computeRowHash($data, $extras);

                        $result = $sqliteRepo->upsertArtikel($data, $seenAt, $extras, $extraKeys, $rowHash, $diffColumns);
                        if ($result['inserted']) $batchStats['inserted']++;
                        if ($result['updated']) $batchStats['updated']++;
                        if ($result['unchanged']) $batchStats['unchanged']++;
                        $afsQueue->add('artikel', $afsArtikelId);
                        $batchStats['masters'] = $masterCount;
                        $batchStats['slaves'] = $slaveCount;
                        $batchStats['normal'] = $normalCount;
                        $attributeAssignments = $attributesBuilder->ingestRowWithAssignments($row);
                        $attributeAssignmentsWritten += $relationWriter->syncAttributeAssignments($afsArtikelId, $attributeAssignments);

                        $mediaAssignments = $this->buildMediaAssignments($row, $seenAt, $relationWriter);
                        $mediaAssignmentsWritten += $relationWriter->syncMediaAssignments($afsArtikelId, $mediaAssignments);
                        $warengruppeAssignments = $this->buildWarengruppeAssignments($row);
                        $warengruppeAssignmentsWritten += $relationWriter->syncWarengruppeAssignments($afsArtikelId, $warengruppeAssignments);

                    } catch (\Throwable $e) {
                        $batchStats['errors_count']++;
                        $this->lastContext = [
                            'currentId' => $row['Artikelnummer'] ?? null,
                            'error' => $e->getMessage(),
                        ];
                        $this->log('Artikel-Import-Fehler: ' . $e->getMessage());
                    }
                }

                $lastRow = end($rows);
                $batchStats['last_key'] = isset($lastRow[$keyField]) ? (string)$lastRow[$keyField] : $afterKey;
                $pdo->commit();
                $batchStats['attributes_parents_created'] = $attributesBuilder->parentsCreated;
                $batchStats['attributes_children_created'] = $attributesBuilder->childrenCreated;
                $batchStats['attribute_assignments_written'] = $attributeAssignmentsWritten;
                $batchStats['media_assignments_written'] = $mediaAssignmentsWritten;
                $batchStats['warengruppe_assignments_written'] = $warengruppeAssignmentsWritten;
            } catch (\Throwable $e) {
                if ($pdo->inTransaction()) {
                    $pdo->rollBack();
                }
                throw $e;
            }
        }

        $isDone = $rows === [] || count($rows) < $batchSize;
        $state = $this->updateState($pdo, $batchStats, $isDone);
        if ((bool)$state['done'] && !$deltaOnly) {
            $deleted = $this->markDeletedMissing($pdo, (string)($state['started_at'] ?? ''));
            $batchStats['deleted_marked'] = $deleted;
        }

        return [
            'done' => (bool)$state['done'],
            'batch_size' => $batchSize,
            'batch_fetched' => $batchStats['batch_fetched'],
            'total_fetched' => (int)$state['total_fetched'],
            'inserted' => (int)$state['inserted'],
            'updated' => (int)$state['updated'],
            'unchanged' => (int)$state['unchanged'],
            'errors_count' => (int)$state['errors_count'],
            'batches' => (int)$state['batches'],
            'last_key' => (string)$state['last_key'],
            'deleted_marked' => $batchStats['deleted_marked'] ?? 0,
        ];
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

    public function getLastContext(): ?array
    {
        return $this->lastContext;
    }

    /**
     * @return array<string, mixed>
     */
    public function getState(): array
    {
        $pdo = $this->factory->localDb();
        $this->ensureStateTable($pdo);
        return $this->loadState($pdo);
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

    private function optionalIntField($value): ?int
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

    private function log(string $message): void
    {
        $timestamp = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $line = "[{$timestamp}] {$message}\n";
        $path = __DIR__ . '/../../../logs/app.log';
        @file_put_contents($path, $line, FILE_APPEND);
    }

    private function logMissingWarengruppeOnce(int $warengruppeId, string $artikelnummer): void
    {
        if (!is_dev_env()) {
            return;
        }

        static $logged = false;
        if ($logged) {
            return;
        }
        $logged = true;

        $this->log("missing wg mapping: warengruppe_id={$warengruppeId} artikelnummer={$artikelnummer}");
    }

    /**
     * @param array<string, mixed> $row
     * @return array<int, array{afs_wg_id:int,position:int,source_field:string}>
     */
    private function buildWarengruppeAssignments(array $row): array
    {
        $items = [];
        $seen = [];
        $position = 0;

        $add = static function (int $wgId, string $sourceField) use (&$items, &$seen, &$position): void {
            if ($wgId <= 0 || isset($seen[$wgId])) {
                return;
            }
            $seen[$wgId] = true;
            $items[] = [
                'afs_wg_id' => $wgId,
                'position' => $position++,
                'source_field' => $sourceField,
            ];
        };

        $primary = (int)trim((string)($row['Warengruppe'] ?? '0'));
        if ($primary > 0) {
            $add($primary, 'Warengruppe');
        }

        $multi = trim((string)($row['Warengruppen'] ?? ''));
        if ($multi !== '' && $items === []) {
            foreach (preg_split('/[^\d]+/', $multi) ?: [] as $token) {
                $token = trim((string)$token);
                if ($token === '') {
                    continue;
                }
                $add((int)$token, 'Warengruppen');
            }
        }

        return $items;
    }

    private function ensureStateTable(\PDO $pdo): void
    {
        if ($this->isMysql($pdo)) {
            $pdo->exec(
                'CREATE TABLE IF NOT EXISTS sync_state (
                    type VARCHAR(64) PRIMARY KEY,
                    last_key VARCHAR(255) NULL,
                    total_fetched INT NOT NULL DEFAULT 0,
                    inserted INT NOT NULL DEFAULT 0,
                    updated INT NOT NULL DEFAULT 0,
                    unchanged INT NOT NULL DEFAULT 0,
                    errors_count INT NOT NULL DEFAULT 0,
                    batches INT NOT NULL DEFAULT 0,
                    started_at VARCHAR(64) NULL,
                    updated_at VARCHAR(64) NULL,
                    done TINYINT NOT NULL DEFAULT 0,
                    delta_only TINYINT NOT NULL DEFAULT 0
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->ensureSyncStateDeltaOnlyColumn($pdo);
            return;
        }
        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS sync_state (
                type TEXT PRIMARY KEY,
                last_key TEXT,
                total_fetched INTEGER,
                inserted INTEGER,
                updated INTEGER,
                unchanged INTEGER,
                errors_count INTEGER,
                batches INTEGER,
                started_at TEXT,
                updated_at TEXT,
                done INTEGER,
                delta_only INTEGER DEFAULT 0
            )'
        );
        $this->ensureSyncStateDeltaOnlyColumn($pdo);
    }

    private function resetState(\PDO $pdo): void
    {
        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $stmt = $pdo->prepare($this->isMysql($pdo)
            ? 'INSERT INTO sync_state (type, last_key, total_fetched, inserted, updated, unchanged, errors_count, batches, started_at, updated_at, done, delta_only)
               VALUES (:type, :last_key, 0, 0, 0, 0, 0, 0, :started_at, :updated_at, 0, 0)
               ON DUPLICATE KEY UPDATE
               last_key = VALUES(last_key),
               total_fetched = 0,
               inserted = 0,
               updated = 0,
               unchanged = 0,
               errors_count = 0,
               batches = 0,
               started_at = VALUES(started_at),
               updated_at = VALUES(updated_at),
               done = 0,
               delta_only = 0'
            : 'INSERT INTO sync_state (type, last_key, total_fetched, inserted, updated, unchanged, errors_count, batches, started_at, updated_at, done, delta_only)
               VALUES (:type, :last_key, 0, 0, 0, 0, 0, 0, :started_at, :updated_at, 0, 0)
               ON CONFLICT(type) DO UPDATE SET
               last_key = :last_key,
               total_fetched = 0,
               inserted = 0,
               updated = 0,
               unchanged = 0,
               errors_count = 0,
               batches = 0,
               started_at = :started_at,
               updated_at = :updated_at,
               done = 0,
               delta_only = 0');
        $stmt->execute([
            ':type' => self::STATE_TYPE,
            ':last_key' => '',
            ':started_at' => $now,
            ':updated_at' => $now,
        ]);
    }

    /**
     * @param array{batch_fetched:int,inserted:int,updated:int,unchanged:int,errors_count:int,last_key:string} $batchStats
     * @return array<string, mixed>
     */
    private function updateState(\PDO $pdo, array $batchStats, bool $done): array
    {
        $state = $this->loadState($pdo);
        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);

        $state['total_fetched'] = (int)$state['total_fetched'] + $batchStats['batch_fetched'];
        $state['inserted'] = (int)$state['inserted'] + $batchStats['inserted'];
        $state['updated'] = (int)$state['updated'] + $batchStats['updated'];
        $state['unchanged'] = (int)$state['unchanged'] + $batchStats['unchanged'];
        $state['errors_count'] = (int)$state['errors_count'] + $batchStats['errors_count'];
        $state['batches'] = (int)$state['batches'] + ($batchStats['batch_fetched'] > 0 ? 1 : 0);
        if ($batchStats['last_key'] !== '') {
            $state['last_key'] = $batchStats['last_key'];
        }
        if ($done) {
            $state['done'] = 1;
        }

        $stmt = $pdo->prepare($this->isMysql($pdo)
            ? 'INSERT INTO sync_state (type, last_key, total_fetched, inserted, updated, unchanged, errors_count, batches, started_at, updated_at, done, delta_only)
               VALUES (:type, :last_key, :total_fetched, :inserted, :updated, :unchanged, :errors_count, :batches, :started_at, :updated_at, :done, :delta_only)
               ON DUPLICATE KEY UPDATE
               last_key = VALUES(last_key),
               total_fetched = VALUES(total_fetched),
               inserted = VALUES(inserted),
               updated = VALUES(updated),
               unchanged = VALUES(unchanged),
               errors_count = VALUES(errors_count),
               batches = VALUES(batches),
               updated_at = VALUES(updated_at),
               done = VALUES(done),
               delta_only = VALUES(delta_only)'
            : 'INSERT INTO sync_state (type, last_key, total_fetched, inserted, updated, unchanged, errors_count, batches, started_at, updated_at, done, delta_only)
               VALUES (:type, :last_key, :total_fetched, :inserted, :updated, :unchanged, :errors_count, :batches, :started_at, :updated_at, :done, :delta_only)
               ON CONFLICT(type) DO UPDATE SET
               last_key = :last_key,
               total_fetched = :total_fetched,
               inserted = :inserted,
               updated = :updated,
               unchanged = :unchanged,
               errors_count = :errors_count,
               batches = :batches,
               updated_at = :updated_at,
               done = :done,
               delta_only = :delta_only');

        $stmt->execute([
            ':type' => self::STATE_TYPE,
            ':last_key' => $state['last_key'] ?? '',
            ':total_fetched' => $state['total_fetched'] ?? 0,
            ':inserted' => $state['inserted'] ?? 0,
            ':updated' => $state['updated'] ?? 0,
            ':unchanged' => $state['unchanged'] ?? 0,
            ':errors_count' => $state['errors_count'] ?? 0,
            ':batches' => $state['batches'] ?? 0,
            ':started_at' => $state['started_at'] ?? $now,
            ':updated_at' => $now,
            ':done' => $state['done'] ?? 0,
            ':delta_only' => $state['delta_only'] ?? 0,
        ]);

        return $state;
    }

    /**
     * @return array<string, true>
     */
    private function loadExistingColumns(\PDO $pdo, string $table): array
    {
        $stmt = $this->isMysql($pdo)
            ? $pdo->query('DESCRIBE ' . $this->quoteIdentifier($table))
            : $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC) ?: [];
        $columns = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? $row['Field'] ?? '');
            if ($name !== '') {
                $columns[strtolower($name)] = $name;
            }
        }
        return $columns;
    }


    private function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }

    private function markDeletedMissing(\PDO $pdo, string $startedAt): int
    {
        if ($startedAt === '') {
            return 0;
        }
        $stmt = $pdo->prepare(
            'UPDATE artikel
             SET is_deleted = 1,
                 changed = 1
             WHERE (last_seen_at IS NULL OR last_seen_at < :started_at)
               AND (is_deleted IS NULL OR is_deleted = 0)'
        );
        $stmt->execute([':started_at' => $startedAt]);
        $this->cleanupDeletedArticleRelations($pdo);
        return $stmt->rowCount();
    }

    private function cleanupDeletedArticleRelations(\PDO $pdo): void
    {
        $pdo->exec(
            'DELETE FROM artikel_attribute_map
             WHERE afs_artikel_id IN (
                 SELECT afs_artikel_id
                 FROM artikel
                 WHERE is_deleted = 1
             )'
        );
        $pdo->exec(
            'DELETE FROM artikel_media_map
             WHERE afs_artikel_id IN (
                 SELECT afs_artikel_id
                 FROM artikel
                 WHERE is_deleted = 1
             )'
        );
    }

    private function loadArtikelMapping(): array
    {
        $mappingLoader = new \Welafix\Config\MappingLoader();
        $all = $mappingLoader->loadAll();
        if (!isset($all['artikel'])) {
            throw new RuntimeException('Mapping "artikel" nicht gefunden.');
        }
        return $all['artikel'];
    }

    private function shouldUseSourceUpdateFilter(array $mapping, \PDO $pdo, string $table): bool
    {
        $hints = $mapping['hints'] ?? [];
        $updateColumn = is_array($hints) ? (string)($hints['on_update_column'] ?? '') : '';
        if ($updateColumn === '') {
            return false;
        }
        $count = (int)$pdo->query('SELECT COUNT(*) FROM ' . $this->quoteIdentifier($table))->fetchColumn();
        return $count > 0;
    }

    private function resolveDeltaOnlyMode(array $mapping, \PDO $pdo, string $table, string $afterKey): bool
    {
        if ($afterKey !== '') {
            $state = $this->loadState($pdo);
            return (int)($state['delta_only'] ?? 0) === 1;
        }

        $deltaOnly = $this->shouldUseSourceUpdateFilter($mapping, $pdo, $table);
        $stmt = $pdo->prepare('UPDATE sync_state SET delta_only = :delta_only WHERE type = :type');
        $stmt->execute([
            ':delta_only' => $deltaOnly ? 1 : 0,
            ':type' => self::STATE_TYPE,
        ]);
        return $deltaOnly;
    }

    private function ensureSyncStateDeltaOnlyColumn(\PDO $pdo): void
    {
        $stmt = $this->isMysql($pdo)
            ? $pdo->query('DESCRIBE ' . $this->quoteIdentifier('sync_state'))
            : $pdo->query('PRAGMA table_info(sync_state)');
        $rows = $stmt ? ($stmt->fetchAll(\PDO::FETCH_ASSOC) ?: []) : [];
        foreach ($rows as $row) {
            if (strcasecmp((string)($row['name'] ?? $row['Field'] ?? ''), 'delta_only') === 0) {
                return;
            }
        }
        $pdo->exec('ALTER TABLE sync_state ADD COLUMN delta_only ' . ($this->isMysql($pdo) ? 'TINYINT DEFAULT 0' : 'INTEGER DEFAULT 0'));
    }

    /**
     * @param array<string, mixed> $mapping
     * @return array<int, string>
     */
    private function getSelectFields(array $mapping): array
    {
        $mapping = new MappingService();
        return $mapping->getAllowedColumns('artikel');
    }

    /**
     * @param array<int, string> $selectFields
     * @return array<string, string>
     */
    private function buildDesiredColumns(array $selectFields): array
    {
        $columns = [
            'afs_artikel_id' => 'TEXT',
            'artikelnummer' => 'TEXT',
            'warengruppe_id' => 'INTEGER',
            'last_seen_at' => 'TEXT',
            'row_hash' => 'TEXT',
            'seo_url' => 'TEXT',
            'master_modell' => 'TEXT',
            'is_master' => 'INTEGER',
        ];

        $numericInt = ['Artikel', 'Art', 'Bestand', 'Warengruppe', 'Zusatzfeld01'];
        $numericReal = ['VK3', 'Umsatzsteuer', 'Bruttogewicht'];

        foreach ($selectFields as $field) {
            if (in_array($field, $numericInt, true)) {
                $columns[$field] = 'INTEGER';
            } elseif (in_array($field, $numericReal, true)) {
                $columns[$field] = 'REAL';
            } else {
                $columns[$field] = 'TEXT';
            }
        }
        return $columns;
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
     * @param array<string, mixed> $row
     * @return array<int, array{filename:string,position:int,is_main:int,source_field:string,media_id:?int}>
     */
    private function buildMediaAssignments(array $row, string $seenAt, ArtikelRelationWriter $relationWriter): array
    {
        $items = [];
        for ($i = 1; $i <= 10; $i++) {
            $field = 'Bild' . $i;
            $filename = normalizeMediaFilename(isset($row[$field]) ? (string)$row[$field] : null);
            if ($filename === null) {
                continue;
            }
            $mediaId = $relationWriter->ensureMedia($filename, 'article', $seenAt);
            $items[] = [
                'filename' => $filename,
                'position' => $i,
                'is_main' => $i === 1 ? 1 : 0,
                'source_field' => $field,
                'media_id' => $mediaId,
            ];
        }
        return $items;
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
     * @return array{is_master:bool, master_modell: ?string}
     */
    private function resolveMasterInfo(mixed $raw): array
    {
        $value = trim((string)($raw ?? ''));
        if ($value === '') {
            return ['is_master' => false, 'master_modell' => null];
        }
        if (strcasecmp($value, 'master') === 0) {
            return ['is_master' => true, 'master_modell' => null];
        }
        return ['is_master' => false, 'master_modell' => $value];
    }

    /**
     * @param array<string, mixed> $data
     * @param array<string, mixed> $extras
     */
    private function computeRowHash(array $data, array $extras): string
    {
        $payload = array_merge($data, $extras);
        ksort($payload, SORT_STRING);
        $json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($json === false) {
            $json = '';
        }
        return sha1($json);
    }

    /**
     * @return array<int, string>
     */
    private function loadWarengruppeSeoMap(\PDO $pdo): array
    {
        $stmt = $pdo->query('SELECT afs_wg_id, seo_url FROM warengruppe');
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC) ?: [];
        $map = [];
        foreach ($rows as $row) {
            $id = (int)($row['afs_wg_id'] ?? 0);
            if ($id !== 0) {
                $map[$id] = (string)($row['seo_url'] ?? '');
            }
        }
        return $map;
    }

    /**
     * @return array<string, mixed>
     */
    private function loadState(\PDO $pdo): array
    {
        $stmt = $pdo->prepare('SELECT * FROM sync_state WHERE type = :type LIMIT 1');
        $stmt->execute([':type' => self::STATE_TYPE]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        if (!$row) {
            return [
                'type' => self::STATE_TYPE,
                'last_key' => '',
                'total_fetched' => 0,
                'inserted' => 0,
                'updated' => 0,
                'unchanged' => 0,
                'errors_count' => 0,
                'batches' => 0,
                'started_at' => null,
                'updated_at' => null,
                'done' => 0,
            ];
        }
        return $row;
    }

    private function isMysql(\PDO $pdo): bool
    {
        return (string)$pdo->getAttribute(\PDO::ATTR_DRIVER_NAME) === 'mysql';
    }
}
