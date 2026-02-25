<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use DateTimeImmutable;
use DateTimeZone;
use RuntimeException;
use Welafix\Config\MappingLoader;
use Welafix\Database\ConnectionFactory;
use Welafix\Domain\FileDb\FileDbCache;

final class ArtikelSyncService
{
    private ConnectionFactory $factory;
    private ?string $lastSql = null;
    private ?array $lastContext = null;
    private array $lastParams = [];
    private const STATE_TYPE = 'artikel';
    private FileDbCache $fileDbCache;
    private const BASE_COLUMNS = [
        'afs_artikel_id',
        'afs_key',
        'artikelnummer',
        'name',
        'warengruppe_id',
        'price',
        'stock',
        'online',
        'row_hash',
        'last_seen_at',
        'changed',
        'change_reason',
        'id',
    ];

    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
        $this->fileDbCache = new FileDbCache();
    }

    /**
     * @return array{done:bool,batch_size:int,batch_fetched:int,total_fetched:int,inserted:int,updated:int,unchanged:int,errors_count:int,batches:int,last_key:string}
     */
    public function processBatch(string $afterKey, int $batchSize = 500): array
    {
        $pdo = $this->factory->sqlite();
        $this->ensureStateTable($pdo);

        if ($afterKey === '') {
            $this->resetState($pdo);
        }

        $mssqlRepo = new ArtikelRepositoryMssql($this->factory->mssql());
        $sqliteRepo = new ArtikelRepositorySqlite($pdo);
        $sqliteRepo->ensureTable();

        $batchSize = max(1, min(1000, $batchSize));
        $seenAt = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);

        $mapping = $this->loadArtikelMapping();
        $selectFields = $this->getSelectFields($mapping);
        $keyField = (string)(($mapping['source']['key'] ?? 'Artikel') ?: 'Artikel');

        try {
            $rows = $mssqlRepo->fetchAfterByMapping($mapping, $afterKey, $batchSize);
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

            $desiredColumns = $this->buildDesiredColumns($selectFields);
            foreach ($desiredColumns as $column => $type) {
                if (!$this->columnExists($existingColumns, $column)) {
                    $newColumns[$column] = $type;
                }
            }

            foreach ($rows as $row) {
                $afsArtikelId = isset($row[$keyField]) ? (string)$row[$keyField] : '';
                if ($afsArtikelId === '') {
                    $preparedRows[] = ['error' => 'missing_key', 'row' => $row];
                    continue;
                }
                $extras = $this->fileDbCache->getMerged('Artikel', $afsArtikelId);
                $normalizedExtras = $this->normalizeExtras($extras);
                $normalizedExtras = $this->filterBaseColumns($normalizedExtras, self::BASE_COLUMNS);
                foreach (array_keys($normalizedExtras) as $field) {
                    if (!$this->columnExists($existingColumns, $field)) {
                        $newColumns[$field] = 'TEXT';
                    }
                }
                $preparedRows[] = [
                    'row' => $row,
                    'extras' => $normalizedExtras,
                ];
            }

            if ($newColumns !== []) {
                foreach ($newColumns as $column => $type) {
                    if (!$this->columnExists($existingColumns, $column)) {
                        $pdo->exec('ALTER TABLE artikel ADD COLUMN ' . $this->quoteIdentifier($column) . ' ' . $type);
                        $existingColumns[strtolower($column)] = $column;
                    }
                }
            }

            $rawFieldNames = $this->getRawFieldNames($selectFields);
            $extraKeys = $this->buildExtraKeys($rawFieldNames, $preparedRows);

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

                        $rawFields = $this->extractRawFields($row, $rawFieldNames);
                        $extras = $this->fillMissingExtras(
                            array_merge($rawFields, $prepared['extras']),
                            $extraKeys
                        );

                        $data = [
                            'afs_artikel_id' => $afsArtikelId,
                            'afs_key' => $afsArtikelId,
                            'artikelnummer' => $artikelnummer,
                            'name' => $name,
                            'warengruppe_id' => $warengruppeId,
                            'price' => (float)($row['VK3'] ?? 0),
                            'stock' => (int)($row['Bestand'] ?? 0),
                            'online' => (int)($row['Internet'] ?? 0),
                        ];

                        $rowHash = $this->computeRowHash($data, $extras);

                        $result = $sqliteRepo->upsertArtikel($data, $seenAt, $extras, $extraKeys, $rowHash);
                        if ($result['inserted']) $batchStats['inserted']++;
                        if ($result['updated']) $batchStats['updated']++;
                        if ($result['unchanged']) $batchStats['unchanged']++;
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
            } catch (\Throwable $e) {
                $pdo->rollBack();
                throw $e;
            }
        }

        $state = $this->updateState($pdo, $batchStats, $rows === []);

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
        $pdo = $this->factory->sqlite();
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

    private function ensureStateTable(\PDO $pdo): void
    {
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
                done INTEGER
            )'
        );
    }

    private function resetState(\PDO $pdo): void
    {
        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $stmt = $pdo->prepare(
            'INSERT INTO sync_state (type, last_key, total_fetched, inserted, updated, unchanged, errors_count, batches, started_at, updated_at, done)
             VALUES (:type, :last_key, 0, 0, 0, 0, 0, 0, :started_at, :updated_at, 0)
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
               done = 0'
        );
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

        $stmt = $pdo->prepare(
            'INSERT INTO sync_state (type, last_key, total_fetched, inserted, updated, unchanged, errors_count, batches, started_at, updated_at, done)
             VALUES (:type, :last_key, :total_fetched, :inserted, :updated, :unchanged, :errors_count, :batches, :started_at, :updated_at, :done)
             ON CONFLICT(type) DO UPDATE SET
               last_key = :last_key,
               total_fetched = :total_fetched,
               inserted = :inserted,
               updated = :updated,
               unchanged = :unchanged,
               errors_count = :errors_count,
               batches = :batches,
               updated_at = :updated_at,
               done = :done'
        );

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
        ]);

        return $state;
    }

    /**
     * @return array<string, true>
     */
    private function loadExistingColumns(\PDO $pdo, string $table): array
    {
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC) ?: [];
        $columns = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $columns[strtolower($name)] = $name;
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
        return '\"' . str_replace('\"', '\"\"', $name) . '\"';
    }

    private function loadArtikelMapping(): array
    {
        $loader = new MappingLoader();
        $mappings = $loader->loadAll();
        if (!isset($mappings['artikel'])) {
            throw new RuntimeException('Mapping "artikel" nicht gefunden.');
        }
        return $mappings['artikel'];
    }

    /**
     * @param array<string, mixed> $mapping
     * @return array<int, string>
     */
    private function getSelectFields(array $mapping): array
    {
        $select = $mapping['select'] ?? [];
        return array_values(array_filter($select, static fn($value): bool => is_string($value) && $value !== ''));
    }

    /**
     * @param array<int, string> $selectFields
     * @return array<string, string>
     */
    private function buildDesiredColumns(array $selectFields): array
    {
        $columns = [
            'afs_artikel_id' => 'TEXT',
            'afs_key' => 'TEXT',
            'artikelnummer' => 'TEXT',
            'name' => 'TEXT',
            'warengruppe_id' => 'INTEGER',
            'price' => 'REAL',
            'stock' => 'INTEGER',
            'online' => 'INTEGER',
            'last_seen_at' => 'TEXT',
            'row_hash' => 'TEXT',
        ];

        $mapped = $this->getMappedFields();
        $numericInt = ['Artikel', 'Art', 'Bestand', 'Warengruppe', 'Zusatzfeld01'];
        $numericReal = ['VK3', 'Umsatzsteuer', 'Bruttogewicht'];

        foreach ($selectFields as $field) {
            if (isset($mapped[$field])) {
                continue;
            }
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
     * @return array<string, string>
     */
    private function getMappedFields(): array
    {
        return [
            'Artikel' => 'afs_artikel_id',
            'Artikelnummer' => 'artikelnummer',
            'Bezeichnung' => 'name',
            'Warengruppe' => 'warengruppe_id',
            'VK3' => 'price',
            'Bestand' => 'stock',
            'Internet' => 'online',
        ];
    }

    /**
     * @param array<string, string> $existingColumns
     */
    private function columnExists(array $existingColumns, string $column): bool
    {
        return isset($existingColumns[strtolower($column)]);
    }

    /**
     * @param array<int, string> $selectFields
     * @return array<int, string>
     */
    private function getRawFieldNames(array $selectFields): array
    {
        $mapped = $this->getMappedFields();
        $raw = [];
        foreach ($selectFields as $field) {
            if (isset($mapped[$field])) {
                continue;
            }
            $raw[] = $field;
        }
        return $raw;
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
     * @param array<int, string> $rawFieldNames
     * @param array<int, array<string, mixed>> $preparedRows
     * @return array<int, string>
     */
    private function buildExtraKeys(array $rawFieldNames, array $preparedRows): array
    {
        $keys = [];
        foreach ($rawFieldNames as $field) {
            $keys[strtolower($field)] = $field;
        }
        foreach ($preparedRows as $prepared) {
            if (!isset($prepared['extras']) || !is_array($prepared['extras'])) {
                continue;
            }
            foreach (array_keys($prepared['extras']) as $field) {
                $keys[strtolower($field)] = $field;
            }
        }
        $extraKeys = array_values($keys);
        sort($extraKeys, SORT_STRING);
        return $extraKeys;
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
}
