<?php
declare(strict_types=1);

namespace Welafix\Domain\Dokument;

use PDO;
use RuntimeException;
use Welafix\Config\MappingService;
use Welafix\Database\Db;
use Welafix\Database\SchemaSyncService;
use Welafix\Infrastructure\Sqlite\SqliteSchemaHelper;

final class DokumentSyncService
{
    private ?string $lastSql = null;
    private SchemaSyncService $schemaSync;
    private SqliteSchemaHelper $schemaHelper;

    public function __construct()
    {
        $this->schemaSync = new SchemaSyncService();
        $this->schemaHelper = new SqliteSchemaHelper();
    }

    /**
     * @return array<string, mixed>
     */
    public function run(): array
    {
        $mappingLoader = new \Welafix\Config\MappingLoader();
        $mappings = $mappingLoader->loadAll();
        if (!isset($mappings['dokument'])) {
            throw new RuntimeException('Mapping "dokument" nicht gefunden.');
        }
        $mapping = $mappings['dokument'];
        $selectFields = $this->getSelectFields();
        $mapping['select'] = $selectFields;

        $mssql = Db::guardMssql(Db::mssql(), __METHOD__);
        $sqlite = Db::guardSqlite(Db::sqlite(), __METHOD__);

        $this->schemaSync->ensureSqliteColumnsMatchMssql($mssql, $sqlite, 'dbo.Dokument', 'documents', $selectFields);
        $this->ensureExtraColumns($sqlite);

        $repo = new DokumentRepositoryMssql($mssql);
        $rows = $repo->fetchAllByMapping($mapping);
        $this->lastSql = $repo->getLastSql();

        $stats = [
            'ok' => true,
            'total' => 0,
            'changedSet' => 0,
            'inserted' => 0,
            'updated' => 0,
        ];

        $selectExisting = $sqlite->prepare('SELECT * FROM documents WHERE source = :source AND source_id = :key LIMIT 1');
        $insert = $this->prepareInsert($sqlite, $selectFields);
        $updateChanged = $this->prepareUpdate($sqlite, $selectFields, true);

        $sqlite->beginTransaction();
        try {
            foreach ($rows as $row) {
                $stats['total']++;
                if (isset($row['Dateiname'])) {
                    $row['Dateiname'] = normalizeMediaFilename((string)$row['Dateiname']);
                }
                if (isset($row['Titel'])) {
                    $row['Titel'] = normalizeMediaFilename((string)$row['Titel']);
                }
                $row = $this->filterRow($row, $selectFields);
                $key = (string)($row['Zaehler'] ?? '');
                if ($key === '') {
                    continue;
                }

                $selectExisting->execute([':source' => 'AFS_DOKUMENT', ':key' => $key]);
                $existing = $selectExisting->fetch(PDO::FETCH_ASSOC) ?: null;

                $hasChanged = false;
                if ($existing === null) {
                    $hasChanged = true;
                } else {
                    foreach ($selectFields as $field) {
                        $old = (string)($existing[$field] ?? '');
                        $new = (string)($row[$field] ?? '');
                        if ($old !== $new) {
                            $hasChanged = true;
                            break;
                        }
                    }
                }

                if ($existing === null) {
                    $params = $this->buildParams($row, $selectFields, $key, true);
                    $this->executeWithLog($insert, $params);
                    $stats['inserted']++;
                } else {
                    if (!$hasChanged) {
                        continue;
                    }
                    $params = $this->buildParams($row, $selectFields, $key, false);
                    if ($hasChanged) {
                        $this->executeWithLog($updateChanged, $params);
                        $stats['changedSet']++;
                    }
                    $stats['updated']++;
                }
            }
            $sqlite->commit();
        } catch (\Throwable $e) {
            $sqlite->rollBack();
            $this->logSqlError($e, $insert->queryString ?? null, $params ?? null);
            throw $e;
        }

        return $stats;
    }

    public function getLastSql(): ?string
    {
        return $this->lastSql;
    }

    /**
     * @return array<int, string>
     */
    private function getSelectFields(): array
    {
        $mapping = new MappingService();
        return $mapping->getAllowedColumns('dokument');
    }

    /**
     * @param array<int, string> $selectFields
     */
    private function ensureExtraColumns(PDO $pdo): void
    {
        $extra = [
            'changed' => 'INTEGER DEFAULT 0',
        ];
        foreach ($extra as $column => $type) {
            if (!$this->schemaHelper->columnExists($pdo, 'documents', $column)) {
                $pdo->exec('ALTER TABLE documents ADD COLUMN ' . $this->quoteIdentifier($column) . ' ' . $type);
            }
        }
    }

    /**
     * @param array<string, mixed> $row
     * @param array<int, string> $selectFields
     */
    private function filterRow(array $row, array $selectFields): array
    {
        $filtered = [];
        foreach ($selectFields as $col) {
            if (array_key_exists($col, $row)) {
                $filtered[$col] = $row[$col];
            }
        }
        return $filtered;
    }

    private function prepareInsert(PDO $pdo, array $selectFields): \PDOStatement
    {
        $cols = array_merge(['source', 'source_id', 'doc_type'], $selectFields, ['changed']);
        $params = array_map(static fn(string $c): string => ':' . $c, $cols);
        $sql = 'INSERT INTO documents (' . implode(',', $cols) . ') VALUES (' . implode(',', $params) . ')';
        return $pdo->prepare($sql);
    }

    private function prepareUpdate(PDO $pdo, array $selectFields, bool $setChanged): \PDOStatement
    {
        $sets = [];
        foreach ($selectFields as $col) {
            $sets[] = $this->quoteIdentifier($col) . ' = :' . $col;
        }
        if ($setChanged) {
            $sets[] = 'changed = 1';
        }
        $sql = 'UPDATE documents SET ' . implode(', ', $sets) . ' WHERE source = :source AND source_id = :_key';
        return $pdo->prepare($sql);
    }

    /**
     * @param array<string, mixed> $row
     * @param array<int, string> $selectFields
     * @return array<string, mixed>
     */
    private function buildParams(array $row, array $selectFields, string $key, bool $insert): array
    {
        $params = [];
        foreach ($selectFields as $col) {
            $params[':' . $col] = $row[$col] ?? null;
        }
        $params[':source'] = 'AFS_DOKUMENT';
        if ($insert) {
            $params[':source_id'] = $key;
            $params[':doc_type'] = $row['Art'] ?? 'AFS_DOKUMENT';
        }
        if ($insert) {
            $params[':changed'] = 1;
        } else {
            $params[':_key'] = $key;
        }
        return $params;
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    /**
     * @param array<string, mixed> $params
     */
    private function executeWithLog(\PDOStatement $stmt, array $params): void
    {
        try {
            $stmt->execute($params);
        } catch (\Throwable $e) {
            $this->logSqlError($e, $stmt->queryString ?? null, $params);
            throw $e;
        }
    }

    /**
     * @param array<string, mixed>|null $params
     */
    private function logSqlError(\Throwable $e, ?string $sql, ?array $params): void
    {
        $payload = [
            'error' => $e->getMessage(),
            'sql' => $sql,
            'params' => $params,
        ];
        $line = 'dokument-sync sql error: ' . json_encode($payload);
        if (PHP_SAPI === 'cli') {
            echo $line . "\n";
        } else {
            error_log($line);
        }
    }
}
