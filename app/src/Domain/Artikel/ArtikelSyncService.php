<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use DateTimeImmutable;
use DateTimeZone;
use RuntimeException;
use Welafix\Database\ConnectionFactory;

final class ArtikelSyncService
{
    private ConnectionFactory $factory;
    private ?string $lastSql = null;
    private ?array $lastContext = null;
    private array $lastParams = [];
    private const STATE_TYPE = 'artikel';

    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
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

        try {
            $rows = $mssqlRepo->fetchAfter($afterKey, $batchSize);
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
            foreach ($rows as $row) {
                try {
                    $afsKey = $this->requireStringField($row, 'Artikelnummer');
                    $name = $this->requireStringField($row, 'Bezeichnung');
                    $warengruppeId = $this->optionalIntField($row['Warengruppe'] ?? null);

                    $data = [
                        'afs_key' => $afsKey,
                        'artikelnummer' => $afsKey,
                        'name' => $name,
                        'warengruppe_id' => $warengruppeId,
                        'price' => (float)($row['VK3'] ?? 0),
                        'stock' => (int)($row['Bestand'] ?? 0),
                        'online' => (int)($row['Internet'] ?? 0),
                    ];

                    $result = $sqliteRepo->upsertArtikel($data, $seenAt);
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
            $batchStats['last_key'] = isset($lastRow['Artikelnummer']) ? (string)$lastRow['Artikelnummer'] : $afterKey;
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
