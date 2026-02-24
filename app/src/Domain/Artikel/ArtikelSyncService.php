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

    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
    }

    /**
     * @return array{total_fetched:int,inserted:int,updated:int,unchanged:int,errors_count:int}
     */
    public function runImport(int $limit = 100): array
    {
        $mssqlRepo = new ArtikelRepositoryMssql($this->factory->mssql());
        $sqliteRepo = new ArtikelRepositorySqlite($this->factory->sqlite());
        $sqliteRepo->ensureTable();

        try {
            $rows = $mssqlRepo->fetchTop($limit);
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
            'errors_count' => 0,
        ];

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
                if ($result['inserted']) $stats['inserted']++;
                if ($result['updated']) $stats['updated']++;
                if ($result['unchanged']) $stats['unchanged']++;
            } catch (\Throwable $e) {
                $stats['errors_count']++;
                $this->lastContext = [
                    'currentId' => $row['Artikelnummer'] ?? null,
                    'error' => $e->getMessage(),
                ];
                $this->log('Artikel-Import-Fehler: ' . $e->getMessage());
            }
        }

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
}
