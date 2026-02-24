<?php
declare(strict_types=1);

namespace Welafix\Domain\Warengruppe;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Config\MappingLoader;
use Welafix\Database\ConnectionFactory;

final class WarengruppeSyncService
{
    private ConnectionFactory $factory;
    private ?string $lastSql = null;
    private ?array $lastContext = null;

    public function __construct(ConnectionFactory $factory)
    {
        $this->factory = $factory;
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

        $mssqlRepo = new WarengruppeRepositoryMssql($this->factory->mssql());
        $sqliteRepo = new WarengruppeRepositorySqlite($this->factory->sqlite());

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

        foreach ($rows as $row) {
            try {
                $afsWgId = $this->requireIntField($row, 'Warengruppe');
                $name = $this->requireStringField($row, 'Bezeichnung');
                $parentId = $this->optionalParentId($row['Anhang'] ?? null);

                $result = $sqliteRepo->upsert($afsWgId, $name, $parentId, $seenAt);
                if ($result['inserted']) $stats['inserted']++;
                if ($result['updated']) $stats['updated']++;
                if ($result['unchanged']) $stats['unchanged']++;
            } catch (\Throwable $e) {
                $stats['errors_count']++;
                $this->log('Import-Fehler: ' . $e->getMessage());
            }
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
        foreach ($items as $id => $item) {
            $idInt = (int)$id;
            [$path, $pathIds] = $this->buildPath($idInt, $items);
            if ($repo->updatePath($idInt, $path, $pathIds)) {
                $updated++;
            }
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
        $filteredNames = array_values(array_filter($names, static fn(string $value): bool => $value !== ''));

        $path = implode(' > ', $filteredNames);
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

    private function log(string $message): void
    {
        $timestamp = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $line = "[{$timestamp}] {$message}\n";
        $path = __DIR__ . '/../../../logs/app.log';
        @file_put_contents($path, $line, FILE_APPEND);
    }
}
