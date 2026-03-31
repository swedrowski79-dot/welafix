<?php
declare(strict_types=1);

namespace Welafix\Domain\FileDb;

use DateTimeImmutable;
use DateTimeZone;
use PDO;

final class FileDbTemplateApplier
{
    private string $baseArtikel;
    private string $baseWarengruppe;
    /** @var array<string, array<string, string>> */
    private array $dirCache = [];
    /** @var array<string, array<string, true>> */
    private array $columnCache = [];
    /** @var array<string, \PDOStatement> */
    private array $updateCache = [];

    public function __construct(?string $basePath = null)
    {
        $base = $basePath ?? (string)env('FILEDB_PATH', __DIR__ . '/../../../storage/data');
        $this->baseArtikel = rtrim($base, '/\\') . '/Artikel';
        $this->baseWarengruppe = rtrim($base, '/\\') . '/Warengruppen';
    }

    public function importArtikelDirectories(PDO $pdo): array
    {
        if (!$this->isReadEnabled()) {
            return ['imported' => 0, 'directories' => 0];
        }
        return $this->importDirectories($pdo, $this->baseArtikel, 'artikel_extra_data', 'Artikelnummer');
    }

    public function importWarengruppeDirectories(PDO $pdo): array
    {
        if (!$this->isReadEnabled()) {
            return ['imported' => 0, 'directories' => 0];
        }
        return $this->importDirectories($pdo, $this->baseWarengruppe, 'warengruppe_extra_data', 'warengruppenname');
    }

    private function isReadEnabled(): bool
    {
        $mode = strtolower((string)env('FILEDB_MODE', 'read'));
        return $mode === 'read';
    }

    /**
     * @return array<string, string>
     */
    private function loadTemplates(string $dir): array
    {
        if (isset($this->dirCache[$dir])) {
            return $this->dirCache[$dir];
        }
        $templates = [];
        if (is_dir($dir)) {
            $items = scandir($dir);
            if (is_array($items)) {
                foreach ($items as $item) {
                    if ($item === '.' || $item === '..') {
                        continue;
                    }
                    if (strtolower(pathinfo($item, PATHINFO_EXTENSION)) !== 'txt') {
                        continue;
                    }
                    $field = pathinfo($item, PATHINFO_FILENAME);
                    if ($field === '') {
                        continue;
                    }
                    $path = $dir . '/' . $item;
                    if (!is_file($path)) {
                        continue;
                    }
                    $value = (string)file_get_contents($path);
                    $templates[$field] = trim($value);
                }
            }
        }
        $this->dirCache[$dir] = $templates;
        return $templates;
    }

    /**
     * @param array<string, string> $values
     */
    private function upsertRow(PDO $pdo, string $table, string $keyColumn, string $keyValue, array $values): void
    {
        if ($values === []) {
            return;
        }

        $this->ensureColumns($pdo, $table, array_keys($values));

        $columns = array_keys($values);
        $cacheKey = $table . '|' . implode(',', $columns) . '|' . $keyColumn . '|upsert';
        if (!isset($this->updateCache[$cacheKey])) {
            $insertColumns = [$this->quoteIdentifier($keyColumn)];
            $insertParams = [':_key'];
            $updates = [];
            foreach ($columns as $col) {
                $insertColumns[] = $this->quoteIdentifier($col);
                $insertParams[] = ':' . $col;
                $updates[] = $this->quoteIdentifier($col) . ' = excluded.' . $this->quoteIdentifier($col);
            }
            $sql = 'INSERT INTO ' . $this->quoteIdentifier($table) .
                ' (' . implode(', ', $insertColumns) . ') VALUES (' . implode(', ', $insertParams) . ')' .
                ' ON CONFLICT(' . $this->quoteIdentifier($keyColumn) . ') DO UPDATE SET ' . implode(', ', $updates);
            $this->updateCache[$cacheKey] = $pdo->prepare($sql);
        }

        $stmt = $this->updateCache[$cacheKey];
        foreach ($values as $col => $value) {
            $stmt->bindValue(':' . $col, $value, PDO::PARAM_STR);
        }
        $stmt->bindValue(':_key', $keyValue, PDO::PARAM_STR);
        $this->executeWithRetry($stmt);
    }

    /**
     * @return array{imported:int,directories:int}
     */
    private function importDirectories(PDO $pdo, string $baseDir, string $table, string $keyColumn): array
    {
        if (!is_dir($baseDir)) {
            return ['imported' => 0, 'directories' => 0];
        }

        $items = scandir($baseDir);
        if (!is_array($items)) {
            return ['imported' => 0, 'directories' => 0];
        }

        $directories = 0;
        $imported = 0;
        foreach ($items as $item) {
            if ($item === '.' || $item === '..') {
                continue;
            }
            $dir = $baseDir . '/' . $item;
            if (!is_dir($dir)) {
                continue;
            }

            $directories++;
            $templates = $this->loadTemplates($dir);
            if ($templates === []) {
                continue;
            }

            $values = $templates;
            $values['updated_at'] = $this->nowIso();
            $this->upsertRow($pdo, $table, $keyColumn, $item, $values);
            $imported++;
        }

        return ['imported' => $imported, 'directories' => $directories];
    }

    /**
     * @param array<int, string> $columns
     */
    private function ensureColumns(PDO $pdo, string $table, array $columns): void
    {
        if (!isset($this->columnCache[$table])) {
            $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
            $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
            $map = [];
            foreach ($rows as $row) {
                $name = strtolower((string)($row['name'] ?? ''));
                if ($name !== '') {
                    $map[$name] = true;
                }
            }
            $this->columnCache[$table] = $map;
        }

        foreach ($columns as $col) {
            $key = strtolower($col);
            if (isset($this->columnCache[$table][$key])) {
                continue;
            }
            $pdo->exec('ALTER TABLE ' . $this->quoteIdentifier($table) . ' ADD COLUMN ' . $this->quoteIdentifier($col) . ' TEXT');
            $this->columnCache[$table][$key] = true;
        }
    }

    private function executeWithRetry(\PDOStatement $stmt, int $maxAttempts = 5): void
    {
        $attempt = 0;
        while (true) {
            try {
                $stmt->execute();
                return;
            } catch (\PDOException $e) {
                $attempt++;
                $message = strtolower($e->getMessage());
                if ($attempt >= $maxAttempts || strpos($message, 'database is locked') === false) {
                    throw $e;
                }
                usleep(150000);
            }
        }
    }

    private function nowIso(): string
    {
        return (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
