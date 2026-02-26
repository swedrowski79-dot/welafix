<?php
declare(strict_types=1);

namespace Welafix\Domain\FileDb;

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

    /**
     * @param array<string, mixed> $context
     */
    public function applyArtikel(PDO $pdo, string $artikelnummer, array $context): void
    {
        if (!$this->isReadEnabled()) {
            return;
        }
        $artikelnummer = trim($artikelnummer);
        if ($artikelnummer === '') {
            return;
        }
        $dir = $this->baseArtikel . '/' . $artikelnummer;
        if (is_dir($dir)) {
            $templates = $this->loadTemplates($dir);
        } else {
            $templates = $this->loadTemplates($this->baseArtikel . '/Standard');
        }
        if ($templates === []) {
            return;
        }
        $values = $this->renderTemplates($templates, $context);
        $this->updateRow($pdo, 'artikel', 'artikelnummer', $artikelnummer, $values);
    }

    /**
     * @param array<string, mixed> $context
     */
    public function applyWarengruppe(PDO $pdo, int $afsWgId, string $bezeichnung, array $context): void
    {
        if (!$this->isReadEnabled()) {
            return;
        }
        $bezeichnung = trim($bezeichnung);
        if ($bezeichnung === '') {
            return;
        }
        $dir = $this->baseWarengruppe . '/' . $bezeichnung;
        if (is_dir($dir)) {
            $templates = $this->loadTemplates($dir);
        } else {
            $templates = $this->loadTemplates($this->baseWarengruppe . '/Standard');
        }
        if ($templates === []) {
            return;
        }
        $values = $this->renderTemplates($templates, $context);
        $this->updateRow($pdo, 'warengruppe', 'afs_wg_id', (string)$afsWgId, $values);
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
     * @param array<string, string> $templates
     * @param array<string, mixed> $context
     * @return array<string, string>
     */
    private function renderTemplates(array $templates, array $context): array
    {
        $rendered = [];
        foreach ($templates as $field => $template) {
            $value = preg_replace_callback('/\\{\\{([A-Za-z0-9_]+)\\}\\}/', function (array $matches) use ($context): string {
                $key = $matches[1] ?? '';
                if ($key === '') {
                    return '';
                }
                $val = $context[$key] ?? '';
                if ($val === null) {
                    return '';
                }
                return (string)$val;
            }, $template);
            if ($value === null) {
                $value = $template;
            }
            $rendered[$field] = $value;
        }
        return $rendered;
    }

    /**
     * @param array<string, string> $values
     */
    private function updateRow(PDO $pdo, string $table, string $keyColumn, string $keyValue, array $values): void
    {
        if ($values === []) {
            return;
        }

        $this->ensureColumns($pdo, $table, array_keys($values));

        $columns = array_keys($values);
        $cacheKey = $table . '|' . implode(',', $columns) . '|' . $keyColumn;
        if (!isset($this->updateCache[$cacheKey])) {
            $sets = [];
            foreach ($columns as $col) {
                $sets[] = $this->quoteIdentifier($col) . ' = :' . $col;
            }
            $sql = 'UPDATE ' . $this->quoteIdentifier($table) . ' SET ' . implode(', ', $sets) .
                ' WHERE ' . $this->quoteIdentifier($keyColumn) . ' = :_key';
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

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
