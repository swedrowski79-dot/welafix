<?php
declare(strict_types=1);

namespace Welafix\Domain\Export;

use PDO;
use RuntimeException;
use Welafix\Config\MappingService;
use Welafix\Database\Db;
use Welafix\Logger\TemplateVarLogger;
use Welafix\Template\PlaceholderRenderer;

final class TemplateExportService
{
    private PlaceholderRenderer $renderer;
    private TemplateVarLogger $logger;
    private string $outputDir;
    private string $templateDir;

    public function __construct(?PlaceholderRenderer $renderer = null, ?TemplateVarLogger $logger = null)
    {
        $this->renderer = $renderer ?? new PlaceholderRenderer();
        $this->logger = $logger ?? new TemplateVarLogger();
        $this->outputDir = (string)env('TEMPLATE_OUTPUT_DIR', __DIR__ . '/../../../storage/exports');
        $this->templateDir = (string)env('TEMPLATE_SOURCE_DIR', __DIR__ . '/../../../storage/templates');
    }

    public function exportArtikelTemplates(): void
    {
        $pdo = Db::guardSqlite(Db::sqlite(), __METHOD__);
        $templates = $this->loadTemplates($pdo);
        $template = $this->pickTemplate($templates, 'artikel');
        if ($template === null) {
            return;
        }

        $mappingService = new MappingService();
        $allowed = $mappingService->getAllowedColumns('artikel');
        $desired = array_values(array_unique(array_merge($allowed, [
            'afs_artikel_id',
            'artikelnummer',
            'name',
            'warengruppe_id',
        ])));
        $existingCols = $this->fetchSqliteColumns($pdo, 'artikel');
        $existingLookup = [];
        foreach ($existingCols as $col) {
            $existingLookup[strtolower($col)] = $col;
        }
        $selectCols = [];
        foreach ($desired as $col) {
            $key = strtolower($col);
            if (isset($existingLookup[$key])) {
                $selectCols[] = 'a.' . $this->quoteIdentifier($existingLookup[$key]);
            }
        }
        if ($selectCols === []) {
            $selectCols = [
                'a.' . $this->quoteIdentifier('afs_artikel_id'),
                'a.' . $this->quoteIdentifier('artikelnummer'),
                'a.' . $this->quoteIdentifier('name'),
                'a.' . $this->quoteIdentifier('warengruppe_id'),
            ];
        }
        $selectCols[] = 'w.path AS warengruppe_path';
        $rows = $pdo->query(
            'SELECT ' . implode(', ', $selectCols) . '
             FROM artikel a
             LEFT JOIN warengruppe w ON w.afs_wg_id = a.warengruppe_id'
        )?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $dir = $this->outputDir . '/artikel';
        $this->ensureDir($dir);

        foreach ($rows as $row) {
            $identity = $this->buildArtikelIdentity($row);
            $lang = 'de';
            $categoryNames = [];
            $pathRaw = (string)($row['warengruppe_path'] ?? '');
            if ($pathRaw !== '') {
                $categoryNames = array_values(array_filter(array_map('trim', explode('/', $pathRaw))));
            }
            $segments = [];
            foreach ($categoryNames as $categoryName) {
                $slug = xt_filterAutoUrlText_inline($categoryName, $lang);
                $slug = strtolower($slug);
                if ($slug !== '') {
                    $segments[] = $slug;
                }
            }
            $productName = (string)($row['Bezeichnung'] ?? $row['name'] ?? '');
            $prodSlug = xt_filterAutoUrlText_inline($productName, $lang, '-', 'product', '0');
            $prodSlug = strtolower($prodSlug);
            $path = implode('/', $segments);
            if ($path !== '') {
                $path = $path . '/' . $prodSlug;
            } else {
                $path = $prodSlug;
            }
            $seoUrl = rtrim($lang, '/') . '/' . ltrim($path, '/');
            $row['seo_url'] = $seoUrl;
            $result = $this->renderer->render($template, $row, 'artikel', $identity);
            if ($result['missing'] !== []) {
                $this->logger->logMissing('artikel', $identity, $result['missing']);
            }
            $filename = $this->artikelFilename($row);
            if ($filename === '') {
                continue;
            }
            file_put_contents($dir . '/' . $filename, $result['rendered']);
        }
    }

    public function exportWarengruppeTemplates(): void
    {
        $pdo = Db::guardSqlite(Db::sqlite(), __METHOD__);
        $templates = $this->loadTemplates($pdo);
        $template = $this->pickTemplate($templates, 'warengruppe');
        if ($template === null) {
            return;
        }

        $cols = $this->fetchSqliteColumns($pdo, 'warengruppe');
        if ($cols === []) {
            $cols = ['afs_wg_id', 'name', 'parent_id', 'path', 'path_ids', 'last_seen_at', 'changed', 'change_reason'];
        }
        $selectList = implode(', ', array_map([$this, 'quoteIdentifier'], $cols));
        $rows = $pdo->query('SELECT ' . $selectList . ' FROM warengruppe')?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $dir = $this->outputDir . '/warengruppe';
        $this->ensureDir($dir);

        foreach ($rows as $row) {
            $identity = $this->buildWarengruppeIdentity($row);
            $result = $this->renderer->render($template, $row, 'warengruppe', $identity);
            if ($result['missing'] !== []) {
                $this->logger->logMissing('warengruppe', $identity, $result['missing']);
            }
            $filename = $this->warengruppeFilename($row);
            if ($filename === '') {
                continue;
            }
            file_put_contents($dir . '/' . $filename, $result['rendered']);
        }
    }

    /**
     * @return array<string, string>
     */
    private function loadTemplates(PDO $sqlite): array
    {
        $table = $this->findTemplateProvider($sqlite);
        if ($table !== null) {
            return $this->loadTemplatesFromTable($sqlite, $table);
        }
        return $this->loadTemplatesFromFiles();
    }

    private function findTemplateProvider(PDO $sqlite): ?string
    {
        $stmt = $sqlite->query(
            "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%template%' OR name LIKE '%text%' OR name LIKE '%export%')"
        );
        $tables = $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
        foreach ($tables as $table) {
            $table = (string)$table;
            if ($table !== '') {
                return $table;
            }
        }
        return null;
    }

    /**
     * @return array<string, string>
     */
    private function loadTemplatesFromTable(PDO $sqlite, string $table): array
    {
        $cols = $this->fetchSqliteColumns($sqlite, $table);
        if ($cols === []) {
            return [];
        }

        $nameCol = $this->pickColumn($cols, ['name', 'key', 'filename']);
        $contentCol = $this->pickColumn($cols, ['content', 'template', 'body']);
        if ($nameCol === null || $contentCol === null) {
            return [];
        }

        $sql = 'SELECT ' . $this->quoteIdentifier($nameCol) . ', ' . $this->quoteIdentifier($contentCol) .
            ' FROM ' . $this->quoteIdentifier($table);
        $rows = $sqlite->query($sql)?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $templates = [];
        foreach ($rows as $row) {
            $name = (string)($row[$nameCol] ?? '');
            $content = (string)($row[$contentCol] ?? '');
            if ($name !== '' && $content !== '') {
                $templates[$name] = $content;
            }
        }
        return $templates;
    }

    /**
     * @return array<string, string>
     */
    private function loadTemplatesFromFiles(): array
    {
        if (!is_dir($this->templateDir)) {
            return [];
        }
        $templates = [];
        foreach (glob($this->templateDir . '/*.txt') ?: [] as $file) {
            $name = pathinfo($file, PATHINFO_FILENAME);
            if ($name === '') {
                continue;
            }
            $content = (string)file_get_contents($file);
            if ($content === '') {
                continue;
            }
            $templates[$name] = $content;
        }
        return $templates;
    }

    /**
     * @param array<int, string> $cols
     */
    private function pickColumn(array $cols, array $candidates): ?string
    {
        $lookup = array_change_key_case(array_fill_keys($cols, true), CASE_LOWER);
        foreach ($candidates as $candidate) {
            if (isset($lookup[strtolower($candidate)])) {
                foreach ($cols as $col) {
                    if (strtolower($col) === strtolower($candidate)) {
                        return $col;
                    }
                }
            }
        }
        return null;
    }

    /**
     * @return array<int, string>
     */
    private function fetchSqliteColumns(PDO $sqlite, string $table): array
    {
        $stmt = $sqlite->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $cols = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $cols[] = $name;
            }
        }
        return $cols;
    }

    private function pickTemplate(array $templates, string $type): ?string
    {
        if ($templates === []) {
            return null;
        }
        foreach ($templates as $name => $content) {
            if (strtolower($name) === strtolower($type)) {
                return $content;
            }
        }
        $first = array_values($templates)[0] ?? null;
        return $first !== null ? (string)$first : null;
    }

    /**
     * @param array<string, mixed> $row
     * @return array<string, mixed>
     */
    private function buildArtikelIdentity(array $row): array
    {
        $identity = [];
        if (isset($row['Artikelnummer'])) {
            $identity['Artikelnummer'] = $row['Artikelnummer'];
        } elseif (isset($row['artikelnummer'])) {
            $identity['Artikelnummer'] = $row['artikelnummer'];
        }
        if (isset($row['Artikel'])) {
            $identity['Artikel'] = $row['Artikel'];
        } elseif (isset($row['afs_artikel_id'])) {
            $identity['Artikel'] = $row['afs_artikel_id'];
        }
        if (isset($row['Bezeichnung'])) {
            $identity['Bezeichnung'] = $row['Bezeichnung'];
        } elseif (isset($row['name'])) {
            $identity['Bezeichnung'] = $row['name'];
        }
        return $identity;
    }

    /**
     * @param array<string, mixed> $row
     */
    private function artikelFilename(array $row): string
    {
        $name = $row['Artikelnummer'] ?? $row['artikelnummer'] ?? '';
        if ($name !== '') {
            return $this->sanitizeFilename((string)$name) . '.txt';
        }
        $fallback = $row['Artikel'] ?? $row['afs_artikel_id'] ?? '';
        if ($fallback !== '') {
            return $this->sanitizeFilename((string)$fallback) . '.txt';
        }
        return '';
    }

    /**
     * @param array<string, mixed> $row
     * @return array<string, mixed>
     */
    private function buildWarengruppeIdentity(array $row): array
    {
        $identity = [];
        if (isset($row['Warengruppe'])) {
            $identity['Warengruppe'] = $row['Warengruppe'];
        } elseif (isset($row['afs_wg_id'])) {
            $identity['Warengruppe'] = $row['afs_wg_id'];
        }
        if (isset($row['Bezeichnung'])) {
            $identity['Bezeichnung'] = $row['Bezeichnung'];
        } elseif (isset($row['name'])) {
            $identity['Bezeichnung'] = $row['name'];
        }
        return $identity;
    }

    /**
     * @param array<string, mixed> $row
     */
    private function warengruppeFilename(array $row): string
    {
        $name = $row['Warengruppe'] ?? $row['afs_wg_id'] ?? '';
        if ($name !== '') {
            return $this->sanitizeFilename((string)$name) . '.txt';
        }
        return '';
    }

    private function ensureDir(string $dir): void
    {
        if (!is_dir($dir)) {
            @mkdir($dir, 0777, true);
        }
    }

    private function sanitizeFilename(string $value): string
    {
        $value = preg_replace('/[^A-Za-z0-9._-]/', '_', $value) ?? $value;
        return trim($value, '_');
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
