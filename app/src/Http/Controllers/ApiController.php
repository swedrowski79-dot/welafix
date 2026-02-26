<?php
declare(strict_types=1);

namespace Welafix\Http\Controllers;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Config\MappingService;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;
use Welafix\Domain\Artikel\ArtikelSyncService;
use Welafix\Domain\Document\DocumentRepositorySqlite;
use Welafix\Domain\FileDb\FileDbTemplateApplier;

final class ApiController
{
    public function __construct(private ConnectionFactory $factory) {}

    public function status(): void
    {
        $time = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $this->jsonResponse([
            'ok' => true,
            'time' => $time,
            'app' => 'welafix',
        ]);
    }

    public function testMssql(): void
    {
        try {
            $pdo = Db::guardMssql(Db::mssql(), __METHOD__);
            $stmt = $pdo->query('SELECT DB_NAME() AS database_name, @@SERVERNAME AS server_name');
            $row = $stmt->fetch(PDO::FETCH_ASSOC) ?: [];

            $server = (string)($row['server_name'] ?? env('MSSQL_HOST', ''));
            $database = (string)($row['database_name'] ?? env('MSSQL_DB', ''));

            $this->jsonResponse([
                'ok' => true,
                'server' => $server,
                'database' => $database,
                'message' => 'LOGIN OK',
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'ok' => false,
                'error' => $e->getMessage(),
                'sql' => null,
                'params' => null,
            ], 500);
        }
    }

    public function testSqlite(): void
    {
        $path = (string)env('SQLITE_PATH', '');

        try {
            if ($path === '') {
                throw new RuntimeException('SQLITE_PATH ist nicht gesetzt.');
            }
            if (!file_exists($path)) {
                throw new RuntimeException('SQLite DB nicht gefunden.');
            }
            if (!is_readable($path)) {
                throw new RuntimeException('SQLite DB ist nicht lesbar.');
            }

            $pdo = Db::guardSqlite(Db::sqlite(), __METHOD__);
            $pdo->query('SELECT 1');

            $this->jsonResponse([
                'ok' => true,
                'path' => $path,
                'readable' => true,
                'writable' => is_writable($path),
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'ok' => false,
                'error' => $e->getMessage(),
                'sql' => null,
                'params' => null,
            ], 500);
        }
    }

    public function syncState(): void
    {
        $type = $_GET['type'] ?? 'artikel';
        if ($type !== 'artikel') {
            $this->jsonResponse([
                'ok' => false,
                'error' => 'Unbekannter Typ.',
                'sql' => null,
                'params' => null,
            ], 400);
            return;
        }

        $service = new ArtikelSyncService($this->factory);
        $state = $service->getState();
        $this->jsonResponse([
            'ok' => true,
            'state' => $state,
        ]);
    }

    public function documentsList(): void
    {
        $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 20;
        $repo = new DocumentRepositorySqlite($this->factory->sqlite());
        $items = $repo->getLatestDocuments($limit);
        $this->jsonResponse([
            'ok' => true,
            'items' => $items,
        ]);
    }

    public function fileDbCheck(): void
    {
        $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 10;
        $limit = max(1, min(200, $limit));

        $base = (string)env('FILEDB_PATH', __DIR__ . '/../../storage/data');
        $base = rtrim($base, "/\\");
        $artikelBase = $base . '/Artikel';
        $wgBase = $base . '/Warengruppen';
        $artikelStd = $artikelBase . '/Standard';
        $wgStd = $wgBase . '/Standard';

        $pdo = $this->factory->sqlite();

        $wgRows = $pdo->query('SELECT afs_wg_id, name FROM warengruppe LIMIT ' . $limit)?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $wgItems = [];
        foreach ($wgRows as $row) {
            $name = trim((string)($row['name'] ?? ''));
            $id = (string)($row['afs_wg_id'] ?? '');
            $dir = $wgBase . '/' . $name;
            $exists = is_dir($dir);
            $wgItems[] = [
                'afs_wg_id' => $id,
                'bezeichnung' => $name,
                'dir' => $dir,
                'exists' => $exists,
                'fallback' => (!$exists && is_dir($wgStd)) ? 'Standard' : null,
            ];
        }

        $artRows = $pdo->query('SELECT artikelnummer FROM artikel LIMIT ' . $limit)?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $artItems = [];
        foreach ($artRows as $row) {
            $nr = trim((string)($row['artikelnummer'] ?? ''));
            $dir = $artikelBase . '/' . $nr;
            $exists = is_dir($dir);
            $artItems[] = [
                'artikelnummer' => $nr,
                'dir' => $dir,
                'exists' => $exists,
                'fallback' => (!$exists && is_dir($artikelStd)) ? 'Standard' : null,
            ];
        }

        $this->jsonResponse([
            'ok' => true,
            'base' => $base,
            'artikel_base' => $artikelBase,
            'artikel_standard' => ['path' => $artikelStd, 'exists' => is_dir($artikelStd)],
            'warengruppe_base' => $wgBase,
            'warengruppe_standard' => ['path' => $wgStd, 'exists' => is_dir($wgStd)],
            'limit' => $limit,
            'warengruppen' => $wgItems,
            'artikel' => $artItems,
        ]);
    }

    public function fileDbApply(): void
    {
        $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 50;
        $limit = max(1, min(500, $limit));
        $offset = isset($_GET['offset']) ? (int)$_GET['offset'] : 0;
        $offset = max(0, $offset);

        $pdo = $this->factory->sqlite();
        $pdo->exec('PRAGMA busy_timeout = 5000');
        $applier = new FileDbTemplateApplier();
        $mode = strtolower((string)env('FILEDB_MODE', 'read'));
        $base = (string)env('FILEDB_PATH', __DIR__ . '/../../storage/data');
        $base = rtrim($base, "/\\");
        $artikelStd = $base . '/Artikel/Standard';
        $wgStd = $base . '/Warengruppen/Standard';
        $artikelTemplates = $this->listTemplateFiles($artikelStd);
        $wgTemplates = $this->listTemplateFiles($wgStd);

        $wgStmt = $pdo->prepare('SELECT * FROM warengruppe LIMIT :limit OFFSET :offset');
        $wgStmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $wgStmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $wgStmt->execute();
        $wgRows = $wgStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        foreach ($wgRows as $row) {
            $id = (int)($row['afs_wg_id'] ?? 0);
            $name = (string)($row['name'] ?? '');
            if ($id > 0 && $name !== '') {
                $applier->applyWarengruppe($pdo, $id, $name, $row);
            }
        }

        $artStmt = $pdo->prepare('SELECT * FROM artikel LIMIT :limit OFFSET :offset');
        $artStmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $artStmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $artStmt->execute();
        $artRows = $artStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        foreach ($artRows as $row) {
            $nr = (string)($row['artikelnummer'] ?? '');
            if ($nr !== '') {
                $applier->applyArtikel($pdo, $nr, $row);
            }
        }

        $this->jsonResponse([
            'ok' => true,
            'limit' => $limit,
            'offset' => $offset,
            'warengruppe_count' => count($wgRows),
            'artikel_count' => count($artRows),
            'filedb_mode' => $mode,
            'filedb_base' => $base,
            'artikel_standard' => ['path' => $artikelStd, 'templates' => $artikelTemplates],
            'warengruppe_standard' => ['path' => $wgStd, 'templates' => $wgTemplates],
        ]);
    }

    /**
     * @return array<int, string>
     */
    private function listTemplateFiles(string $dir): array
    {
        if (!is_dir($dir)) {
            return [];
        }
        $files = [];
        $items = scandir($dir);
        if (!is_array($items)) {
            return [];
        }
        foreach ($items as $item) {
            if ($item === '.' || $item === '..') {
                continue;
            }
            if (strtolower(pathinfo($item, PATHINFO_EXTENSION)) !== 'txt') {
                continue;
            }
            $files[] = $item;
        }
        sort($files, SORT_STRING);
        return $files;
    }

    public function documentDetail(int $id): void
    {
        $repo = new DocumentRepositorySqlite($this->factory->sqlite());
        $doc = $repo->getDocumentWithItems($id);
        if ($doc === null) {
            $this->jsonResponse([
                'ok' => false,
                'error' => 'Dokument nicht gefunden.',
                'sql' => null,
                'params' => null,
            ], 404);
            return;
        }
        $this->jsonResponse([
            'ok' => true,
            'document' => $doc,
        ]);
    }

    public function mediaList(): void
    {
        $source = isset($_GET['source']) ? trim((string)$_GET['source']) : '';
        $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 200;
        $offset = isset($_GET['offset']) ? (int)$_GET['offset'] : 0;
        $limit = max(1, min(1000, $limit));
        $offset = max(0, $offset);

        $pdo = $this->factory->sqlite();
        $sql = 'SELECT id, filename, source, type, storage_path, checksum, last_checked_at, is_deleted, created_at FROM media';
        $params = [];
        if ($source !== '') {
            $sql .= ' WHERE source = :source';
            $params[':source'] = $source;
        }
        $sql .= ' ORDER BY id ASC LIMIT :limit OFFSET :offset';
        $stmt = $pdo->prepare($sql);
        foreach ($params as $key => $value) {
            $stmt->bindValue($key, $value, PDO::PARAM_STR);
        }
        $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        $stmt->execute();
        $items = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];

        $this->jsonResponse([
            'ok' => true,
            'items' => $items,
        ]);
    }

    public function mediaStats(): void
    {
        $pdo = $this->factory->sqlite();
        $assetStmt = $pdo->query('SELECT source, COUNT(*) AS count FROM media GROUP BY source');
        $assets = $assetStmt ? $assetStmt->fetchAll(PDO::FETCH_ASSOC) : [];

        $this->jsonResponse([
            'ok' => true,
            'assets' => $assets,
        ]);
    }

    public function xtApiCheck(): void
    {
        $base = trim((string)env('XT_API_BASE_URL', (string)env('XT_API_BASE', '')));
        if ($base === '') {
            $this->jsonResponse([
                'ok' => false,
                'status' => 'missing_base_url',
            ], 500);
            return;
        }

        $key = (string)env('XT_API_KEY', '');
        if ($key === '') {
            $this->jsonResponse([
                'ok' => false,
                'status' => 'missing_key',
            ], 500);
            return;
        }

        $url = rtrim($base, "/\\") . '/health';
        $ts = (string)time();
        $method = 'GET';
        $path = parse_url($url, PHP_URL_PATH) ?: '/health';
        $body = '';
        $baseString = $method . "\n" . $path . "\n" . $ts . "\n" . $body;
        $sig = hash_hmac('sha256', $baseString, $key);

        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, 8);
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'X-API-KEY: default',
            'X-API-TS: ' . $ts,
            'X-API-SIG: ' . $sig,
            'Accept: application/json',
        ]);
        $resp = curl_exec($ch);
        $err = curl_error($ch);
        $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        if ($resp === false) {
            $this->jsonResponse([
                'ok' => false,
                'status' => 'unreachable',
                'error' => $err,
            ], 502);
            return;
        }

        $json = json_decode((string)$resp, true);
        if ($code === 404) {
            $this->jsonResponse([
                'ok' => false,
                'status' => 'auth_fail',
                'code' => $code,
            ], 401);
            return;
        }

        if ($code >= 400) {
            $this->jsonResponse([
                'ok' => false,
                'status' => 'http_error',
                'code' => $code,
                'body' => $resp,
            ], 502);
            return;
        }

        $dbOk = (bool)($json['db'] ?? false);
        $this->jsonResponse([
            'ok' => (bool)($json['ok'] ?? false),
            'status' => $dbOk ? 'ok' : 'db_fail',
            'code' => $code,
            'response' => $json,
        ]);
    }

    public function mediaUsage(): void
    {
        $filename = isset($_GET['filename']) ? trim((string)$_GET['filename']) : '';
        if ($filename === '') {
            $this->jsonResponse([
                'ok' => false,
                'error' => 'filename fehlt',
                'sql' => null,
                'params' => null,
            ], 400);
            return;
        }

        $normalized = normalizeMediaFilename($filename);
        if ($normalized === null) {
            $this->jsonResponse([
                'ok' => false,
                'error' => 'filename ungültig',
                'sql' => null,
                'params' => null,
            ], 400);
            return;
        }

        $pdo = $this->factory->sqlite();
        $docCols = $this->findExistingColumns($pdo, 'documents', ['Titel','titel','Dateiname','dateiname']);
        $docArtCols = $this->findExistingColumns($pdo, 'documents', ['Artikel_ID','artikel_id','Artikel','artikel']);
        $docFileCol = $docCols[0] ?? null;
        $docArtCol = $docArtCols[0] ?? null;

        $docs = [];
        if ($docFileCol !== null) {
            $stmt = $pdo->prepare(
                'SELECT id, ' . $this->quoteIdentifier($docFileCol) . ' AS fname' .
                ($docArtCol ? ', ' . $this->quoteIdentifier($docArtCol) . ' AS art' : '') .
                ' FROM documents WHERE lower(' . $this->quoteIdentifier($docFileCol) . ') = lower(:fname)'
            );
            $stmt->execute([':fname' => $normalized]);
            $docs = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        }

        $artikelMap = [];
        $stmt = $pdo->query('SELECT afs_artikel_id, Artikel, artikelnummer FROM artikel');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        foreach ($rows as $row) {
            $afsId = trim((string)($row['afs_artikel_id'] ?? ''));
            $artikelId = trim((string)($row['Artikel'] ?? ''));
            $nr = trim((string)($row['artikelnummer'] ?? ''));
            if ($afsId !== '' && $nr !== '') {
                $artikelMap['afs:' . $afsId] = $nr;
            }
            if ($artikelId !== '' && $nr !== '') {
                $artikelMap['artikel:' . $artikelId] = $nr;
            }
            if ($nr !== '') {
                $artikelMap['nr:' . $nr] = $nr;
            }
        }

        $resolved = [];
        foreach ($docs as $row) {
            $art = trim((string)($row['art'] ?? ''));
            $nr = '';
            if ($art !== '') {
                $nr = $artikelMap['afs:' . $art] ?? ($artikelMap['artikel:' . $art] ?? ($artikelMap['nr:' . $art] ?? ''));
            }
            if ($nr === '' && $art !== '') {
                $stmt = $pdo->prepare(
                    'SELECT artikelnummer FROM artikel WHERE Artikel = :art OR afs_artikel_id = :art OR artikelnummer = :art LIMIT 1'
                );
                $stmt->execute([':art' => $art]);
                $nr = (string)($stmt->fetchColumn() ?: '');
            }
            $resolved[] = [
                'doc_id' => $row['id'] ?? null,
                'doc_art' => $art,
                'artikelnummer' => $nr ?: null,
            ];
        }

        $this->jsonResponse([
            'ok' => true,
            'input' => $filename,
            'normalized' => $normalized,
            'doc_file_col' => $docFileCol,
            'doc_art_col' => $docArtCol,
            'documents' => $resolved,
        ]);
    }

    public function artikelList(): void
    {
        $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 50;
        $limit = max(1, min(200, $limit));
        $offset = isset($_GET['offset']) ? (int)$_GET['offset'] : 0;
        $offset = max(0, $offset);

        try {
            $mapping = new MappingService();
            $allowed = array_values(array_unique(array_merge(
                $mapping->getAllowedColumns('artikel'),
                ['seo_url', 'changed', 'changed_fields', 'last_synced_at', 'is_deleted']
            )));
            $pdo = $this->factory->sqlite();
            $selectList = implode(', ', array_map(fn(string $col): string => 'a.' . $this->quoteIdentifier($col), $allowed));
            $selectList .= ', w.seo_url AS wg_seo_url, a.warengruppe_id AS _wg_id';
            $sql = 'SELECT ' . $selectList . ' FROM artikel a LEFT JOIN warengruppe w ON w.afs_wg_id = a.warengruppe_id LIMIT :limit OFFSET :offset';
            $stmt = $pdo->prepare($sql);
            $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
            $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
            $stmt->execute();
            $rawRows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
            $rows = $this->appendArtikelSeoUrl($mapping->filterRows($rawRows, $allowed), $rawRows);
            $this->logSeoUrlKeysOnce('artikel', $rows);
            $this->jsonResponse([
                'ok' => true,
                'items' => $rows,
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'ok' => false,
                'error' => $e->getMessage(),
                'sql' => null,
                'params' => null,
            ], 500);
        }
    }

    public function warengruppeList(): void
    {
        $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 100;
        $limit = max(1, min(500, $limit));
        $offset = isset($_GET['offset']) ? (int)$_GET['offset'] : 0;
        $offset = max(0, $offset);

        try {
            $mapping = new MappingService();
            $allowed = array_values(array_unique(array_merge(
                $mapping->getAllowedColumns('warengruppe'),
                ['seo_url', 'changed', 'changed_fields', 'last_synced_at']
            )));
            $pdo = $this->factory->sqlite();
            $selectList = implode(', ', array_map([$this, 'quoteIdentifier'], $allowed));
            $sql = 'SELECT ' . $selectList . ' FROM warengruppe LIMIT :limit OFFSET :offset';
            $stmt = $pdo->prepare($sql);
            $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
            $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
            $stmt->execute();
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
            $rows = $mapping->filterRows($rows, $allowed);
            $this->logSeoUrlKeysOnce('warengruppe', $rows);
            $this->jsonResponse([
                'ok' => true,
                'items' => $rows,
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'ok' => false,
                'error' => $e->getMessage(),
                'sql' => null,
                'params' => null,
            ], 500);
        }
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    /**
     * @param array<int, string> $names
     * @return array<int, string>
     */
    private function findExistingColumns(PDO $pdo, string $table, array $names): array
    {
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $existing = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $existing[strtolower($name)] = $name;
            }
        }
        $found = [];
        foreach ($names as $name) {
            $key = strtolower($name);
            if (isset($existing[$key])) {
                $found[] = $existing[$key];
            }
        }
        return $found;
    }

    private function jsonResponse(array $payload, int $status = 200): void
    {
        http_response_code($status);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($payload, JSON_PRETTY_PRINT);
    }

    /**
     * @param array<int, array<string, mixed>> $filteredRows
     * @param array<int, array<string, mixed>> $rawRows
     * @return array<int, array<string, mixed>>
     */
    private function appendArtikelSeoUrl(array $filteredRows, array $rawRows): array
    {
        $result = [];
        foreach ($filteredRows as $index => $row) {
            $raw = $rawRows[$index] ?? [];
            $wgSeo = (string)($raw['wg_seo_url'] ?? '');
            $name = (string)($row['Bezeichnung'] ?? ($row['name'] ?? ''));
            $slug = strtolower(xt_filterAutoUrlText_inline($name, 'de'));

            if ($wgSeo === '') {
                $this->logMissingWarengruppeOnce($raw);
                $wgSeo = 'de';
            }

            $row['seo_url'] = rtrim($wgSeo, '/') . ($slug !== '' ? '/' . $slug : '');
            $result[] = $row;
        }
        return $result;
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<int, string> $allowed
     * @return array<int, array<string, mixed>>
     */

    /**
     * @param array<string, mixed> $row
     */
    private function logMissingWarengruppeOnce(array $row): void
    {
        if (!is_dev_env()) {
            return;
        }

        static $logged = false;
        if ($logged) {
            return;
        }
        $logged = true;

        $artikelnummer = (string)($row['Artikelnummer'] ?? ($row['artikelnummer'] ?? ''));
        $warengruppeId = (string)($row['_wg_id'] ?? ($row['warengruppe_id'] ?? ''));
        $timestamp = (new \DateTimeImmutable('now', new \DateTimeZone('UTC')))->format(DATE_ATOM);
        $line = "[{$timestamp}] missing wg mapping: warengruppe_id={$warengruppeId} artikelnummer={$artikelnummer}\n";
        $path = __DIR__ . '/../../../logs/app.log';
        @file_put_contents($path, $line, FILE_APPEND);
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     */
    private function logSeoUrlKeysOnce(string $type, array $rows): void
    {
        if (!$this->isDev()) {
            return;
        }

        static $logged = [
            'artikel' => false,
            'warengruppe' => false,
        ];

        if (!isset($logged[$type]) || $logged[$type]) {
            return;
        }

        if ($rows === []) {
            return;
        }

        $keys = array_keys($rows[0]);
        $timestamp = (new \DateTimeImmutable('now', new \DateTimeZone('UTC')))->format(DATE_ATOM);
        $line = "[{$timestamp}] api_debug {$type} keys: " . implode(',', $keys) . "\n";
        $path = __DIR__ . '/../../../logs/app.log';
        @file_put_contents($path, $line, FILE_APPEND);

        $logged[$type] = true;
    }

    private function isDev(): bool
    {
        return is_dev_env();
    }
}
