<?php
declare(strict_types=1);

namespace Welafix\Http\Controllers;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Database\ConnectionFactory;
use Welafix\Domain\Artikel\ArtikelSyncService;
use Welafix\Domain\Document\DocumentRepositorySqlite;

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
            $pdo = $this->factory->mssql();
            $stmt = $pdo->query('SELECT DB_NAME() AS database_name, @@SERVERNAME AS server_name');
            $row = $stmt->fetch(PDO::FETCH_ASSOC) ?: [];

            $server = (string)($row['server_name'] ?? (getenv('MSSQL_HOST') ?: ''));
            $database = (string)($row['database_name'] ?? (getenv('MSSQL_DB') ?: ''));

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
            ], 500);
        }
    }

    public function testSqlite(): void
    {
        $path = getenv('SQLITE_PATH') ?: '';

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

            $pdo = $this->factory->sqlite();
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

    public function documentDetail(int $id): void
    {
        $repo = new DocumentRepositorySqlite($this->factory->sqlite());
        $doc = $repo->getDocumentWithItems($id);
        if ($doc === null) {
            $this->jsonResponse([
                'ok' => false,
                'error' => 'Dokument nicht gefunden.',
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
        $entityType = isset($_GET['entity_type']) ? trim((string)$_GET['entity_type']) : '';
        $entityId = isset($_GET['entity_id']) ? trim((string)$_GET['entity_id']) : '';

        if ($entityType === '' || $entityId === '') {
            $this->jsonResponse([
                'ok' => false,
                'error' => 'entity_type und entity_id sind erforderlich.',
            ], 400);
            return;
        }

        $pdo = $this->factory->media();
        $stmt = $pdo->prepare(
            'SELECT ma.id, ma.source, ma.source_key, ma.asset_type, ma.filename, ma.mime_type, ma.storage_path,
                    ma.checksum, ma.size_bytes, ma.updated_at, ma.created_at, ml.field_name
             FROM media_links ml
             INNER JOIN media_assets ma ON ma.id = ml.asset_id
             WHERE ml.entity_type = :entity_type AND ml.entity_id = :entity_id
             ORDER BY ma.id ASC'
        );
        $stmt->execute([
            ':entity_type' => $entityType,
            ':entity_id' => $entityId,
        ]);
        $items = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];

        $this->jsonResponse([
            'ok' => true,
            'items' => $items,
        ]);
    }

    public function mediaStats(): void
    {
        $pdo = $this->factory->media();

        $assetStmt = $pdo->query('SELECT asset_type, COUNT(*) AS count FROM media_assets GROUP BY asset_type');
        $assets = $assetStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];

        $linkStmt = $pdo->query('SELECT entity_type, COUNT(*) AS count FROM media_links GROUP BY entity_type');
        $links = $linkStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];

        $this->jsonResponse([
            'ok' => true,
            'assets' => $assets,
            'links' => $links,
        ]);
    }

    private function jsonResponse(array $payload, int $status = 200): void
    {
        http_response_code($status);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($payload, JSON_PRETTY_PRINT);
    }
}
