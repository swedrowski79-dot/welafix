<?php
declare(strict_types=1);

namespace Welafix\Domain\Media;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use Welafix\Database\ConnectionFactory;

final class MediaImporter
{
    private const IMAGE_FIELD_PREFIX = 'Bild';

    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function importArticleImages(int $batchSize = 500): array
    {
        $sqlite = $this->factory->sqlite();
        $media = $this->factory->media();

        $columns = $this->getTableColumns($sqlite, 'artikel');
        $imageColumns = $this->filterImageColumns($columns);

        if ($imageColumns === []) {
            return [
                'ok' => true,
                'message' => 'Keine Bild-Spalten in artikel gefunden.',
                'total_rows' => 0,
                'assets_upserted' => 0,
                'links_created' => 0,
            ];
        }

        $batchSize = max(1, min(2000, $batchSize));
        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);

        $selectSql = 'SELECT id, artikelnummer, ' . implode(', ', array_map([$this, 'quoteIdentifier'], $imageColumns)) .
            ' FROM artikel WHERE id > :last_id ORDER BY id ASC LIMIT :limit';
        $selectStmt = $sqlite->prepare($selectSql);

        $upsertAsset = $media->prepare(
            'INSERT INTO media_assets (source, source_key, asset_type, filename, mime_type, storage_path, checksum, size_bytes, updated_at, created_at)
             VALUES (:source, :source_key, :asset_type, :filename, :mime_type, :storage_path, :checksum, :size_bytes, :updated_at, :created_at)
             ON CONFLICT(source, source_key) DO UPDATE SET
               asset_type = excluded.asset_type,
               filename = excluded.filename,
               mime_type = excluded.mime_type,
               storage_path = excluded.storage_path,
               checksum = excluded.checksum,
               size_bytes = excluded.size_bytes,
               updated_at = excluded.updated_at'
        );

        $selectAssetId = $media->prepare(
            'SELECT id FROM media_assets WHERE source = :source AND source_key = :source_key LIMIT 1'
        );

        $insertLink = $media->prepare(
            'INSERT OR IGNORE INTO media_links (asset_id, entity_type, entity_id, field_name)
             VALUES (:asset_id, :entity_type, :entity_id, :field_name)'
        );

        $stats = [
            'ok' => true,
            'total_rows' => 0,
            'assets_upserted' => 0,
            'links_created' => 0,
        ];

        $lastId = 0;
        while (true) {
            $selectStmt->bindValue(':last_id', $lastId, PDO::PARAM_INT);
            $selectStmt->bindValue(':limit', $batchSize, PDO::PARAM_INT);
            $selectStmt->execute();
            $rows = $selectStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
            if ($rows === []) {
                break;
            }

            $media->beginTransaction();
            try {
                foreach ($rows as $row) {
                    $stats['total_rows']++;
                    $lastId = (int)($row['id'] ?? $lastId);
                    $artikelnummer = trim((string)($row['artikelnummer'] ?? ''));
                    if ($artikelnummer === '') {
                        continue;
                    }

                    foreach ($imageColumns as $field) {
                        $rawValue = $row[$field] ?? null;
                        $path = trim((string)$rawValue);
                        if ($path === '') {
                            continue;
                        }

                        $sourceKey = $artikelnummer . ':' . $field;
                        $filename = $this->guessFilename($path);

                        $upsertAsset->execute([
                            ':source' => 'AFS_ARTIKEL',
                            ':source_key' => $sourceKey,
                            ':asset_type' => 'image',
                            ':filename' => $filename,
                            ':mime_type' => null,
                            ':storage_path' => $path,
                            ':checksum' => null,
                            ':size_bytes' => null,
                            ':updated_at' => $now,
                            ':created_at' => $now,
                        ]);
                        $stats['assets_upserted']++;

                        $selectAssetId->execute([
                            ':source' => 'AFS_ARTIKEL',
                            ':source_key' => $sourceKey,
                        ]);
                        $assetId = (int)$selectAssetId->fetchColumn();
                        if ($assetId > 0) {
                            $insertLink->execute([
                                ':asset_id' => $assetId,
                                ':entity_type' => 'artikel',
                                ':entity_id' => $artikelnummer,
                                ':field_name' => $field,
                            ]);
                            if ($insertLink->rowCount() > 0) {
                                $stats['links_created']++;
                            }
                        }
                    }
                }
                $media->commit();
            } catch (\Throwable $e) {
                $media->rollBack();
                throw $e;
            }
        }

        return $stats;
    }

    /**
     * @return array<string, mixed>
     */
    public function importDocuments(int $batchSize = 500): array
    {
        $sqlite = $this->factory->sqlite();
        $media = $this->factory->media();

        $batchSize = max(1, min(2000, $batchSize));
        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);

        $selectFiles = $sqlite->prepare(
            'SELECT df.id, df.document_id, df.file_name, df.mime_type, df.storage_path, df.checksum, df.created_at,
                    d.source
             FROM document_files df
             LEFT JOIN documents d ON d.id = df.document_id
             WHERE df.id > :last_id
             ORDER BY df.id ASC
             LIMIT :limit'
        );

        $selectArticleNos = $sqlite->prepare(
            'SELECT DISTINCT article_no FROM document_items WHERE document_id = :document_id AND article_no IS NOT NULL AND article_no != ""'
        );

        $upsertAsset = $media->prepare(
            'INSERT INTO media_assets (source, source_key, asset_type, filename, mime_type, storage_path, checksum, size_bytes, updated_at, created_at)
             VALUES (:source, :source_key, :asset_type, :filename, :mime_type, :storage_path, :checksum, :size_bytes, :updated_at, :created_at)
             ON CONFLICT(source, source_key) DO UPDATE SET
               asset_type = excluded.asset_type,
               filename = excluded.filename,
               mime_type = excluded.mime_type,
               storage_path = excluded.storage_path,
               checksum = excluded.checksum,
               size_bytes = excluded.size_bytes,
               updated_at = excluded.updated_at'
        );

        $selectAssetId = $media->prepare(
            'SELECT id FROM media_assets WHERE source = :source AND source_key = :source_key LIMIT 1'
        );

        $insertLink = $media->prepare(
            'INSERT OR IGNORE INTO media_links (asset_id, entity_type, entity_id, field_name)
             VALUES (:asset_id, :entity_type, :entity_id, :field_name)'
        );

        $stats = [
            'ok' => true,
            'total_files' => 0,
            'assets_upserted' => 0,
            'links_created' => 0,
        ];

        $lastId = 0;
        while (true) {
            $selectFiles->bindValue(':last_id', $lastId, PDO::PARAM_INT);
            $selectFiles->bindValue(':limit', $batchSize, PDO::PARAM_INT);
            $selectFiles->execute();
            $rows = $selectFiles->fetchAll(PDO::FETCH_ASSOC) ?: [];
            if ($rows === []) {
                break;
            }

            $media->beginTransaction();
            try {
                foreach ($rows as $row) {
                    $stats['total_files']++;
                    $lastId = (int)($row['id'] ?? $lastId);

                    $documentId = (int)($row['document_id'] ?? 0);
                    if ($documentId <= 0) {
                        continue;
                    }

                    $source = trim((string)($row['source'] ?? ''));
                    if ($source === '') {
                        $source = 'AFS_DOK';
                    }

                    $sourceKey = 'doc:' . $documentId . ':file:' . (int)($row['id'] ?? 0);
                    $storagePath = trim((string)($row['storage_path'] ?? ''));
                    if ($storagePath === '') {
                        continue;
                    }

                    $upsertAsset->execute([
                        ':source' => $source,
                        ':source_key' => $sourceKey,
                        ':asset_type' => 'document',
                        ':filename' => (string)($row['file_name'] ?? ''),
                        ':mime_type' => $row['mime_type'] ?? null,
                        ':storage_path' => $storagePath,
                        ':checksum' => $row['checksum'] ?? null,
                        ':size_bytes' => null,
                        ':updated_at' => $now,
                        ':created_at' => (string)($row['created_at'] ?? $now),
                    ]);
                    $stats['assets_upserted']++;

                    $selectAssetId->execute([
                        ':source' => $source,
                        ':source_key' => $sourceKey,
                    ]);
                    $assetId = (int)$selectAssetId->fetchColumn();
                    if ($assetId <= 0) {
                        continue;
                    }

                    $selectArticleNos->execute([':document_id' => $documentId]);
                    $articleNos = $selectArticleNos->fetchAll(PDO::FETCH_COLUMN) ?: [];
                    foreach ($articleNos as $articleNo) {
                        $articleNo = trim((string)$articleNo);
                        if ($articleNo === '') {
                            continue;
                        }
                        $insertLink->execute([
                            ':asset_id' => $assetId,
                            ':entity_type' => 'artikel',
                            ':entity_id' => $articleNo,
                            ':field_name' => 'Dokument',
                        ]);
                        if ($insertLink->rowCount() > 0) {
                            $stats['links_created']++;
                        }
                    }
                }
                $media->commit();
            } catch (\Throwable $e) {
                $media->rollBack();
                throw $e;
            }
        }

        return $stats;
    }

    /**
     * @return array<int, string>
     */
    private function getTableColumns(PDO $pdo, string $table): array
    {
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $columns = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $columns[] = $name;
            }
        }
        return $columns;
    }

    /**
     * @param array<int, string> $columns
     * @return array<int, string>
     */
    private function filterImageColumns(array $columns): array
    {
        $imageColumns = [];
        foreach ($columns as $column) {
            if (stripos($column, self::IMAGE_FIELD_PREFIX) === 0) {
                $imageColumns[] = $column;
            }
        }
        return $imageColumns;
    }

    private function guessFilename(string $path): ?string
    {
        $trimmed = trim($path);
        if ($trimmed === '') {
            return null;
        }
        $trimmed = str_replace('\\', '/', $trimmed);
        $parts = explode('/', $trimmed);
        $name = end($parts);
        if ($name === false || $name === '') {
            return null;
        }
        return $name;
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
