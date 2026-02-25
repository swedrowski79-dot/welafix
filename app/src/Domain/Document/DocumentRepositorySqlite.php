<?php
declare(strict_types=1);

namespace Welafix\Domain\Document;

use PDO;
use RuntimeException;

final class DocumentRepositorySqlite
{
    private PDO $pdo;
    private const DOCUMENT_COLUMNS = [
        'id',
        'source',
        'source_id',
        'doc_type',
        'doc_no',
        'doc_date',
        'customer_no',
        'total_gross',
        'currency',
        'updated_at',
        'synced_at',
    ];
    private const ITEM_COLUMNS = [
        'id',
        'document_id',
        'line_no',
        'article_no',
        'title',
        'qty',
        'unit_price',
        'total',
        'vat',
    ];
    private const FILE_COLUMNS = [
        'id',
        'document_id',
        'file_name',
        'mime_type',
        'storage_path',
        'checksum',
        'created_at',
    ];

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @param array<string, mixed> $doc
     */
    public function upsertDocument(array $doc): int
    {
        $stmt = $this->pdo->prepare(
            'INSERT INTO documents (source, source_id, doc_type, doc_no, doc_date, customer_no, total_gross, currency, updated_at, synced_at)
             VALUES (:source, :source_id, :doc_type, :doc_no, :doc_date, :customer_no, :total_gross, :currency, :updated_at, :synced_at)
             ON CONFLICT(source, source_id) DO UPDATE SET
               doc_type = excluded.doc_type,
               doc_no = excluded.doc_no,
               doc_date = excluded.doc_date,
               customer_no = excluded.customer_no,
               total_gross = excluded.total_gross,
               currency = excluded.currency,
               updated_at = excluded.updated_at,
               synced_at = excluded.synced_at'
        );

        $stmt->execute([
            ':source' => (string)$doc['source'],
            ':source_id' => (string)$doc['source_id'],
            ':doc_type' => (string)$doc['doc_type'],
            ':doc_no' => $doc['doc_no'] ?? null,
            ':doc_date' => $doc['doc_date'] ?? null,
            ':customer_no' => $doc['customer_no'] ?? null,
            ':total_gross' => $doc['total_gross'] ?? null,
            ':currency' => $doc['currency'] ?? null,
            ':updated_at' => $doc['updated_at'] ?? null,
            ':synced_at' => $doc['synced_at'] ?? null,
        ]);

        $idStmt = $this->pdo->prepare('SELECT id FROM documents WHERE source = :source AND source_id = :source_id LIMIT 1');
        $idStmt->execute([
            ':source' => (string)$doc['source'],
            ':source_id' => (string)$doc['source_id'],
        ]);
        $id = $idStmt->fetchColumn();
        if ($id === false) {
            throw new RuntimeException('Dokument konnte nicht gespeichert werden.');
        }
        return (int)$id;
    }

    /**
     * @param array<int, array<string, mixed>> $items
     */
    public function replaceItems(int $documentId, array $items): void
    {
        $this->pdo->beginTransaction();
        try {
            $del = $this->pdo->prepare('DELETE FROM document_items WHERE document_id = :id');
            $del->execute([':id' => $documentId]);

            $ins = $this->pdo->prepare(
                'INSERT INTO document_items (document_id, line_no, article_no, title, qty, unit_price, total, vat)
                 VALUES (:document_id, :line_no, :article_no, :title, :qty, :unit_price, :total, :vat)'
            );

            foreach ($items as $item) {
                $ins->execute([
                    ':document_id' => $documentId,
                    ':line_no' => (int)($item['line_no'] ?? 0),
                    ':article_no' => $item['article_no'] ?? null,
                    ':title' => $item['title'] ?? null,
                    ':qty' => $item['qty'] ?? null,
                    ':unit_price' => $item['unit_price'] ?? null,
                    ':total' => $item['total'] ?? null,
                    ':vat' => $item['vat'] ?? null,
                ]);
            }

            $this->pdo->commit();
        } catch (\Throwable $e) {
            $this->pdo->rollBack();
            throw $e;
        }
    }

    /**
     * @param array<string, mixed> $file
     */
    public function addFile(int $documentId, array $file): void
    {
        $stmt = $this->pdo->prepare(
            'INSERT INTO document_files (document_id, file_name, mime_type, storage_path, checksum, created_at)
             VALUES (:document_id, :file_name, :mime_type, :storage_path, :checksum, :created_at)'
        );
        $stmt->execute([
            ':document_id' => $documentId,
            ':file_name' => (string)$file['file_name'],
            ':mime_type' => $file['mime_type'] ?? null,
            ':storage_path' => (string)$file['storage_path'],
            ':checksum' => $file['checksum'] ?? null,
            ':created_at' => $file['created_at'] ?? null,
        ]);
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function getLatestDocuments(int $limit = 20): array
    {
        $limit = max(1, min(200, $limit));
        $stmt = $this->pdo->query(
            'SELECT ' . implode(', ', self::DOCUMENT_COLUMNS) . ' FROM documents ORDER BY id DESC LIMIT ' . $limit
        );
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * @return array<string, mixed>|null
     */
    public function getDocumentWithItems(int $id): ?array
    {
        $stmt = $this->pdo->prepare(
            'SELECT ' . implode(', ', self::DOCUMENT_COLUMNS) . ' FROM documents WHERE id = :id LIMIT 1'
        );
        $stmt->execute([':id' => $id]);
        $doc = $stmt->fetch(PDO::FETCH_ASSOC);
        if (!$doc) {
            return null;
        }

        $itemsStmt = $this->pdo->prepare(
            'SELECT ' . implode(', ', self::ITEM_COLUMNS) . ' FROM document_items WHERE document_id = :id ORDER BY line_no ASC, id ASC'
        );
        $itemsStmt->execute([':id' => $id]);
        $filesStmt = $this->pdo->prepare(
            'SELECT ' . implode(', ', self::FILE_COLUMNS) . ' FROM document_files WHERE document_id = :id ORDER BY id ASC'
        );
        $filesStmt->execute([':id' => $id]);

        $doc['items'] = $itemsStmt->fetchAll(PDO::FETCH_ASSOC);
        $doc['files'] = $filesStmt->fetchAll(PDO::FETCH_ASSOC);

        return $doc;
    }
}
