<?php
declare(strict_types=1);

namespace Welafix\Domain\Document;

use PDO;

final class DocumentRepositorySqlite
{
    private PDO $pdo;
    /** @var array<int, string>|null */
    private ?array $documentColumns = null;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function getLatestDocuments(int $limit = 20): array
    {
        $limit = max(1, min(200, $limit));
        $columns = $this->getDocumentColumns();
        $selectList = implode(', ', array_map([$this, 'quoteIdentifier'], $columns));
        $stmt = $this->pdo->query('SELECT ' . $selectList . ' FROM documents ORDER BY id DESC LIMIT ' . $limit);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * @return array<string, mixed>|null
     */
    public function getDocumentWithItems(int $id): ?array
    {
        $columns = $this->getDocumentColumns();
        $selectList = implode(', ', array_map([$this, 'quoteIdentifier'], $columns));
        $stmt = $this->pdo->prepare('SELECT ' . $selectList . ' FROM documents WHERE id = :id LIMIT 1');
        $stmt->execute([':id' => $id]);
        $doc = $stmt->fetch(PDO::FETCH_ASSOC);
        if (!$doc) {
            return null;
        }
        $doc['items'] = [];
        $doc['files'] = [];

        return $doc;
    }

    /**
     * @return array<int, string>
     */
    private function getDocumentColumns(): array
    {
        if ($this->documentColumns !== null) {
            return $this->documentColumns;
        }
        $stmt = $this->pdo->query('PRAGMA table_info(documents)');
        $cols = [];
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $cols[] = $name;
            }
        }
        if ($cols === []) {
            $cols = ['id'];
        }
        $this->documentColumns = $cols;
        return $cols;
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
