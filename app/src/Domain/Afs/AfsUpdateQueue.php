<?php
declare(strict_types=1);

namespace Welafix\Domain\Afs;

use PDO;

final class AfsUpdateQueue
{
    public function __construct(private PDO $pdo) {}
    private bool $ensured = false;

    private function isMysql(): bool
    {
        return (string)$this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql';
    }

    public function ensureTable(): void
    {
        if ($this->ensured) {
            return;
        }
        if ($this->isMysql()) {
            $this->pdo->exec(
                'CREATE TABLE IF NOT EXISTS afs_update_pending (
                    entity VARCHAR(64) NOT NULL,
                    source_id VARCHAR(255) NOT NULL,
                    PRIMARY KEY(entity, source_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
            $this->ensured = true;
            return;
        }
        $this->pdo->exec(
            'CREATE TABLE IF NOT EXISTS afs_update_pending (
                entity TEXT NOT NULL,
                source_id TEXT NOT NULL,
                PRIMARY KEY(entity, source_id)
            )'
        );
        $this->ensured = true;
    }

    public function add(string $entity, string $sourceId): void
    {
        if ($entity === '' || $sourceId === '') {
            return;
        }
        $this->ensureTable();
        $sql = $this->isMysql()
            ? 'INSERT IGNORE INTO afs_update_pending (entity, source_id) VALUES (:entity, :source_id)'
            : 'INSERT OR IGNORE INTO afs_update_pending (entity, source_id) VALUES (:entity, :source_id)';
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([
            ':entity' => $entity,
            ':source_id' => $sourceId,
        ]);
    }

    /**
     * @return array<string, array<int, string>>
     */
    public function allGrouped(): array
    {
        $this->ensureTable();
        $rows = $this->pdo->query('SELECT entity, source_id FROM afs_update_pending ORDER BY entity, source_id')
            ?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $grouped = [];
        foreach ($rows as $row) {
            $entity = (string)($row['entity'] ?? '');
            $sourceId = (string)($row['source_id'] ?? '');
            if ($entity === '' || $sourceId === '') {
                continue;
            }
            $grouped[$entity][] = $sourceId;
        }
        return $grouped;
    }

    /**
     * @param array<int, string> $sourceIds
     */
    public function remove(string $entity, array $sourceIds): void
    {
        $sourceIds = array_values(array_filter(array_map('strval', $sourceIds), static fn(string $id): bool => $id !== ''));
        if ($entity === '' || $sourceIds === []) {
            return;
        }
        $this->ensureTable();
        $chunks = array_chunk($sourceIds, 500);
        foreach ($chunks as $chunk) {
            $placeholders = implode(',', array_fill(0, count($chunk), '?'));
            $sql = 'DELETE FROM afs_update_pending WHERE entity = ? AND source_id IN (' . $placeholders . ')';
            $stmt = $this->pdo->prepare($sql);
            $params = array_merge([$entity], $chunk);
            $stmt->execute($params);
        }
    }
}
