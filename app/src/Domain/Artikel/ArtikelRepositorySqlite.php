<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use Welafix\Domain\ChangeTracking\ChangeTracker;
use PDO;

final class ArtikelRepositorySqlite
{
    private PDO $pdo;
    private ChangeTracker $tracker;
    /** @var array<string, \PDOStatement> */
    private array $insertCache = [];
    /** @var array<string, \PDOStatement> */
    private array $updateCache = [];

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
        $this->tracker = new ChangeTracker();
    }

    public function ensureTable(): void
    {
        $sql = 'CREATE TABLE IF NOT EXISTS artikel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            afs_artikel_id TEXT UNIQUE,
            artikelnummer TEXT,
            warengruppe_id INTEGER,
            seo_url TEXT,
            master_modell TEXT,
            is_master INTEGER,
            is_deleted INTEGER DEFAULT 0,
            row_hash TEXT,
            last_seen_at TEXT,
            last_synced_at TEXT,
            changed INTEGER DEFAULT 0,
            changed_fields TEXT,
            change_reason TEXT
        )';
        $this->pdo->exec($sql);
    }

    /**
     * @param array<int, string> $diffColumns
     * @return array<string, mixed>|null
     */
    public function findByAfsArtikelId(string $afsArtikelId, array $diffColumns = []): ?array
    {
        $select = ['row_hash', 'change_reason', 'changed', 'changed_fields', 'last_synced_at'];
        foreach ($diffColumns as $col) {
            $select[] = $this->quoteIdentifier($col);
        }
        $selectList = implode(', ', array_values(array_unique($select)));
        $stmt = $this->pdo->prepare('SELECT ' . $selectList . ' FROM artikel WHERE afs_artikel_id = :id LIMIT 1');
        $stmt->bindValue(':id', $afsArtikelId, PDO::PARAM_STR);
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return $row ?: null;
    }

    /**
     * @param array<string, mixed> $data
     * @param array<string, mixed> $extraFields
     * @param array<int, string> $extraKeys
     * @param array<int, string> $diffColumns
     * @return array{inserted:bool,updated:bool,unchanged:bool}
     */
    public function upsertArtikel(array $data, string $seenAtIso, array $extraFields, array $extraKeys, string $rowHash, array $diffColumns = []): array
    {
        $afsArtikelId = (string)$data['afs_artikel_id'];
        $existing = $this->findByAfsArtikelId($afsArtikelId, $diffColumns);
        $diff = $this->tracker->buildDiff($existing, $extraFields, $diffColumns);

        if ($existing === null) {
            $columns = ['afs_artikel_id', 'artikelnummer', 'warengruppe_id', 'is_deleted'];
            $params = [':afs_artikel_id', ':artikelnummer', ':warengruppe_id', ':is_deleted'];
            foreach ($extraKeys as $key) {
                $columns[] = $this->quoteIdentifier($key);
                $params[] = ':' . $key;
            }
            $columns[] = 'row_hash';
            $columns[] = 'last_seen_at';
            $columns[] = 'last_synced_at';
            $columns[] = 'changed';
            $columns[] = 'changed_fields';
            $columns[] = 'change_reason';
            $params[] = ':row_hash';
            $params[] = ':last_seen_at';
            $params[] = ':last_synced_at';
            $params[] = ':changed';
            $params[] = ':changed_fields';
            $params[] = ':change_reason';

            $stmt = $this->prepareInsert($extraKeys, $columns, $params);
            $stmt->bindValue(':afs_artikel_id', $afsArtikelId, PDO::PARAM_STR);
            $stmt->bindValue(':artikelnummer', (string)$data['artikelnummer'], PDO::PARAM_STR);
            $this->bindNullableInt($stmt, ':warengruppe_id', $data['warengruppe_id'] ?? null);
            $stmt->bindValue(':is_deleted', 0, PDO::PARAM_INT);
            foreach ($extraKeys as $key) {
                $this->bindNullableText($stmt, ':' . $key, $extraFields[$key] ?? null);
            }
            $stmt->bindValue(':row_hash', $rowHash, PDO::PARAM_STR);
            $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':last_synced_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':changed', 1, PDO::PARAM_INT);
            $stmt->bindValue(':changed_fields', $this->tracker->encodeDiff($diff), PDO::PARAM_STR);
            $stmt->bindValue(':change_reason', 'new', PDO::PARAM_STR);
            $stmt->execute();
            return ['inserted' => true, 'updated' => false, 'unchanged' => false, 'diff' => $diff];
        }

        $existingHash = (string)($existing['row_hash'] ?? '');
        if ($existingHash !== '' && $existingHash === $rowHash) {
            $stmt = $this->pdo->prepare(
                'UPDATE artikel
                 SET last_seen_at = :last_seen_at,
                     last_synced_at = :last_synced_at,
                     is_deleted = 0
                 WHERE afs_artikel_id = :id'
            );
            $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':last_synced_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':id', $afsArtikelId, PDO::PARAM_STR);
            $stmt->execute();
            return ['inserted' => false, 'updated' => false, 'unchanged' => true, 'diff' => []];
        }

        $changeReason = $this->mergeReasons((string)($existing['change_reason'] ?? ''), ['fields']);

        $setParts = [
            'artikelnummer = :artikelnummer',
            'warengruppe_id = :warengruppe_id',
            'is_deleted = :is_deleted',
        ];
        foreach ($extraKeys as $key) {
            $setParts[] = $this->quoteIdentifier($key) . ' = :' . $key;
        }
        $setParts[] = 'row_hash = :row_hash';
        $setParts[] = 'last_seen_at = :last_seen_at';
        $setParts[] = 'last_synced_at = :last_synced_at';
        if ($diff !== []) {
            $setParts[] = 'changed = 1';
            $setParts[] = 'changed_fields = :changed_fields';
        }
        $setParts[] = 'change_reason = :change_reason';

        $stmt = $this->prepareUpdate($extraKeys, $setParts);
        $stmt->bindValue(':artikelnummer', (string)$data['artikelnummer'], PDO::PARAM_STR);
        $this->bindNullableInt($stmt, ':warengruppe_id', $data['warengruppe_id'] ?? null);
        $stmt->bindValue(':is_deleted', 0, PDO::PARAM_INT);
        foreach ($extraKeys as $key) {
            $this->bindNullableText($stmt, ':' . $key, $extraFields[$key] ?? null);
        }
        $stmt->bindValue(':row_hash', $rowHash, PDO::PARAM_STR);
        $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
        $stmt->bindValue(':last_synced_at', $seenAtIso, PDO::PARAM_STR);
        if ($diff !== []) {
            $stmt->bindValue(':changed_fields', $this->tracker->encodeDiff($diff), PDO::PARAM_STR);
        }
        $stmt->bindValue(':change_reason', $changeReason, PDO::PARAM_STR);
        $stmt->bindValue(':id', $afsArtikelId, PDO::PARAM_STR);
        $stmt->execute();

        return ['inserted' => false, 'updated' => true, 'unchanged' => false, 'diff' => $diff];
    }

    /**
     * @param string[] $reasons
     */
    private function mergeReasons(string $existing, array $reasons): string
    {
        $items = [];
        if ($existing !== '') {
            foreach (explode(',', $existing) as $item) {
                $item = trim($item);
                if ($item !== '') {
                    $items[$item] = true;
                }
            }
        }
        foreach ($reasons as $reason) {
            $items[$reason] = true;
        }
        return implode(',', array_keys($items));
    }

    public function encodeDiff(array $diff): string
    {
        return $this->tracker->encodeDiff($diff);
    }

    private function bindNullableInt(\PDOStatement $stmt, string $param, $value): void
    {
        if ($value === null || $value === '') {
            $stmt->bindValue($param, null, PDO::PARAM_NULL);
            return;
        }
        $stmt->bindValue($param, (int)$value, PDO::PARAM_INT);
    }

    private function bindNullableText(\PDOStatement $stmt, string $param, $value): void
    {
        if ($value === null || $value === '') {
            $stmt->bindValue($param, null, PDO::PARAM_NULL);
            return;
        }
        $stmt->bindValue($param, (string)$value, PDO::PARAM_STR);
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    /**
     * @param array<int, string> $extraKeys
     * @param array<int, string> $columns
     * @param array<int, string> $params
     */
    private function prepareInsert(array $extraKeys, array $columns, array $params): \PDOStatement
    {
        $cacheKey = implode('|', $extraKeys);
        if (isset($this->insertCache[$cacheKey])) {
            return $this->insertCache[$cacheKey];
        }
        $stmt = $this->pdo->prepare(
            'INSERT INTO artikel (' . implode(',', $columns) . ')
             VALUES (' . implode(',', $params) . ')'
        );
        $this->insertCache[$cacheKey] = $stmt;
        return $stmt;
    }

    /**
     * @param array<int, string> $extraKeys
     * @param array<int, string> $setParts
     */
    private function prepareUpdate(array $extraKeys, array $setParts): \PDOStatement
    {
        $cacheKey = implode('|', $extraKeys);
        if (isset($this->updateCache[$cacheKey])) {
            return $this->updateCache[$cacheKey];
        }
        $stmt = $this->pdo->prepare(
            'UPDATE artikel
             SET ' . implode(', ', $setParts) . '
             WHERE afs_artikel_id = :id'
        );
        $this->updateCache[$cacheKey] = $stmt;
        return $stmt;
    }
}
