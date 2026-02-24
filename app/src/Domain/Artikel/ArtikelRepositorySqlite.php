<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use PDO;

final class ArtikelRepositorySqlite
{
    private PDO $pdo;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    public function ensureTable(): void
    {
        $sql = 'CREATE TABLE IF NOT EXISTS artikel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            afs_artikel_id TEXT UNIQUE,
            afs_key TEXT UNIQUE,
            artikelnummer TEXT,
            name TEXT,
            warengruppe_id INTEGER,
            price REAL,
            stock INTEGER,
            online INTEGER,
            last_seen_at TEXT,
            changed INTEGER DEFAULT 0,
            change_reason TEXT
        )';
        $this->pdo->exec($sql);
    }

    /**
     * @return array<string, mixed>|null
     */
    public function findByAfsKey(string $afsKey): ?array
    {
        $stmt = $this->pdo->prepare('SELECT * FROM artikel WHERE afs_key = :key LIMIT 1');
        $stmt->bindValue(':key', $afsKey, PDO::PARAM_STR);
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return $row ?: null;
    }

    /**
     * @param array<string, mixed> $data
     * @return array{inserted:bool,updated:bool,unchanged:bool}
     */
    public function upsertArtikel(array $data, string $seenAtIso): array
    {
        $afsKey = (string)$data['afs_key'];
        $existing = $this->findByAfsKey($afsKey);

        if ($existing === null) {
            $stmt = $this->pdo->prepare(
                'INSERT INTO artikel (afs_key, artikelnummer, name, warengruppe_id, price, stock, online, last_seen_at, changed, change_reason)
                 VALUES (:afs_key, :artikelnummer, :name, :warengruppe_id, :price, :stock, :online, :last_seen_at, :changed, :change_reason)'
            );
            $stmt->bindValue(':afs_key', $afsKey, PDO::PARAM_STR);
            $stmt->bindValue(':artikelnummer', (string)$data['artikelnummer'], PDO::PARAM_STR);
            $stmt->bindValue(':name', (string)$data['name'], PDO::PARAM_STR);
            $this->bindNullableInt($stmt, ':warengruppe_id', $data['warengruppe_id'] ?? null);
            $stmt->bindValue(':price', (float)$data['price'], PDO::PARAM_STR);
            $stmt->bindValue(':stock', (int)$data['stock'], PDO::PARAM_INT);
            $stmt->bindValue(':online', (int)$data['online'], PDO::PARAM_INT);
            $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':changed', 1, PDO::PARAM_INT);
            $stmt->bindValue(':change_reason', 'new', PDO::PARAM_STR);
            $stmt->execute();
            return ['inserted' => true, 'updated' => false, 'unchanged' => false];
        }

        $newHash = $this->hashRow($data);
        $existingHash = $this->hashRow($existing);

        if ($newHash === $existingHash) {
            $stmt = $this->pdo->prepare(
                'UPDATE artikel SET last_seen_at = :last_seen_at WHERE afs_key = :afs_key'
            );
            $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
            $stmt->bindValue(':afs_key', $afsKey, PDO::PARAM_STR);
            $stmt->execute();
            return ['inserted' => false, 'updated' => false, 'unchanged' => true];
        }

        $changeReason = $this->mergeReasons((string)($existing['change_reason'] ?? ''), ['fields']);

        $stmt = $this->pdo->prepare(
            'UPDATE artikel
             SET artikelnummer = :artikelnummer,
                 name = :name,
                 warengruppe_id = :warengruppe_id,
                 price = :price,
                 stock = :stock,
                 online = :online,
                 last_seen_at = :last_seen_at,
                 changed = 1,
                 change_reason = :change_reason
             WHERE afs_key = :afs_key'
        );
        $stmt->bindValue(':artikelnummer', (string)$data['artikelnummer'], PDO::PARAM_STR);
        $stmt->bindValue(':name', (string)$data['name'], PDO::PARAM_STR);
        $this->bindNullableInt($stmt, ':warengruppe_id', $data['warengruppe_id'] ?? null);
        $stmt->bindValue(':price', (float)$data['price'], PDO::PARAM_STR);
        $stmt->bindValue(':stock', (int)$data['stock'], PDO::PARAM_INT);
        $stmt->bindValue(':online', (int)$data['online'], PDO::PARAM_INT);
        $stmt->bindValue(':last_seen_at', $seenAtIso, PDO::PARAM_STR);
        $stmt->bindValue(':change_reason', $changeReason, PDO::PARAM_STR);
        $stmt->bindValue(':afs_key', $afsKey, PDO::PARAM_STR);
        $stmt->execute();

        return ['inserted' => false, 'updated' => true, 'unchanged' => false];
    }

    /**
     * @param array<string, mixed> $row
     */
    private function hashRow(array $row): string
    {
        $values = [
            (string)($row['artikelnummer'] ?? ''),
            (string)($row['name'] ?? ''),
            (string)($row['warengruppe_id'] ?? ''),
            (string)($row['price'] ?? ''),
            (string)($row['stock'] ?? ''),
            (string)($row['online'] ?? ''),
        ];
        return sha1(implode('|', $values));
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

    private function bindNullableInt(\PDOStatement $stmt, string $param, $value): void
    {
        if ($value === null || $value === '') {
            $stmt->bindValue($param, null, PDO::PARAM_NULL);
            return;
        }
        $stmt->bindValue($param, (int)$value, PDO::PARAM_INT);
    }
}
