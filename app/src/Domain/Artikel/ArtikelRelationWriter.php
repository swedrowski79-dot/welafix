<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use PDO;

final class ArtikelRelationWriter
{
    public function __construct(private PDO $pdo) {}

    public function ensureSchema(): void
    {
        $this->pdo->exec(
            'CREATE TABLE IF NOT EXISTS artikel_attribute_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                afs_artikel_id TEXT NOT NULL,
                attributes_parent_id INTEGER NOT NULL,
                attributes_id INTEGER NOT NULL,
                position INTEGER NOT NULL DEFAULT 0,
                attribute_name TEXT NULL,
                attribute_value TEXT NULL,
                changed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(afs_artikel_id, attributes_parent_id, attributes_id)
            )'
        );
        $this->pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_attribute_map_artikel
             ON artikel_attribute_map(afs_artikel_id)'
        );

        $this->pdo->exec(
            'CREATE TABLE IF NOT EXISTS artikel_media_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                afs_artikel_id TEXT NOT NULL,
                media_id INTEGER NULL,
                filename TEXT NOT NULL,
                position INTEGER NOT NULL DEFAULT 0,
                is_main INTEGER NOT NULL DEFAULT 0,
                source_field TEXT NULL,
                changed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(afs_artikel_id, position, filename)
            )'
        );
        $this->pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_media_map_artikel
             ON artikel_media_map(afs_artikel_id)'
        );
        $this->pdo->exec(
            'CREATE TABLE IF NOT EXISTS artikel_warengruppe (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                afs_artikel_id TEXT NOT NULL,
                afs_wg_id INTEGER NOT NULL,
                position INTEGER NOT NULL DEFAULT 0,
                source_field TEXT NULL,
                changed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(afs_artikel_id, afs_wg_id)
            )'
        );
        $this->pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_warengruppe_artikel
             ON artikel_warengruppe(afs_artikel_id)'
        );
    }

    /**
     * @param array<int, array{position:int,parent_id:int,attribute_id:int,name:string,value:string}> $assignments
     */
    public function syncAttributeAssignments(string $afsArtikelId, array $assignments): int
    {
        $existing = $this->loadExistingAttributeAssignments($afsArtikelId);
        $incoming = [];
        foreach ($assignments as $assignment) {
            $key = $assignment['parent_id'] . '|' . $assignment['attribute_id'];
            $incoming[$key] = $assignment;
        }

        $changed = 0;
        $delete = $this->pdo->prepare(
            'DELETE FROM artikel_attribute_map
             WHERE afs_artikel_id = :afs_artikel_id
               AND attributes_parent_id = :attributes_parent_id
               AND attributes_id = :attributes_id'
        );
        foreach ($existing as $key => $row) {
            if (isset($incoming[$key])) {
                continue;
            }
            $delete->execute([
                ':afs_artikel_id' => $afsArtikelId,
                ':attributes_parent_id' => $row['attributes_parent_id'],
                ':attributes_id' => $row['attributes_id'],
            ]);
            $changed++;
        }

        $upsert = $this->pdo->prepare(
            'INSERT INTO artikel_attribute_map
                (afs_artikel_id, attributes_parent_id, attributes_id, position, attribute_name, attribute_value, changed)
             VALUES
                (:afs_artikel_id, :attributes_parent_id, :attributes_id, :position, :attribute_name, :attribute_value, 1)
             ON CONFLICT(afs_artikel_id, attributes_parent_id, attributes_id) DO UPDATE SET
                position = excluded.position,
                attribute_name = excluded.attribute_name,
                attribute_value = excluded.attribute_value,
                changed = CASE
                    WHEN artikel_attribute_map.position != excluded.position
                      OR COALESCE(artikel_attribute_map.attribute_name, \'\') != COALESCE(excluded.attribute_name, \'\')
                      OR COALESCE(artikel_attribute_map.attribute_value, \'\') != COALESCE(excluded.attribute_value, \'\')
                    THEN 1 ELSE artikel_attribute_map.changed END'
        );

        foreach ($incoming as $key => $assignment) {
            $before = $existing[$key] ?? null;
            $upsert->execute([
                ':afs_artikel_id' => $afsArtikelId,
                ':attributes_parent_id' => $assignment['parent_id'],
                ':attributes_id' => $assignment['attribute_id'],
                ':position' => $assignment['position'],
                ':attribute_name' => $assignment['name'],
                ':attribute_value' => $assignment['value'],
            ]);
            if ($before === null
                || (int)$before['position'] !== (int)$assignment['position']
                || (string)$before['attribute_name'] !== (string)$assignment['name']
                || (string)$before['attribute_value'] !== (string)$assignment['value']) {
                $changed++;
            }
        }

        if ($changed > 0) {
            $stmt = $this->pdo->prepare('UPDATE artikel_attribute_map SET changed = 1 WHERE afs_artikel_id = :afs_artikel_id');
            $stmt->execute([':afs_artikel_id' => $afsArtikelId]);
        }

        return $changed;
    }

    /**
     * @param array<int, array{filename:string,position:int,is_main:int,source_field:string,media_id:?int}> $items
     */
    public function syncMediaAssignments(string $afsArtikelId, array $items): int
    {
        $existing = $this->loadExistingMediaAssignments($afsArtikelId);
        $incoming = [];
        foreach ($items as $item) {
            $key = $item['position'] . '|' . strtolower($item['filename']);
            $incoming[$key] = $item;
        }

        $changed = 0;
        $delete = $this->pdo->prepare(
            'DELETE FROM artikel_media_map
             WHERE afs_artikel_id = :afs_artikel_id
               AND position = :position
               AND lower(filename) = lower(:filename)'
        );
        foreach ($existing as $key => $row) {
            if (isset($incoming[$key])) {
                continue;
            }
            $delete->execute([
                ':afs_artikel_id' => $afsArtikelId,
                ':position' => $row['position'],
                ':filename' => $row['filename'],
            ]);
            $changed++;
        }

        $upsert = $this->pdo->prepare(
            'INSERT INTO artikel_media_map
                (afs_artikel_id, media_id, filename, position, is_main, source_field, changed)
             VALUES
                (:afs_artikel_id, :media_id, :filename, :position, :is_main, :source_field, 1)
             ON CONFLICT(afs_artikel_id, position, filename) DO UPDATE SET
                media_id = excluded.media_id,
                is_main = excluded.is_main,
                source_field = excluded.source_field,
                changed = CASE
                    WHEN COALESCE(artikel_media_map.media_id, 0) != COALESCE(excluded.media_id, 0)
                      OR artikel_media_map.is_main != excluded.is_main
                      OR COALESCE(artikel_media_map.source_field, \'\') != COALESCE(excluded.source_field, \'\')
                    THEN 1 ELSE artikel_media_map.changed END'
        );

        foreach ($incoming as $key => $item) {
            $before = $existing[$key] ?? null;
            $upsert->bindValue(':afs_artikel_id', $afsArtikelId, PDO::PARAM_STR);
            if ($item['media_id'] === null) {
                $upsert->bindValue(':media_id', null, PDO::PARAM_NULL);
            } else {
                $upsert->bindValue(':media_id', $item['media_id'], PDO::PARAM_INT);
            }
            $upsert->bindValue(':filename', $item['filename'], PDO::PARAM_STR);
            $upsert->bindValue(':position', $item['position'], PDO::PARAM_INT);
            $upsert->bindValue(':is_main', $item['is_main'], PDO::PARAM_INT);
            $upsert->bindValue(':source_field', $item['source_field'], PDO::PARAM_STR);
            $upsert->execute();
            if ($before === null
                || (int)($before['media_id'] ?? 0) !== (int)($item['media_id'] ?? 0)
                || (int)$before['is_main'] !== (int)$item['is_main']
                || (string)$before['source_field'] !== (string)$item['source_field']) {
                $changed++;
            }
        }

        if ($changed > 0) {
            $stmt = $this->pdo->prepare('UPDATE artikel_media_map SET changed = 1 WHERE afs_artikel_id = :afs_artikel_id');
            $stmt->execute([':afs_artikel_id' => $afsArtikelId]);
        }

        return $changed;
    }

    /**
     * @param array<int, array{afs_wg_id:int,position:int,source_field:string}> $items
     */
    public function syncWarengruppeAssignments(string $afsArtikelId, array $items): int
    {
        $existing = $this->loadExistingWarengruppeAssignments($afsArtikelId);
        $incoming = [];
        foreach ($items as $item) {
            $incoming[(string)$item['afs_wg_id']] = $item;
        }

        $changed = 0;
        $delete = $this->pdo->prepare(
            'DELETE FROM artikel_warengruppe
             WHERE afs_artikel_id = :afs_artikel_id
               AND afs_wg_id = :afs_wg_id'
        );
        foreach ($existing as $key => $row) {
            if (isset($incoming[$key])) {
                continue;
            }
            $delete->execute([
                ':afs_artikel_id' => $afsArtikelId,
                ':afs_wg_id' => $row['afs_wg_id'],
            ]);
            $changed++;
        }

        $upsert = $this->pdo->prepare(
            'INSERT INTO artikel_warengruppe
                (afs_artikel_id, afs_wg_id, position, source_field, changed)
             VALUES
                (:afs_artikel_id, :afs_wg_id, :position, :source_field, 1)
             ON CONFLICT(afs_artikel_id, afs_wg_id) DO UPDATE SET
                position = excluded.position,
                source_field = excluded.source_field,
                changed = CASE
                    WHEN artikel_warengruppe.position != excluded.position
                      OR COALESCE(artikel_warengruppe.source_field, \'\') != COALESCE(excluded.source_field, \'\')
                    THEN 1 ELSE artikel_warengruppe.changed END'
        );

        foreach ($incoming as $key => $item) {
            $before = $existing[$key] ?? null;
            $upsert->execute([
                ':afs_artikel_id' => $afsArtikelId,
                ':afs_wg_id' => $item['afs_wg_id'],
                ':position' => $item['position'],
                ':source_field' => $item['source_field'],
            ]);
            if ($before === null
                || (int)$before['position'] !== (int)$item['position']
                || (string)$before['source_field'] !== (string)$item['source_field']) {
                $changed++;
            }
        }

        if ($changed > 0) {
            $stmt = $this->pdo->prepare('UPDATE artikel_warengruppe SET changed = 1 WHERE afs_artikel_id = :afs_artikel_id');
            $stmt->execute([':afs_artikel_id' => $afsArtikelId]);
        }

        return $changed;
    }

    public function ensureMedia(string $filename, string $source, string $createdAt): ?int
    {
        $insert = $this->pdo->prepare(
            'INSERT OR IGNORE INTO media (filename, source, created_at, changed)
             VALUES (:filename, :source, :created_at, 1)'
        );
        $insert->execute([
            ':filename' => $filename,
            ':source' => $source,
            ':created_at' => $createdAt,
        ]);

        $select = $this->pdo->prepare('SELECT id FROM media WHERE lower(filename) = lower(:filename) LIMIT 1');
        $select->execute([':filename' => $filename]);
        $id = $select->fetchColumn();
        if ($id === false) {
            return null;
        }
        return (int)$id;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function loadExistingAttributeAssignments(string $afsArtikelId): array
    {
        $stmt = $this->pdo->prepare(
            'SELECT attributes_parent_id, attributes_id, position, attribute_name, attribute_value
             FROM artikel_attribute_map
             WHERE afs_artikel_id = :afs_artikel_id'
        );
        $stmt->execute([':afs_artikel_id' => $afsArtikelId]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $map = [];
        foreach ($rows as $row) {
            $key = (int)$row['attributes_parent_id'] . '|' . (int)$row['attributes_id'];
            $map[$key] = $row;
        }
        return $map;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function loadExistingMediaAssignments(string $afsArtikelId): array
    {
        $stmt = $this->pdo->prepare(
            'SELECT media_id, filename, position, is_main, source_field
             FROM artikel_media_map
             WHERE afs_artikel_id = :afs_artikel_id'
        );
        $stmt->execute([':afs_artikel_id' => $afsArtikelId]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $map = [];
        foreach ($rows as $row) {
            $key = (int)$row['position'] . '|' . strtolower((string)$row['filename']);
            $map[$key] = $row;
        }
        return $map;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function loadExistingWarengruppeAssignments(string $afsArtikelId): array
    {
        $stmt = $this->pdo->prepare(
            'SELECT afs_wg_id, position, source_field
             FROM artikel_warengruppe
             WHERE afs_artikel_id = :afs_artikel_id'
        );
        $stmt->execute([':afs_artikel_id' => $afsArtikelId]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $map = [];
        foreach ($rows as $row) {
            $map[(string)$row['afs_wg_id']] = $row;
        }
        return $map;
    }
}
