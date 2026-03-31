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
                UNIQUE(afs_artikel_id, position, filename)
            )'
        );
        $this->pdo->exec(
            'CREATE INDEX IF NOT EXISTS idx_artikel_media_map_artikel
             ON artikel_media_map(afs_artikel_id)'
        );
    }

    /**
     * @param array<int, array{position:int,parent_id:int,attribute_id:int,name:string,value:string}> $assignments
     */
    public function syncAttributeAssignments(string $afsArtikelId, array $assignments): int
    {
        $delete = $this->pdo->prepare('DELETE FROM artikel_attribute_map WHERE afs_artikel_id = :afs_artikel_id');
        $delete->execute([':afs_artikel_id' => $afsArtikelId]);

        if ($assignments === []) {
            return 0;
        }

        $insert = $this->pdo->prepare(
            'INSERT INTO artikel_attribute_map
                (afs_artikel_id, attributes_parent_id, attributes_id, position, attribute_name, attribute_value)
             VALUES
                (:afs_artikel_id, :attributes_parent_id, :attributes_id, :position, :attribute_name, :attribute_value)'
        );

        $count = 0;
        foreach ($assignments as $assignment) {
            $insert->execute([
                ':afs_artikel_id' => $afsArtikelId,
                ':attributes_parent_id' => $assignment['parent_id'],
                ':attributes_id' => $assignment['attribute_id'],
                ':position' => $assignment['position'],
                ':attribute_name' => $assignment['name'],
                ':attribute_value' => $assignment['value'],
            ]);
            $count++;
        }

        return $count;
    }

    /**
     * @param array<int, array{filename:string,position:int,is_main:int,source_field:string,media_id:?int}> $items
     */
    public function syncMediaAssignments(string $afsArtikelId, array $items): int
    {
        $delete = $this->pdo->prepare('DELETE FROM artikel_media_map WHERE afs_artikel_id = :afs_artikel_id');
        $delete->execute([':afs_artikel_id' => $afsArtikelId]);

        if ($items === []) {
            return 0;
        }

        $insert = $this->pdo->prepare(
            'INSERT INTO artikel_media_map
                (afs_artikel_id, media_id, filename, position, is_main, source_field)
             VALUES
                (:afs_artikel_id, :media_id, :filename, :position, :is_main, :source_field)'
        );

        $count = 0;
        foreach ($items as $item) {
            $insert->bindValue(':afs_artikel_id', $afsArtikelId, PDO::PARAM_STR);
            if ($item['media_id'] === null) {
                $insert->bindValue(':media_id', null, PDO::PARAM_NULL);
            } else {
                $insert->bindValue(':media_id', $item['media_id'], PDO::PARAM_INT);
            }
            $insert->bindValue(':filename', $item['filename'], PDO::PARAM_STR);
            $insert->bindValue(':position', $item['position'], PDO::PARAM_INT);
            $insert->bindValue(':is_main', $item['is_main'], PDO::PARAM_INT);
            $insert->bindValue(':source_field', $item['source_field'], PDO::PARAM_STR);
            $insert->execute();
            $count++;
        }

        return $count;
    }

    public function ensureMedia(string $filename, string $source, string $createdAt): ?int
    {
        $insert = $this->pdo->prepare(
            'INSERT OR IGNORE INTO media (filename, source, created_at)
             VALUES (:filename, :source, :created_at)'
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
}
