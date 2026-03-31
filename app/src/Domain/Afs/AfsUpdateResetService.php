<?php
declare(strict_types=1);

namespace Welafix\Domain\Afs;

use PDO;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;

final class AfsUpdateResetService
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function run(): array
    {
        $sqlite = Db::guardSqlite($this->factory->sqlite(), __METHOD__ . ':sqlite');
        $mssql = Db::guardMssql($this->factory->mssql(), __METHOD__ . ':mssql');
        $queue = new AfsUpdateQueue($sqlite);
        $grouped = $queue->allGrouped();

        $stats = [
            'ok' => true,
            'afs_reset' => [],
            'local_reset' => [],
        ];

        foreach ($grouped as $entity => $sourceIds) {
            $count = $this->resetMssqlUpdateFlag($mssql, $entity, $sourceIds);
            $queue->remove($entity, $sourceIds);
            $stats['afs_reset'][$entity] = $count;
        }

        foreach ($this->localResetStatements() as $table => $sql) {
            $stmt = $sqlite->prepare($sql);
            $stmt->execute();
            $stats['local_reset'][$table] = $stmt->rowCount();
        }

        return $stats;
    }

    /**
     * @param array<int, string> $sourceIds
     */
    private function resetMssqlUpdateFlag(PDO $pdo, string $entity, array $sourceIds): int
    {
        $map = [
            'artikel' => ['table' => 'dbo.Artikel', 'key' => 'Artikel'],
            'warengruppe' => ['table' => 'dbo.Warengruppe', 'key' => 'Warengruppe'],
            'dokument' => ['table' => 'dbo.Dokument', 'key' => 'Zaehler'],
        ];
        if (!isset($map[$entity])) {
            return 0;
        }
        $config = $map[$entity];
        $total = 0;
        foreach (array_chunk($sourceIds, 500) as $chunk) {
            $placeholders = implode(',', array_fill(0, count($chunk), '?'));
            $sql = 'UPDATE ' . $config['table'] . ' SET [Update] = 0 WHERE [' . $config['key'] . '] IN (' . $placeholders . ')';
            $stmt = $pdo->prepare($sql);
            $stmt->execute($chunk);
            $total += $stmt->rowCount();
        }
        return $total;
    }

    /**
     * @return array<string, string>
     */
    private function localResetStatements(): array
    {
        return [
            'artikel' => 'UPDATE artikel SET changed = 0 WHERE changed = 1',
            'warengruppe' => 'UPDATE warengruppe SET changed = 0 WHERE changed = 1',
            'documents' => 'UPDATE documents SET changed = 0 WHERE changed = 1',
            'media' => 'UPDATE media SET changed = 0 WHERE changed = 1',
            'attributes' => 'UPDATE attributes SET changed = 0 WHERE changed = 1',
            'artikel_attribute_map' => 'UPDATE artikel_attribute_map SET changed = 0 WHERE changed = 1',
            'artikel_media_map' => 'UPDATE artikel_media_map SET changed = 0 WHERE changed = 1',
            'Meta_Data_Artikel' => 'UPDATE Meta_Data_Artikel SET updated = 0 WHERE updated = 1',
            'Meta_Data_Waregruppen' => 'UPDATE Meta_Data_Waregruppen SET updated = 0 WHERE updated = 1',
        ];
    }
}
