<?php
declare(strict_types=1);

namespace Welafix\Domain\Media;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;

final class MediaFilenameImporter
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function importFromAfs(): array
    {
        $pdo = $this->factory->localDb();

        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $stats = [
            'ok' => true,
            'found' => 0,
            'unique' => 0,
            'inserted' => 0,
        ];

        $map = [];

        $articleFields = [];
        for ($i = 1; $i <= 10; $i++) {
            $articleFields[] = 'Bild' . $i;
        }
        $articleSql = 'SELECT ' . implode(', ', array_map([$this, 'quoteIdentifier'], $articleFields)) . ' FROM artikel WHERE changed = 1 AND COALESCE(is_deleted, 0) = 0';
        $stmt = $pdo->query($articleSql);
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            foreach ($articleFields as $field) {
                $stats['found']++;
                $filename = normalizeMediaFilename($row[$field] ?? null);
                if ($filename === null) {
                    continue;
                }
                $key = strtolower($filename);
                $map[$key] = ['filename' => $filename, 'source' => 'article'];
            }
        }

        $wgFields = ['Bild', 'Bild_gross'];
        $wgSql = 'SELECT ' . implode(', ', array_map([$this, 'quoteIdentifier'], $wgFields)) . ' FROM warengruppe WHERE changed = 1 AND COALESCE(is_deleted, 0) = 0';
        $stmt = $pdo->query($wgSql);
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            foreach ($wgFields as $field) {
                $stats['found']++;
                $filename = normalizeMediaFilename($row[$field] ?? null);
                if ($filename === null) {
                    continue;
                }
                $key = strtolower($filename);
                if (!isset($map[$key])) {
                    $map[$key] = ['filename' => $filename, 'source' => 'warengruppe'];
                }
            }
        }

        $stats['unique'] = count($map);

        $insert = $pdo->prepare(
            $this->isMysql($pdo)
                ? 'INSERT IGNORE INTO media (filename, source, created_at, changed) VALUES (:filename, :source, :created_at, 1)'
                : 'INSERT OR IGNORE INTO media (filename, source, created_at, changed) VALUES (:filename, :source, :created_at, 1)'
        );

        $pdo->beginTransaction();
        try {
            foreach ($map as $item) {
                $insert->execute([
                    ':filename' => $item['filename'],
                    ':source' => $item['source'],
                    ':created_at' => $now,
                ]);
                if ($insert->rowCount() > 0) {
                    $stats['inserted']++;
                }
            }
            $pdo->commit();
        } catch (\Throwable $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }

        return $stats;
    }

    /**
     * @return array<string, mixed>
     */
    public function importFromDocuments(): array
    {
        $pdo = $this->factory->localDb();

        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $stats = [
            'ok' => true,
            'found' => 0,
            'unique' => 0,
            'inserted' => 0,
            'skipped' => 0,
            'table' => null,
            'column' => null,
        ];

        $table = 'documents';
        $column = $this->findColumn($pdo, $table, ['Titel', 'titel']);
        if ($column === null) {
            $stats['ok'] = false;
            $stats['error'] = 'Titel-Spalte nicht gefunden';
            return $stats;
        }

        $stats['table'] = $table;
        $stats['column'] = $column;

        $stmt = $pdo->query(
            'SELECT ' . $this->quoteIdentifier($column) . ' FROM ' . $this->quoteIdentifier($table) . ' WHERE changed = 1 AND COALESCE(is_deleted, 0) = 0'
        );
        $map = [];
        if ($stmt) {
            while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                $stats['found']++;
                $filename = normalizeMediaFilename($row[$column] ?? null);
                if ($filename === null) {
                    continue;
                }
                $key = strtolower($filename);
                $map[$key] = $filename;
            }
        }

        $stats['unique'] = count($map);

        $insert = $pdo->prepare(
            $this->isMysql($pdo)
                ? 'INSERT IGNORE INTO media (filename, source, type, created_at, changed) VALUES (:filename, :source, :type, :created_at, 1)'
                : 'INSERT OR IGNORE INTO media (filename, source, type, created_at, changed) VALUES (:filename, :source, :type, :created_at, 1)'
        );

        $pdo->beginTransaction();
        try {
            foreach ($map as $filename) {
                $insert->execute([
                    ':filename' => $filename,
                    ':source' => 'artikel',
                    ':type' => 'dokument',
                    ':created_at' => $now,
                ]);
                if ($insert->rowCount() > 0) {
                    $stats['inserted']++;
                } else {
                    $stats['skipped']++;
                }
            }
            $pdo->commit();
        } catch (\Throwable $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }

        return $stats;
    }

    private function findColumn(PDO $pdo, string $table, array $names): ?string
    {
        $stmt = $this->isMysql($pdo)
            ? $pdo->query('DESCRIBE ' . $this->quoteIdentifier($table))
            : $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $lookup = [];
        foreach ($names as $name) {
            $lookup[strtolower($name)] = $name;
        }
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? $row['Field'] ?? '');
            if ($name !== '' && isset($lookup[strtolower($name)])) {
                return $name;
            }
        }
        return null;
    }

    private function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }

    private function isMysql(PDO $pdo): bool
    {
        return (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql';
    }
}
