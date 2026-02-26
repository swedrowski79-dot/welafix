<?php
declare(strict_types=1);

namespace Welafix\Domain\Media;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;
use Welafix\Database\ConnectionFactory;

final class MediaStorageChecker
{
    private const IMAGE_EXTS = [
        'jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'tif', 'tiff',
    ];
    private const DOCUMENT_EXTS = [
        'pdf', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'rtf', 'csv', 'xml', 'zip',
        'jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'tif', 'tiff',
    ];

    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function check(): array
    {
        $pdo = $this->factory->sqlite();
        $imageBase = $this->imageStorageBase();
        $docBase = $this->documentStorageBase();
        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);

        $log = [];
        $usageMap = $this->buildUsageMap($pdo);
        $imageMap = $this->scanStorage($imageBase, self::IMAGE_EXTS, $log);
        $docMap = $this->scanStorage($docBase, self::DOCUMENT_EXTS, $log);

        $stmt = $pdo->query('SELECT id, filename, checksum, type, is_deleted FROM media');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];

        $update = $pdo->prepare(
            'UPDATE media
             SET type = :type,
                 storage_path = :storage_path,
                 checksum = :checksum,
                 is_deleted = :is_deleted,
                 last_checked_at = :last_checked_at
             WHERE id = :id'
        );
        $updateChanged = $pdo->prepare(
            'UPDATE media
             SET type = :type,
                 storage_path = :storage_path,
                 checksum = :checksum,
                 changed = 1,
                 is_deleted = :is_deleted,
                 last_checked_at = :last_checked_at
             WHERE id = :id'
        );

        $stats = [
            'checked' => 0,
            'found' => 0,
            'notFound' => 0,
            'changed' => 0,
            'warnings' => 0,
        ];

        foreach ($rows as $row) {
            $stats['checked']++;
            $id = (int)($row['id'] ?? 0);
            $filename = (string)($row['filename'] ?? '');
            $type = strtolower((string)($row['type'] ?? 'image'));
            $isDeleted = (int)($row['is_deleted'] ?? 0);
            $key = strtolower($filename);
            $oldChecksum = (string)($row['checksum'] ?? '');

            $usageList = $usageMap[$key] ?? [];
            if ($isDeleted === 1 && $usageList === []) {
                // skip until article usage exists again
                continue;
            }

            $storagePath = null;
            $newChecksum = 'notFound';
            $lookup = ($type === 'dokument') ? $docMap : $imageMap;
            if ($key !== '' && isset($lookup[$key])) {
                $storagePath = $lookup[$key];
                $newChecksum = $this->quickChecksum($storagePath);
                $stats['found']++;
            } else {
                $stats['notFound']++;
                $usage = $this->formatUsage($usageList);
                $line = ($type === 'dokument' ? 'document file ' : 'file ') . $filename . ' nicht gefunden' . $usage;
                $this->emitLog($line, $log);
            }

            $hasChanged = $newChecksum !== $oldChecksum;
            if ($hasChanged) {
                $stats['changed']++;
                $line = ($type === 'dokument' ? 'document file ' : 'file ') . $filename . ' geändert';
                $this->emitLog($line, $log);
            }

            $stmt = $hasChanged ? $updateChanged : $update;
            $stmt->execute([
                ':type' => $type,
                ':storage_path' => $storagePath,
                ':checksum' => $newChecksum,
                ':is_deleted' => ($newChecksum === 'notFound' && $usageList === []) ? 1 : 0,
                ':last_checked_at' => $now,
                ':id' => $id,
            ]);
        }

        return [
            'ok' => true,
            'base' => [
                'images' => $imageBase,
                'documents' => $docBase,
            ],
            'counts' => $stats,
            'log' => $log,
        ];
    }

    private function imageStorageBase(): string
    {
        $base = (string)env('IMAGES_STORAGE_PATH', __DIR__ . '/../../../data/storage/pictures');
        return rtrim($base, "/\\");
    }

    private function documentStorageBase(): string
    {
        $base = (string)env('DOCUMENTS_STORAGE_PATH', __DIR__ . '/../../../storage/documents');
        return rtrim($base, "/\\");
    }

    /**
     * @param array<int, string> $log
     * @return array<string, string>
     */
    private function scanStorage(string $base, array $exts, array &$log): array
    {
        $map = [];
        if (!is_dir($base)) {
            $this->emitLog('storage path nicht gefunden: ' . $base, $log);
            return $map;
        }

        $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($base));
        foreach ($iterator as $file) {
            if (!$file instanceof SplFileInfo || !$file->isFile()) {
                continue;
            }
            $ext = strtolower($file->getExtension());
            if (!in_array($ext, $exts, true)) {
                continue;
            }
            $name = $file->getFilename();
            $key = strtolower($name);
            if (isset($map[$key])) {
                $this->emitLog('warn: mehrfach gefunden ' . $name, $log);
                continue;
            }
            $map[$key] = $file->getPathname();
        }
        return $map;
    }

    private function quickChecksum(string $path): string
    {
        $size = @filesize($path);
        $mtime = @filemtime($path);
        if ($size === false || $mtime === false) {
            return 'notFound';
        }
        return $size . '-' . $mtime;
    }

    /**
     * @return array<string, array<int, string>>
     */
    private function buildUsageMap(PDO $pdo): array
    {
        $usage = [];

        $artikelnummerMap = [];
        $artikelnummerSet = [];
        $artikelIdMap = [];
        $stmt = $pdo->query('SELECT afs_artikel_id, artikelnummer, Artikel FROM artikel');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        foreach ($rows as $row) {
            $id = trim((string)($row['afs_artikel_id'] ?? ''));
            $nr = trim((string)($row['artikelnummer'] ?? ''));
            $artikelId = trim((string)($row['Artikel'] ?? ''));
            if ($id !== '' && $nr !== '') {
                $artikelnummerMap[$id] = $nr;
            }
            if ($nr !== '') {
                $artikelnummerSet[$nr] = $nr;
            }
            if ($artikelId !== '' && $nr !== '') {
                $artikelIdMap[$artikelId] = $nr;
            }
        }

        $artikelImageCols = $this->findExistingColumns($pdo, 'artikel', [
            'Bild1','Bild2','Bild3','Bild4','Bild5','Bild6','Bild7','Bild8','Bild9','Bild10'
        ]);
        if ($artikelImageCols !== []) {
            $sql = 'SELECT artikelnummer, ' . implode(', ', array_map([$this, 'quoteIdentifier'], $artikelImageCols)) . ' FROM artikel';
            $stmt = $pdo->query($sql);
            $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
            foreach ($rows as $row) {
                $nr = (string)($row['artikelnummer'] ?? '');
                if ($nr === '') {
                    continue;
                }
                foreach ($artikelImageCols as $col) {
                    $filename = normalizeMediaFilename($row[$col] ?? null);
                    if ($filename === null) {
                        continue;
                    }
                    $key = strtolower($filename);
                    $usage[$key][$nr] = $nr;
                }
            }
        }

        $docFileCol = $this->findColumn($pdo, 'documents', ['Titel','titel','Dateiname','dateiname']);
        $docArticleIdCol = $this->findColumn($pdo, 'documents', ['Artikel_ID','artikel_id','Artikel','artikel']);
        if ($docFileCol !== null && $docArticleIdCol !== null) {
            $sql = 'SELECT ' . $this->quoteIdentifier($docFileCol) . ' AS fname, ' .
                $this->quoteIdentifier($docArticleIdCol) . ' AS art_id FROM documents';
            $stmt = $pdo->query($sql);
            $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
            foreach ($rows as $row) {
                $filename = normalizeMediaFilename($row['fname'] ?? null);
                if ($filename === null) {
                    continue;
                }
                $artId = trim((string)($row['art_id'] ?? ''));
                $nr = $artikelnummerMap[$artId] ?? ($artikelIdMap[$artId] ?? ($artikelnummerSet[$artId] ?? ''));
                if ($nr === '') {
                    continue;
                }
                $key = strtolower($filename);
                $usage[$key][$nr] = $nr;
            }
        }

        $out = [];
        foreach ($usage as $key => $set) {
            $out[$key] = array_values($set);
        }
        return $out;
    }

    /**
     * @param array<int, string> $artikelnr
     */
    private function formatUsage(array $artikelnr): string
    {
        if ($artikelnr === []) {
            return ' (keine Zuordnung)';
        }
        sort($artikelnr, SORT_STRING);
        return ' (verwendet in Artikel: ' . implode(', ', $artikelnr) . ')';
    }

    private function emitLog(string $line, array &$log): void
    {
        if (PHP_SAPI === 'cli') {
            echo $line . "\n";
        } else {
            error_log($line);
        }
        $log[] = $line;
    }

    /**
     * @param array<int, string> $names
     * @return array<int, string>
     */
    private function findExistingColumns(PDO $pdo, string $table, array $names): array
    {
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $existing = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $existing[strtolower($name)] = $name;
            }
        }
        $found = [];
        foreach ($names as $name) {
            $key = strtolower($name);
            if (isset($existing[$key])) {
                $found[] = $existing[$key];
            }
        }
        return $found;
    }

    private function findColumn(PDO $pdo, string $table, array $names): ?string
    {
        $cols = $this->findExistingColumns($pdo, $table, $names);
        return $cols[0] ?? null;
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
