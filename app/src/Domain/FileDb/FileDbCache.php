<?php
declare(strict_types=1);

namespace Welafix\Domain\FileDb;

use FilesystemIterator;

final class FileDbCache
{
    /** @var array<string, array<string, string>> */
    private array $standardCache = [];
    /** @var array<string, array<string, array<string, string>>> */
    private array $entityCache = [];

    /**
     * @return array<string, string>
     */
    public function loadStandard(string $entityType): array
    {
        $key = $this->normalizeType($entityType);
        if (isset($this->standardCache[$key])) {
            return $this->standardCache[$key];
        }

        $dir = $this->baseDir($key) . '/Standard';
        $map = $this->readFieldDirectory($dir);
        $this->standardCache[$key] = $map;
        return $map;
    }

    /**
     * @return array<string, string>
     */
    public function loadEntity(string $entityType, string $entityId): array
    {
        $key = $this->normalizeType($entityType);
        $entityId = trim($entityId);
        if ($entityId === '') {
            return [];
        }

        if (isset($this->entityCache[$key][$entityId])) {
            return $this->entityCache[$key][$entityId];
        }

        $dir = $this->baseDir($key) . '/' . $entityId;
        $map = $this->readFieldDirectory($dir);
        $this->entityCache[$key][$entityId] = $map;
        return $map;
    }

    /**
     * @return array<string, string>
     */
    public function getMerged(string $entityType, string $entityId): array
    {
        $standard = $this->loadStandard($entityType);
        $entity = $this->loadEntity($entityType, $entityId);
        if ($entity === []) {
            return $standard;
        }
        return array_merge($standard, $entity);
    }

    private function normalizeType(string $entityType): string
    {
        $entityType = strtolower(trim($entityType));
        if ($entityType === 'artikel') return 'artikel';
        if ($entityType === 'warengruppe' || $entityType === 'warengruppen') return 'warengruppen';
        return $entityType;
    }

    private function baseDir(string $entityKey): string
    {
        $base = __DIR__ . '/../../../storage/data';
        if ($entityKey === 'artikel') {
            return $base . '/Artikel';
        }
        if ($entityKey === 'warengruppen') {
            return $base . '/Warengruppen';
        }
        return $base . '/' . $entityKey;
    }

    /**
     * @return array<string, string>
     */
    private function readFieldDirectory(string $dir): array
    {
        $map = [];
        if (!is_dir($dir)) {
            return $map;
        }

        $iterator = new FilesystemIterator($dir, FilesystemIterator::SKIP_DOTS);
        foreach ($iterator as $file) {
            if (!$file->isFile()) {
                continue;
            }
            if (strtolower($file->getExtension()) !== 'txt') {
                continue;
            }
            $field = trim($file->getBasename('.txt'));
            if ($field === '') {
                continue;
            }
            $value = (string)file_get_contents($file->getPathname());
            $value = rtrim($value, "\r\n");
            $value = trim($value);
            $map[$field] = $value;
        }
        return $map;
    }
}
