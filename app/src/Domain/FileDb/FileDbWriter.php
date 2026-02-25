<?php
declare(strict_types=1);

namespace Welafix\Domain\FileDb;

final class FileDbWriter
{
    private string $baseDir;

    public function __construct(?string $baseDir = null)
    {
        $this->baseDir = $baseDir ?? (__DIR__ . '/../../../storage/data');
    }

    /**
     * @param array<string, mixed> $fields
     * @param array<int, string> $changedKeys
     * @param array<int, string> $extraKeys
     */
    public function writeArtikel(string $artikelnummer, array $fields, array $changedKeys, array $extraKeys = []): void
    {
        $entityDir = $this->baseDir . '/artikel/' . $this->safeDirName($artikelnummer, 'artikel');
        $this->writeFields($entityDir, $fields, $changedKeys, $extraKeys);
    }

    /**
     * @param array<string, mixed> $fields
     * @param array<int, string> $changedKeys
     * @param array<int, string> $extraKeys
     */
    public function writeWarengruppe(string $bezeichnung, string $fallbackKey, array $fields, array $changedKeys, array $extraKeys = []): void
    {
        $dirName = $this->safeWarengruppeDir($bezeichnung, $fallbackKey);
        $entityDir = $this->baseDir . '/warengruppe/' . $dirName;
        $this->writeFields($entityDir, $fields, $changedKeys, $extraKeys);
    }

    /**
     * @param array<string, mixed> $fields
     * @param array<int, string> $changedKeys
     * @param array<int, string> $extraKeys
     */
    private function writeFields(string $entityDir, array $fields, array $changedKeys, array $extraKeys = []): void
    {
        if ($changedKeys === [] && $extraKeys === []) {
            return;
        }

        $keys = array_values(array_unique(array_merge($changedKeys, $extraKeys)));
        if (!is_dir($entityDir)) {
            mkdir($entityDir, 0777, true);
        }

        foreach ($keys as $field) {
            if (!array_key_exists($field, $fields)) {
                continue;
            }
            $value = $fields[$field];
            $content = $value === null ? '' : (string)$value;
            $path = $entityDir . '/' . $this->safeFileName($field) . '.txt';
            file_put_contents($path, $content);
        }
    }

    private function safeWarengruppeDir(string $bezeichnung, string $fallbackKey): string
    {
        $slug = strtolower(xt_filterAutoUrlText_inline($bezeichnung, 'de'));
        if ($slug === '') {
            $slug = 'wg_' . $this->safeDirName($fallbackKey, 'wg');
        }
        return $slug;
    }

    private function safeDirName(string $value, string $fallback): string
    {
        $value = trim($value);
        if ($value === '') {
            return $fallback;
        }
        $value = preg_replace('/[^\w\-]+/u', '-', $value) ?? $value;
        $value = trim($value, '-');
        if ($value === '') {
            return $fallback;
        }
        return $value;
    }

    private function safeFileName(string $value): string
    {
        $value = trim($value);
        if ($value === '') {
            return 'field';
        }
        $value = preg_replace('/[^\w\-]+/u', '_', $value) ?? $value;
        $value = trim($value, '_');
        if ($value === '') {
            return 'field';
        }
        return $value;
    }
}
