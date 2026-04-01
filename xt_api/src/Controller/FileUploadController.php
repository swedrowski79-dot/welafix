<?php
declare(strict_types=1);

namespace XtApi\Controller;

use XtApi\Http\Response;
use function XtApi\env;

final class FileUploadController
{
    public function uploadFromBody(): void
    {
        $raw = file_get_contents('php://input') ?: '';
        $payload = json_decode($raw, true);
        if (!is_array($payload)) {
            Response::json(['ok' => false, 'error' => 'Invalid JSON'], 400);
            return;
        }

        $files = $payload['files'] ?? [];
        if (!is_array($files) || $files === []) {
            Response::json(['ok' => false, 'error' => 'files payload fehlt'], 400);
            return;
        }

        $stats = [
            'ok' => true,
            'received' => 0,
            'written' => 0,
            'skipped' => 0,
            'errors' => [],
            'written_paths' => [],
            'skipped_paths' => [],
        ];

        foreach ($files as $file) {
            if (!is_array($file)) {
                continue;
            }
            $stats['received']++;
            try {
                $this->writeFile($file, $stats);
            } catch (\Throwable $e) {
                $stats['errors'][] = $e->getMessage();
            }
        }

        Response::json($stats, empty($stats['errors']) ? 200 : 207);
    }

    /**
     * @param array<string, mixed> $file
     * @param array<string, mixed> $stats
     */
    private function writeFile(array $file, array &$stats): void
    {
        $kind = strtolower(trim((string)($file['kind'] ?? 'images')));
        $filename = basename(trim((string)($file['filename'] ?? '')));
        $checksum = trim((string)($file['checksum'] ?? ''));
        $content = (string)($file['content_base64'] ?? '');

        if ($filename === '' || $checksum === '' || $content === '') {
            throw new \RuntimeException('upload payload unvollständig');
        }

        $baseDir = $this->targetBaseDir($kind);
        if ($baseDir === '') {
            throw new \RuntimeException('upload basisverzeichnis fehlt für ' . $kind);
        }
        if (!is_dir($baseDir) && !@mkdir($baseDir, 0777, true) && !is_dir($baseDir)) {
            throw new \RuntimeException('zielverzeichnis konnte nicht erstellt werden: ' . $baseDir);
        }

        $targetPath = rtrim($baseDir, '/\\') . DIRECTORY_SEPARATOR . $filename;
        $checksumPath = $targetPath . '.wela.checksum';
        $existingChecksum = is_file($checksumPath) ? trim((string)@file_get_contents($checksumPath)) : '';
        if ($existingChecksum === $checksum && is_file($targetPath)) {
            $stats['skipped']++;
            $stats['skipped_paths'][] = $targetPath;
            return;
        }

        $decoded = base64_decode($content, true);
        if ($decoded === false) {
            throw new \RuntimeException('base64 decode fehlgeschlagen für ' . $filename);
        }
        if (@file_put_contents($targetPath, $decoded) === false) {
            throw new \RuntimeException('datei konnte nicht geschrieben werden: ' . $filename);
        }
        @file_put_contents($checksumPath, $checksum);
        $stats['written']++;
        $stats['written_paths'][] = $targetPath;
    }

    private function targetBaseDir(string $kind): string
    {
        $shopRoot = dirname(__DIR__, 3);

        return match ($kind) {
            'documents', 'document', 'dokument' => $this->normalizePath((string)env('XT_UPLOAD_DOCUMENTS_DIR', $shopRoot . '/media/files')),
            default => $this->normalizePath((string)env('XT_UPLOAD_IMAGES_DIR', $shopRoot . '/media/images/org')),
        };
    }

    private function normalizePath(string $path): string
    {
        $trimmed = trim($path);
        if ($trimmed === '') {
            return '';
        }

        return rtrim(str_replace(['/', '\\'], DIRECTORY_SEPARATOR, $trimmed), '/\\');
    }
}
