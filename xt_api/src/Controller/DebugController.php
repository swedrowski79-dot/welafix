<?php
declare(strict_types=1);

namespace XtApi\Controller;

use XtApi\Http\Response;
use function XtApi\env;

final class DebugController
{
    public function uploadEnv(): void
    {
        $shopRoot = dirname(__DIR__, 3);
        $docDir = $this->normalizePath((string)env('XT_UPLOAD_DOCUMENTS_DIR', $shopRoot . '/media/files'));
        $imgDir = $this->normalizePath((string)env('XT_UPLOAD_IMAGES_DIR', $shopRoot . '/media/images/org'));

        Response::json([
            'ok' => true,
            'php_os_family' => PHP_OS_FAMILY,
            'php_uname' => php_uname(),
            'cwd' => getcwd() ?: '',
            '__dir__' => __DIR__,
            'document_target' => $docDir,
            'image_target' => $imgDir,
            'document_target_realpath' => realpath($docDir) ?: null,
            'image_target_realpath' => realpath($imgDir) ?: null,
        ]);
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
