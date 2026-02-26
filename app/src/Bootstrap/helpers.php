<?php
declare(strict_types=1);

if (!function_exists('env')) {
    /**
     * @param mixed $default
     * @return mixed
     */
    function env(string $key, $default = null)
    {
        $value = $_ENV[$key] ?? ($_SERVER[$key] ?? getenv($key));
        if ($value === false || $value === null) {
            return $default;
        }
        if (is_string($value)) {
            $trimmed = trim($value);
            if ($trimmed === '') {
                return $default;
            }
            return $trimmed;
        }
        return $value;
    }
}

if (!function_exists('is_dev_env')) {
    function is_dev_env(): bool
    {
        $envValue = strtolower((string)(env('APP_ENV', env('ENV', ''))));
        return $envValue === 'dev' || $envValue === 'development' || $envValue === 'local';
    }
}

if (!function_exists('rtfToHtmlSimple')) {
    function rtfToHtmlSimple(string $rtf): string
    {
        $trimmed = ltrim($rtf);
        if ($trimmed === '' || stripos($trimmed, '{\\rtf') !== 0) {
            return nl2br(htmlspecialchars($rtf, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8'));
        }

        $text = $rtf;

        // Remove common RTF groups
        $text = preg_replace('/\\{\\\\(fonttbl|colortbl|stylesheet|info|\\*\\\\[^\\s]+)[\\s\\S]*?\\}/i', '', $text) ?? $text;

        // Convert RTF paragraph to line break (avoid matching \\pard)
        $text = preg_replace('/\\\\par(?![a-zA-Z])/', '<br>', $text) ?? $text;

        // Decode hex escapes (cp1252)
        $text = preg_replace_callback("/\\\\'([0-9a-fA-F]{2})/", function (array $m): string {
            $byte = chr(hexdec($m[1]));
            return iconv('CP1252', 'UTF-8//IGNORE', $byte) ?: '';
        }, $text) ?? $text;

        // Remove control words
        $text = preg_replace('/\\\\[a-zA-Z]+-?\\d*\\s?/', '', $text) ?? $text;

        // Remove leftover braces and backslashes
        $text = str_replace(['{', '}', '\\'], '', $text);

        // Normalize whitespace and <br>
        $text = preg_replace('/\\s+/', ' ', $text) ?? $text;
        $text = preg_replace('/(<br>\\s*){2,}/i', '<br>', $text) ?? $text;

        return trim($text);
    }
}

if (!function_exists('normalizeMediaFilename')) {
    function normalizeMediaFilename(?string $value): ?string
    {
        if ($value === null) {
            return null;
        }
        $raw = trim($value);
        if ($raw === '') {
            return null;
        }
        $raw = str_replace('\\', '/', $raw);
        $raw = preg_replace('/[\\?#].*$/', '', $raw) ?? $raw;
        $raw = trim($raw);
        if ($raw === '' || $raw === '0' || $raw === '-') {
            return null;
        }
        if (strcasecmp($raw, 'null') === 0) {
            return null;
        }
        $filename = basename($raw);
        $filename = trim($filename);
        if ($filename === '' || $filename === '0' || $filename === '-') {
            return null;
        }
        if (strcasecmp($filename, 'null') === 0) {
            return null;
        }
        return $filename;
    }
}
