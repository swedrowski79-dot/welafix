<?php
declare(strict_types=1);

namespace Welafix\Bootstrap;

final class Env
{
    public static function load(string $path = ''): void
    {
        if ($path === '') {
            $path = self::projectRoot() . '/.env';
        } elseif (is_dir($path)) {
            $path = rtrim($path, '/\\') . '/.env';
        } elseif (!self::isAbsolutePath($path)) {
            $path = rtrim(self::projectRoot(), '/\\') . '/' . ltrim($path, '/\\');
        }

        if (!is_file($path) || !is_readable($path)) {
            return;
        }
        $lines = file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        if ($lines === false) {
            return;
        }
        foreach ($lines as $line) {
            $line = trim($line);
            if ($line === '' || str_starts_with($line, '#')) {
                continue;
            }
            $line = ltrim($line);
            if (str_starts_with($line, '#')) {
                continue;
            }
            $line = preg_replace('/^export\s+/', '', $line) ?? $line;
            [$key, $value] = array_pad(explode('=', $line, 2), 2, '');
            $key = trim($key);
            if ($key === '') {
                continue;
            }
            $value = self::stripQuotes(trim($value));
            putenv($key . '=' . $value);
            $_ENV[$key] = $value;
            $_SERVER[$key] = $value;
        }
    }

    private static function stripQuotes(string $value): string
    {
        if ($value === '') {
            return $value;
        }
        $first = $value[0];
        $last = $value[strlen($value) - 1];
        if (($first === '"' && $last === '"') || ($first === "'" && $last === "'")) {
            return substr($value, 1, -1);
        }
        return $value;
    }

    private static function projectRoot(): string
    {
        return dirname(__DIR__, 3);
    }

    private static function isAbsolutePath(string $path): bool
    {
        if ($path === '') {
            return false;
        }
        if ($path[0] === '/' || $path[0] === '\\') {
            return true;
        }
        return (bool)preg_match('/^[A-Za-z]:[\\\\\\/]/', $path);
    }
}
