<?php
declare(strict_types=1);

namespace XtApi;

final class Env
{
    public static function load(string $path): void
    {
        if (!is_file($path)) {
            return;
        }
        $lines = file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        if (!is_array($lines)) {
            return;
        }
        foreach ($lines as $line) {
            $line = trim($line);
            if ($line === '' || str_starts_with($line, '#')) {
                continue;
            }
            $pos = strpos($line, '=');
            if ($pos === false) {
                continue;
            }
            $key = trim(substr($line, 0, $pos));
            $val = trim(substr($line, $pos + 1));
            if ($key === '') {
                continue;
            }
            if ((str_starts_with($val, '"') && str_ends_with($val, '"')) ||
                (str_starts_with($val, "'") && str_ends_with($val, "'"))) {
                $val = substr($val, 1, -1);
            }
            $_ENV[$key] = $val;
            $_SERVER[$key] = $val;
            putenv($key . '=' . $val);
        }
    }
}

function env(string $key, mixed $default = null): mixed
{
    if (array_key_exists($key, $_ENV)) {
        $val = $_ENV[$key];
        return ($val === '' || $val === null) ? $default : $val;
    }
    if (array_key_exists($key, $_SERVER)) {
        $val = $_SERVER[$key];
        return ($val === '' || $val === null) ? $default : $val;
    }
    $val = getenv($key);
    if ($val === false || $val === '') {
        return $default;
    }
    return $val;
}
