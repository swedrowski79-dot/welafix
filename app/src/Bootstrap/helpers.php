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
