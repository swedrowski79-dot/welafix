<?php
declare(strict_types=1);

spl_autoload_register(function (string $class): void {
    $prefix = 'Welafix\\';
    $baseDir = __DIR__ . '/..';

    if (strncmp($prefix, $class, strlen($prefix)) !== 0) {
        return;
    }

    $relativeClass = substr($class, strlen($prefix));
    $relativePath = str_replace('\\', '/', $relativeClass) . '.php';
    $file = $baseDir . '/' . $relativePath;

    if (is_file($file)) {
        require_once $file;
    }
});

require_once __DIR__ . '/../Seo/xt_seo.php';
require_once __DIR__ . '/helpers.php';
