<?php
declare(strict_types=1);

namespace XtApi;

spl_autoload_register(function (string $class): void {
    $prefix = 'XtApi\\';
    if (str_starts_with($class, $prefix)) {
        $rel = str_replace('\\', '/', substr($class, strlen($prefix)));
        $path = __DIR__ . '/' . $rel . '.php';
        if (is_file($path)) {
            require $path;
        }
    }
});

require_once __DIR__ . '/env.php';

Env::load(__DIR__ . '/../.env.xt');
